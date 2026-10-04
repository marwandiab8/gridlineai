// Labour hours sent from Site Logistics (Tasks -> My hours). A labourer there picks a work area, a keyword and
// hours for each job; Site Logistics' server sends the day here and stores our answer on its side.
//
// Trust: the request must carry a Google-signed ID token for Site Logistics' Cloud Functions service account,
// with this endpoint as its audience. The labourer is matched by the sign-in Site Logistics knows them by
// (email, or phone for text sign-in), which a supervisor links in Labour -> Labourer manager.
//
// One entry per labourer per day, as everywhere else. A day first entered by text or the web form is not
// overwritten from Site Logistics, and once approved a day can only be changed by the supervisor.
const { OAuth2Client } = require("google-auth-library");
const { getLabourActivity, normalizeLabourLines, formatMinutesAsHours } = require("./labourActivityCodes");
const {
  normalizeLabourEntryText,
  validateLabourReportDateKey,
  loadLabourEntries,
} = require("./labourRepository");
const { resolveSiteRef } = require("./siteLogisticsReport");

const SITE_LOGISTICS_CALLER = "960520609653-compute@developer.gserviceaccount.com";
const KNOWN_PROJECTS = ["docksteader"];
const SOURCE = "site-logistics";

function normalizeSiteLogisticsId(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (raw.includes("@")) {
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(raw)) throw new Error(`${value} is not a valid email.`);
    return raw;
  }
  const digits = raw.replace(/\D/g, "");
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  throw new Error(`${value} is not an email or a 10-digit phone number.`);
}

function normalizeSiteLogisticsIds(list) {
  const out = [...new Set((list || []).map(normalizeSiteLogisticsId).filter(Boolean))];
  if (out.length > 3) throw new Error("Link at most 3 Site Logistics sign-ins to one labourer.");
  return out;
}

/** The gridlineai project for a Site Logistics site, or "" when the site isn't one we track. */
function projectSlugForSite({ siteId, siteName }, env = process.env) {
  for (const slug of KNOWN_PROJECTS) {
    const ref = resolveSiteRef(slug, env);
    if (!ref) continue;
    if (ref.siteId ? ref.siteId === siteId : String(siteName || "").toLowerCase().includes(ref.nameContains)) return slug;
  }
  return "";
}

let authClient = null;
/** True when the request comes from Site Logistics' Cloud Functions (Google-signed ID token for this URL). */
async function verifySiteLogisticsCaller(req, audience) {
  const header = String((req.headers && req.headers.authorization) || "");
  const token = header.replace(/^Bearer\s+/i, "").trim();
  if (!token || token === header) return false;
  authClient = authClient || new OAuth2Client();
  try {
    const ticket = await authClient.verifyIdToken({ idToken: token, audience });
    const payload = ticket.getPayload() || {};
    return payload.email === SITE_LOGISTICS_CALLER && payload.email_verified === true;
  } catch (_) {
    return false;
  }
}

function fail(status, code, message) {
  return { status, body: { ok: false, code, message } };
}

function entryIdFor(phone, dateKey) {
  return `sl_${String(phone).replace(/\D/g, "")}_${dateKey}`;
}

/** "3h Hoarding - install from scissor lift @ Z1 East Wing - L2 (east windows) - 2h Snow removal @ Roof B" */
function workOnFromSiteLines(lines) {
  return lines.map((line) => {
    const activity = getLabourActivity(line.code);
    return `${formatMinutesAsHours(line.minutes)}h ${activity.label}${line.location ? ` @ ${line.location}` : ""}${line.text ? ` (${line.text})` : ""}`;
  }).join(" - ");
}

/**
 * Saves one labourer-day from Site Logistics. `body`: { siteId, siteName, submissionId, by, byName, dateKey,
 * lines: [{ code, hours, location, note }] }. Returns { status, body } for the HTTP reply.
 */
async function saveSiteLogisticsLabourDay({ db, FieldValue, body, now = new Date(), env = process.env }) {
  const by = (() => {
    try {
      return normalizeSiteLogisticsId(body && body.by);
    } catch (_) {
      return "";
    }
  })();
  if (!by) return fail(400, "bad_request", "Missing who sent the hours.");
  const dateKey = String((body && body.dateKey) || "").trim();
  const dateCheck = validateLabourReportDateKey(dateKey, now);
  if (!dateCheck.ok) return fail(400, "bad_date", "Choose today or a recent day (not a future day).");
  const projectSlug = projectSlugForSite({ siteId: String(body.siteId || ""), siteName: body.siteName }, env);
  if (!projectSlug) return fail(400, "unknown_site", "This site isn't set up to send hours to gridlineai.");

  let lines;
  try {
    lines = normalizeLabourLines(
      (Array.isArray(body.lines) ? body.lines : []).map((l) => ({ code: l && l.code, hours: l && l.hours, location: l && l.location, text: l && l.note })),
      { requireCodes: true, codedBy: "labourer" },
    );
  } catch (error) {
    return fail(400, "bad_lines", error.message);
  }
  const minutes = lines.reduce((sum, line) => sum + line.minutes, 0);
  if (lines.some((line) => line.minutes % 15 !== 0)) return fail(400, "bad_lines", "Enter hours in quarter hours (0.25, 0.5, 0.75).");
  if (minutes > 24 * 60) return fail(400, "bad_lines", "The hours add up to more than 24.");

  const labourerSnap = await db.collection("labourers").where("siteLogisticsIds", "array-contains", by).get();
  const labourers = labourerSnap.docs.filter((d) => d.get("active") !== false);
  if (labourers.length !== 1) {
    return fail(403, "not_linked", "Your Site Logistics sign-in isn't linked to a labourer in gridlineai yet. Ask your supervisor to link it.");
  }
  const labourer = labourers[0];
  const phone = String(labourer.get("phoneE164") || labourer.id);
  const labourerName = String(labourer.get("displayName") || labourer.get("name") || phone).trim();

  const existing = await loadLabourEntries(db, { startKey: dateKey, endKey: dateKey, labourerPhone: phone });
  const id = entryIdFor(phone, dateKey);
  const other = existing.find((e) => e.id !== id);
  if (other) {
    return fail(409, "already_entered", `Your hours for ${dateKey} were already sent by ${other.source === "sms" ? "text" : "another way (web form or the office)"}. Ask your supervisor to change them.`);
  }
  const mine = existing.find((e) => e.id === id);
  if (mine && mine.review && mine.review.status === "approved") {
    return fail(409, "approved", `Your supervisor already approved ${dateKey}. Ask them to make any change.`);
  }

  const workOn = normalizeLabourEntryText(workOnFromSiteLines(lines));
  const ref = db.collection("labourEntries").doc(id);
  await ref.set({
    labourerName,
    labourerPhone: phone,
    projectSlug,
    reportDateKey: dateKey,
    minutesWorked: minutes,
    workOn,
    notes: "",
    lines,
    review: { status: "pending" },
    source: SOURCE,
    enteredByEmail: by.includes("@") ? by : null,
    enteredByPhone: by.includes("@") ? null : by,
    siteLogistics: {
      siteId: String(body.siteId || ""),
      submissionId: String(body.submissionId || "").slice(0, 200),
      by,
    },
    createdAt: mine && mine.createdAt ? mine.createdAt : FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });
  return {
    status: 200,
    body: { ok: true, entryId: id, labourerName, hours: formatMinutesAsHours(minutes), updated: Boolean(mine), message: `Sent ${formatMinutesAsHours(minutes)}h for ${dateKey}. Your supervisor will review it.` },
  };
}

function createSiteLogisticsLabourHandler({ db, FieldValue, logger = console, audience }) {
  return async (req, res) => {
    res.set("Cache-Control", "no-store");
    if (req.method !== "POST") return res.status(405).set("Allow", "POST").json({ ok: false, code: "method" });
    const aud = typeof audience === "function" ? audience(req) : audience;
    if (!(await verifySiteLogisticsCaller(req, aud))) {
      logger.warn("siteLogisticsLabourHours: rejected caller");
      return res.status(401).json({ ok: false, code: "unauthorized", message: "Not allowed." });
    }
    try {
      const out = await saveSiteLogisticsLabourDay({ db, FieldValue, body: req.body || {} });
      logger.info("siteLogisticsLabourHours", { status: out.status, code: out.body.code || "ok", dateKey: req.body && req.body.dateKey });
      return res.status(out.status).json(out.body);
    } catch (error) {
      logger.error("siteLogisticsLabourHours: failed", { message: error.message });
      return res.status(500).json({ ok: false, code: "error", message: "gridlineai could not save the hours. Try again." });
    }
  };
}

module.exports = {
  SITE_LOGISTICS_CALLER,
  createSiteLogisticsLabourHandler,
  entryIdFor,
  normalizeSiteLogisticsIds,
  projectSlugForSite,
  saveSiteLogisticsLabourDay,
  verifySiteLogisticsCaller,
  workOnFromSiteLines,
};
