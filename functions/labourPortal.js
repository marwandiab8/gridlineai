const { createHash } = require("crypto");
const {
  normalizeLabourerName,
  normalizeLabourEntryText,
  validateLabourReportDateKey,
  loadLabourEntries,
  writeLabourEntry,
} = require("./labourRepository");
const { LABOUR_ACTIVITIES, LABOUR_CATEGORIES, getLabourActivity, normalizeLabourLines } = require("./labourActivityCodes");

const LINE_COUNT = 6;

const attempts = new Map();

const INSTALL_TAGS = `<link rel="manifest" href="/labour-manifest.json"><link rel="apple-touch-icon" href="/labour-icon-192.png"><meta name="mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-title" content="Labour Hours">`;

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeCode(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length === 4 ? digits : "";
}

async function findLabourersByCode(db, code) {
  const snap = await db.collection("labourers").get();
  return snap.docs
    .map((doc) => ({ id: doc.id, ...doc.data() }))
    .filter((labourer) => labourer.active !== false && String(labourer.phoneE164 || "").endsWith(code));
}

function easternDateKey(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(now).reduce((out, part) => ({ ...out, [part.type]: part.value }), {});
  return `${parts.year}-${parts.month}-${parts.day}`;
}

const SHARED_STYLE = `*{box-sizing:border-box}body{margin:0;background:#eef2f7;color:#172033;font-family:system-ui,-apple-system,Segoe UI,sans-serif}.wrap{max-width:520px;margin:0 auto;padding:28px 18px 44px}.brand{font-size:14px;font-weight:800;letter-spacing:.12em;color:#3564a8;text-transform:uppercase}.card{margin-top:12px;background:#fff;border-radius:20px;padding:24px;box-shadow:0 12px 35px #17203318}h1{margin:0 0 8px;font-size:28px}p{margin:0 0 22px;color:#5d687b;line-height:1.45}label{display:block;margin:16px 0 7px;font-weight:750}input,textarea{width:100%;font:inherit;font-size:17px;border:1px solid #cbd4e1;border-radius:11px;padding:13px;background:#fff}input:focus,textarea:focus{outline:3px solid #4a83cf35;border-color:#4a83cf}textarea{min-height:110px;resize:vertical}.row{display:grid;grid-template-columns:1fr 1fr;gap:12px}button{width:100%;margin-top:22px;border:0;border-radius:12px;padding:15px;background:#17643a;color:#fff;font-size:18px;font-weight:800}.pick{width:100%;margin-top:10px;border:1px solid #cbd4e1;border-radius:11px;padding:13px;background:#fff;color:#172033;font-size:17px;font-weight:700;text-align:left}.notice{padding:13px;border-radius:11px;margin-bottom:18px;background:${"__ERR__"};color:${"__ERRTEXT__"};font-weight:700}.help{font-size:13px;margin-top:14px;color:#788397}.trap{position:absolute;left:-9999px}select{width:100%;font:inherit;font-size:16px;border:1px solid #cbd4e1;border-radius:11px;padding:12px;background:#fff;color:#172033}.line{border-top:1px solid #e3e8ef;padding-top:12px;margin-top:12px}.line:first-of-type{border-top:0;margin-top:0}.line .row{grid-template-columns:2fr 1fr;margin-top:8px}.line input,.line select{margin:0}.line .note{margin-top:8px}.legend{font-size:13px;color:#5d687b;margin:6px 0 0}@media(max-width:430px){.row{grid-template-columns:1fr}.card{padding:20px}.wrap{padding:20px 12px}}`;

function noticeStyle(error) {
  return SHARED_STYLE
    .replace("__ERR__", error ? "#fff0f0" : "#eaf8ef")
    .replace("__ERRTEXT__", error ? "#9b2323" : "#17643a");
}

function activityOptions(selected) {
  const groups = LABOUR_CATEGORIES.map((category) => {
    const options = LABOUR_ACTIVITIES.filter((a) => a.category === category.id)
      .map((a) => `<option value="${a.code}"${a.code === selected ? " selected" : ""}>${escapeHtml(a.label)} [${escapeHtml(a.keyword)}]</option>`)
      .join("");
    return `<optgroup label="${escapeHtml(`${category.label} (${category.note})`)}">${options}</optgroup>`;
  });
  return `<option value="">Choose activity</option>${groups.join("")}`;
}

function lineFields(values) {
  const lines = Array.isArray(values.lines) ? values.lines : [];
  let html = "";
  for (let i = 0; i < LINE_COUNT; i += 1) {
    const line = lines[i] || {};
    const n = i + 1;
    html += `<div class="line"><div class="row"><select name="line${n}Code" aria-label="Activity ${n}">${activityOptions(line.code)}</select><input name="line${n}Hours" value="${escapeHtml(line.hours)}" type="number" inputmode="decimal" min="0.25" max="24" step="0.25" placeholder="Hours" aria-label="Hours for activity ${n}"></div><input class="note" name="line${n}Note" value="${escapeHtml(line.note)}" maxlength="300" placeholder="Note (where / what) - optional" aria-label="Note for activity ${n}"></div>`;
  }
  return html;
}

function page({ message = "", error = false, values = {} } = {}) {
  const today = easternDateKey();
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#12233f"><title>Submit Labour Hours</title>${INSTALL_TAGS}<style>${noticeStyle(error)}</style></head><body><main class="wrap"><div class="brand">GridlineAI</div><section class="card"><h1>Submit your hours</h1><p>Enter your 4-digit code (the last 4 digits of your registered phone number) then one row per activity with its hours. Your total is added up for you.</p>${message ? `<div class="notice" role="status">${escapeHtml(message)}</div>` : ""}<form method="post" action="/labour" autocomplete="on"><label for="code">Your code</label><input id="code" name="code" value="${escapeHtml(values.code)}" type="tel" inputmode="numeric" pattern="[0-9]{4}" maxlength="4" placeholder="1234" autocomplete="off" required autofocus><label for="date">Work date</label><input id="date" name="date" type="date" value="${escapeHtml(values.date || today)}" max="${today}" required><label>What you worked on</label><p class="legend">Winter Heat work (hoarding, tarps, heaters, snow) is billed separately - pick it whenever it applies.</p>${lineFields(values)}<label class="trap">Leave blank<input name="website" tabindex="-1" autocomplete="off"></label><button type="submit">Submit hours</button></form><div class="help">One entry is allowed per person per day. Contact your supervisor if a correction is needed.</div></section></main></body></html>`;
}

function pickerPage({ candidates, values }) {
  const buttons = candidates
    .map((candidate) => `<button class="pick" type="submit" name="chosenPhone" value="${escapeHtml(candidate.phoneE164)}">${escapeHtml(candidate.name)}</button>`)
    .join("");
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#12233f"><title>Submit Labour Hours</title>${INSTALL_TAGS}<style>${noticeStyle(false)}</style></head><body><main class="wrap"><div class="brand">GridlineAI</div><section class="card"><h1>Which one are you?</h1><p>More than one labourer uses that code. Tap your name to continue.</p><form method="post" action="/labour"><input type="hidden" name="code" value="${escapeHtml(values.code)}"><input type="hidden" name="date" value="${escapeHtml(values.date)}">${hiddenLineFields(values)}${buttons}</form></section></main></body></html>`;
}

function hiddenLineFields(values) {
  return (values.lines || []).map((line, i) => ["Code", "Hours", "Note"]
    .map((k) => `<input type="hidden" name="line${i + 1}${k}" value="${escapeHtml(line[k.toLowerCase()])}">`).join("")).join("");
}

/** The filled-in rows of the form, as typed. */
function readFormLines(body) {
  const lines = [];
  for (let n = 1; n <= LINE_COUNT; n += 1) {
    const line = {
      code: String(body[`line${n}Code`] || "").trim(),
      hours: String(body[`line${n}Hours`] || "").trim(),
      note: String(body[`line${n}Note`] || "").trim(),
    };
    lines.push(line);
  }
  return lines;
}

/** Work text for the entry, e.g. "5h Hoarding / winter protection (east windows) - 3h Site housekeeping". */
function workOnFromLines(lines) {
  return lines.map((line) => {
    const activity = getLabourActivity(line.code);
    const hours = Math.round(line.minutes / 60 * 100) / 100;
    return `${hours}h ${activity ? activity.label : "Uncoded"}${line.text ? ` (${line.text})` : ""}`;
  }).join(" - ");
}

function formBody(req) {
  return req && req.body && typeof req.body === "object" ? req.body : {};
}

function rateLimited(req) {
  const raw = String(req.ip || req.headers?.["x-forwarded-for"] || "unknown").split(",")[0].trim();
  const key = createHash("sha256").update(raw).digest("hex").slice(0, 16);
  const now = Date.now();
  const current = attempts.get(key);
  if (!current || now - current.startedAt > 15 * 60_000) {
    attempts.set(key, { startedAt: now, count: 1 });
    return false;
  }
  current.count += 1;
  return current.count > 20;
}

function projectForLabourer(data) {
  const active = String(data.activeProjectSlug || "").trim();
  if (active) return active;
  const slugs = Array.isArray(data.projectSlugs) ? data.projectSlugs.filter(Boolean) : [];
  return slugs.length === 1 ? String(slugs[0]).trim() : "";
}

function createLabourPortalHandler({ db, FieldValue, logger = console }) {
  return async (req, res) => {
    res.set("Cache-Control", "no-store");
    res.set("X-Content-Type-Options", "nosniff");
    res.set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'");
    if (req.method === "GET") return res.status(200).type("html").send(page());
    if (req.method !== "POST") return res.status(405).set("Allow", "GET, POST").send("Method Not Allowed");
    if (rateLimited(req)) return res.status(429).type("html").send(page({ message: "Too many attempts. Please wait 15 minutes and try again.", error: true }));

    const body = formBody(req);
    const values = { code: String(body.code || ""), date: String(body.date || ""), lines: readFormLines(body) };
    const fail = (message, status = 400) => res.status(status).type("html").send(page({ message, error: true, values }));
    if (body.website) return fail("Unable to submit the entry.");
    const code = normalizeCode(body.code);
    const date = String(body.date || "").trim();
    if (!code) return fail("Enter your 4-digit code.");
    const filled = values.lines.filter((line) => line.code || line.hours || line.note);
    let lines;
    try {
      lines = normalizeLabourLines(filled.map((line) => ({ code: line.code, hours: line.hours, text: line.note })), { requireCodes: true, codedBy: "labourer" });
    } catch (error) {
      return fail(filled.length ? error.message : "Add at least one activity with its hours.");
    }
    const minutes = lines.reduce((total, line) => total + line.minutes, 0);
    const hours = Math.round(minutes / 60 * 100) / 100;
    if (minutes % 15 !== 0) return fail("Enter hours in quarter hours (0.25, 0.5, 0.75).");
    if (hours > 24) return fail("The hours add up to more than 24.");
    const workOn = normalizeLabourEntryText(workOnFromLines(lines));
    const dateValidation = validateLabourReportDateKey(date, new Date());
    if (!dateValidation.ok) return fail("Choose a valid recent work date.");

    try {
      const candidates = await findLabourersByCode(db, code);
      if (!candidates.length) return fail("No active labourer is registered with that code. Contact your supervisor.", 403);

      let labourer = candidates[0];
      if (candidates.length > 1) {
        const chosenPhone = String(body.chosenPhone || "").trim();
        const chosen = chosenPhone && candidates.find((candidate) => candidate.phoneE164 === chosenPhone);
        if (!chosen) return res.status(200).type("html").send(pickerPage({ candidates, values }));
        labourer = chosen;
      }

      const phone = labourer.phoneE164;
      const registeredName = normalizeLabourerName(labourer.name || labourer.displayName);
      const projectSlug = projectForLabourer(labourer);
      if (!projectSlug) return fail("No active project is assigned to this labourer. Contact your supervisor.", 403);
      const existing = await loadLabourEntries(db, { startKey: date, endKey: date, labourerPhone: phone });
      if (existing.length) return fail(`Hours were already submitted for ${date}. Contact your supervisor to make a correction.`, 409);
      await writeLabourEntry(db, FieldValue, { labourerName: registeredName, labourerPhone: phone, projectSlug, reportDateKey: date, hours, workOn, notes: workOn, lines, source: "labour-web", enteredByPhone: phone });
      logger.info("labourPortal: entry saved", { labourerPhoneLast4: phone.slice(-4), projectSlug, reportDateKey: date });
      return res.status(201).type("html").send(page({ message: `Saved ${hours} hours for ${registeredName} on ${date}.`, values: { code, date: easternDateKey() } }));
    } catch (error) {
      logger.error("labourPortal: submission failed", { message: error.message });
      return fail("The hours could not be saved. Please try again or contact your supervisor.", 500);
    }
  };
}

module.exports = { createLabourPortalHandler, normalizeCode, projectForLabourer, easternDateKey, readFormLines, workOnFromLines };
