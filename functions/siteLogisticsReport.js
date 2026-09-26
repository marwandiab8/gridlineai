// Site Logistics data for the Docksteader daily construction report.
//
// The Site Logistics app (Firebase project "site-logistics") keeps, per site:
//   sites/{siteId}/days/{YYYY-MM-DD}  { crews: [{ trade, company, workers }], notes }
//   sites/{siteId}/bookings           { areaId, trade, company, start, end, notes, activity }
//   sites/{siteId}/items              work areas etc. ({ type, label })
// This module reads the report day from there, read-only, and folds it into the daily site log:
// crews go into the Workforce Summary and the day's activities and notes get their own section.
// Everything the report already gets from field entries is kept. Reading needs the Cloud
// Functions service account to have read access (roles/datastore.viewer) on that project;
// if it is missing, or the site can't be found, the report is produced exactly as before.
const admin = require("firebase-admin");

const SITE_LOGISTICS_PROJECT_ID = "site-logistics";
const SITE_LOGISTICS_APP_NAME = "site-logistics-reader";
const DASH = "-";

// Report project slug -> the Site Logistics site to read. `siteId` pins an exact site; otherwise the
// site is found by name (case-insensitive, contains). SITE_LOGISTICS_SITE_ID overrides for Docksteader.
const PROJECT_SITE_MAP = {
  docksteader: { siteId: "", nameContains: "docksteader" },
};

function getSiteLogisticsDb() {
  const existing = admin.apps.find((app) => app && app.name === SITE_LOGISTICS_APP_NAME);
  const app = existing || admin.initializeApp({ projectId: SITE_LOGISTICS_PROJECT_ID }, SITE_LOGISTICS_APP_NAME);
  return app.firestore();
}

function clean(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function norm(value) {
  return clean(value).toLowerCase();
}

/** Crews with a real headcount entered for the day. Zero means not on site. */
function crewsFromDay(dayDoc) {
  const list = dayDoc && Array.isArray(dayDoc.crews) ? dayDoc.crews : [];
  const seen = new Set();
  const out = [];
  for (const c of list) {
    const workers = Math.round(Number(c && c.workers));
    if (!Number.isFinite(workers) || workers <= 0) continue;
    const trade = clean(c.trade);
    const company = clean(c.company);
    if (!trade && !company) continue;
    const key = `${norm(trade)}|${norm(company)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ trade, company, workers });
  }
  return out;
}

/** Bookings that cover the report day, with the work area's name. */
function activitiesForDay(bookings, items, dateKey) {
  const areaLabel = new Map((items || []).map((i) => [i.id, clean(i.label)]));
  return (bookings || [])
    .filter((b) => b && /^\d{4}-\d{2}-\d{2}$/.test(String(b.start)) && b.start <= dateKey && (b.end || b.start) >= dateKey)
    .map((b) => ({
      trade: clean(b.trade),
      company: clean(b.company),
      activity: clean(b.activity || b.notes),
      area: areaLabel.get(b.areaId) || "",
      start: b.start,
      end: b.end || b.start,
    }))
    .filter((a) => a.trade || a.company)
    .sort((a, b) => (a.company || a.trade).localeCompare(b.company || b.trade) || a.start.localeCompare(b.start));
}

/** The day as the report needs it. */
function buildSiteLogisticsDay({ dayDoc, bookings, items, dateKey }) {
  const crews = crewsFromDay(dayDoc);
  return {
    dateKey,
    crews,
    totalWorkers: crews.reduce((sum, c) => sum + c.workers, 0),
    notes: clean(dayDoc && dayDoc.notes),
    activities: activitiesForDay(bookings, items, dateKey),
  };
}

function crewLabel(c) {
  return c.company && c.trade && norm(c.company) !== norm(c.trade) ? `${c.company} (${c.trade})` : c.company || c.trade;
}

function isPlaceholderRow(row) {
  return /^not stated in log entries/i.test(clean(row && row[3]));
}

/**
 * Workforce rows are [Trade, Foreman, Workers, Notes]. Site Logistics crews are added; when a row already
 * exists for the same company or trade, the Site Logistics count replaces it (no double counting).
 */
function mergeManpowerRows(existingRows, crews) {
  const rows = (Array.isArray(existingRows) ? existingRows : []).filter((r) => !isPlaceholderRow(r)).map((r) => [...r]);
  if (!crews || !crews.length) return Array.isArray(existingRows) ? existingRows : rows;
  const matched = new Set();
  for (const crew of crews) {
    const keys = [norm(crew.company), norm(crew.trade)].filter(Boolean);
    const at = rows.findIndex((row, i) => {
      if (matched.has(i)) return false;
      const label = norm(row[0]);
      const foreman = norm(row[1]);
      return keys.some((k) => label === k || foreman === k || (k.length >= 4 && (label.includes(k) || (label.length >= 4 && k.includes(label)))));
    });
    if (at >= 0) {
      matched.add(at);
      rows[at][2] = String(crew.workers);
      const note = clean(rows[at][3]);
      rows[at][3] = note && note !== DASH ? `${note} (count from Site Logistics)` : "Count from Site Logistics";
    } else {
      rows.push([crewLabel(crew), DASH, String(crew.workers), "Site Logistics"]);
      matched.add(rows.length - 1);
    }
  }
  return rows;
}

function resolveSiteRef(projectSlug, env = process.env) {
  const cfg = PROJECT_SITE_MAP[norm(projectSlug)];
  if (!cfg) return null;
  const pinned = norm(projectSlug) === "docksteader" && clean(env.SITE_LOGISTICS_SITE_ID) ? clean(env.SITE_LOGISTICS_SITE_ID) : cfg.siteId;
  return { siteId: pinned, nameContains: cfg.nameContains };
}

async function findSiteId(db, ref) {
  if (ref.siteId) return ref.siteId;
  const snap = await db.collection("sites").get();
  const hits = snap.docs.filter((d) => norm(d.get("name")).includes(ref.nameContains));
  return hits.length === 1 ? hits[0].id : null;
}

/**
 * Loads the report day from Site Logistics. Returns null (never throws) when the project has no Site
 * Logistics site, the site can't be found or read, or nothing was recorded that day.
 */
async function loadSiteLogisticsForReport({ projectSlug, dateKey, logger = console, db = null }) {
  const ref = resolveSiteRef(projectSlug);
  if (!ref) return null;
  try {
    const store = db || getSiteLogisticsDb();
    const siteId = await findSiteId(store, ref);
    if (!siteId) {
      logger.warn && logger.warn("siteLogistics: no unique site found for project", { projectSlug });
      return null;
    }
    const site = store.collection("sites").doc(siteId);
    const [dayDoc, bookingsSnap, itemsSnap] = await Promise.all([
      site.collection("days").doc(dateKey).get(),
      site.collection("bookings").where("start", "<=", dateKey).get(),
      site.collection("items").get(),
    ]);
    const day = buildSiteLogisticsDay({
      dayDoc: dayDoc.exists ? dayDoc.data() : null,
      bookings: bookingsSnap.docs.map((d) => ({ id: d.id, ...d.data() })),
      items: itemsSnap.docs.map((d) => ({ id: d.id, ...d.data() })),
      dateKey,
    });
    if (!day.crews.length && !day.notes && !day.activities.length) return null;
    return day;
  } catch (error) {
    logger.warn && logger.warn("siteLogistics: could not read Site Logistics, report continues without it", { message: error && error.message });
    return null;
  }
}

module.exports = {
  activitiesForDay,
  buildSiteLogisticsDay,
  crewsFromDay,
  loadSiteLogisticsForReport,
  mergeManpowerRows,
  resolveSiteRef,
};
