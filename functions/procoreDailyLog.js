// A project's Procore daily log, shown as a section of that day's journal (Docksteader).
//
// Procore can't be read from here (the Gridlineai app isn't installed in Matheson's Procore), so Marwan's computer
// sends each completed log: the procore-daily-notes job reads it in his signed-in Chrome the morning after (Dhruv
// completes the log by then) and takes it to public/procore-relay.html, which sends it to
// ingestProcoreDailyLogCallable as him. The log is kept in procoreDailyLogs/{projectSlug}__{dateKey}; the journal for
// that day shows it, and the first copy of a day's log queues that day's journal to be texted to management
// (managementJournalPushQueue, id ending "__procore").
const crypto = require("crypto");
const { COL_PROCORE_PHOTO_QUEUE, normalizeProcorePhotoSources, procorePhotoSourceHash } = require("./procoreDailyLogPhotos");

const COL_PROCORE_DAILY_LOGS = "procoreDailyLogs";
const COL_MANAGEMENT_JOURNAL_PUSH_QUEUE = "managementJournalPushQueue";
// Procore project id -> GridlineAI project slug. Logs of other projects are refused.
const PROCORE_PROJECTS = { 3118812: "docksteader" };
const DATE_KEY_RE = /^\d{4}-\d{2}-\d{2}$/;
const MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
const MAX_ENTRIES = 200;
const MAX_TEXT = 2000;

// The other Procore daily log sections, in Procore's order, as the journal titles them.
const SECTION_TITLES = {
  weather_logs: "Weather notes",
  notes_logs: "Notes",
  delay_logs: "Delays",
  accident_logs: "Accidents",
  safety_violation_logs: "Safety violations",
  delivery_logs: "Deliveries",
  equipment_logs: "Equipment",
  work_logs: "Work",
  call_logs: "Calls",
  dumpster_logs: "Dumpsters",
  quantity_logs: "Quantities",
  waste_logs: "Waste",
  productivity_logs: "Productivity",
  plan_revision_logs: "Plan revisions",
  daily_construction_report_logs: "Daily construction report",
};
// Fields that are bookkeeping rather than what happened.
const SKIP_FIELDS = /^(id|date|datetime|status|position|time|custom_fields|location_id|origin|vendor_id)$/;
const CLOCK_FIELDS = /^(time|begin|start|end)_(hour|minute)$/;
// Fields that are the entry's own words: they lead its line, unlabelled.
const TEXT_FIELDS = ["comments", "notes", "details", "description", "subject"];

function cleanText(value) {
  const s = String(value == null ? "" : value).replace(/\s+/g, " ").trim();
  if (!s || s === ".") return "";
  return s.length > MAX_TEXT ? `${s.slice(0, MAX_TEXT - 1)}…` : s;
}

function toNumber(value) {
  const n = Number(String(value == null ? "" : value).replace(/,/g, "").trim());
  return String(value == null ? "" : value).trim() !== "" && Number.isFinite(n) ? n : null;
}

/** "40" for 40, "8.5" for 8.5. */
function formatHours(value) {
  const n = Number(value) || 0;
  return Number.isInteger(n) ? String(n) : n.toFixed(1);
}

/** 12, 0 -> "12:00 PM"; nothing when the hour isn't given. */
function formatClock(hour, minute) {
  const h = toNumber(hour);
  if (h == null || h < 0 || h > 23) return "";
  const m = Math.max(0, Math.min(59, toNumber(minute) || 0));
  return `${h % 12 || 12}:${String(m).padStart(2, "0")} ${h < 12 ? "AM" : "PM"}`;
}

function timeRange(from, to) {
  if (from && to) return `${from} – ${to}`;
  return from || to || "";
}

/** "Mon, November 2, 2026" -> "2026-11-02". */
function dateKeyFromLongDate(text) {
  const m = /([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})/.exec(String(text || ""));
  if (!m) return "";
  const month = MONTHS.indexOf(m[1].toLowerCase());
  if (month < 0) return "";
  return `${m[3]}-${String(month + 1).padStart(2, "0")}-${m[2].padStart(2, "0")}`;
}

/**
 * Reads the Daily Log page's summary (its text, the parts joined by " / "): the day, who completed the log and
 * when, and the observed weather - the hourly readings and the day's temperature, precipitation, wind and humidity.
 */
function parseProcorePageSummary(summary) {
  const tokens = String(summary || "").split(/\s*\/\s*/).map((t) => t.trim()).filter(Boolean);
  const out = { dateKey: "", completed: false, completedBy: "", completedOn: "", weather: null };
  const dateToken = tokens.find((t) => dateKeyFromLongDate(t));
  out.dateKey = dateToken ? dateKeyFromLongDate(dateToken) : "";
  out.completed = tokens.some((t) => /^Daily log completed$/i.test(t));
  for (const t of tokens) {
    const m = /^The daily log was completed by (.+?) on (.+?)\.?$/i.exec(t);
    if (m) {
      out.completedBy = m[1].trim();
      out.completedOn = m[2].trim();
    }
  }

  const temp = (t) => {
    const m = /^(-?\d+(?:\.\d+)?)\s*[º°]\s*C$/i.exec(String(t || ""));
    return m ? Number(m[1]) : null;
  };
  const amount = (t) => {
    const m = /^(-?\d+(?:\.\d+)?)/.exec(String(t || ""));
    return m ? Number(m[1]) : null;
  };
  // The value after `label` within the few tokens after `section` ("Temperature / Low / 8 ºC / Avg. / ...").
  const stat = (section, label, read) => {
    const at = tokens.findIndex((t) => t.toLowerCase() === section.toLowerCase());
    if (at < 0) return null;
    for (let i = at + 1; i < Math.min(tokens.length - 1, at + 9); i += 1) {
      if (tokens[i].toLowerCase() === label.toLowerCase()) return read(tokens[i + 1]);
    }
    return null;
  };

  const hourly = [];
  for (let i = 0; i + 2 < tokens.length; i += 1) {
    if (!/^\d{1,2} (AM|PM)$/i.test(tokens[i])) continue;
    const tempC = temp(tokens[i + 2]);
    if (tempC == null || temp(tokens[i + 1]) != null) continue;
    hourly.push({ time: tokens[i].toUpperCase(), sky: tokens[i + 1], tempC });
    i += 2;
  }
  const weather = {
    hourly,
    lowC: stat("Temperature", "Low", temp),
    avgC: stat("Temperature", "Avg.", temp),
    highC: stat("Temperature", "High", temp),
    precipCm: stat("Precipitation since", "Midnight", amount),
    windAvgKmh: stat("Wind speed", "Avg.", amount),
    windMaxKmh: stat("Wind speed", "Max", amount),
    gustsKmh: stat("Wind speed", "Gusts", amount),
    humidityLow: stat("Humidity", "Low", amount),
    humidityHigh: stat("Humidity", "High", amount),
  };
  const any = hourly.length || Object.entries(weather).some(([k, v]) => k !== "hourly" && v != null);
  out.weather = any ? weather : null;
  return out;
}

/** "Photos 1-30 of 104" -> 104. */
function parsePhotoCount(text) {
  const m = /\bof\s+(\d+)/i.exec(String(text || ""));
  return m ? Number(m[1]) : null;
}

/** A Procore entry as the export sends it (field -> plain value), with empty and bookkeeping fields left out. */
function entryFields(entry) {
  const out = {};
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) return out;
  for (const [key, raw] of Object.entries(entry)) {
    if (SKIP_FIELDS.test(key)) continue;
    const value = raw && typeof raw === "object" ? raw.name || raw.label || raw.value || "" : raw;
    const text = cleanText(value);
    if (text && text !== "No") out[key] = text;
  }
  return out;
}

function entriesOf(logs, type) {
  const list = logs && Array.isArray(logs[type]) ? logs[type] : [];
  return list.slice(0, MAX_ENTRIES).map(entryFields);
}

function labelOf(key) {
  const words = key.replace(/_/g, " ").replace(/\bnum\b/, "number of").trim();
  return words.charAt(0).toUpperCase() + words.slice(1);
}

/**
 * One line for an entry of the other sections: when, its own words, then its other fields. An entry with nothing but
 * a time (a weather reading the page already shows) is left out.
 */
function genericEntryText(fields) {
  const when = timeRange(
    formatClock(fields.time_hour ?? fields.begin_hour ?? fields.start_hour, fields.time_minute ?? fields.begin_minute ?? fields.start_minute),
    formatClock(fields.end_hour, fields.end_minute)
  );
  const lead = TEXT_FIELDS.map((k) => fields[k]).filter(Boolean);
  const rest = Object.entries(fields)
    .filter(([k]) => !TEXT_FIELDS.includes(k) && !CLOCK_FIELDS.test(k))
    .map(([k, v]) => `${labelOf(k)}: ${v}`);
  if (!lead.length && !rest.length) return "";
  return [when, ...lead, ...rest].filter(Boolean).join(" · ");
}

/**
 * Checks and shapes what the export posts:
 *   { schema: 1, procoreProjectId, dateKey, page: { summary, photos }, logs: { manpower_logs: [...], ... } }
 * Only a completed log of a known project, for the day the page shows, is accepted.
 */
function normalizeProcoreDailyLog(body) {
  const src = body && typeof body === "object" ? body : {};
  const projectSlug = PROCORE_PROJECTS[String(src.procoreProjectId || "").trim()];
  if (!projectSlug) return { ok: false, error: "unknown_project", message: "procoreProjectId is not a known project" };
  const dateKey = String(src.dateKey || "").trim();
  if (!DATE_KEY_RE.test(dateKey)) return { ok: false, error: "bad_date", message: "dateKey must be YYYY-MM-DD" };
  const page = src.page && typeof src.page === "object" ? src.page : {};
  const summary = parseProcorePageSummary(page.summary);
  if (!summary.completed || !summary.completedBy) {
    return { ok: false, error: "not_completed", message: "the page summary doesn't say the daily log was completed" };
  }
  if (summary.dateKey !== dateKey) {
    return { ok: false, error: "date_mismatch", message: `the page shows ${summary.dateKey || "no date"}, not ${dateKey}` };
  }
  const logs = src.logs && typeof src.logs === "object" ? src.logs : {};

  const manpower = entriesOf(logs, "manpower_logs").map((f) => {
    const workers = Math.max(0, Math.round(toNumber(f.num_workers) || 0));
    const hoursEach = toNumber(f.num_hours);
    const manHours = toNumber(f.man_hours) != null ? toNumber(f.man_hours) : workers * (hoursEach || 0);
    return { company: f.vendor || f.contact || f.trade || "Company not named", workers, hoursEach, manHours, notes: f.notes || "" };
  });
  const totals = {
    workers: manpower.reduce((sum, row) => sum + row.workers, 0),
    manHours: Math.round(manpower.reduce((sum, row) => sum + row.manHours, 0) * 10) / 10,
  };
  const visitors = entriesOf(logs, "visitor_logs").map((f) => ({
    who: f.vendor || "",
    from: formatClock(f.begin_hour, f.begin_minute),
    to: formatClock(f.end_hour, f.end_minute),
    subject: f.subject || "",
    details: f.details || f.comments || "",
  }));
  const inspections = entriesOf(logs, "inspection_logs").map((f) => ({
    entity: f.vendor || f.inspecting_entity || "",
    type: f.inspection_type || "",
    inspector: f.inspector_name || "",
    from: formatClock(f.start_hour, f.start_minute),
    to: formatClock(f.end_hour, f.end_minute),
    area: f.area || f.location || "",
    comments: f.comments || "",
  }));
  const sections = [];
  for (const [type, title] of Object.entries(SECTION_TITLES)) {
    const entries = entriesOf(logs, type).map(genericEntryText).filter(Boolean);
    if (entries.length) sections.push({ type, title, entries });
  }

  // The day's photos (for the site log, procoreDailyLogPhotos.js) aren't part of the log itself: their links are
  // signed and only kept until the photos are copied.
  const photoSources = normalizeProcorePhotoSources(src.photos);
  return {
    ok: true,
    log: {
      projectSlug,
      procoreProjectId: String(src.procoreProjectId).trim(),
      dateKey,
      completedBy: summary.completedBy,
      completedOn: summary.completedOn,
      weather: summary.weather,
      photos: parsePhotoCount(page.photos) ?? (photoSources.length || null),
      manpower,
      totals,
      visitors,
      inspections,
      sections,
    },
    photoSources,
  };
}

/** Changes when anything in the log changes (a re-opened and re-completed log), not when it's merely sent again. */
function procoreDailyLogHash(log) {
  return crypto.createHash("sha256").update(JSON.stringify(log)).digest("hex");
}

function procoreDailyLogDocId(projectSlug, dateKey) {
  return `${projectSlug}__${dateKey}`;
}

/** The journal's push queue entry for a day's Procore log: the 9 PM entry's id with "__procore" after it. */
function procoreJournalQueueId(projectSlug, dateKey) {
  return `${dateKey}__${projectSlug}__journal__management__procore`;
}

/**
 * Keeps a day's log (already checked: normalizeProcoreDailyLog), and the first time a day's log arrives, queues that
 * day's journal to be texted. `deliver: false` keeps the log only, for filling in past days. The same log sent again
 * changes nothing; a changed one (re-opened and completed again) replaces it without texting again.
 */
async function storeProcoreDailyLog({ db, FieldValue, logger, log, photoSources = [], deliver = true, source = "", exportedAt = "" }) {
  const id = procoreDailyLogDocId(log.projectSlug, log.dateKey);
  const contentHash = procoreDailyLogHash(log);
  const logRef = db.collection(COL_PROCORE_DAILY_LOGS).doc(id);
  const queueRef = db.collection(COL_MANAGEMENT_JOURNAL_PUSH_QUEUE).doc(procoreJournalQueueId(log.projectSlug, log.dateKey));
  const projectRef = db.collection("projects").doc(log.projectSlug);
  const photoQueueRef = db.collection(COL_PROCORE_PHOTO_QUEUE).doc(id);
  const photoHash = photoSources.length ? procorePhotoSourceHash(photoSources) : "";

  const outcome = await db.runTransaction(async (tx) => {
    const existing = await tx.get(logRef);
    const queued = deliver && !existing.exists ? !(await tx.get(queueRef)).exists : false;
    const project = queued ? await tx.get(projectRef) : null;
    // The photos are copied (again) unless the same photos, with the same stars, already were.
    const photoSet = existing.exists ? existing.get("photoSet") : null;
    const photosQueued = Boolean(photoHash) && !(photoSet && photoSet.status === "done" && photoSet.sourceHash === photoHash);
    if (photosQueued) {
      tx.set(photoQueueRef, {
        projectSlug: log.projectSlug,
        dateKey: log.dateKey,
        sources: photoSources,
        sourceHash: photoHash,
        createdAt: FieldValue.serverTimestamp(),
      });
    }
    if (existing.exists && existing.get("contentHash") === contentHash) return { created: false, changed: false, queued: false, photosQueued };
    const doc = {
      projectSlug: log.projectSlug,
      dateKey: log.dateKey,
      log,
      contentHash,
      source: String(source || "procore-daily-notes").slice(0, 80),
      exportedAt: String(exportedAt || "").slice(0, 40),
      updatedAt: FieldValue.serverTimestamp(),
    };
    if (existing.exists) {
      tx.update(logRef, doc);
      return { created: false, changed: true, queued: false, photosQueued };
    }
    tx.set(logRef, { ...doc, createdAt: FieldValue.serverTimestamp(), journalQueued: queued });
    if (queued) {
      tx.set(queueRef, {
        status: "queued",
        projectSlug: log.projectSlug,
        projectName: String((project && project.exists && project.get("name")) || log.projectSlug).trim(),
        reportDateKey: log.dateKey,
        reportType: "journal",
        audience: "management",
        variant: "procore",
        includeAllManagementEntries: true,
        runId: `procore-journal-${log.projectSlug}-${log.dateKey}`,
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      });
    }
    return { created: true, changed: true, queued, photosQueued };
  });

  logger.info("procoreDailyLog: stored", { id, ...outcome, workers: log.totals.workers, manHours: log.totals.manHours });
  return { ok: true, id, ...outcome };
}

/** The day's log for the journal, or null when it hasn't been sent (yet). */
async function loadProcoreDailyLog(db, projectSlug, dateKey) {
  if (!projectSlug || !DATE_KEY_RE.test(String(dateKey || ""))) return null;
  const snap = await db.collection(COL_PROCORE_DAILY_LOGS).doc(procoreDailyLogDocId(projectSlug, dateKey)).get();
  const log = snap.exists ? snap.get("log") : null;
  if (!log || !Array.isArray(log.manpower)) return null;
  const list = (value) => (Array.isArray(value) ? value.filter((item) => item && typeof item === "object") : []);
  return {
    ...log,
    totals: log.totals && typeof log.totals === "object" ? log.totals : { workers: 0, manHours: 0 },
    manpower: list(log.manpower),
    visitors: list(log.visitors),
    inspections: list(log.inspections),
    sections: list(log.sections).filter((section) => Array.isArray(section.entries)),
  };
}

// ----- Wording for the journal's section -----

/** "Completed by Dhruv Patel on Tue, November 3, 2026 · 42 workers, 360 hours · 104 photos in Procore". */
function describeProcoreLogFacts(log) {
  return [
    log.completedBy ? `Completed by ${log.completedBy}${log.completedOn ? ` on ${log.completedOn}` : ""}` : "",
    `${log.totals.workers} ${log.totals.workers === 1 ? "worker" : "workers"}, ${formatHours(log.totals.manHours)} hours`,
    log.photos ? `${log.photos} ${log.photos === 1 ? "photo" : "photos"} in Procore` : "",
  ].filter(Boolean).join(" · ");
}

/**
 * The observed weather as a sentence ("Partly cloudy, 8 to 17 °C, wind 4.4 km/h average with gusts to 20 km/h, no
 * precipitation.") and the hourly readings ("6 AM 9 °C · 9 AM 12 °C ...").
 */
function describeProcoreWeather(weather) {
  if (!weather) return { summary: "", hourly: "" };
  const skies = [];
  for (const reading of weather.hourly || []) {
    const sky = String(reading.sky || "").trim();
    if (sky && skies[skies.length - 1] !== sky) skies.push(sky);
  }
  const sky = skies.length ? skies.map((s, i) => (i ? s.toLowerCase() : s)).join(", then ") : "";
  const temps = weather.lowC != null && weather.highC != null ? `${weather.lowC} to ${weather.highC} °C` : "";
  let wind = "";
  if (weather.windAvgKmh != null) {
    wind = `wind ${weather.windAvgKmh} km/h average${weather.gustsKmh != null ? ` with gusts to ${weather.gustsKmh} km/h` : ""}`;
  }
  let precip = "";
  if (weather.precipCm != null) {
    if (weather.precipCm === 0) precip = "no precipitation";
    else if (weather.precipCm < 0.05) precip = "trace precipitation";
    else precip = `${weather.precipCm} cm precipitation`;
  }
  const parts = [sky, temps, wind, precip].filter(Boolean);
  const summary = parts.length ? `${parts.join(", ")}.` : "";
  const hourly = (weather.hourly || []).map((r) => `${r.time} ${r.tempC} °C`).join(" · ");
  return { summary: summary.charAt(0).toUpperCase() + summary.slice(1), hourly };
}

/** "12:00 PM – 1:00 PM · Turner: Daniel from Turner and the Township was on site ..." */
function describeProcoreVisitor(visitor) {
  const head = [timeRange(visitor.from, visitor.to), visitor.who].filter(Boolean).join(" · ");
  const body = [visitor.subject, visitor.details].filter(Boolean).join(" – ");
  return [head, body].filter(Boolean).join(": ");
}

/** "9:00 AM – 10:00 AM · Englobe Corp. (Akshay Sharma), Subgrade and compaction: Inspected the transformer pad ..." */
function describeProcoreInspection(inspection) {
  const who = [inspection.entity, inspection.inspector ? `(${inspection.inspector})` : ""].filter(Boolean).join(" ");
  const what = [who, inspection.type, inspection.area].filter(Boolean).join(", ");
  const head = [timeRange(inspection.from, inspection.to), what].filter(Boolean).join(" · ");
  return [head, inspection.comments].filter(Boolean).join(": ");
}

module.exports = {
  COL_PROCORE_DAILY_LOGS,
  PROCORE_PROJECTS,
  parseProcorePageSummary,
  parsePhotoCount,
  normalizeProcoreDailyLog,
  procoreDailyLogHash,
  procoreDailyLogDocId,
  procoreJournalQueueId,
  storeProcoreDailyLog,
  loadProcoreDailyLog,
  formatHours,
  formatClock,
  describeProcoreLogFacts,
  describeProcoreWeather,
  describeProcoreVisitor,
  describeProcoreInspection,
};
