/**
 * Unified daily log lines — one document per saved inbound (structured or journal).
 * All entries are day-level first; category is metadata for typed issues + reporting sections.
 */

const OpenAI = require("openai");
const {
  dateKeyEastern,
  startOfEasternDayForDateKey,
  addCalendarDaysToDateKey,
  extractExplicitReportDate,
} = require("./logClassifier");
const {
  completionText,
  chatCompletionWithFallback,
} = require("./openaiHelpers");
const { normalizeProjectSlug } = require("./projectAccess");

function logFirestoreQueryError(context, err) {
  try {
    require("firebase-functions").logger.error(context, {
      message: err && err.message,
      code: err && err.code,
    });
  } catch (_) {
    // ignore (unit tests / non-functions context)
  }
}
const {
  computeDailySummarySections,
  isLikelyOpenItem,
} = require("./dailySectionMapper");

const COL = "logEntries";

/** Per-doc Firestore flag. PDF applies additional filters in `dailyReportIntegrity.js` (project + meta). */
function filterEntriesForDailySummary(entries) {
  return (entries || []).filter((e) => e.includeInDailySummary !== false);
}

function getLogEntryEffectiveDateKey(entry) {
  const storedReportDate =
    entry && typeof entry.reportDateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(entry.reportDateKey.trim())
      ? entry.reportDateKey.trim()
      : null;
  if (storedReportDate) return storedReportDate;

  const layers = [
    entry && entry.rawText,
    entry && entry.normalizedText,
    entry && entry.summaryText,
  ];
  for (const layer of layers) {
    const parsed = extractExplicitReportDate(layer || "");
    if (parsed.reportDateKey) return parsed.reportDateKey;
  }
  return entry && typeof entry.dateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(entry.dateKey.trim())
    ? entry.dateKey.trim()
    : null;
}

function dedupeEntries(entries) {
  const seen = new Set();
  const out = [];
  for (const entry of entries || []) {
    const id = entry && entry.id ? String(entry.id) : "";
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(entry);
  }
  return out;
}

async function loadLegacyBackdatedLogEntriesForDay(db, phoneE164, dateKey, projectSlug) {
  const nextDayStart = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 1));
  const lookaheadEnd = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 3));
  const snap = await db
    .collection(COL)
    .where("senderPhone", "==", phoneE164)
    .where("createdAt", ">=", nextDayStart)
    .where("createdAt", "<", lookaheadEnd)
    .orderBy("createdAt", "asc")
    .limit(300)
    .get()
    .catch(() => null);

  if (!snap) return [];
  const wantProject =
    projectSlug != null && String(projectSlug).trim() !== ""
      ? String(projectSlug).trim()
      : null;
  return snap.docs
    .map((d) => ({ id: d.id, ...d.data() }))
    .filter((entry) => {
      const entryProject =
        entry.projectSlug != null && String(entry.projectSlug).trim() !== ""
          ? String(entry.projectSlug).trim()
          : null;
      if (entryProject !== wantProject) return false;
      if (entry.dateKey === dateKey || entry.reportDateKey === dateKey) return false;
      return getLogEntryEffectiveDateKey(entry) === dateKey;
    });
}

function entryBelongsToNormalizedProject(entry, psNorm) {
  const slugPart =
    entry.projectSlug != null && String(entry.projectSlug).trim() !== ""
      ? normalizeProjectSlug(entry.projectSlug)
      : "";
  const idPart =
    entry.projectId != null && String(entry.projectId).trim() !== ""
      ? normalizeProjectSlug(entry.projectId)
      : "";
  return slugPart === psNorm || idPart === psNorm;
}

/** Firestore equality is exact; include raw trimmed slug when it differs from normalized (legacy casing). */
function projectSlugQueryVariants(projectSlug) {
  const rawTrim =
    projectSlug != null && String(projectSlug).trim() !== ""
      ? String(projectSlug).trim()
      : "";
  const psNorm = normalizeProjectSlug(projectSlug);
  return [...new Set([psNorm, rawTrim].filter(Boolean))];
}

/** Back-dated log lines stored on a later calendar day, all senders on one project. */
async function loadLegacyBackdatedLogEntriesForProjectDay(db, dateKey, projectSlug) {
  const psNorm = normalizeProjectSlug(projectSlug);
  if (!psNorm) return [];
  const variants = projectSlugQueryVariants(projectSlug);
  const nextDayStart = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 1));
  const lookaheadEnd = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 3));

  const runLegacyQuery = async (field, value) => {
    const snap = await db
      .collection(COL)
      .where(field, "==", value)
      .where("createdAt", ">=", nextDayStart)
      .where("createdAt", "<", lookaheadEnd)
      .orderBy("createdAt", "asc")
      .limit(400)
      .get()
      .catch((err) => {
        logFirestoreQueryError(`logEntryRepository:${field}:${value}:legacyProjectDay`, err);
        return null;
      });
    if (!snap) return [];
    return snap.docs
      .map((d) => ({ id: d.id, ...d.data() }))
      .filter((entry) => {
        if (!entryBelongsToNormalizedProject(entry, psNorm)) return false;
        if (entry.dateKey === dateKey || entry.reportDateKey === dateKey) return false;
        return getLogEntryEffectiveDateKey(entry) === dateKey;
      });
  };

  const pairs = variants.flatMap((v) => [
    ["projectSlug", v],
    ["projectId", v],
  ]);
  const chunks = await Promise.all(pairs.map(([field, v]) => runLegacyQuery(field, v)));
  return dedupeEntries(chunks.flat());
}

async function writeLogEntry(db, FieldValue, input) {
  const {
    phoneE164,
    projectSlug,
    authorName = null,
    authorEmail = null,
    authorLabel = null,
    reportDateKey = null,
    rawText,
    normalizedText,
    category,
    subtype,
    tags = [],
    severity = null,
    sourceMessageId = null,
    canonicalIssueId = null,
    issueCollection = null,
    includeInDailySummary = true,
    dailySummarySections: sectionsIn = null,
    linkedMediaIds = null,
    status: statusIn = null,
  } = input;

  const raw = String(rawText || "");
  const norm = String(normalizedText || rawText || "");
  const cat = String(category || "journal");
  const effectiveReportDateKey =
    typeof reportDateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(reportDateKey.trim())
      ? reportDateKey.trim()
      : dateKeyEastern(new Date());

  const dailySummarySections =
    Array.isArray(sectionsIn) && sectionsIn.length
      ? [...new Set([...sectionsIn, "dayLog"])]
      : computeDailySummarySections({
          category: cat,
          tags,
          rawText: raw,
          normalizedText: norm,
        });

  const openItem = isLikelyOpenItem({
    category: cat,
    rawText: raw,
    normalizedText: norm,
  });
  const status =
    statusIn || (openItem ? "open" : "recorded");

  const slugNorm = normalizeProjectSlug(projectSlug) || null;
  const ref = await db.collection(COL).add({
    dateKey: effectiveReportDateKey,
    reportDateKey: effectiveReportDateKey,
    projectId: slugNorm,
    senderPhone: phoneE164,
    authorPhone: phoneE164,
    authorName: authorName || null,
    authorEmail: authorEmail || null,
    authorLabel: authorLabel || authorName || authorEmail || phoneE164 || null,
    projectSlug: slugNorm,
    rawText: raw,
    normalizedText: norm,
    category: cat,
    subtype: subtype || null,
    tags: Array.isArray(tags) ? tags : [],
    severity,
    status,
    openItem,
    sourceMessageId: sourceMessageId || null,
    createdAt: FieldValue.serverTimestamp(),
    aiEnhanced: false,
    aiError: null,
    summaryText: null,
    canonicalIssueId: canonicalIssueId || null,
    issueCollection: issueCollection || null,
    includeInDailySummary: includeInDailySummary !== false,
    dailySummarySections,
    linkedMediaIds: Array.isArray(linkedMediaIds) ? linkedMediaIds : [],
    aiReportExtract: null,
  });

  return { logEntryId: ref.id };
}

/**
 * Normalize AI-extracted manpower rows for the PDF table (4 columns).
 * @param {unknown} rows
 * @returns {string[][] | null}
 */
function sanitizeAiManpowerRows(rows) {
  if (!Array.isArray(rows)) return null;
  const out = [];
  for (const r of rows.slice(0, 24)) {
    if (!Array.isArray(r) || r.length < 1) continue;
    const trade = String(r[0] ?? "").trim();
    if (!trade || trade === "—") continue;
    out.push([
      trade.slice(0, 36),
      String(r[1] ?? "—").trim().slice(0, 44) || "—",
      String(r[2] ?? "—").trim().slice(0, 24) || "—",
      String(r[3] ?? "—").trim().slice(0, 220) || "—",
    ]);
  }
  return out.length ? out : null;
}

function sanitizeAiReportExtract(parsed) {
  const intent = String(parsed.messageIntent || "").trim().slice(0, 500);
  const manpowerRows = sanitizeAiManpowerRows(parsed.manpowerRows);
  if (!intent && !manpowerRows) return null;
  const o = {};
  if (intent) o.messageIntent = intent;
  if (manpowerRows) o.manpowerRows = manpowerRows;
  return Object.keys(o).length ? o : null;
}

async function loadLogEntriesForDay(db, phoneE164, dateKey) {
  const dk = dateKey || dateKeyEastern(new Date());
  const snap = await db
    .collection(COL)
    .where("senderPhone", "==", phoneE164)
    .where("dateKey", "==", dk)
    .orderBy("createdAt", "asc")
    .limit(300)
    .get()
    .catch(() => null);

  const exactRows = snap ? snap.docs.map((d) => ({ id: d.id, ...d.data() })) : [];
  const legacyRows = await loadLegacyBackdatedLogEntriesForDay(db, phoneE164, dk, null);
  return dedupeEntries([...exactRows, ...legacyRows]);
}

/**
 * Same as loadLogEntriesForDay but scoped to one project (or unassigned when projectSlug is null).
 * Prevents mixing log lines when one number logs multiple projects the same Eastern calendar day.
 */
async function loadLogEntriesForDayForProject(db, phoneE164, dateKey, projectSlug) {
  const dk = dateKey || dateKeyEastern(new Date());
  const ps =
    projectSlug != null && String(projectSlug).trim() !== ""
      ? String(projectSlug).trim()
      : null;
  const snap = await db
    .collection(COL)
    .where("senderPhone", "==", phoneE164)
    .where("dateKey", "==", dk)
    .where("projectSlug", "==", ps)
    .orderBy("createdAt", "asc")
    .limit(300)
    .get()
    .catch(() => null);

  const exactRows = snap ? snap.docs.map((d) => ({ id: d.id, ...d.data() })) : [];
  const legacyRows = await loadLegacyBackdatedLogEntriesForDay(db, phoneE164, dk, ps);
  return dedupeEntries([...exactRows, ...legacyRows]);
}

/**
 * All log lines for an Eastern `dateKey` on one project (every sender).
 * Used for project daily PDFs so management is not limited to their own SMS number.
 */
async function loadLogEntriesForProjectDay(db, dateKey, projectSlug) {
  const psNorm = normalizeProjectSlug(projectSlug);
  if (!psNorm) return [];
  const variants = projectSlugQueryVariants(projectSlug);
  const dk = dateKey || dateKeyEastern(new Date());

  const runExact = async (field, value) => {
    const snap = await db
      .collection(COL)
      .where(field, "==", value)
      .where("dateKey", "==", dk)
      .orderBy("createdAt", "asc")
      .limit(800)
      .get()
      .catch((err) => {
        logFirestoreQueryError(`logEntryRepository:${field}:${value}:projectDay`, err);
        return null;
      });
    if (!snap) return [];
    return snap.docs
      .map((d) => ({ id: d.id, ...d.data() }))
      .filter((e) => entryBelongsToNormalizedProject(e, psNorm));
  };

  const pairs = variants.flatMap((v) => [
    ["projectSlug", v],
    ["projectId", v],
  ]);
  const chunks = await Promise.all(pairs.map(([field, v]) => runExact(field, v)));
  const exactRows = dedupeEntries(chunks.flat());
  const legacyRows = await loadLegacyBackdatedLogEntriesForProjectDay(db, dk, projectSlug);
  return dedupeEntries([...exactRows, ...legacyRows]);
}

async function loadTodayLogEntries(db, phoneE164) {
  const rows = await loadLogEntriesForDay(db, phoneE164, dateKeyEastern(new Date()));
  return filterEntriesForDailySummary(rows);
}

async function loadTodayLogEntriesForProject(db, phoneE164, projectSlug) {
  const rows = await loadLogEntriesForDayForProject(
    db,
    phoneE164,
    dateKeyEastern(new Date()),
    projectSlug
  );
  return filterEntriesForDailySummary(rows);
}

function lineText(e) {
  return (e.summaryText || e.normalizedText || e.rawText || "").trim();
}

function formatGroupedDayLog(entries) {
  const list = filterEntriesForDailySummary(entries);
  if (!list.length) {
    return { lines: [], counts: {}, byCat: {} };
  }
  const byCat = {};
  for (const e of list) {
    const c = e.category || "journal";
    if (!byCat[c]) byCat[c] = [];
    const line = lineText(e);
    if (line) byCat[c].push(line);
  }
  const counts = {};
  for (const k of Object.keys(byCat)) counts[k] = byCat[k].length;

  const order = [
    "safety",
    "delay",
    "deficiency",
    "issue",
    "delivery",
    "inspection",
    "note",
    "progress",
    "journal",
  ];
  const lines = [];
  for (const cat of order) {
    if (!byCat[cat] || !byCat[cat].length) continue;
    lines.push(`${cat}: ${byCat[cat].length}`);
    for (const L of byCat[cat].slice(-3)) {
      lines.push(`- ${L.slice(0, 120)}${L.length > 120 ? "…" : ""}`);
    }
  }
  for (const cat of Object.keys(byCat)) {
    if (order.includes(cat)) continue;
    lines.push(`${cat}: ${byCat[cat].length}`);
  }
  return { lines, counts, byCat };
}

/**
 * Group the same filtered entries by dailySummarySections for SMS / PDF hints.
 */
function formatRollupByReportSections(entries) {
  const list = filterEntriesForDailySummary(entries);
  const bySection = {};
  for (const e of list) {
    const text = lineText(e);
    if (!text) continue;
    const secs = Array.isArray(e.dailySummarySections) && e.dailySummarySections.length
      ? e.dailySummarySections
      : ["dayLog"];
    for (const s of secs) {
      if (!bySection[s]) bySection[s] = [];
      bySection[s].push(`[${e.category || "?"}] ${text.slice(0, 140)}`);
    }
  }
  return bySection;
}

async function appendLinkedMediaIds(db, FieldValue, logEntryId, storagePaths) {
  if (!logEntryId || !storagePaths || !storagePaths.length) return;
  const paths = [...new Set(storagePaths.filter(Boolean))];
  if (!paths.length) return;
  await db
    .collection(COL)
    .doc(logEntryId)
    .update({
      linkedMediaIds: FieldValue.arrayUnion(...paths),
      dailySummarySections: FieldValue.arrayUnion("photos", "dayLog"),
    });
}

/**
 * Best-effort JSON enhancement for a single log entry (post-reply).
 */
async function maybeEnhanceLogEntry({
  db,
  openaiApiKey,
  logEntryId,
  logger,
  runId,
  modelsOverride,
}) {
  if (!logEntryId || !openaiApiKey) return;
  const ref = db.collection(COL).doc(logEntryId);
  const snap = await ref.get();
  if (!snap.exists) return;

  const client = new OpenAI({ apiKey: openaiApiKey });
  const data = snap.data() || {};
  const text = (data.rawText || "").slice(0, 2000);

  try {
    const completion = await chatCompletionWithFallback(
      client,
      {
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content: `You analyze one SMS or pasted note from a construction superintendent. Understand intent and facts — do not rely on rigid formatting.

Output JSON only:
{
  "summaryText": string (one line, max 160 chars, field-friendly tense),
  "tags": string[] (0-8 short tokens, e.g. concrete,rebar,weather,excavation),
  "severity": null|"low"|"medium"|"high"|"critical",
  "extraSections": string[] (optional: subset of weather,manpower,workCompleted,workInProgress,delays,deficiencies,safety,issues,inspections,deliveries,concrete,openItems,photos,notes,journal),
  "messageIntent": string (one sentence describing what this message is about — e.g. end-of-day manpower roll call, safety note, pour status),
  "manpowerRows": [ ["Trade name","Foreman or —","Worker count as digits or —","Brief scope notes"], ... ]
}

Rules for manpowerRows:
- Include one row per subcontractor or crew when the message states headcounts, crew sizes, or who was on site. Infer trade names and numbers from natural language (not only "ALC 20" patterns).
- Use "—" for unknown foreman or count when not stated.
- If the message is not about manpower/subs on site, use [].
- Never invent trades or numbers not supported by the text.

If unsure on tags or sections, prefer [].`,
          },
          { role: "user", content: text },
        ],
        max_completion_tokens: 900,
        temperature: 0.15,
      },
      logger,
      runId,
      modelsOverride
    );
    const raw = completionText(completion) || "{}";
    const parsed = JSON.parse(raw);
    const summaryText = String(parsed.summaryText || "").trim().slice(0, 200);
    const newTags = Array.isArray(parsed.tags)
      ? parsed.tags.map((t) => String(t).trim()).filter(Boolean).slice(0, 8)
      : [];
    const severity = ["low", "medium", "high", "critical"].includes(
      String(parsed.severity || "").toLowerCase()
    )
      ? String(parsed.severity).toLowerCase()
      : null;

    const existingTags = Array.isArray(data.tags) ? data.tags : [];
    const mergedTags = [...new Set([...existingTags, ...newTags])].slice(0, 12);

    const baseSections = Array.isArray(data.dailySummarySections)
      ? data.dailySummarySections
      : [];
    const extra = Array.isArray(parsed.extraSections)
      ? parsed.extraSections.map((s) => String(s).trim()).filter(Boolean)
      : [];
    const mergedSections = [...new Set([...baseSections, ...extra, "dayLog"])].slice(
      0,
      24
    );

    const aiReportExtract = sanitizeAiReportExtract(parsed);

    await ref.update({
      aiEnhanced: true,
      aiError: null,
      dailySummarySections: mergedSections,
      ...(summaryText ? { summaryText } : {}),
      ...(mergedTags.length ? { tags: mergedTags } : {}),
      ...(severity ? { severity } : {}),
      ...(aiReportExtract ? { aiReportExtract } : {}),
    });
    logger.info("logEntry: ai enhancement ok", { runId, logEntryId });
  } catch (e) {
    logger.warn("logEntry: ai enhancement failed", {
      runId,
      logEntryId,
      message: e.message,
    });
    await ref.update({
      aiEnhanced: false,
      aiError: String(e.message).slice(0, 500),
    });
  }
}

module.exports = {
  COL_LOG_ENTRIES: COL,
  writeLogEntry,
  loadLogEntriesForDay,
  loadLogEntriesForDayForProject,
  loadLogEntriesForProjectDay,
  loadTodayLogEntries,
  loadTodayLogEntriesForProject,
  filterEntriesForDailySummary,
  formatGroupedDayLog,
  formatRollupByReportSections,
  appendLinkedMediaIds,
  maybeEnhanceLogEntry,
  sanitizeAiManpowerRows,
  getLogEntryEffectiveDateKey,
};
