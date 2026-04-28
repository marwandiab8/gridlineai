/**
 * End-of-day site report PDF — server-side, stored in Firebase Storage + dailyReports doc.
 * Structured Daily Site Log layout: dailyPdfReportBuilder + deterministic dailyReportContent.
 */

const { randomUUID, randomBytes } = require("crypto");
const { PDFDocument, StandardFonts } = require("pdf-lib");
const { FieldValue, Timestamp } = require("firebase-admin/firestore");
const {
  dateKeyEastern,
  startOfEasternDayForDateKey,
  DAILY_REPORT_TIME_ZONE,
  formatDailySiteLogTitleEastern,
  formatConcreteSummaryLabelEastern,
  addCalendarDaysToDateKey,
} = require("./logClassifier");
const { normalizeProjectSlug } = require("./projectAccess");
const { normalizePhoneE164 } = require("./twilioSecrets");
const { roleAtLeast } = require("./authz");
const {
  loadLogEntriesForDay,
  loadLogEntriesForDayForProject,
  loadLogEntriesForProjectDay,
  filterEntriesForDailySummary,
} = require("./logEntryRepository");
const { loadMediaForDailyReport, loadMediaForProjectDailyReport } = require("./mediaRepository");
const {
  fetchDailyWeatherSnapshot,
  DEFAULT_WEATHER_LOCATION_LINE,
} = require("./dailyReportWeather");

/** PDF cover — fixed values (Docksteader); no “default” wording on cover. */
const COVER_LOCATION_DOCKSTEADER = "6 Docksteader Rd, Brampton, ON L6R 3Y2";
const COVER_PREPARED_BY = "Marwan Diab";

function isDocksteaderProject(slug) {
  return slug != null && String(slug).trim().toLowerCase() === "docksteader";
}

/** Prepended to AI INPUT so the model uses fetched Open-Meteo data only (see dailyReportAiJson rules). */
function buildWeatherPrefixForAi(weatherSnapshot) {
  if (!weatherSnapshot || !weatherSnapshot.ok) {
    const msg =
      weatherSnapshot && weatherSnapshot.message
        ? String(weatherSnapshot.message).slice(0, 400)
        : "Weather unavailable.";
    return `WEATHER LOOKUP FAILED: ${msg}\n\n`;
  }
  const loc = weatherSnapshot.resolvedLabel || weatherSnapshot.locationQuery || "—";
  const line = weatherSnapshot.summaryLine || "—";
  return `AUTHORITATIVE WEATHER FOR REPORT DAY (${weatherSnapshot.dateKey}): ${line} Location: ${loc}.\n\n`;
}

function normalizeReportType(value) {
  return String(value || "").trim() === "journal" ? "journal" : "dailySiteLog";
}

function makeStorageDownloadToken() {
  if (typeof randomUUID === "function") return randomUUID();
  return randomBytes(16).toString("hex");
}

function buildFirebaseDownloadUrl(bucketName, storagePath, token) {
  return `https://firebasestorage.googleapis.com/v0/b/${encodeURIComponent(
    bucketName
  )}/o/${encodeURIComponent(storagePath)}?alt=media&token=${encodeURIComponent(token)}`;
}

function buildDailyReportSequenceDocId(phoneE164, dateKey) {
  return `${encodeURIComponent(String(phoneE164 || "").trim())}__${String(dateKey || "").trim()}`;
}

function allocateDailyReportSequence(db, phoneE164, dateKey) {
  const seqRef = db
    .collection("dailyReportSequences")
    .doc(buildDailyReportSequenceDocId(phoneE164, dateKey));
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(seqRef);
    const current = snap.exists ? Number(snap.data().lastSequence || 0) : 0;
    const next = Number.isFinite(current) && current > 0 ? current + 1 : 1;
    tx.set(
      seqRef,
      {
        phoneE164,
        dateKey,
        lastSequence: next,
        updatedAt: Timestamp.now(),
      },
      { merge: true }
    );
    return next;
  });
}

function formatDailyReportPdfFileName(dayStart, sequenceNumber, reportType = "dailySiteLog") {
  const d0 = dayStart instanceof Date ? dayStart : new Date(dayStart);
  const d = new Date(d0.getTime() + 12 * 3600000);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
  }).formatToParts(d);
  const get = (type) => parts.find((p) => p.type === type)?.value || "";
  const seq = String(Math.max(1, Number(sequenceNumber) || 1)).padStart(3, "0");
  const prefix =
    normalizeReportType(reportType) === "journal" ? "Journal" : "Construction_Report";
  return `${prefix}_${get("weekday")}_${get("month")}_${get("day")}_${get("year")}_${seq}.pdf`;
}

function formatCoverDateEastern(dayStart) {
  const d0 = dayStart instanceof Date ? dayStart : new Date(dayStart);
  const d = new Date(d0.getTime() + 12 * 3600000);
  return new Intl.DateTimeFormat("en-US", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(d);
}

function normalizeJournalAuthorName(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function journalPhoneCandidates(value) {
  const raw = String(value || "").trim();
  const candidates = [];
  if (raw) candidates.push(raw);
  const normalized = normalizePhoneE164(raw);
  if (normalized && !candidates.includes(normalized)) candidates.push(normalized);
  return candidates;
}

function normalizePhoneDigits(value) {
  const normalized = normalizePhoneE164(String(value || "").trim());
  return normalized ? normalized.replace(/\D/g, "") : "";
}

function messageSenderDigits(message) {
  const candidates = [
    message && message.phoneE164,
    message && message.from,
    message && message.threadKey,
    message && message.senderPhone,
  ];
  for (const value of candidates) {
    const digits = normalizePhoneDigits(value);
    if (digits) return digits;
  }
  return "";
}

function logEntrySenderDigits(entry) {
  const candidates = [
    entry && entry.authorPhone,
    entry && entry.senderPhone,
    entry && entry.phoneE164,
  ];
  for (const value of candidates) {
    const digits = normalizePhoneDigits(value);
    if (digits) return digits;
  }
  return "";
}

function mediaSenderDigits(media) {
  const candidates = [
    media && media.senderPhone,
    media && media.uploadedBy,
  ];
  for (const value of candidates) {
    const digits = normalizePhoneDigits(value);
    if (digits) return digits;
  }
  return "";
}

async function loadManagementPhonesForProject(db, projectSlug) {
  const project = normalizeProjectSlug(projectSlug);
  if (!project) return new Set();
  const snap = await db
    .collection("appMembers")
    .where("active", "==", true)
    .limit(500)
    .get()
    .catch(() => null);
  if (!snap) return new Set();
  const phones = new Set();
  for (const docSnap of snap.docs) {
    const member = docSnap.data() || {};
    if (!roleAtLeast(member.role, "management")) continue;
    const allProjects = member.allProjects === true;
    const projectSlugs = Array.isArray(member.projectSlugs)
      ? member.projectSlugs.map((slug) => normalizeProjectSlug(slug)).filter(Boolean)
      : [];
    if (!allProjects && !projectSlugs.includes(project)) continue;
    const digits = normalizePhoneDigits(member.approvedPhoneE164 || "");
    if (digits) phones.add(digits);
  }
  return phones;
}

async function resolveJournalAuthorLabels(db, entries) {
  const labelMap = new Map();
  const phones = new Set();
  for (const entry of entries || []) {
    for (const value of [entry && entry.authorPhone, entry && entry.senderPhone, entry && entry.phoneE164]) {
      for (const candidate of journalPhoneCandidates(value)) {
        if (candidate) phones.add(candidate);
      }
    }
  }

  const lookupPromises = [...phones].map(async (phone) => {
    let label = "";

    try {
      const memberSnap = await db
        .collection("appMembers")
        .where("approvedPhoneE164", "==", phone)
        .where("active", "==", true)
        .limit(1)
        .get();
      if (!memberSnap.empty) {
        const member = memberSnap.docs[0].data() || {};
        label = normalizeJournalAuthorName(member.displayName || member.email || "");
      }
    } catch (_) {}

    if (!label) {
      try {
        const smsSnap = await db.collection("smsUsers").doc(phone).get();
        if (smsSnap.exists) {
          const sms = smsSnap.data() || {};
          label = normalizeJournalAuthorName(sms.displayName || sms.approvedMemberEmail || "");
        }
      } catch (_) {}
    }

    if (label) {
      for (const candidate of journalPhoneCandidates(phone)) {
        const digits = candidate.replace(/\D/g, "");
        if (digits) labelMap.set(`phone:${digits}`, label);
      }
    }
  });

  await Promise.all(lookupPromises);
  return labelMap;
}

function dedupeMediaDocs(groups) {
  const seen = new Set();
  const out = [];
  for (const group of groups) {
    for (const m of group || []) {
      const id = m && m.id != null ? String(m.id) : "";
      if (!id || seen.has(id)) continue;
      seen.add(id);
      out.push(m);
    }
  }
  return out;
}

function mediaMatchesExactDateKey(m, dateKey) {
  if (!dateKey) return true;
  const mediaDateKey = m && typeof m.dateKey === "string" ? m.dateKey.trim() : "";
  return mediaDateKey === dateKey;
}

function filterJournalMediaForReport(mediaDocs, entryIdSetOrEntries, projectKey, options = {}) {
  const entryIdSet =
    entryIdSetOrEntries instanceof Set
      ? entryIdSetOrEntries
      : new Set(
          (entryIdSetOrEntries || [])
            .map((e) =>
              e != null && typeof e === "object" && e.id != null ? String(e.id) : String(e || "")
            )
            .filter(Boolean)
        );
  const wantProject =
    projectKey != null && String(projectKey).trim() !== ""
      ? String(projectKey).trim()
      : null;
  const exactDateKey =
    options && typeof options.dateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(options.dateKey.trim())
      ? options.dateKey.trim()
      : null;

  return (mediaDocs || []).filter((m) => {
    if (!mediaMatchesExactDateKey(m, exactDateKey)) return false;
    const linkId = m && m.linkedLogEntryId != null ? String(m.linkedLogEntryId).trim() : "";
    if (linkId) return entryIdSet.has(linkId);
    if (!wantProject) return true;
    const mediaProject =
      m && m.projectId != null && String(m.projectId).trim() !== ""
        ? String(m.projectId).trim()
        : m && m.projectSlug != null && String(m.projectSlug).trim() !== ""
          ? String(m.projectSlug).trim()
          : null;
    return mediaProject === wantProject || mediaProject === "_unassigned";
  });
}

async function loadAllMediaForDailyReport(db, phoneE164, dateKey) {
  const snap = await db
    .collection("media")
    .where("senderPhone", "==", phoneE164)
    .where("dateKey", "==", dateKey)
    .get()
    .catch(() => null);
  if (!snap) return [];
  return snap.docs
    .map((d) => ({ id: d.id, ...d.data() }))
    .filter((m) => m.storagePath);
}
const {
  buildDailyReportModel,
  buildJournalReportModel,
  mergeAiIntoDeterministic,
  mergeStructuredDailyReportJson,
  extractStructuredTableOverrides,
  formatReportBundleForAi,
  formatJournalBundleForAi,
  filterEntriesForJournalReport,
} = require("./dailyReportContent");
const {
  generateStructuredDailyReportJson,
  generateStructuredJournalReportJson,
} = require("./dailyReportAiJson");
const {
  renderDailySiteLogPdf,
  renderJournalPdf,
} = require("./dailyPdfReportBuilder");
const {
  filterLogEntriesForProjectDailyReport,
  filterMediaForProjectDailyReport,
  promoteFieldReportSections,
  auditDailyReportEntries,
  curateFieldEntriesForDailyReport,
} = require("./dailyReportIntegrity");

/**
 * Inbound messages for fallback / audit only (optional).
 */
async function loadMessagesForDailyReport(db, phoneE164, dayStart, nextDayStart, projectSlug, options = {}) {
  const includeAllProjects = options.includeAllProjects === true;
  const ps =
    projectSlug != null && String(projectSlug).trim() !== ""
      ? String(projectSlug).trim()
      : null;

  if (includeAllProjects) {
    const snap = await db
      .collection("messages")
      .where("threadKey", "==", phoneE164)
      .where("createdAt", ">=", dayStart)
      .where("createdAt", "<", nextDayStart)
      .orderBy("createdAt", "asc")
      .limit(180)
      .get()
      .catch(() => null);
    return snap
      ? snap.docs.map((d) => ({ id: d.id, ...d.data() }))
      : [];
  }

  if (ps) {
    const snap = await db
      .collection("messages")
      .where("threadKey", "==", phoneE164)
      .where("projectSlug", "==", ps)
      .where("createdAt", ">=", dayStart)
      .where("createdAt", "<", nextDayStart)
      .orderBy("createdAt", "asc")
      .limit(120)
      .get()
      .catch(() => null);
    return snap
      ? snap.docs.map((d) => ({ id: d.id, ...d.data() }))
      : [];
  }

  const snap = await db
    .collection("messages")
    .where("threadKey", "==", phoneE164)
    .where("createdAt", ">=", dayStart)
    .where("createdAt", "<", nextDayStart)
    .orderBy("createdAt", "asc")
    .limit(120)
    .get()
    .catch(() => null);
  const rows = snap ? snap.docs.map((d) => ({ id: d.id, ...d.data() })) : [];
  return rows.filter((m) => !m.projectSlug);
}

function messageProjectQueryVariants(projectSlug) {
  const raw = String(projectSlug || "").trim();
  const norm = normalizeProjectSlug(projectSlug);
  return [...new Set([norm, raw].filter(Boolean))];
}

/** Inbound messages for one project + Eastern day window (all threads / senders). */
async function loadMessagesForProjectDay(db, dayStart, nextDayStart, projectSlug) {
  const variants = messageProjectQueryVariants(projectSlug);
  if (!variants.length) return [];
  const psNorm = normalizeProjectSlug(projectSlug);
  const chunks = await Promise.all(
    variants.map((ps) =>
      db
        .collection("messages")
        .where("projectSlug", "==", ps)
        .where("createdAt", ">=", dayStart)
        .where("createdAt", "<", nextDayStart)
        .orderBy("createdAt", "asc")
        .limit(200)
        .get()
        .catch((err) => {
          try {
            require("firebase-functions").logger.error("dailyReportPdf:messagesProjectDay", {
              message: err && err.message,
              code: err && err.code,
              projectSlug: ps,
            });
          } catch (_) {}
          return null;
        })
    )
  );
  const rows = [];
  const seen = new Set();
  for (const snap of chunks) {
    if (!snap) continue;
    for (const d of snap.docs) {
      const row = { id: d.id, ...d.data() };
      const slugN = normalizeProjectSlug(row.projectSlug);
      if (psNorm && slugN !== psNorm) continue;
      if (!seen.has(d.id)) {
        seen.add(d.id);
        rows.push(row);
      }
    }
  }
  rows.sort((a, b) => {
    const ta = a.createdAt && a.createdAt.toMillis ? a.createdAt.toMillis() : 0;
    const tb = b.createdAt && b.createdAt.toMillis ? b.createdAt.toMillis() : 0;
    return ta - tb;
  });
  return rows.slice(0, 320);
}

/**
 * @param {object} opts
 * @param {import('firebase-admin').firestore.Firestore} opts.db
 * @param {import('@google-cloud/storage').Bucket} opts.bucket
 * @param {string} opts.phoneE164
 * @param {string|null} opts.projectSlug
 * @param {string|null} opts.projectName
 * @param {string|null} opts.openaiApiKey
 * @param {import('firebase-functions').logger} opts.logger
 * @param {string} opts.runId
 * @param {object} [opts.modelsOverride]
 */
async function generateDailyReportPdf(opts) {
  const {
    db,
    bucket,
    phoneE164,
    projectSlug,
    projectName,
    openaiApiKey,
    logger,
    runId,
    modelsOverride,
    reportDateKey: reportDateKeyIn,
    reportType: reportTypeIn,
    includeAllManagementEntries = false,
  } = opts;

  const reportType = normalizeReportType(reportTypeIn);
  const projectSlugRaw =
    projectSlug != null && String(projectSlug).trim() !== ""
      ? String(projectSlug).trim()
      : null;
  const projectKey = projectSlugRaw ? normalizeProjectSlug(projectSlugRaw) : null;
  const projectSlugForQueries = projectSlugRaw || projectKey;

  let resolvedProjectName = projectName || null;
  let projectLocation = "";
  let logoStoragePath = null;
  let projectNotes = "";
  if (projectKey) {
    const ps = await db.collection("projects").doc(projectKey).get();
    if (ps.exists) {
      const pd = ps.data() || {};
      if (!resolvedProjectName) resolvedProjectName = pd.name || projectKey;
      projectLocation = String(pd.address || pd.siteAddress || pd.location || "").trim();
      logoStoragePath = pd.reportLogoStoragePath || null;
      projectNotes = String(pd.notes || "").trim();
    } else if (!resolvedProjectName) {
      resolvedProjectName = projectKey;
    }
  }
  if (isDocksteaderProject(projectKey)) {
    projectLocation = COVER_LOCATION_DOCKSTEADER;
  }

  let preparedByLine = COVER_PREPARED_BY;
  let footerBrand = "Gridline";
  try {
    const companySnap = await db.collection("adminSettings").doc("company").get();
    if (companySnap.exists) {
      const c = companySnap.data() || {};
      if (c.dailyReportFooterBrand) footerBrand = String(c.dailyReportFooterBrand);
      if (!logoStoragePath && c.dailyReportLogoStoragePath) {
        logoStoragePath = String(c.dailyReportLogoStoragePath).trim() || null;
      }
    }
  } catch (_) {}

  const dk =
    typeof reportDateKeyIn === "string" &&
    /^\d{4}-\d{2}-\d{2}$/.test(reportDateKeyIn.trim())
      ? reportDateKeyIn.trim()
      : dateKeyEastern(new Date());
  const dayStart = startOfEasternDayForDateKey(dk);
  const nextDayStart = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dk, 1));

  let messages = [];
  let logEntriesRaw = [];
  let mediaDocs = [];
  /** When a project is selected, PDFs include every contributor on that project (not only the generator's phone). */
  const useProjectAggregate = !!projectKey;
  if (reportType === "journal") {
    const isProjectScopedJournal = useProjectAggregate;
    const [msgs, logEntriesAll, mediaToday] = await Promise.all([
      isProjectScopedJournal
        ? loadMessagesForProjectDay(db, dayStart, nextDayStart, projectSlugForQueries)
        : loadMessagesForDailyReport(db, phoneE164, dayStart, nextDayStart, projectKey, {
            includeAllProjects: true,
          }),
      isProjectScopedJournal
        ? loadLogEntriesForProjectDay(db, dk, projectSlugForQueries)
        : loadLogEntriesForDay(db, phoneE164, dk),
      isProjectScopedJournal
        ? loadMediaForProjectDailyReport(db, dk, projectKey)
        : loadAllMediaForDailyReport(db, phoneE164, dk),
    ]);
    messages = msgs;
    logEntriesRaw = logEntriesAll;
    mediaDocs = dedupeMediaDocs([mediaToday]);
  } else {
    const [msgs, logEntriesScoped, mediaToday] = await Promise.all([
      useProjectAggregate
        ? loadMessagesForProjectDay(db, dayStart, nextDayStart, projectSlugForQueries)
        : loadMessagesForDailyReport(db, phoneE164, dayStart, nextDayStart, projectKey),
      useProjectAggregate
        ? loadLogEntriesForProjectDay(db, dk, projectSlugForQueries)
        : loadLogEntriesForDayForProject(db, phoneE164, dk, projectKey),
      useProjectAggregate
        ? loadMediaForProjectDailyReport(db, dk, projectKey)
        : loadMediaForDailyReport(db, phoneE164, dk, projectKey),
    ]);
    messages = msgs;
    logEntriesRaw = logEntriesScoped;
    mediaDocs = dedupeMediaDocs([mediaToday]);
  }

  let managementPhoneDigits = new Set();
  if (useProjectAggregate && includeAllManagementEntries) {
    managementPhoneDigits = await loadManagementPhonesForProject(db, projectKey);
    if (managementPhoneDigits.size > 0) {
      messages = messages.filter((m) => managementPhoneDigits.has(messageSenderDigits(m)));
      logEntriesRaw = logEntriesRaw.filter((e) => managementPhoneDigits.has(logEntrySenderDigits(e)));
      mediaDocs = mediaDocs.filter((m) => managementPhoneDigits.has(mediaSenderDigits(m)));
    }
  }

  if (logger) {
    logger.info("dailyReportPdf: source counts", {
      runId,
      reportType,
      projectKey,
      dk,
      useProjectAggregate,
      includeAllManagementEntries: Boolean(includeAllManagementEntries),
      managementPhoneCount: managementPhoneDigits.size,
      messages: messages.length,
      logEntriesRaw: logEntriesRaw.length,
      mediaDocs: mediaDocs.length,
    });
  }

  let aiNarrativeApplied = false;
  let curatedEntries = [];
  let mediaForReport = [];
  let model;
  let merged;
  let titleStr;
  let coverMeta;
  let concreteLabel = "";
  let weatherSnapshot = null;

  if (reportType === "journal") {
    const logEntries = filterEntriesForDailySummary(logEntriesRaw);
    curatedEntries = filterEntriesForJournalReport(logEntries, dk);
    const journalMediaEntryIds = new Set(
      (logEntriesRaw || [])
        .map((e) => (e && e.id != null ? String(e.id) : ""))
        .filter(Boolean)
    );
    mediaForReport = filterJournalMediaForReport(mediaDocs, journalMediaEntryIds, projectKey, {
      dateKey: dk,
    });
    const authorLabelSource = [...(logEntriesRaw || []), ...curatedEntries];
    const authorLabelsByIdentity = await resolveJournalAuthorLabels(db, authorLabelSource);

    model = buildJournalReportModel(curatedEntries, mediaForReport, {
      dayStart,
      reportDateKey: dk,
      authorLabelsByIdentity,
    });

    const journalBundle = formatJournalBundleForAi(curatedEntries, dk, {
      authorLabelsByIdentity,
    });
    let journalJson = null;
    if (openaiApiKey && journalBundle.trim()) {
      journalJson = await generateStructuredJournalReportJson({
        openaiApiKey,
        dateKey: dk,
        timeZoneLabel: DAILY_REPORT_TIME_ZONE,
        reportBundle: journalBundle,
        logger,
        runId,
        modelsOverride,
      });
      aiNarrativeApplied = !!journalJson;
    }

    merged = journalJson
      ? {
          overview: journalJson.overview || model.deterministic.overview,
          keyMoments:
            Array.isArray(journalJson.keyMoments) && journalJson.keyMoments.length
              ? journalJson.keyMoments
              : model.deterministic.keyMoments,
          reflections:
            Array.isArray(journalJson.reflections) && journalJson.reflections.length
              ? journalJson.reflections
              : model.deterministic.reflections,
          closingNote: journalJson.closingNote || model.deterministic.closingNote,
        }
      : {
          overview: model.deterministic.overview,
          keyMoments: model.deterministic.keyMoments,
          reflections: model.deterministic.reflections,
          closingNote: model.deterministic.closingNote,
        };

    const titleDate = formatCoverDateEastern(dayStart);
    const journalScopeLabel = resolvedProjectName || projectKey || "Personal journal";
    titleStr = projectKey
      ? `Daily Journal - ${journalScopeLabel} - ${titleDate}`
      : `Daily Journal - ${titleDate}`;
    coverMeta = {
      titleMain: "Daily Journal",
      titleDate,
      projectHeadline: journalScopeLabel,
      brandLine: `Personal daily journal - ${footerBrand}`,
      lines: [],
      grid: [
        { label: "Report type", value: "Journal" },
        {
          label: "Project scope",
          value: projectKey
            ? journalScopeLabel
            : "All notes, messages, and photos captured for this day.",
        },
        { label: "Report day", value: dk },
        {
          label: "Source",
          value: projectKey
            ? "Project-scoped notes, messages, and photos captured for this day."
            : "Personal notes, messages, and photos captured for this day.",
        },
      ],
    };
  } else {
    let logEntries = filterEntriesForDailySummary(logEntriesRaw);
    logEntries = filterLogEntriesForProjectDailyReport(logEntries, projectKey);
    logEntries = promoteFieldReportSections(logEntries);
    curatedEntries = curateFieldEntriesForDailyReport(logEntries, dk);
    if (logger) {
      logger.info("dailyReportPdf: after daily site log filters", {
        runId,
        afterSummary: filterEntriesForDailySummary(logEntriesRaw).length,
        afterProjectMeta: logEntries.length,
        curatedEntries: curatedEntries.length,
      });
    }
    const entryIdSet = new Set(curatedEntries.map((e) => String(e.id)));

    weatherSnapshot = await fetchDailyWeatherSnapshot({
      addressLine: projectLocation,
      dateKey: dk,
      timeZone: DAILY_REPORT_TIME_ZONE,
      logger,
      runId,
    });
    const allowedSourceMessageIds = new Set(
      curatedEntries
        .map((e) => e.sourceMessageId)
        .filter((id) => id != null && String(id).trim() !== "")
        .map((id) => String(id).trim())
    );
    mediaForReport = filterMediaForProjectDailyReport(mediaDocs, projectKey, entryIdSet, {
      allowedSourceMessageIds,
    });

    if (process.env.DEBUG_DAILY_REPORT_AUDIT === "1" && logger) {
      try {
        logger.info("dailyReportPdf: audit trail", {
          runId,
          projectKey,
          rows: auditDailyReportEntries(logEntriesRaw, logEntries, projectKey),
        });
      } catch (aErr) {
        logger.warn("dailyReportPdf: audit failed", { runId, message: aErr.message });
      }
    }

    const reportBundle = formatReportBundleForAi(curatedEntries, dk);
    const reportBundleForAi = `${buildWeatherPrefixForAi(weatherSnapshot)}${reportBundle}`;

    let structuredJson = null;
    if (openaiApiKey && reportBundleForAi.trim()) {
      structuredJson = await generateStructuredDailyReportJson({
        openaiApiKey,
        projectName: resolvedProjectName || projectKey || "project",
        dateKey: dk,
        timeZoneLabel: DAILY_REPORT_TIME_ZONE,
        reportBundle: reportBundleForAi,
        logger,
        runId,
        modelsOverride,
      });
      aiNarrativeApplied = !!structuredJson;
    }

    const structuredOverrides = structuredJson
      ? extractStructuredTableOverrides(structuredJson)
      : null;
    model = buildDailyReportModel(curatedEntries, mediaForReport, {
      dayStart,
      structuredOverrides,
      reportDateKey: dk,
    });

    if (structuredJson) {
      merged = mergeStructuredDailyReportJson(model.deterministic, structuredJson, dayStart);
    } else {
      merged = mergeAiIntoDeterministic(model.deterministic, null, dayStart);
    }

    merged.weatherDaily = {
      snapshot: weatherSnapshot,
      narrativeFromLog: model.deterministic.weatherToday,
    };

    titleStr = formatDailySiteLogTitleEastern(dayStart);
    concreteLabel = formatConcreteSummaryLabelEastern(dayStart);

    const dashIdx = titleStr.indexOf(" – ");
    let coverLocationLine = String(projectLocation || "").trim();
    if (!coverLocationLine) {
      coverLocationLine = isDocksteaderProject(projectKey)
        ? COVER_LOCATION_DOCKSTEADER
        : DEFAULT_WEATHER_LOCATION_LINE;
    }

    coverMeta = {
      titleMain: dashIdx >= 0 ? titleStr.slice(0, dashIdx) : titleStr,
      titleDate: dashIdx >= 0 ? titleStr.slice(dashIdx + 3) : "",
      projectHeadline: resolvedProjectName || projectKey || "Not assigned",
      brandLine: `Daily site documentation - ${footerBrand}`,
      lines: [],
      grid: [
        { label: "Project", value: resolvedProjectName || projectKey || "Not assigned" },
        { label: "Location", value: coverLocationLine },
        { label: "Report day", value: dk },
        { label: "Prepared by", value: preparedByLine },
      ],
    };
  }

  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);

  if (reportType === "journal") {
    await renderJournalPdf({
      pdf,
      font,
      fontBold,
      storageBucket: bucket,
      titleStr,
      footerBrand,
      coverTitle: titleStr,
      coverMeta,
      logoStoragePath,
      projectNotes,
      merged,
      model,
      logger,
      runId,
    });
  } else {
    await renderDailySiteLogPdf({
      pdf,
      font,
      fontBold,
      storageBucket: bucket,
      titleStr,
      footerBrand,
      coverTitle: titleStr,
      coverMeta,
      logoStoragePath,
      projectNotes,
      merged,
      model,
      concreteLabel,
      logger,
      runId,
    });
  }

  const bytes = await pdf.save();
  const buf = Buffer.from(bytes);
  const reportSequence = await allocateDailyReportSequence(db, phoneE164, dk);
  const fileName = formatDailyReportPdfFileName(dayStart, reportSequence, reportType);
  const storagePath = `dailyReports/${encodeURIComponent(phoneE164)}/${dk}/${reportType}/${fileName}`;
  const file = bucket.file(storagePath);
  const downloadToken = makeStorageDownloadToken();
  await file.save(buf, {
    contentType: "application/pdf",
    contentDisposition: `attachment; filename="${fileName}"`,
    metadata: {
      metadata: {
        phoneE164,
        projectSlug: projectKey || "",
        reportDateKey: dk,
        reportType,
        reportFileName: fileName,
        reportSequence: String(reportSequence),
        firebaseStorageDownloadTokens: downloadToken,
      },
    },
  });

  let downloadURL = null;
  let downloadUrlError = null;
  try {
    const [url] = await file.getSignedUrl({
      action: "read",
      expires: Date.now() + 1000 * 60 * 60 * 24 * 365,
    });
    downloadURL = url || null;
  } catch (signErr) {
    downloadUrlError = String(signErr.message || signErr).slice(0, 500);
    logger.warn("dailyReportPdf: getSignedUrl failed; report still stored", {
      runId,
      storagePath,
      message: downloadUrlError,
    });
    downloadURL = buildFirebaseDownloadUrl(bucket.name, storagePath, downloadToken);
  }

  const reportRef = await db.collection("dailyReports").add({
    phoneE164,
    projectId: projectKey,
    projectName: projectKey ? resolvedProjectName || projectKey : null,
    reportType,
    includeAllManagementEntries: Boolean(includeAllManagementEntries),
    managementPhoneCount: managementPhoneDigits.size,
    reportTitle: titleStr,
    reportFileName: fileName,
    reportSequence,
    reportDate: Timestamp.fromDate(dayStart),
    dateKey: dk,
    storagePath,
    downloadURL,
    downloadUrlError,
    messageCount: messages.length,
    logEntryCount: curatedEntries.length,
    mediaCount: mediaForReport.length,
    unifiedDayLog: curatedEntries.length > 0,
    aiNarrative: aiNarrativeApplied,
    weatherSnapshot: reportType === "dailySiteLog" && weatherSnapshot
      ? weatherSnapshot.ok
        ? {
            provider: weatherSnapshot.provider,
            dateKey: weatherSnapshot.dateKey,
            usedFallbackLocation: !!weatherSnapshot.usedFallbackLocation,
            locationQuery: weatherSnapshot.locationQuery,
            resolvedLabel: weatherSnapshot.resolvedLabel,
            latitude: weatherSnapshot.latitude,
            longitude: weatherSnapshot.longitude,
            summaryLine: weatherSnapshot.summaryLine,
            highC: weatherSnapshot.highC,
            lowC: weatherSnapshot.lowC,
            precipInches: weatherSnapshot.precipInches,
            windMphMax: weatherSnapshot.windMphMax,
            conditions: weatherSnapshot.conditions,
          }
        : {
            ok: false,
            reason: weatherSnapshot.reason,
            message: weatherSnapshot.message,
            usedFallbackLocation: !!weatherSnapshot.usedFallbackLocation,
          }
      : null,
    createdAt: FieldValue.serverTimestamp(),
  });

  return {
    reportId: reportRef.id,
    reportDateKey: dk,
    reportType,
    fileName,
    reportSequence,
    downloadURL,
    downloadUrlError,
    storagePath,
  };
}

module.exports = {
  generateDailyReportPdf,
  formatDailyReportPdfFileName,
  buildDailyReportSequenceDocId,
  filterJournalMediaForReport,
};
