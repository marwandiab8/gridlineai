/**
 * Daily PDF report data integrity: project scope, meta/control exclusion, media linkage,
 * and light promotion of journal lines into report sections. Raw Firestore docs are unchanged.
 */

const {
  isDailyReportPdfRequest,
  isAnyDayRollupRequest,
} = require("./logClassifier");
const { filterEntriesForDailySummary } = require("./logEntryRepository");
const { normalizeReportLineText } = require("./dailyReportBulkText");
const { normalizeProjectSlug } = require("./projectAccess");

function lineText(e) {
  return (e.summaryText || e.normalizedText || e.rawText || "").trim();
}

function normSlug(s) {
  if (s == null || String(s).trim() === "") return null;
  return String(s).trim();
}

function mediaContentTypeLooksImage(value) {
  return String(value || "").trim().toLowerCase().startsWith("image/");
}

/** Reduces false positives when broad “show photos” / UI patterns overlap real field notes. */
function looksLikeFieldLogContext(t) {
  return /\b(grid|pour|slab|rebar|concrete|log\s*:|daily\s+log\s*:|deficiency|inspection|crew|footing|GL\/|elevation|waterproof|trench|scaffold|crane|formwork|backfill|submittal)\b/i.test(
    t
  );
}

/**
 * SMS / UI control chatter — excluded from PDF body & appendix (still in logEntries for audit).
 */
function isMetaOrControlChatter(text, _entry) {
  const raw = String(text || "").trim();
  if (!raw) return true;
  const t = raw.replace(/\s+/g, " ").trim();
  const low = t.toLowerCase();

  if (isDailyReportPdfRequest(t)) return true;
  if (isAnyDayRollupRequest(t)) return true;

  if (/^(continue|ok|okay|k|yep|nope|yes|no|thanks?|thank you|ty|\.{2,})$/i.test(low)) return true;

  if (
    /show\s+me\s+(the\s+)?(pictures?|photos?|pics|images?)/i.test(t) &&
    !looksLikeFieldLogContext(t)
  ) {
    return true;
  }
  if (/show\s+them\s+to\s+me\s+in\s+(the\s+)?standard\s+format/i.test(t)) return true;
  if (/in\s+the\s+standard\s+format/i.test(t) && /show|display|send/i.test(low)) return true;

  if (
    /\b(open|view|launch)\s+(the\s+)?(\w+\s+){0,2}(photo|pic|image)s?\s+gallery\b/i.test(t) &&
    !looksLikeFieldLogContext(t)
  ) {
    return true;
  }
  if (/\ball\s+my\s+(photos?|pics|images?)\b/i.test(t) && !looksLikeFieldLogContext(t)) return true;
  if (
    /\b(send|text|email)\s+(me\s+)?(the\s+)?(photos?|pics|images?)\s+from\s+(the\s+)?(home|gallery|app)\b/i.test(
      t
    )
  ) {
    return true;
  }

  if (/add\s+those\s+photos?\s+to\s+(a\s+)?project/i.test(t)) return true;
  if (/project\s+named\s+home\b/i.test(t) && /photo|picture|pic|add|move/i.test(low)) return true;

  if (/request\s+to\s+receive\s+(the\s+)?daily\s+log/i.test(t)) return true;
  if (/awaiting\s+sms\s+input/i.test(t)) return true;
  if (/generate\s+field\s+log\s+entry/i.test(t)) return true;
  if (/receive\s+(the\s+)?daily\s+(log|report)\s+currently\s+on\s+record/i.test(t)) return true;

  if (/^project\s+\S+\s*$/i.test(t)) return true;
  if (/^(switch|change)\s+to\s+project\s+\S+/i.test(t)) return true;
  if (/\bnow\s+(logging|on)\s+(to\s+)?project\s+\S+/i.test(t)) return true;
  if (/\bset\s+(my\s+)?(default\s+)?project\s+to\s+\S+/i.test(t)) return true;
  if (/\buse\s+project\s+\S+\s+for\s+(logs?|texts?|messages?)\b/i.test(t)) return true;
  if (/\breassign\s+(this\s+)?(photo|pic|image)s?\s+to\s+project\b/i.test(t)) return true;

  if (/\b(this\s+)?(report|pdf|daily\s+report)\s+(is\s+)?(wrong|broken|bad|useless|not\s+right)/i.test(t))
    return true;
  if (/\b(format|layout|styling)\s+(is\s+)?(wrong|bad|broken|ugly)/i.test(t)) return true;
  if (/why\s+(isn't|is not|arent|aren't)\s+.+\s+(in|on)\s+(the\s+)?(report|pdf)/i.test(t)) return true;
  if (/\b(report|pdf|daily\s+report)\s+(doesn'?t|does\s+not)\s+(match|include|show|list)/i.test(t))
    return true;
  if (/\b(missing|not\s+showing|omitted)\s+(from|in)\s+(the\s+)?(report|pdf)\b/i.test(t)) return true;
  if (/\bwrong\s+(project|site)\s+(on|in)\s+(the\s+)?(report|pdf)\b/i.test(t)) return true;
  if (/\b(that'?s|this\s+is)\s+not\s+(my|the)\s+(site|project)\b/i.test(t)) return true;
  if (
    /\b(fix|correct|update|revise|clean\s*up|move|adjust|change)\b[\s\S]{0,60}\b(report|pdf|daily\s+report|summary|header|title|layout|spacing)\b/i.test(
      t
    )
  ) {
    return true;
  }
  if (
    /\b(add|include|put|show|attach|leave|keep|use)\b[\s\S]{0,70}\b(photo|photos|picture|pictures|pic|pics|image|images|this)\b[\s\S]{0,50}\b(report|pdf|daily\s+report|summary)\b/i.test(
      t
    )
  ) {
    return true;
  }
  if (
    /\bmake\s+sure\b[\s\S]{0,70}\b(photo|photos|picture|pictures|pic|pics|image|images|this|that)\b[\s\S]{0,50}\b(in|on)\s+(the\s+)?(report|pdf|daily\s+report|summary)\b/i.test(
      t
    )
  ) {
    return true;
  }

  if (/\b(text|send)\s+me\s+(the\s+)?link\s+(to|for)\s+(the\s+)?(pdf|report)/i.test(t)) return true;

  if (/\b(tap\s+(here|below|the\s+link)|from\s+the\s+menu|reply\s+with\s+\d)\b/i.test(t)) return true;

  if (
    /^(hi|hello|hey)\b/i.test(t) &&
    t.length < 40 &&
    !/\b(slab|pour|crew|site|concrete|rebar)\b/i.test(low)
  ) {
    return true;
  }

  if (
    /\b(at\s+)?home\b/i.test(t) &&
    /\b(kids|family|dog|personal|weekend|dinner|groceries)\b/i.test(low) &&
    !/\b(home\s+depot|homestretch|homeowner)\b/i.test(low)
  ) {
    return true;
  }

  if (/no\s+(field\s+)?updates?\s+provided/i.test(t)) return true;
  if (/no\s+details?\s+provided/i.test(t)) return true;
  if (/no\s+details?\s+(available|given|listed)/i.test(t)) return true;
  if (/no\s+field\s+updates?\b/i.test(t)) return true;

  if (
    /\b(request(?:ed)?|requesting)\s+(a\s+|the\s+)?(pdf|daily\s+report|eod\s+report|end[\s-]of[\s-]day\s+report)\b/i.test(
      t
    )
  ) {
    return true;
  }
  if (/requested\s+item\s+to\s+be\s+included\s+in\s+the\s+daily\s+summary/i.test(t)) return true;
  if (/request(?:ing)?\s+(?:a\s+)?plan\s+for\s+tomorrow/i.test(t)) return true;
  if (/\basks?\s+for\s+tomorrow'?s?\s+weather\s+forecast\b/i.test(t)) return true;
  if (/unable\s+to\s+retrieve\s+live\s+weather\s+data/i.test(t)) return true;
  if (/\bweather\s+(?:data|forecast)\s+(?:unavailable|not\s+available)\b/i.test(t)) return true;
  if (/\b(send|text|email)\s+(me\s+)?(the\s+)?(daily\s+)?report\s+(by\s+)?(text|sms|mms)\b/i.test(t))
    return true;
  if (/\bgenerate\s+(the\s+|a\s+)?(pdf|daily\s+report|eod\s+report)\b/i.test(t)) return true;
  if (/^daily\s+report\s*[.!…]*$/i.test(t.trim())) return true;

  if (
    /\b(complaint|complain|frustrated|ridiculous|unacceptable|terrible|horrible|useless)\b/i.test(
      low
    ) &&
    /\b(report|pdf|format|layout|daily\s+log|app|bot)\b/i.test(low)
  ) {
    return true;
  }

  return false;
}

/** Workflow / clarification lines that pass generic meta filters but are not field facts. */
function isCuratedNonFieldChatter(text) {
  const t = String(text || "").replace(/\s+/g, " ").trim();
  if (!t) return true;
  if (/manpower\s+update\s+requested/i.test(t)) return true;
  if (/clarif(y|ies|es)\b.*\b(manpower|report|today|yesterday)/i.test(t)) return true;
  if (/requested\s+item\s+to\s+be\s+included\s+in\s+the\s+daily\s+summary/i.test(t)) return true;
  if (/request(?:ing)?\s+(?:a\s+)?plan\s+for\s+tomorrow/i.test(t)) return true;
  if (/\basks?\s+for\s+tomorrow'?s?\s+weather\s+forecast\b/i.test(t)) return true;
  if (/unable\s+to\s+retrieve\s+live\s+weather\s+data/i.test(t)) return true;
  if (/\b(keywords?\s+to\s+use|chatgpt|openai)\b/i.test(t)) return true;
  if (/\bgive\s+me\s+a\s+list\s+of\s+keywords\b/i.test(t)) return true;
  if (/continue\s+(the\s+)?previous\s+conversation/i.test(t)) return true;
  if (/no\s+message\s+content\s+provided/i.test(t)) return true;
  if (/no\s+content\s+provided\s+to\s+analyze/i.test(t)) return true;
  if (/request\s+to\s+continue/i.test(t)) return true;
  if (/^\s*internal\s*$/i.test(t)) return true;
  return false;
}

/**
 * Second-pass curation: drop non-field workflow lines before AI / appendix.
 * @param {string} [reportDateKey] Eastern YYYY-MM-DD — when set, entries that normalize to empty (bulk wrappers only) are removed.
 */
function curateFieldEntriesForDailyReport(entries, reportDateKey) {
  return (entries || []).filter((e) => {
    if (entryIsExcludedFromReport(e)) return false;
    const layers = [
      lineText(e),
      String(e.rawText || "").trim(),
      String(e.normalizedText || "").trim(),
      String(e.summaryText || "").trim(),
    ].filter(Boolean);
    for (const layer of layers) {
      if (isCuratedNonFieldChatter(layer)) return false;
    }
    if (reportDateKey) {
      const cleaned = normalizeReportLineText(lineText(e), reportDateKey);
      if (!cleaned.trim()) return false;
    }
    return true;
  });
}

/** Reject verb-like / system words used as fake trade headings in work grouping or AI JSON. */
function isValidTradeHeading(s) {
  const t = String(s || "").trim();
  if (t.length < 2 || t.length > 48) return false;
  if (t.length === 2) {
    const bad2 = new Set([
      "it",
      "or",
      "as",
      "at",
      "in",
      "on",
      "to",
      "of",
      "we",
      "no",
      "so",
      "if",
      "do",
      "go",
      "up",
      "us",
    ]);
    if (bad2.has(t.toLowerCase())) return false;
  }
  if (t.length === 2) return true;
  if (t.length < 3) return false;
  const low = t.toLowerCase();
  const junk = new Set([
    "add",
    "clarifies",
    "received",
    "give",
    "request",
    "internal",
    "two",
    "the",
    "a",
    "an",
    "and",
    "or",
    "but",
    "list",
    "keywords",
    "chatgpt",
    "conversation",
    "message",
    "content",
    "analyze",
    "analysis",
    "following",
    "previous",
    "sent",
    "updated",
    "confirming",
    "noting",
    "requested",
    "none",
    "n/a",
    "na",
  ]);
  if (junk.has(low)) return false;
  if (
    /^(add|clarif|receiv|give|request|internal|two|list|following|previous|sent|updated|confirming|noting|message)\b/i.test(
      t
    )
  ) {
    return false;
  }
  return true;
}

/**
 * True if this log entry should be excluded from PDF (any text layer is control/meta/filler).
 */
function entryIsExcludedFromReport(entry) {
  const layers = [
    lineText(entry),
    String(entry.rawText || "").trim(),
    String(entry.normalizedText || "").trim(),
    String(entry.summaryText || "").trim(),
  ].filter(Boolean);
  for (const layer of layers) {
    if (isMetaOrControlChatter(layer, entry)) return true;
  }
  return false;
}

/**
 * Defensive: entry.projectSlug must match report project (query should already enforce).
 */
function entryMatchesReportProject(entry, reportProjectSlug) {
  const want = normalizeProjectSlug(reportProjectSlug) || normSlug(reportProjectSlug);
  const slugRaw =
    entry.projectSlug != null && String(entry.projectSlug).trim() !== ""
      ? String(entry.projectSlug).trim()
      : null;
  const idRaw =
    entry.projectId != null && String(entry.projectId).trim() !== ""
      ? String(entry.projectId).trim()
      : null;
  const gotSlug = slugRaw ? normalizeProjectSlug(slugRaw) || slugRaw : null;
  const gotId = idRaw ? normalizeProjectSlug(idRaw) || idRaw : null;
  if (want) return gotSlug === want || gotId === want;
  const unassigned = !gotSlug && !gotId;
  return unassigned || gotSlug === "" || gotSlug === "_unassigned";
}

/**
 * Rescue path for legacy/mis-scoped rows: if text explicitly says "Project: <slug>",
 * allow it into that project's report even when projectSlug/projectId on the doc is empty.
 */
function entryExplicitProjectSlug(entry) {
  const layers = [
    String(entry?.rawText || ""),
    String(entry?.normalizedText || ""),
    String(entry?.summaryText || ""),
  ];
  const re = /\bproject\s*[:\-–—]?\s*([a-z0-9][a-z0-9-_]{1,79})\b/i;
  for (const layer of layers) {
    const m = layer.match(re);
    if (!m) continue;
    const slug = normalizeProjectSlug(m[1]);
    if (slug) return slug;
  }
  return "";
}

function entryHasNoProject(entry) {
  const slug = normalizeProjectSlug(String(entry?.projectSlug || "").trim());
  const pid = normalizeProjectSlug(String(entry?.projectId || "").trim());
  return !slug && !pid;
}

/**
 * @param {object[]} entries already passed filterEntriesForDailySummary
 * @param {string|null} reportProjectSlug
 */
function filterLogEntriesForProjectDailyReport(entries, reportProjectSlug) {
  const wantedProject = normalizeProjectSlug(reportProjectSlug);
  return (entries || []).filter((e) => {
    const explicitProject = entryExplicitProjectSlug(e);
    if (wantedProject && explicitProject && explicitProject !== wantedProject) return false;
    if (!entryMatchesReportProject(e, reportProjectSlug)) {
      const canRescue =
        wantedProject &&
        explicitProject === wantedProject &&
        entryHasNoProject(e);
      if (!canRescue) return false;
    }
    if (entryIsExcludedFromReport(e)) return false;
    return true;
  });
}

function mediaProjectMatches(m, reportProjectSlug) {
  const want = normalizeProjectSlug(reportProjectSlug) || normSlug(reportProjectSlug);
  const rawPid =
    m.projectId != null && String(m.projectId).trim() !== ""
      ? String(m.projectId).trim()
      : m.projectSlug != null && String(m.projectSlug).trim() !== ""
        ? String(m.projectSlug).trim()
        : "";
  const pid = rawPid ? normalizeProjectSlug(rawPid) || rawPid : "";
  const expected = want || "_unassigned";
  if (!want) {
    return pid === "" || pid === "_unassigned";
  }
  return pid === want;
}

function normSourceMessageId(id) {
  if (id == null) return "";
  const s = String(id).trim();
  return s;
}

/**
 * Unlinked MMS: projectId alone is not enough (mis-tagged photos). Require explicit inclusion,
 * tie to an included log entry's inbound message id, or a caption that reads like a field log line.
 */
function isUnlinkedMediaCaptionReportable(captionText) {
  const raw = String(captionText || "").trim();
  if (!raw) return false;
  if (isMetaOrControlChatter(raw, null)) return false;
  if (raw.length < 8) return false;

  const low = raw.replace(/\s+/g, " ").toLowerCase();
  if (
    /\b(kids|family\s+dinner|birthday|vacation|grocery|groceries|pet\s+photo)\b/i.test(low) &&
    !looksLikeFieldLogContext(raw)
  ) {
    return false;
  }

  const fieldish =
    /^(log|daily\s+log)\s+/i.test(raw) ||
    /\b(log\s*:|daily\s+log\s*:|safety\s*:|delay\s*:|deficiency|inspection\s*:|note\s*:|progress\s*:|delivery\s*:)\b/i.test(
      raw
    ) ||
    /\b(pour|concrete|rebar|slab|mud\s*mat|footing|wall|deck|crane|waterproof|excavat|trench|backfill)\b/i.test(
      low
    ) ||
    /\b(crew|foreman|manpower|headcount|gc|subcontractor|trade)\b/i.test(low) ||
    /\b(grid|GL\/|bay|zone|level|core|shaft)\b/i.test(low) ||
    /\d+\s*(cy|yards?|m3|m³|cfm|psi)\b/i.test(low) ||
    /^[A-Z]{1,3}[\d./\-]+/i.test(raw) ||
    /\b(drone|uav|aerial|orthophoto|site\s+photo|progress\s+photo)\b/i.test(low);

  return fieldish;
}

function unlinkedMediaPassesIntegrity(m, reportProjectSlug, allowedSourceMessageIds) {
  const msgIds =
    allowedSourceMessageIds instanceof Set
      ? allowedSourceMessageIds
      : new Set(allowedSourceMessageIds || []);

  if (m.includeInDailyReport === true) return true;

  const sid = normSourceMessageId(m.sourceMessageId);
  if (sid && msgIds.has(sid)) return true;

  if (mediaProjectMatches(m, reportProjectSlug) && isUnlinkedMediaCaptionReportable(m.captionText)) {
    return true;
  }

  return false;
}

/**
 * Drop media not for this project, linked outside the report entry set, or unlinked without a
 * strong inclusion signal (wrong project tag is not enough).
 * @param {object} [options]
 * @param {Set<string>|string[]} [options.allowedSourceMessageIds] — `sourceMessageId` values from final PDF log entries
 */
function filterMediaForProjectDailyReport(mediaDocs, reportProjectSlug, allowedEntryIds, options = {}) {
  const ids = allowedEntryIds instanceof Set ? allowedEntryIds : new Set(allowedEntryIds || []);
  const allowedSourceMessageIds = options.allowedSourceMessageIds ?? new Set();

  return (mediaDocs || []).filter((m) => {
    if (!m.storagePath) return false;
    if (!mediaContentTypeLooksImage(m.contentType)) return false;
    if (!mediaProjectMatches(m, reportProjectSlug)) return false;

    const linkId = m.linkedLogEntryId != null ? String(m.linkedLogEntryId).trim() : "";
    if (linkId) {
      if (!ids.has(linkId)) return false;
      return true;
    }

    return unlinkedMediaPassesIntegrity(m, reportProjectSlug, allowedSourceMessageIds);
  });
}

/**
 * Promote journal lines into section hints so real field notes surface outside generic "journal".
 */
function promoteFieldReportSections(entries) {
  return (entries || []).map((e) => {
    const text = String(e.summaryText || e.normalizedText || e.rawText || "");
    const low = text.toLowerCase();
    const cat = String(e.category || "journal").toLowerCase();
    const base = Array.isArray(e.dailySummarySections) && e.dailySummarySections.length
      ? [...e.dailySummarySections]
      : ["dayLog"];
    const secs = new Set(base);

    if (cat === "journal" || cat === "note") {
      if (
        /\b(high|low)\s*[:\-]?\s*-?\d|\d+\s*°\s*[cf]\b|weather\s*:|forecast|precip|humidity/i.test(
          text
        )
      ) {
        secs.add("weather");
      }
      if (
        /\b(roofing|waterproof|blindside|excavat|sacrificial|backfill|trench|rebar|pier\b|piers|water\s*rain|rainwater|o['\u2019]connor|temp\s+power|electrical\s+rough|coreydale|installing)\b/i.test(
          low
        )
      ) {
        secs.add("workInProgress");
      }
      if (/\b(pour|concrete\s+slab|mud\s*mat|placement|ready\s*mix)\b/i.test(low)) {
        secs.add("concrete");
      }
    }

    return { ...e, dailySummarySections: [...secs] };
  });
}

/**
 * Debug / audit: one row per loaded log entry explaining report inclusion.
 * @param {object[]} rawLoaded from loadLogEntriesForDayForProject
 * @param {object[]} reportEntries after full pipeline
 * @param {string|null} reportProjectSlug
 */
function auditDailyReportEntries(rawLoaded, reportEntries, reportProjectSlug) {
  const included = new Set((reportEntries || []).map((e) => e.id));

  return (rawLoaded || []).map((e) => {
    const txt = lineText(e);
    const afterSummary = filterEntriesForDailySummary([e]).length > 0;
    const excluded = entryIsExcludedFromReport(e);
    const projectOk = entryMatchesReportProject(e, reportProjectSlug);
    const inReport = included.has(e.id);
    let reason = "included";
    if (!afterSummary) reason = "excluded: includeInDailySummary false";
    else if (!projectOk) reason = "excluded: projectSlug mismatch";
    else if (excluded) reason = "excluded: meta/control/filler";
    else if (!inReport) reason = "excluded: unknown";

    return {
      id: e.id,
      projectSlug: e.projectSlug ?? null,
      includeInDailySummary: e.includeInDailySummary !== false,
      isMeta: excluded,
      isReportable: afterSummary && projectOk && !excluded,
      includedInPdf: inReport,
      reason,
      sections: e.dailySummarySections || [],
      preview: txt.slice(0, 120),
    };
  });
}

module.exports = {
  lineText,
  isMetaOrControlChatter,
  isCuratedNonFieldChatter,
  curateFieldEntriesForDailyReport,
  isValidTradeHeading,
  entryIsExcludedFromReport,
  filterLogEntriesForProjectDailyReport,
  filterMediaForProjectDailyReport,
  promoteFieldReportSections,
  auditDailyReportEntries,
  mediaProjectMatches,
  entryMatchesReportProject,
};
