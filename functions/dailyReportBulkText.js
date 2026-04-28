/**
 * Strip pasted "bulk update" wrappers and contradictory embedded calendar dates from report copy.
 * Used for PDF appendix, AI bundle, and trade inference — not for raw Firestore storage.
 */

const { dateKeyEastern } = require("./logClassifier");

/**
 * Remove ingestion / command lines often pasted above field facts.
 */
function stripBulkUpdateSourcePhrasing(text) {
  let s = String(text || "").replace(/\r\n/g, "\n");
  const lines = s.split("\n");
  const kept = [];
  for (const line of lines) {
    const L = line.trim();
    if (!L) {
      kept.push(line);
      continue;
    }
    if (/^add\s+the\s+below\s+updates?\b/i.test(L)) continue;
    if (/^add\s+the\s+following\b/i.test(L)) continue;
    if (/^please\s+(add|include|post)\s+(the\s+)?(below|following)\s+updates?\b/i.test(L)) {
      continue;
    }
    if (
      /^project\s+[A-Za-z0-9\-_]+\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/i.test(
        L
      )
    ) {
      continue;
    }
    if (/^project\s+[A-Za-z0-9\-_]+\s+\d{1,2}\s+[A-Za-z]+\s+\d{4}/i.test(L)) continue;
    kept.push(line);
  }
  return kept.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

/**
 * Remove weekday/month date phrases that refer to a different calendar day than the report.
 */
function stripConflictingEmbeddedDates(text, reportDateKey) {
  if (!reportDateKey || !text) return String(text || "");
  let t = String(text);
  const reWeekday =
    /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})\b/gi;
  t = t.replace(reWeekday, (full, mon, day, year) => {
    const d = new Date(`${mon} ${day}, ${year}`);
    if (Number.isNaN(d.getTime())) return full;
    return dateKeyEastern(d) === reportDateKey ? full : "";
  });
  const reMon =
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})\b/gi;
  t = t.replace(reMon, (full, mon, day, year) => {
    const d = new Date(`${mon} ${day}, ${year}`);
    if (Number.isNaN(d.getTime())) return full;
    return dateKeyEastern(d) === reportDateKey ? full : "";
  });
  return t.replace(/[ \t]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").trim();
}

function normalizeReportLineText(raw, reportDateKey) {
  let t = stripBulkUpdateSourcePhrasing(raw);
  if (reportDateKey) t = stripConflictingEmbeddedDates(t, reportDateKey);
  return t.replace(/[ \t]+/g, " ").replace(/\n{3,}/g, "\n\n").trim();
}

module.exports = {
  stripBulkUpdateSourcePhrasing,
  stripConflictingEmbeddedDates,
  normalizeReportLineText,
};
