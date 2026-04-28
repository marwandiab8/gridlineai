/**
 * Deterministic construction daily report model from logEntries + media.
 * Structured tables, parsing, and photo anchoring for PDF layout.
 */

const {
  filterEntriesForDailySummary,
  sanitizeAiManpowerRows,
} = require("./logEntryRepository");
const {
  DAILY_REPORT_TIME_ZONE,
  dateKeyEastern,
  addCalendarDaysToDateKey,
  fmtMonDayEasternDateKey,
  weekdayShortEasternDateKey,
} = require("./logClassifier");
const {
  parseManpowerRollcallLine,
  textContainsManpowerRollcall,
  tailAfterManpowerRollcall,
  escapeRegExp,
} = require("./manpowerRollcall");
const {
  entryIsExcludedFromReport,
  isValidTradeHeading,
} = require("./dailyReportIntegrity");
const { normalizeReportLineText } = require("./dailyReportBulkText");

function lineText(e) {
  return (e.summaryText || e.normalizedText || e.rawText || "").trim();
}

function isPhoneLikeAuthorLabel(value) {
  const raw = String(value || "").trim();
  const digits = raw.replace(/\D/g, "");
  return digits.length >= 10 && /^[+\d\s().-]+$/.test(raw);
}

function authorIdentityFromPhone(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length >= 10 ? `phone:${digits}` : "";
}

function entryAuthorIdentity(e) {
  if (!e) return "";
  const email = String(e.authorEmail || "").trim().toLowerCase();
  if (email) return `email:${email}`;
  const phoneIdentity = authorIdentityFromPhone(e.authorPhone || e.senderPhone || e.phoneE164);
  if (phoneIdentity) return phoneIdentity;
  const label = String(e.authorLabel || e.authorName || "").trim().toLowerCase();
  return label ? `label:${label}` : "";
}

function entryAuthorLabel(e, authorLabelsByIdentity = null) {
  const identity = entryAuthorIdentity(e);
  if (authorLabelsByIdentity && identity && authorLabelsByIdentity.has(identity)) {
    return authorLabelsByIdentity.get(identity);
  }
  const value = String(
    (e && (e.authorLabel || e.authorName || e.authorEmail || e.authorPhone || e.senderPhone)) || ""
  ).trim();
  return value || "Unknown sender";
}

function preferAuthorLabel(existing, next) {
  const current = String(existing || "").trim();
  const candidate = String(next || "").trim();
  if (!current || current === "Unknown sender") return candidate || current;
  if (!candidate || candidate === "Unknown sender") return current;
  if (isPhoneLikeAuthorLabel(current) && !isPhoneLikeAuthorLabel(candidate)) return candidate;
  return current;
}

function buildAuthorLabelContext(entries, initialLabels = null) {
  const labelsByIdentity = new Map(initialLabels || []);
  for (const entry of entries || []) {
    const identity = entryAuthorIdentity(entry);
    if (!identity) continue;
    const current = labelsByIdentity.get(identity);
    labelsByIdentity.set(identity, preferAuthorLabel(current, entryAuthorLabel(entry)));
  }
  return labelsByIdentity;
}

function journalContributors(entries, authorLabelsByIdentity = null) {
  const labelsByIdentity = buildAuthorLabelContext(entries || [], authorLabelsByIdentity);
  const contributors = [];
  const seen = new Set();
  for (const entry of entries || []) {
    const identity = entryAuthorIdentity(entry) || `entry:${contributors.length}`;
    if (seen.has(identity)) continue;
    seen.add(identity);
    contributors.push({
      identity,
      label: entryAuthorLabel(entry, labelsByIdentity),
    });
  }
  return contributors;
}

function journalLineWithOptionalAuthor(entry, reportDateKey, includeAuthor, authorLabelsByIdentity) {
  const text = reportDateKey ? reportLineText(entry, reportDateKey) : lineText(entry);
  if (!includeAuthor || !text) return text;
  const author = entryAuthorLabel(entry, authorLabelsByIdentity);
  return `${author}: ${text}`;
}

/** Display text for PDF / AI â€” strips bulk-ingestion lines and conflicting embedded dates. */
function reportLineText(e, reportDateKey) {
  const raw = lineText(e);
  if (!reportDateKey) return stripReportFiller(normalizeReportLineText(raw, null));
  return stripReportFiller(normalizeReportLineText(raw, reportDateKey));
}

function fmtTimeShort(ts) {
  if (!ts) return "";
  try {
    let d;
    if (ts.toDate) d = ts.toDate();
    else if (ts.seconds) d = new Date(ts.seconds * 1000);
    else return "";
    return (
      new Intl.DateTimeFormat("en-US", {
        timeZone: DAILY_REPORT_TIME_ZONE,
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      }).format(d) + " ET"
    );
  } catch (_) {
    return "";
  }
}

/** Always 7 rows: week starting Eastern calendar day of `dayStart` (best-effort horizon). */
function buildFallbackWeatherWeekRows(dayStart) {
  const d0 = dayStart instanceof Date ? dayStart : new Date(dayStart);
  const key0 = dateKeyEastern(d0);
  const rows = [];
  for (let i = 0; i < 7; i++) {
    const key = addCalendarDaysToDateKey(key0, i);
    rows.push([
      fmtMonDayEasternDateKey(key),
      weekdayShortEasternDateKey(key),
      "â€”",
      "â€”",
      "â€”",
      "Not stated in log entries.",
    ]);
  }
  return rows;
}

/**
 * Parse AI or pasted forecast into 6-col rows. Pipes preferred; else one row per line into Notes.
 */
function parseWeatherWeeklyTextToRows(text, dayStart) {
  const raw = String(text || "").trim();
  if (!raw || /^not stated/i.test(raw)) return null;
  const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const rows = [];
  for (const line of lines) {
    if (line.includes("|")) {
      const p = line.split("|").map((c) => c.trim());
      while (p.length < 6) p.push("â€”");
      rows.push(p.slice(0, 6));
    } else {
      rows.push(["â€”", "â€”", "â€”", "â€”", "â€”", sanitizeCell(line).slice(0, 120)]);
    }
  }
  return rows.length ? rows : null;
}

function sanitizeCell(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

/** Remove placeholder / control phrases that should never appear in PDF copy. */
function stripReportFiller(s) {
  let t = String(s || "").trim();
  if (!t) return t;
  t = t.replace(/\bno\s+details?\s+provided\b/gi, "").trim();
  t = t.replace(/\bno\s+field\s+updates?\s+provided\b/gi, "").trim();
  t = t.replace(/\bno\s+field\s+updates?\b/gi, "").trim();
  t = t.replace(/\bno\s+message\s+content\s+provided\b/gi, "").trim();
  t = t.replace(/\bno\s+content\s+provided\s+to\s+analyze\b/gi, "").trim();
  t = t.replace(/\brequested\s+item\s+to\s+be\s+included\s+in\s+the\s+daily\s+summary\b/gi, "").trim();
  t = t.replace(/\brequest(?:ing)?\s+(?:a\s+)?plan\s+for\s+tomorrow\b/gi, "").trim();
  t = t.replace(/\basks?\s+for\s+tomorrow'?s?\s+weather\s+forecast\b/gi, "").trim();
  t = t.replace(/\bunable\s+to\s+retrieve\s+live\s+weather\s+data\b/gi, "").trim();
  return t.replace(/\s+/g, " ").trim();
}

/**
 * Ordered, numbered bundle for the daily-report AI â€” field messages only (caller should pre-filter).
 */
function formatReportBundleForAi(entries, reportDateKey) {
  const list = [...(entries || [])]
    .filter((e) => !entryIsExcludedFromReport(e))
    .sort((a, b) => entryTimeMs(a) - entryTimeMs(b));
  return list
    .map((e, i) => {
      const body = reportDateKey ? reportLineText(e, reportDateKey) : lineText(e);
      const secs = (e.dailySummarySections || ["dayLog"]).join(",");
      const tm = fmtTimeShort(e.createdAt);
      return `[#${i + 1}] ${tm} [category=${e.category || "journal"}; sections=${secs}] ${body}`;
    })
    .join("\n")
    .slice(0, 10_000);
}

const BAD_TRADE_TOKEN =
  /^(WE|THE|TODAY|THERE|HERE|SITE|TEAM|CREW|WEATHER|NOTE|POUR|CONCRETE|MANPOWER|WORK|LOG)\b/i;

function isPlausibleTradeName(s) {
  const t = sanitizeCell(s);
  if (t.length < 2) return false;
  if (BAD_TRADE_TOKEN.test(t)) return false;
  if (/^(A|AN|I|IT|AS|AT|IN|ON|TO|OF|OR|IF|BE|SO|NO|UP|US|DO|GO|AM|PM)\b/i.test(t)) return false;
  return true;
}

function titleCaseTrade(s) {
  const t = sanitizeCell(s);
  if (!t) return t;
  return t
    .split(/\s+/)
    .map((w) => {
      if (/^[A-Z]{2,}$/.test(w)) return w;
      if (/[a-z]+-[A-Z]/.test(w) || /[A-Z]+-[a-z]/.test(w)) return w;
      return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    })
    .join(" ");
}

/** Parse WEATHER_WEEKLY_TABLE block (pipe rows, 6 cols). */
function parseWeatherWeeklyTableBlock(block) {
  return parsePipeRows(block, 6);
}

function parsePipeRows(block, minCols) {
  const lines = String(block || "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);
  const rows = [];
  for (const line of lines) {
    const low = line.toLowerCase();
    if (low === "none" || low.startsWith("not stated")) continue;
    const parts = line.includes("|")
      ? line.split("|").map((c) => c.trim())
      : line.split(/\t+/).map((c) => c.trim());
    if (parts.length >= minCols) rows.push(parts);
  }
  return rows;
}

function mergeWeatherRows(fallback7, parsed, aiRows) {
  const pad = (r) => padRow6(r);
  if (aiRows && aiRows.length >= 1) {
    const m = aiRows.map(pad);
    for (let i = m.length; i < 7; i++) m.push(fallback7[i] || fallback7[0]);
    return m.slice(0, 7);
  }
  if (parsed && parsed.length) {
    const m = parsed.map(pad);
    for (let i = 0; i < 7; i++) {
      if (!m[i]) m[i] = fallback7[i];
    }
    return m.slice(0, 7);
  }
  return fallback7;
}

function padRow6(r) {
  const x = [...r];
  while (x.length < 6) x.push("â€”");
  return x.slice(0, 6);
}

/**
 * Trade / Foreman / Workers / Notes â€” field-style SMS patterns.
 * Examples: "ALC (Foreman: Ramzi Azar) 7 workers", "Road-Ex â€“ foreman Joe â€“ 4 men", "Englobe on site, 2 techs"
 */
function manpowerNotesColumn(raw, trade, foreman, workers) {
  let n = sanitizeCell(raw);
  if (!n) return "â€”";
  if (trade !== "â€”" && n.toLowerCase().startsWith(trade.toLowerCase())) {
    n = n.slice(trade.length).replace(/^[\s\-â€“â€”:,]+/, "").trim();
  }
  if (foreman !== "â€”") {
    const esc = foreman.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    n = n.replace(new RegExp(`\\(\\s*Foreman\\s*:\\s*${esc}\\s*\\)`, "i"), " ").trim();
    n = n.replace(new RegExp(`\\b[Ff]oreman\\s*:?\\s*${esc}\\b`, "i"), " ").trim();
  }
  n = sanitizeCell(n.replace(/\s+/g, " "));
  if (n.length < 4) return "â€”";
  return n.slice(0, 200);
}

function parseManpowerFields(text) {
  const t = String(text || "").trim();
  let trade = "â€”";
  let foreman = "â€”";
  let workers = "â€”";

  let m = t.match(/^\s*([A-Z][A-Za-z0-9 &./\-]{1,30}?)\s*\(\s*Foreman\s*:\s*([^)]+)\)/i);
  if (m) {
    trade = sanitizeCell(m[1]).slice(0, 32);
    foreman = sanitizeCell(m[2]).slice(0, 44);
    const wAfter = t.match(/\)\s*(\d+)\s*(?:workers?|men|crew|people|heads?|bodies|guys?|pax|ppl)/i);
    if (wAfter) workers = String(wAfter[1]);
  }
  if (trade === "â€”") {
    m = t.match(
      /^\s*([A-Za-z][A-Za-z0-9 &./\-]{1,32}?)\s*[|]\s*[Ff]oreman:?\s*([^|]+?)\s*[|]\s*(\d+)/i
    );
    if (m) {
      trade = sanitizeCell(m[1]).slice(0, 32);
      foreman = sanitizeCell(m[2]).slice(0, 44);
      workers = String(m[3]);
    }
  }
  if (trade === "â€”") {
    m = t.match(
      /^\s*([A-Za-z][A-Za-z0-9 &./\-]{1,32}?)\s*[â€“â€”\-]\s*foreman\s+([^â€“â€”|]+?)\s*[â€“â€”\-]\s*(\d+)\s*(?:men|workers?|people|crew|guys?)/i
    );
    if (m) {
      trade = sanitizeCell(m[1]).slice(0, 32);
      foreman = sanitizeCell(m[2]).slice(0, 44);
      workers = String(m[3]);
    }
  }
  if (trade === "â€”") {
    m = t.match(
      /^\s*([A-Za-z][A-Za-z0-9 &./\-]{1,28}?)\s+on\s+site,?\s*(\d+)\s*(?:techs?|workers?|people|staff|guys?|pax)/i
    );
    if (m) {
      trade = sanitizeCell(m[1]).slice(0, 32);
      workers = String(m[2]);
    }
  }
  if (trade === "â€”") {
    m = t.match(
      /^\s*([A-Za-z][A-Za-z0-9 &./\-]{1,30}?)\s*[-â€“â€”]\s*(\d+)\s*(?:men|workers?|guys?|heads?|pax|ppl|bodies)\b/i
    );
    if (m) {
      trade = sanitizeCell(m[1]).slice(0, 32);
      workers = String(m[2]);
    }
  }
  if (workers === "â€”") {
    m = t.match(/\bcrew\s+(?:of\s+)?(\d+)\b/i) || t.match(/\b(?:total|ttl)\s*:?\s*(\d+)\b/i);
    if (m) workers = String(m[1]);
  }
  if (foreman === "â€”") {
    m =
      t.match(/\(\s*Foreman\s*:\s*([^)]+)\)/i) ||
      t.match(/\b[Ff]oreman\s*:\s*([^|â€“â€”,;\n]+)/i) ||
      t.match(/\b[Ss]up(?:ervisor)?\s*:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b/) ||
      t.match(/\b[Ll]ead\s*:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b/);
    if (m) foreman = sanitizeCell(m[1]).slice(0, 44);
  }
  if (foreman === "â€”") {
    m =
      t.match(/\bforeman\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b(?=\s|[-â€“â€”,;]|$)/i) ||
      t.match(/[â€“â€”\-]\s*foreman\s+([^â€“â€”|,;\n]+)/i);
    if (m) foreman = sanitizeCell(m[1]).slice(0, 44);
  }
  if (workers === "â€”") {
    m =
      t.match(/(\d+)\s*(?:workers?|crew|men|people|techs?|heads?|bodies|staff|guys?|pax|ppl)/i) ||
      t.match(/headcount:?\s*(\d+)/i) ||
      t.match(/\b(\d+)\s*(?:on\s*site|pob|in\s+field)\b/i) ||
      t.match(/\b(\d+)\s+(?:men|women)\b/i);
    if (m) workers = String(m[1]);
  }
  if (trade === "â€”") {
    m = t.match(/^([A-Z][A-Za-z0-9 &./\-]{1,28})\s*[\(:]/);
    if (m) trade = sanitizeCell(m[1]).slice(0, 32);
  }
  if (trade === "â€”") {
    m = t.match(/^([A-Z][a-z]+(?:-[A-Z][a-z]+)?)\b/);
    if (m) trade = m[1];
  }

  if (trade !== "â€”" && !isPlausibleTradeName(trade)) trade = "â€”";
  const notes = manpowerNotesColumn(t, trade, foreman, workers);

  return { trade, foreman, workers, notes };
}

function isSkippableManpowerContextLine(line) {
  const t = String(line || "").trim();
  if (!t || t.length < 2) return true;
  if (/^manpower\b/i.test(t) && t.length < 55) return true;
  if (
    /^project\s+/i.test(t) &&
    /\b20\d{2}\b/.test(t) &&
    t.length < 160 &&
    !parseManpowerRollcallLine(t)
  ) {
    return true;
  }
  return false;
}

function splitNarrativeTailChunks(tail) {
  if (!tail) return [];
  const s = tail.trim();
  if (!s) return [];
  const bySentence = s.split(/\.\s+(?=[A-Z])/).map((c) => c.trim()).filter(Boolean);
  if (bySentence.length > 1) return bySentence;
  return [s];
}

function attachNarrativesToRollcallRows(rollRows, narrativeLines) {
  for (const nl of narrativeLines) {
    const t = nl.trim();
    if (!t || isSkippableManpowerContextLine(t)) continue;
    let matched = false;
    for (const r of rollRows) {
      const re = new RegExp(`^${escapeRegExp(r.trade)}\\b`, "i");
      if (re.test(t)) {
        r.notes =
          r.notes === "â€”"
            ? t
            : `${r.notes} ${t}`.replace(/\s+/g, " ").trim().slice(0, 500);
        matched = true;
        break;
      }
    }
    if (!matched && rollRows.length) {
      const last = rollRows[rollRows.length - 1];
      last.notes =
        last.notes === "â€”"
          ? t
          : `${last.notes} ${t}`.replace(/\s+/g, " ").trim().slice(0, 500);
    }
  }
}

/**
 * Multi-trade headcount line(s) + per-sub narratives (same entry).
 * @returns {string[][] | null}
 */
function buildManpowerRollcallTableRows(txt) {
  const raw = String(txt || "");
  const lines = raw
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  const rollRows = [];
  const narrativeLines = [];

  for (const line of lines) {
    const pairs = parseManpowerRollcallLine(line);
    if (pairs && pairs.length >= 2) {
      for (const p of pairs) {
        rollRows.push({ trade: p.trade, workers: p.workers, notes: "â€”" });
      }
    } else {
      narrativeLines.push(line);
    }
  }

  if (rollRows.length >= 2) {
    attachNarrativesToRollcallRows(rollRows, narrativeLines);
    return rollRows.map((r) => [
      r.trade.slice(0, 30),
      "â€”",
      r.workers,
      (r.notes === "â€”" ? "â€”" : r.notes).slice(0, 200),
    ]);
  }

  const flat = raw.replace(/\s+/g, " ").trim();
  const flatPairs = parseManpowerRollcallLine(flat);
  if (flatPairs && flatPairs.length >= 2) {
    const rollRows2 = flatPairs.map((p) => ({
      trade: p.trade,
      workers: p.workers,
      notes: "â€”",
    }));
    const fromLines = lines.filter((ln) => !parseManpowerRollcallLine(ln));
    const tail = tailAfterManpowerRollcall(flat);
    const fromTail = splitNarrativeTailChunks(tail);
    const merged = [];
    const seen = new Set();
    for (const x of [...fromLines, ...fromTail]) {
      const k = x.trim();
      if (!k || seen.has(k)) continue;
      seen.add(k);
      merged.push(x);
    }
    attachNarrativesToRollcallRows(rollRows2, merged);
    return rollRows2.map((r) => [
      r.trade.slice(0, 30),
      "â€”",
      r.workers,
      (r.notes === "â€”" ? "â€”" : r.notes).slice(0, 200),
    ]);
  }

  return null;
}

function buildManpowerTableRows(entries, reportDateKey) {
  const touched = entries.filter((e) => {
    const secs = e.dailySummarySections || [];
    const aiMp =
      e.aiReportExtract &&
      Array.isArray(e.aiReportExtract.manpowerRows) &&
      e.aiReportExtract.manpowerRows.length > 0;
    return (
      aiMp ||
      secs.includes("manpower") ||
      /\b(crew|workers|labou?r|manpower|headcount|foreman|on\s*site)\b/i.test(lineText(e)) ||
      textContainsManpowerRollcall(lineText(e))
    );
  });
  if (!touched.length) {
    return [["â€”", "â€”", "â€”", "Not stated in log entries for this report day."]];
  }
  const rows = [];
  for (const e of touched) {
    if (e.aiReportExtract) {
      const aiRows = sanitizeAiManpowerRows(e.aiReportExtract.manpowerRows);
      if (aiRows && aiRows.length) {
        for (const row of aiRows) {
          const trade = String((row || [])[0] || "").trim();
          if (!trade || isGenericWorkTradeLabel(trade)) continue;
          rows.push(row);
        }
        continue;
      }
    }
    const txt = lineText(e);
    const roll = buildManpowerRollcallTableRows(txt);
    if (roll && roll.length) {
      for (const row of roll) rows.push(row);
      continue;
    }
    const p = parseManpowerFields(txt);
    const fallbackTrade =
      inferNamedContractorFromLine(txt) ||
      inferCommonTradeFromLine(txt) ||
      inferWorkTradeName(txt, e.category, reportDateKey);
    const trade =
      p.trade !== "â€”"
        ? p.trade
        : fallbackTrade && !isGenericWorkTradeLabel(fallbackTrade)
          ? fallbackTrade
          : "â€”";
    if (trade === "â€”" && p.foreman === "â€”" && p.workers === "â€”" && p.notes === "â€”") continue;
    rows.push([trade.slice(0, 30), p.foreman, p.workers, p.notes.slice(0, 200)]);
  }
  return rows.length ? rows : [["â€”", "â€”", "â€”", "Not stated in log entries for this report day."]];
}

const LOC_HINT =
  /\b(?:GL[/\s]|grid|bay|zone|elevator|core|pour|slab|mud\s*mat|footing|wall|level|deck|tank|pit|mat|scope|area|tower|wing|phase)\b/i;
const STATUS_WORDS =
  /\b(?:cancel(?:led|ed)|complete|completed|poured|placed|scheduled|delayed|resched(?:uled)?|postponed|tbd|on\s*hold|wip|in\s*progress|pending|finished|done|tentative|no\s*pour|scrubbed)\b/i;

function extractConcreteVolume(t) {
  const s = String(t || "");
  let m = s.match(
    /(?:approx\.?|~|about|abt\.?)\s*(\d+(?:\.\d+)?)\s*(m3|mÂ³|cu\.?\s*m|cubic\s*m(?:eters?)?|cy|yards?|yds?|yd)\b/i
  );
  if (m) return `${m[1]} ${m[2]}`.replace(/\s+/g, " ").trim();
  m = s.match(
    /(\d+(?:\.\d+)?)\s*(m3|mÂ³|cu\.?\s*m|cubic\s*m(?:eters?)?|cy|yards?|yds?|yd)\b/i
  );
  if (m) return `${m[1]} ${m[2]}`.replace(/\s+/g, " ").trim();
  return null;
}

function extractConcreteLocation(t) {
  let x = String(t || "");
  x = x.replace(/^(?:concrete|pour|placement|rmc|ready\s*mix)\s*[:\-]?\s*/i, "").trim();
  const mScope = x.match(
    /\b(?:for|re:)\s+([^.\n;]{4,90})/i
  );
  if (mScope) return sanitizeCell(mScope[1]);
  const mLoc =
    x.match(
      /(?:pour\s+at|placed\s+at|at|@|location|scope|zone|bay|grid|level|area)\s*:?\s*([^.\n;]{2,100})/i
    ) || x.match(/\b(GL[/\s][^\s,.;]{1,44})/i);
  if (mLoc) return sanitizeCell(mLoc[1]);
  if (LOC_HINT.test(x)) return sanitizeCell(x.slice(0, 100));
  return sanitizeCell(x.slice(0, 100));
}

function parseConcreteFields(text) {
  const t = String(text || "");
  const volRaw = extractConcreteVolume(t);
  let status = "Recorded";
  const sm = t.match(STATUS_WORDS);
  if (sm) {
    const raw = sm[0];
    const low = raw.toLowerCase();
    if (/\b(wip|in\s*progress)\b/i.test(low)) status = "In progress";
    else if (/\b(complete|completed|finished|done|poured|placed)\b/i.test(low)) status = "Complete / placed";
    else if (/\b(no\s*pour|scrubbed|cancel)\b/i.test(low)) status = "Cancelled / no pour";
    else if (/\b(delay|postpon|resched|hold|tbd|tentative|pending)\b/i.test(low))
      status = raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase();
    else status = raw.slice(0, 28);
  }
  let loc = extractConcreteLocation(t);
  if (!loc || loc.length < 3) loc = sanitizeCell(t.slice(0, 100));
  return {
    location: loc.slice(0, 120),
    volume: volRaw || "â€”",
    status: status.slice(0, 28),
  };
}

function buildConcreteTableRows(entries, _reportDateKey) {
  const concrete = entries.filter(
    (e) =>
      (e.dailySummarySections || []).includes("concrete") ||
      /\b(pour|concrete|m3|mÂ³|mud\s*mat|slab|ready\s*mix|placement)\b/i.test(lineText(e))
  );
  if (!concrete.length) {
    return [["â€”", "â€”", "Not stated in log entries."]];
  }
  return concrete.slice(0, 16).map((e) => {
    const p = parseConcreteFields(lineText(e));
    return [p.location, p.volume, p.status];
  });
}

function normalizeOpenStatus(word) {
  const w = String(word || "").trim().toLowerCase();
  if (!w) return "Open";
  if (/^(pending|awaiting|hold)$/.test(w)) return "Pending";
  if (/^(closed|resolved|done|complete|completed)$/.test(w)) return "Closed";
  if (/^(in\s*progress|ongoing|active|wip|monitoring)$/.test(w)) return "In progress";
  if (/^open$/.test(w)) return "Open";
  const raw = String(word).trim();
  return raw ? raw.charAt(0).toUpperCase() + raw.slice(1) : "Open";
}

function textLooksWeatherOnly(text) {
  const t = String(text || "").replace(/\s+/g, " ").trim();
  if (!t) return false;
  const hasWeatherSignal =
    /\b(weather|forecast|temperature|temp\b|high\b|low\b|humidity|humid|wind|gust|rain|snow|sleet|precip|sunny|cloudy|clear skies?|overcast|storm|thunder|fog|frost|freezing|drizzle)\b/i.test(
      t
    ) || /\b-?\d+\s*°\s*[cf]\b/i.test(t);
  if (!hasWeatherSignal) return false;
  const hasOperationalSignal =
    /\b(crew|workers?|manpower|foreman|installed?|installing|completed?|continu(?:ed|ing)|started?|progress|working|formed?|formwork|rebar|pour|concrete|slab|footing|deficiency|issue|inspection|consultant|delivery|truck|material|membrane|waterproof|excavat|backfill|crane|gc\b|subcontractor)\b/i.test(
      t
    );
  return !hasOperationalSignal;
}

function entryHasOpenItemSignals(entry, text) {
  if (!entry) return false;
  if (entry.openItem === true) return true;
  if (entry.assignedTo || entry.dueDate || entry.status) return true;
  const t = String(text || "").replace(/\s+/g, " ").trim();
  if (!t) return false;
  if (
    /\b(open\s+item|action\s+item|required\s+action|follow[\s-]?up|needs?\s+(?:to\s+be\s+)?(?:fixed|completed|resolved|addressed)|responsible|assigned(?:\s+to)?|owner:|status:|due\b|target\s+date|by\s+friday|by\s+monday|by\s+tuesday|by\s+wednesday|by\s+thursday|by\s+saturday|by\s+sunday)\b/i.test(
      t
    )
  ) {
    return true;
  }
  if (/\b(open|pending|awaiting|in\s*progress|on\s*hold|monitoring|resolved|closed)\b/i.test(t)) {
    return true;
  }
  return false;
}

function cleanOpenItemBody(text) {
  let s = String(text || "");
  s = s.replace(/\bowner:?\s*[^,\n|]+/gi, " ");
  s = s.replace(/\bassigned(?:\s+to)?\s*:?\s*[^,\n|]+/gi, " ");
  s = s.replace(/\breported\s+by\s+[^,\n|]+/gi, " ");
  s = s.replace(/\bby:\s*[^,\n|]+/gi, " ");
  s = s.replace(/\bstatus:?\s*[^,\n|]+/gi, " ");
  s = s.replace(/\b(?:due|target|needed\s*by)\s*:?\s*[^,\n]+/gi, " ");
  s = s.replace(/\s+/g, " ").trim();
  return s.slice(0, 240);
}

function parseOwnerStatus(text, entry) {
  const t = String(text || "");
  let owner = "â€”";
  const om =
    t.match(/\bowner:?\s*([^,\n|]+)/i) ||
    t.match(/\bassigned(?:\s+to)?\s*:?\s*([^,\n|]+)/i) ||
    t.match(/\breported\s+by\s+([^,\n|]+)/i) ||
    t.match(/\bby:\s*([^,\n|]+)/i) ||
    t.match(/\b(?:pm|super|gc|responsible|action\s*by)\s*:?\s*([^,\n|]+)/i);
  if (om) owner = sanitizeCell(om[1]).replace(/\b(pending|open|closed)\b/gi, "").trim().slice(0, 40);
  if (entry.assignedTo) owner = String(entry.assignedTo).slice(0, 40);
  let status = normalizeOpenStatus(entry.status || "open");
  const sm =
    t.match(/\b(pending|awaiting|closed|in\s*progress|resolved|open|complete|done|on\s*hold|monitoring)\b/i) ||
    t.match(/\b(wip|ongoing|active)\b/i);
  if (sm) status = normalizeOpenStatus(sm[0]);
  return { owner, status };
}

function buildOpenItemRows(entries, reportDateKey) {
  const rows = [];
  let n = 0;
  for (const e of entries) {
    const text = reportDateKey ? reportLineText(e, reportDateKey) : lineText(e);
    if (!text) continue;
    if (!entryHasOpenItemSignals(e, text)) continue;
    n++;
    const { owner, status } = parseOwnerStatus(text, e);
    const body = cleanOpenItemBody(text) || text.slice(0, 240);
    rows.push([String(n), body, owner, status]);
  }
  return rows;
}

function placementBucketForEntry(e) {
  const cat = String(e.category || "journal").toLowerCase();
  const secs = Array.isArray(e.dailySummarySections) ? e.dailySummarySections : [];
  const order = [
    "weather",
    "manpower",
    "workCompleted",
    "workInProgress",
    "delays",
    "deficiencies",
    "issues",
    "safety",
    "inspections",
    "deliveries",
    "concrete",
    "openItems",
    "photos",
    "notes",
    "journal",
  ];
  for (const k of order) {
    if (secs.includes(k)) return k;
  }
  const catMap = {
    safety: "safety",
    delay: "delays",
    deficiency: "deficiencies",
    issue: "issues",
    delivery: "deliveries",
    inspection: "inspections",
    progress: "workInProgress",
    note: "notes",
    journal: "journal",
  };
  return catMap[cat] || "workInProgress";
}

function placementBucketForMedia(m, entryById) {
  const lid = m.linkedLogEntryId != null ? String(m.linkedLogEntryId).trim() : "";
  if (lid && entryById.has(lid)) {
    return placementBucketForEntry(entryById.get(lid));
  }
  return "photos";
}

function stripLeadBullet(s) {
  return String(s || "")
    .replace(/^\s*(?:[-*â€¢]\s+|\d+[\.)]\s+)/, "")
    .trim();
}

/** Prefer named subs when they appear anywhere in the line (not only the first token). */
function inferNamedContractorFromLine(t) {
  if (!t) return null;
  const patterns = [
    [/\b(ALC)\b/i, "ALC"],
    [/\b(Road-Ex|Road\s*Ex)\b/i, "Road-Ex"],
    [/\b(Coreydale)\b/i, "Coreydale"],
    [/\b(O[''\u2019]Connor)\b/i, "O'Connor"],
    [/\b(SteelCon)\b/i, "SteelCon"],
  ];
  for (const [re, label] of patterns) {
    if (re.test(t)) return label;
  }
  return null;
}

const COMMON_TRADE_PATTERNS = [
  [/\b(formwork|forms?)\b/i, "Formwork"],
  [/\b(rebar|reinforcing|ironworkers?)\b/i, "Reinforcing"],
  [/\b(concrete|ready\s*mix|rmc|pour)\b/i, "Concrete"],
  [/\b(excavat(?:ion|ing|or)?|earthworks?|grading|backfill|trench(?:ing)?)\b/i, "Earthworks"],
  [/\b(waterproof(?:ing)?|damp\s*proof(?:ing)?)\b/i, "Waterproofing"],
  [/\b(masonry|blockwork|brick(?:work|laying)?)\b/i, "Masonry"],
  [/\b(roof(?:ing)?|membrane)\b/i, "Roofing"],
  [/\b(glazing|window|curtain\s*wall)\b/i, "Glazing"],
  [/\b(mechanical|hvac|duct|sheet\s*metal|plumb(?:ing|er)?)\b/i, "Mechanical / Plumbing"],
  [/\b(electrical|electrician|lighting)\b/i, "Electrical"],
  [/\b(drywall|taping|framing|steel\s+stud)\b/i, "Interior Framing / Drywall"],
  [/\b(paint(?:ing|er)?|coating)\b/i, "Painting / Coatings"],
  [/\b(fire\s*stop|fireproof(?:ing)?)\b/i, "Fireproofing"],
  [/\b(landscape|sod|irrigation)\b/i, "Landscaping"],
  [/\b(paving|asphalt|curb|sidewalk)\b/i, "Site Civil / Paving"],
];

function inferCommonTradeFromLine(t) {
  const txt = String(t || "").trim();
  if (!txt) return null;
  for (const [re, label] of COMMON_TRADE_PATTERNS) {
    if (re.test(txt)) return label;
  }
  return null;
}

function isGenericWorkTradeLabel(value) {
  return /^(site\s*\/\s*general|general|general\s+conditions|journal|notes?|misc(?:ellaneous)?|photos?)$/i.test(
    String(value || "").trim()
  );
}

/**
 * Prefer contractor / trade at line start; keep technical wording (hyphenated names, acronyms).
 */
function inferWorkTradeName(raw, category, reportDateKey) {
  const pre = reportDateKey
    ? normalizeReportLineText(String(raw || ""), reportDateKey)
    : String(raw || "");
  const t = stripLeadBullet(pre);
  if (!t) return "Site / General";
  const named = inferNamedContractorFromLine(t);
  if (named && isValidTradeHeading(named)) return named;
  const commonTrade = inferCommonTradeFromLine(t);
  if (commonTrade && isValidTradeHeading(commonTrade)) return commonTrade;
  if (
    /^(Add|Clarifies|Received|Give|Request|Internal|Two|Following|Previous|Sent|Updated|Confirming|Noting)\b/i.test(
      t
    )
  ) {
    return "Site / General";
  }
  if (/^(We\b|I\b|Today[,\s]|The weather\b|Weather:)/i.test(t)) return "Site / General";

  let out = "Site / General";
  let m = t.match(/^\s*([A-Z][A-Za-z0-9 &./\-]{2,42}?)\s*[â€“â€”]\s+/);
  if (m) out = sanitizeCell(m[1]).slice(0, 42);
  if (out === "Site / General") {
    m = t.match(
      /^\s*([A-Z][A-Za-z0-9 &./\-]{1,40}?)\s*(?:\(|â€“|â€”|:|\n)/
    );
    if (m) out = sanitizeCell(m[1]).slice(0, 42);
  }
  if (out === "Site / General") {
    m = t.match(/^\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}(?:-[A-Z][a-z]+)?)\s+/);
    if (m && m[1].length >= 4) out = sanitizeCell(m[1]).slice(0, 42);
  }
  if (out === "Site / General") {
    m = t.match(/^\s*([A-Z][a-z]+(?:-[A-Z][a-z]+)?)\s+/);
    if (m && m[1].length >= 3) out = m[1];
  }
  if (out === "Site / General") {
    m = t.match(/^\s*([A-Z]{2,}[A-Za-z0-9]*(?:\s+[A-Z][a-z]+)?)\b/);
    if (m) out = sanitizeCell(m[1]).slice(0, 42);
  }
  if (out === "Site / General") {
    const cat = String(category || "journal").toLowerCase();
    if (cat && cat !== "journal") out = cat.replace(/^./, (c) => c.toUpperCase());
  }
  if (out !== "Site / General" && !isPlausibleTradeName(out)) return "Site / General";
  if (out !== "Site / General" && !isValidTradeHeading(out)) return "Site / General";
  return out;
}

function groupWorkByTrade(entries, reportDateKey) {
  const workLike = entries.filter((e) => {
    const cat = String(e.category || "journal").toLowerCase();
    const secs = e.dailySummarySections || [];
    if (["progress", "note", "journal", "delivery", "inspection"].includes(cat)) return true;
    if (secs.some((s) => /work|deliver|inspect|note|journal/i.test(s))) return true;
    return cat === "journal";
  });
  const groups = {};
  const disp = (e) => (reportDateKey ? reportLineText(e, reportDateKey) : lineText(e));
  for (const e of workLike) {
    const raw = disp(e);
    if (!raw) continue;
    const cat = String(e.category || "journal").toLowerCase();
    const trade = inferWorkTradeName(raw, cat, reportDateKey);
    if (!groups[trade]) groups[trade] = [];
    groups[trade].push({
      id: e.id,
      text: raw,
      createdAt: e.createdAt,
      authorLabel: entryAuthorLabel(e),
    });
  }
  return groups;
}

function entriesBySection(entries, reportDateKey) {
  const out = {
    weather: [],
    manpower: [],
    work: [],
    inspections: [],
    concrete: [],
    issues: [],
  };
  const hasText = (e) => (reportDateKey ? reportLineText(e, reportDateKey) : lineText(e));
  for (const e of entries) {
    const text = hasText(e);
    if (!text) continue;
    const b = placementBucketForEntry(e);
    if (b === "weather") {
      if (textLooksWeatherOnly(text)) out.weather.push(e);
      else out.work.push(e);
    }
    else if (b === "manpower") out.manpower.push(e);
    else if (b === "inspections") out.inspections.push(e);
    else if (b === "concrete") out.concrete.push(e);
    else if (["deficiencies", "issues", "delays", "safety"].includes(b)) out.issues.push(e);
    else out.work.push(e);
  }
  return out;
}

function photoCreatedMs(p) {
  try {
    if (p.createdAt && p.createdAt.toDate) return p.createdAt.toDate().getTime();
    if (p.createdAt && p.createdAt.seconds) return p.createdAt.seconds * 1000;
  } catch (_) {}
  return 0;
}

function indexPhotosByEntry(photos) {
  const byEntry = new Map();
  for (const p of photos) {
    if (!p.linkedLogEntryId) continue;
    const k = String(p.linkedLogEntryId).trim();
    if (!k) continue;
    if (!byEntry.has(k)) byEntry.set(k, []);
    byEntry.get(k).push(p);
  }
  for (const arr of byEntry.values()) {
    arr.sort((a, b) => photoCreatedMs(a) - photoCreatedMs(b) || String(a.mediaId).localeCompare(String(b.mediaId)));
  }
  return byEntry;
}

function chunkEntriesWithPhotos(entries, byEntry, reportDateKey) {
  const txt = (e) => (reportDateKey ? reportLineText(e, reportDateKey) : lineText(e));
  return entries.map((e) => ({
    entryId: e.id,
    authorLabel: entryAuthorLabel(e),
    text: txt(e),
    photos: byEntry.get(String(e.id)) || [],
  }));
}

function entryTimeMs(e) {
  try {
    if (e.createdAt && e.createdAt.toDate) return e.createdAt.toDate().getTime();
    if (e.createdAt && e.createdAt.seconds) return e.createdAt.seconds * 1000;
  } catch (_) {}
  return 0;
}

function buildWorkBlocksWithPhotos(workGroups, byEntry) {
  const blocks = [];
  for (const [trade, items] of Object.entries(workGroups)) {
    const rows = items
      .map((it) => ({
        ...it,
        authorLabel: entryAuthorLabel(it),
        photos: byEntry.get(String(it.id)) || [],
      }))
      .sort((a, b) => entryTimeMs(a) - entryTimeMs(b));
    blocks.push({ trade, rows });
  }
  return blocks;
}

/**
 * @param {object[]} logEntriesRaw
 * @param {object[]} mediaDocs
 * @param {{ aiBlocks?: object|null, dayStart?: Date, reportDateKey?: string, structuredOverrides?: { manpowerRows?: string[][], concreteRows?: string[][] }|null }} [options]
 */
function buildDailyReportModel(logEntriesRaw, mediaDocs, options = {}) {
  const dayStart = options.dayStart || new Date();
  const structuredOverrides = options.structuredOverrides || null;
  const reportDateKey = options.reportDateKey || dateKeyEastern(dayStart);
  let entries = filterEntriesForDailySummary(logEntriesRaw || []).filter(
    (e) => !entryIsExcludedFromReport(e)
  );
  entries.sort((a, b) => entryTimeMs(a) - entryTimeMs(b));
  const entryMap = new Map(entries.map((e) => [String(e.id), e]));

  const unifiedAppendix = entries.map((e, i) => ({
    num: i + 1,
    time: fmtTimeShort(e.createdAt),
    authorLabel: entryAuthorLabel(e),
    category: e.category || "journal",
    text: reportLineText(e, reportDateKey),
  }));

  const bySec = entriesBySection(entries, reportDateKey);
  const weatherLines = bySec.weather.map((e) => reportLineText(e, reportDateKey)).filter(Boolean);
  const inspectionLines = bySec.inspections.map((e) => reportLineText(e, reportDateKey)).filter(Boolean);
  const issueLines = bySec.issues.map((e) => reportLineText(e, reportDateKey)).filter(Boolean);

  const weatherToday =
    weatherLines.length > 0
      ? `Today â€“ ${weatherLines.join(" ")}`.slice(0, 900)
      : "Not stated in field messages.";

  const workGroups = groupWorkByTrade(entries, reportDateKey);
  const workNarrativeBlock = (function () {
    const parts = [];
    for (const [trade, items] of Object.entries(workGroups)) {
      parts.push(`${trade}`);
      for (const it of items) {
        parts.push(`  - ${it.text}`);
      }
      parts.push("");
    }
    return parts.join("\n").trim() || "â€”";
  })();
  const concreteNarrativeBlock =
    bySec.concrete.map((e) => reportLineText(e, reportDateKey)).join("\n\n") || "â€”";

  const photos = (mediaDocs || [])
    .filter((m) => m.storagePath)
    .map((m) => ({
      mediaId: m.id,
      storagePath: m.storagePath,
      captionText: m.captionText || "",
      linkedLogEntryId: m.linkedLogEntryId || null,
      createdAt: m.createdAt || null,
      includeInDailyReport: m.includeInDailyReport === true,
      bucket: placementBucketForMedia({ ...m, id: m.id }, entryMap),
    }))
    .sort((a, b) => String(a.mediaId).localeCompare(String(b.mediaId)));

  const photosByBucket = {};
  for (const p of photos) {
    if (!photosByBucket[p.bucket]) photosByBucket[p.bucket] = [];
    photosByBucket[p.bucket].push(p);
  }

  const byEntry = indexPhotosByEntry(photos);

  const structured = {
    weatherChunks: chunkEntriesWithPhotos(bySec.weather, byEntry, reportDateKey),
    manpowerChunks: chunkEntriesWithPhotos(bySec.manpower, byEntry, reportDateKey),
    workBlocks: buildWorkBlocksWithPhotos(workGroups, byEntry),
    inspectionChunks: chunkEntriesWithPhotos(bySec.inspections, byEntry, reportDateKey),
    concreteChunks: chunkEntriesWithPhotos(bySec.concrete, byEntry, reportDateKey),
    issueChunks: chunkEntriesWithPhotos(bySec.issues, byEntry, reportDateKey),
    unlinkedPhotos: photos.filter((p) => !p.linkedLogEntryId),
  };

  const fallbackWeek = buildFallbackWeatherWeekRows(dayStart);

  const openItemRows = buildOpenItemRows(entries, reportDateKey);
  let manpowerRows = buildManpowerTableRows(entries, reportDateKey);
  let concreteRows = buildConcreteTableRows(entries, reportDateKey);
  if (structuredOverrides?.manpowerRows?.length) {
    manpowerRows = structuredOverrides.manpowerRows;
  }
  if (structuredOverrides?.concreteRows?.length) {
    concreteRows = structuredOverrides.concreteRows;
  }

  const aiBlocks = options.aiBlocks || null;

  return {
    entries,
    entryById: entryMap,
    reportDateKey,
    unifiedAppendix,
    dayStart,
    structured,
    photos,
    photosByBucket,
    deterministic: {
      weatherToday,
      manpowerNarrative: (function () {
        if (structuredOverrides?.manpowerRows?.length) return "";
        const parts = bySec.manpower.map((e) => {
          const intent = e.aiReportExtract && String(e.aiReportExtract.messageIntent || "").trim();
          const intentClean = intent
            ? stripReportFiller(normalizeReportLineText(intent, reportDateKey))
            : "";
          const body = reportLineText(e, reportDateKey);
          if (intentClean && body) return `${intentClean}\n\n${body}`;
          if (intentClean) return intentClean;
          return body;
        });
        return parts.filter(Boolean).join("\n\n") || "â€”";
      })(),
      inspectionText: inspectionLines.join("\n\n") || "Not stated in field messages.",
      issuesText: issueLines.join("\n\n") || "â€”",
      workGroups,
      workNarrativeBlock,
      concreteNarrativeBlock,
      openItemRows,
      manpowerRows,
      concreteRows,
    },
    aiBlocks,
    fallbackWeatherWeek: fallbackWeek,
  };
}

function sanitizeWorkSectionTradeMerge(trade) {
  const t = String(trade || "").trim();
  if (!t || !isValidTradeHeading(t)) return "Site / General";
  return t.slice(0, 48);
}

function mergeAdjacentWorkSections(sections) {
  const out = [];
  for (const s of sections) {
    const last = out[out.length - 1];
    if (last && last.trade === s.trade) {
      last.items.push(...s.items);
    } else {
      out.push({ trade: s.trade, items: [...s.items] });
    }
  }
  return out;
}

/**
 * Map OpenAI JSON manpower/concrete tables into the same row shapes as deterministic builders.
 * @returns {{ manpowerRows?: string[][], concreteRows?: string[][] }|null}
 */
function extractStructuredTableOverrides(json) {
  if (!json || typeof json !== "object") return null;
  const mp = json.manpower?.rows;
  const manpowerRows =
    Array.isArray(mp) && mp.length
      ? mp.map((r) => [
          String(r.trade || "â€”").trim().slice(0, 36) || "â€”",
          String(r.foreman || "â€”").trim().slice(0, 44) || "â€”",
          String(r.workers || "â€”").trim().slice(0, 24) || "â€”",
          String(r.notes || "â€”").trim().slice(0, 220) || "â€”",
        ]).filter((row) => {
          const trade = String(row[0] || "").trim();
          return trade && trade !== "â€”" && !isGenericWorkTradeLabel(trade);
        })
      : null;
  const cr = json.concreteSummary?.rows;
  const concreteRows =
    Array.isArray(cr) && cr.length
      ? cr.map((r) => [
          String(r.location || "â€”").trim().slice(0, 120) || "â€”",
          String(r.volume || "â€”").trim().slice(0, 40) || "â€”",
          String(r.status || "â€”").trim().slice(0, 28) || "â€”",
        ])
      : null;
  if (!manpowerRows && !concreteRows) return null;
  return { manpowerRows: manpowerRows || undefined, concreteRows: concreteRows || undefined };
}

/**
 * Merge strict JSON daily report into the `merged` shape used by the PDF renderer.
 */
function mergeStructuredDailyReportJson(det, json, dayStart) {
  const dk = dateKeyEastern(dayStart instanceof Date ? dayStart : new Date(dayStart));
  function normOut(s) {
    return stripReportFiller(normalizeReportLineText(String(s || ""), dk));
  }
  const fb = buildFallbackWeatherWeekRows(dayStart);
  const w = json.weather || {};
  let weatherWeeklyRows = fb;
  const wwf = w.weeklyForecastRows;
  if (Array.isArray(wwf) && wwf.length) {
    const parsed = wwf.map((row) => padRow6(Array.isArray(row) ? row : [row]));
    weatherWeeklyRows = mergeWeatherRows(fb, parsed, null);
  }

  const mp = json.manpower || {};
  const sec = json.workCompletedInProgress?.sections || [];
  const rawWorkSectionsAi = sec
    .map((s) => ({
      trade: sanitizeWorkSectionTradeMerge(s.trade),
      items: Array.isArray(s.items)
        ? s.items.map((x) => normOut(String(x))).filter(Boolean)
        : [],
    }))
    .filter((s) => s.items.length > 0);
  const hasGenericAiTrade = rawWorkSectionsAi.some((s) => isGenericWorkTradeLabel(s.trade));
  const workSectionsAi = hasGenericAiTrade ? [] : mergeAdjacentWorkSections(rawWorkSectionsAi);

  const issues = (json.issuesDeficienciesDelays?.items || []).filter(Boolean);
  const insps = (json.inspections?.items || []).filter(Boolean);

  const conc = json.concreteSummary || {};
  const openRows = (json.openItems?.rows || []).map((r) => [
    String(r.actionItem || "").trim().slice(0, 240),
    String(r.responsible || "â€”").trim().slice(0, 40),
    String(r.status || "â€”").trim().slice(0, 28),
  ]);
  const openItemsTableRaw = openRows.length
    ? openRows.map((r, i) => `${i + 1}|${r[0]}|${r[1]}|${r[2]}`).join("\n")
    : "";

  const execSummary = normOut(json.executiveSummary || "");
  const useStructuredWorkLayout = workSectionsAi.length > 0;

  let manpowerNarrative = normOut(mp.summaryNote || det.manpowerNarrative);
  if (Array.isArray(mp.rows) && mp.rows.length) {
    const t = manpowerNarrative.replace(/[â€”\-]/g, "").trim();
    if (!t || t.length < 28) manpowerNarrative = "";
  }

  return {
    execSummary,
    weatherToday: normOut(w.todaySummary || det.weatherToday),
    weatherWeeklyRows,
    manpowerNarrative,
    workNarrative: useStructuredWorkLayout
      ? ""
      : normOut(det.workNarrativeBlock || "â€”"),
    issuesText: normOut(issues.length ? issues.join("\n\n") : det.issuesText),
    inspectionText: normOut(insps.length ? insps.join("\n\n") : det.inspectionText),
    concreteNarrative: normOut(
      conc.narrativeNote != null && String(conc.narrativeNote).trim()
        ? conc.narrativeNote
        : det.concreteNarrativeBlock || "â€”"
    ),
    openIntro: normOut(
      json.openItems?.narrativeNote != null && String(json.openItems.narrativeNote).trim()
        ? json.openItems.narrativeNote
        : ""
    ),
    openItemsTableRaw,
    useStructuredWorkLayout,
    workSectionsAi,
  };
}

function mergeAiIntoDeterministic(det, aiBlocks, dayStart) {
  const fb = buildFallbackWeatherWeekRows(dayStart || new Date());
  const aiTable =
    aiBlocks && aiBlocks.WEATHER_WEEKLY_TABLE
      ? parseWeatherWeeklyTableBlock(aiBlocks.WEATHER_WEEKLY_TABLE)
      : null;

  let weatherWeeklyRows = fb;
  if (!aiBlocks) {
    return {
      execSummary: "",
      weatherToday: stripReportFiller(det.weatherToday),
      weatherWeeklyRows,
      manpowerNarrative: stripReportFiller(det.manpowerNarrative),
      workNarrative: stripReportFiller(det.workNarrativeBlock || "â€”"),
      issuesText: stripReportFiller(det.issuesText),
      inspectionText: stripReportFiller(det.inspectionText),
      concreteNarrative: stripReportFiller(det.concreteNarrativeBlock || "â€”"),
      openIntro: "",
      openItemsTableRaw: "",
      useStructuredWorkLayout: false,
      workSectionsAi: [],
    };
  }
  const g = (k, fbv) => {
    const v = aiBlocks[k];
    return v && String(v).trim() ? String(v).trim() : fbv;
  };

  const manpowerRowsStructured = (det.manpowerRows || []).filter(
    (r) => r && r[0] && r[0] !== "â€”" && r[2] && /^\d{1,3}$/.test(String(r[2]).trim())
  );
  const detMan = String(det.manpowerNarrative || "").trim();
  const aiMan = String(aiBlocks.MANPOWER || "").trim();
  const aiExec = String(aiBlocks.EXEC_SUMMARY || "").trim();
  const keepDeterministicManpower =
    manpowerRowsStructured.length >= 3 &&
    detMan.length > 40 &&
    !(aiExec.length > 20 && aiMan.length > 25);
  const manpowerNarrativeMerged = stripReportFiller(
    keepDeterministicManpower
      ? detMan
      : aiMan.length > 15
        ? aiMan
        : detMan || aiMan || "â€”"
  );

  const wwf = g("WEATHER_WEEKLY_FORECAST", "");
  const reParsed = parseWeatherWeeklyTextToRows(wwf, dayStart);
  weatherWeeklyRows = mergeWeatherRows(fb, reParsed, aiTable && aiTable.length ? aiTable : null);

  const workAi = stripReportFiller(g("WORK_COMPLETED", ""));
  const workFallback = stripReportFiller(det.workNarrativeBlock || "â€”");
  const workNarrativeMerged =
    workAi && workAi.length > 25 ? workAi : workFallback;

  return {
    execSummary: stripReportFiller(g("EXEC_SUMMARY", "")),
    weatherToday: stripReportFiller(g("WEATHER_TODAY", det.weatherToday)),
    weatherWeeklyRows,
    manpowerNarrative: manpowerNarrativeMerged,
    workNarrative: workNarrativeMerged,
    issuesText: stripReportFiller(g("ISSUES_AND_DEFICIENCIES", det.issuesText)),
    inspectionText: stripReportFiller(g("INSPECTIONS", det.inspectionText)),
    concreteNarrative: stripReportFiller(g("CONCRETE", det.concreteNarrativeBlock || "â€”")),
    openIntro: stripReportFiller(g("OPEN_ITEMS_NARRATIVE", "")),
    openItemsTableRaw: aiBlocks.OPEN_ITEMS_TABLE || "",
    useStructuredWorkLayout: false,
    workSectionsAi: [],
  };
}

function isJournalMetaOrControlText(text) {
  const raw = String(text || "").trim();
  if (!raw) return true;
  const low = raw.replace(/\s+/g, " ").trim().toLowerCase();

  if (/^(continue|ok|okay|k|yes|no|thanks?|thank you|ty|help|status|reset|contacts?)$/i.test(low)) {
    return true;
  }
  if (/^project\s+\S+$/i.test(low)) return true;
  if (/^(daily\s+report|daily\s+summary|daily\s+log)\s*[.!]*$/i.test(low)) return true;
  if (/\b(requested\s+item\s+to\s+be\s+included\s+in\s+the\s+daily\s+summary)\b/i.test(low)) return true;
  if (/request(?:ing)?\s+(?:a\s+)?plan\s+for\s+tomorrow/i.test(low)) return true;
  if (/\basks?\s+for\s+tomorrow'?s?\s+weather\s+forecast\b/i.test(low)) return true;
  if (/unable\s+to\s+retrieve\s+live\s+weather\s+data/i.test(low)) return true;
  if (/\b(generate|build|create|send|text|email|link)\b.*\b(pdf|report|daily\s+report)\b/i.test(low)) {
    return true;
  }
  if (/show\s+me\s+(the\s+)?(pictures?|photos?|pics|images?)/i.test(low)) return true;
  if (/\b(show|read|give)\s+me\s+.*\b(journal|input|note|notes)\b/i.test(low)) return true;
  if (/\bwhat\s+(did\s+i\s+(say|send)|was\s+my\s+(journal|note|input))\b/i.test(low)) return true;
  if (
    /^(can|could|would|will)\s+you\b/i.test(low) &&
    /\b(summarize|rewrite|reword|show|find|search|tell|explain|generate|create|make|send|text|email|read|review)\b/i.test(
      low
    )
  ) {
    return true;
  }
  if (
    /^(please\s+)?(summarize|rewrite|reword|show|find|search|tell|explain|generate|create|make|send|text|email|read|review)\b/i.test(
      low
    )
  ) {
    return true;
  }
  if (/tap\s+(here|below)|reply\s+with\s+\d/i.test(low)) return true;
  return false;
}

function filterEntriesForJournalReport(entries, reportDateKey) {
  return filterEntriesForDailySummary(entries || []).filter((e) => {
    const layers = [
      reportDateKey ? reportLineText(e, reportDateKey) : lineText(e),
      String(e.rawText || "").trim(),
      String(e.normalizedText || "").trim(),
      String(e.summaryText || "").trim(),
    ].filter(Boolean);
    if (!layers.length) return false;
    // Keep the row if any layer is real diary content. Requiring every layer to be
    // non-meta dropped legitimate notes when summaryText echoed a system phrase while
    // rawText held the actual message (common for shared home journals).
    return layers.some((layer) => !isJournalMetaOrControlText(layer));
  });
}

function uniqueJournalLines(lines, limit) {
  const out = [];
  const seen = new Set();
  for (const line of lines) {
    const t = stripReportFiller(String(line || "").replace(/\s+/g, " ").trim());
    if (!t || t.length < 6) continue;
    const key = t.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(t);
    if (limit && out.length >= limit) break;
  }
  return out;
}

function pickJournalKeyMoments(entries, reportDateKey, limit = 8, authorLabelsByIdentity = null) {
  const labelsByIdentity = buildAuthorLabelContext(entries || [], authorLabelsByIdentity);
  const includeAuthor = journalContributors(entries || [], labelsByIdentity).length > 1;
  const lines = [...(entries || [])]
    .sort((a, b) => entryTimeMs(a) - entryTimeMs(b))
    .map((e) => journalLineWithOptionalAuthor(e, reportDateKey, includeAuthor, labelsByIdentity));
  return uniqueJournalLines(lines, limit);
}

function pickJournalReflections(entries, reportDateKey, limit = 6, authorLabelsByIdentity = null) {
  const reflective = [];
  const fallback = [];
  const labelsByIdentity = buildAuthorLabelContext(entries || [], authorLabelsByIdentity);
  const includeAuthor = journalContributors(entries || [], labelsByIdentity).length > 1;
  for (const e of [...(entries || [])].sort((a, b) => entryTimeMs(a) - entryTimeMs(b))) {
    const text = journalLineWithOptionalAuthor(e, reportDateKey, includeAuthor, labelsByIdentity);
    if (!text) continue;
    fallback.push(text);
    if (
      /\b(feel|felt|think|thought|realiz(?:e|ed)|wonder(?:ed)?|worr(?:y|ied)|stress(?:ed)?|grateful|thankful|happy|sad|tired|hope|frustrat(?:ed|ing)|excited|anxious|learned|noticed|appreciated)\b/i.test(
        text
      )
    ) {
      reflective.push(text);
    }
  }
  const chosen = reflective.length ? reflective : fallback.slice(-Math.max(2, limit));
  return uniqueJournalLines(chosen, limit);
}

function buildJournalOverview(entries, reportDateKey, authorLabelsByIdentity = null) {
  const moments = pickJournalKeyMoments(entries, reportDateKey, 4, authorLabelsByIdentity);
  if (!moments.length) {
    return "No journal-worthy notes were captured for this day.";
  }
  let text = moments.slice(0, 3).join(" ");
  if (text.length > 900) text = `${text.slice(0, 897)}...`;
  return text;
}

function buildJournalClosingNote(entries, reportDateKey, authorLabelsByIdentity = null) {
  const labelsByIdentity = buildAuthorLabelContext(entries || [], authorLabelsByIdentity);
  const includeAuthor = journalContributors(entries || [], labelsByIdentity).length > 1;
  const lines = [...(entries || [])]
    .sort((a, b) => entryTimeMs(a) - entryTimeMs(b))
    .map((e) => journalLineWithOptionalAuthor(e, reportDateKey, includeAuthor, labelsByIdentity))
    .filter(Boolean);
  if (!lines.length) return "";
  const last = stripReportFiller(lines[lines.length - 1]);
  return last.length > 320 ? `${last.slice(0, 317)}...` : last;
}

function sanitizeJournalMetaValue(value) {
  return String(value || "")
    .replace(/[\r\n[\];]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 120);
}

function formatJournalBundleForAi(entries, reportDateKey, options = {}) {
  const list = filterEntriesForJournalReport(entries || [], reportDateKey)
    .sort((a, b) => entryTimeMs(a) - entryTimeMs(b));
  const authorLabelsByIdentity = buildAuthorLabelContext(list, options.authorLabelsByIdentity || null);
  const contributors = journalContributors(list, authorLabelsByIdentity)
    .map((c) => c.label)
    .filter(Boolean);
  const header = contributors.length
    ? `Contributors: ${contributors.map((label) => sanitizeJournalMetaValue(label)).join(", ")}\n`
    : "";
  const lines = list
    .map((e, i) => {
      const body = reportDateKey ? reportLineText(e, reportDateKey) : lineText(e);
      const tm = fmtTimeShort(e.createdAt);
      const author = sanitizeJournalMetaValue(entryAuthorLabel(e, authorLabelsByIdentity));
      return `[#${i + 1}] ${tm} [author=${author}; category=${e.category || "journal"}] ${body}`;
    })
    .join("\n");
  return `${header}${lines}`
    .slice(0, 10_000);
}

function buildJournalReportModel(logEntriesRaw, mediaDocs, options = {}) {
  const dayStart = options.dayStart || new Date();
  const reportDateKey = options.reportDateKey || dateKeyEastern(dayStart);
  const entries = filterEntriesForJournalReport(logEntriesRaw || [], reportDateKey)
    .sort((a, b) => entryTimeMs(a) - entryTimeMs(b));
  const entryMap = new Map(entries.map((e) => [String(e.id), e]));

  const photos = (mediaDocs || [])
    .filter((m) => m && m.storagePath)
    .map((m) => ({
      mediaId: m.id,
      storagePath: m.storagePath,
      captionText: m.captionText || "",
      linkedLogEntryId: m.linkedLogEntryId || null,
      createdAt: m.createdAt || null,
    }))
    .sort((a, b) => photoCreatedMs(a) - photoCreatedMs(b));

  const byEntry = indexPhotosByEntry(photos);
  const authorLabelsByIdentity = buildAuthorLabelContext(entries, options.authorLabelsByIdentity || null);
  const contributors = journalContributors(entries, authorLabelsByIdentity);
  const timeline = entries
    .map((e) => ({
      entryId: e.id,
      time: fmtTimeShort(e.createdAt),
      authorLabel: entryAuthorLabel(e, authorLabelsByIdentity),
      text: reportDateKey ? reportLineText(e, reportDateKey) : lineText(e),
      photos: byEntry.get(String(e.id)) || [],
    }))
    .filter((row) => row.text);

  return {
    entries,
    entryById: entryMap,
    reportDateKey,
    dayStart,
    photos,
    timeline,
    contributors,
    isCoauthored: contributors.length > 1,
    deterministic: {
      overview: buildJournalOverview(entries, reportDateKey, authorLabelsByIdentity),
      keyMoments: pickJournalKeyMoments(entries, reportDateKey, 8, authorLabelsByIdentity),
      reflections: pickJournalReflections(entries, reportDateKey, 6, authorLabelsByIdentity),
      closingNote: buildJournalClosingNote(entries, reportDateKey, authorLabelsByIdentity),
    },
  };
}

module.exports = {
  buildDailyReportModel,
  buildJournalReportModel,
  mergeAiIntoDeterministic,
  mergeStructuredDailyReportJson,
  extractStructuredTableOverrides,
  formatReportBundleForAi,
  formatJournalBundleForAi,
  filterEntriesForJournalReport,
  stripReportFiller,
  lineText,
  reportLineText,
  textLooksWeatherOnly,
  placementBucketForEntry,
  parsePipeRows,
};

