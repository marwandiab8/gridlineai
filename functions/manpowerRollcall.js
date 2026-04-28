/**
 * Parses superintendent-style manpower roll-call lines: repeated "TradeName count" tokens.
 * Shared by daily report table builder and section tagging (avoid circular requires).
 */

function sanitizeCell(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

const BAD_TRADE_TOKEN =
  /^(WE|THE|TODAY|THERE|HERE|SITE|TEAM|CREW|WEATHER|NOTE|POUR|CONCRETE|MANPOWER|WORK|LOG)\b/i;

/** Tokens that match CapitalWord + number but are not subs (months, headers, layout words). */
const ROLLCALL_NOISE_TRADE = new RegExp(
  "^(Line|Lines|Grid|Bay|Zone|Level|Core|Stair|West|East|North|South|Lift|Return|Hole|Tank|Manhole|Area|Garage|Formwork|Forms|" +
    "Project|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|" +
    "January|February|March|April|May|June|July|August|September|October|November|December|" +
    "Daily|Site|Log)$",
  "i"
);

const CALENDAR_WORD_IN_TRADE =
  /\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|January|February|March|April|May|June|July|August|September|October|November|December)\b/i;

function isPlausibleTradeName(s) {
  const t = sanitizeCell(s);
  if (t.length < 2) return false;
  if (BAD_TRADE_TOKEN.test(t)) return false;
  if (/^(A|AN|I|IT|AS|AT|IN|ON|TO|OF|OR|IF|BE|SO|NO|UP|US|DO|GO|AM|PM)\b/i.test(t)) return false;
  return true;
}

function isRollcallTradeToken(name) {
  const t = sanitizeCell(name);
  if (!isPlausibleTradeName(t)) return false;
  if (ROLLCALL_NOISE_TRADE.test(t)) return false;
  if (CALENDAR_WORD_IN_TRADE.test(t)) return false;
  if (/\b(between|following|including|underneath)\b/i.test(t)) return false;
  if (t.length > 44) return false;
  return true;
}

/** Drop "Project … Monday … Manpower" style preamble so "Manpower ALC 20" is not one token. */
function stripRollcallPreamble(lineTrim) {
  const s = sanitizeCell(lineTrim);
  if (!s) return s;
  const idx = s.search(/\bManpower\b\s*/i);
  if (idx >= 0) {
    return s.slice(idx).replace(/^\s*\bManpower\b\s*/i, "").trim();
  }
  return s;
}

/**
 * One line containing two or more "Trade 12" style pairs (e.g. subs + headcounts).
 * @returns {{ trade: string, workers: string }[] | null}
 */
function parseManpowerRollcallLine(line) {
  const lineTrim = stripRollcallPreamble(sanitizeCell(line));
  if (!lineTrim) return null;

  const pairRe =
    /\b([A-Z][A-Za-z0-9'./\-]*(?:\s+[A-Za-z][a-zA-Z0-9'./\-]*){0,2})\s+(\d{1,3})\b/g;
  const pairs = [];
  let m;
  while ((m = pairRe.exec(lineTrim)) !== null) {
    const trade = sanitizeCell(m[1]);
    const num = parseInt(m[2], 10);
    if (!isRollcallTradeToken(trade)) continue;
    if (!Number.isFinite(num) || num < 1 || num > 500) continue;
    pairs.push({ trade, workers: String(num) });
  }

  if (pairs.length < 2) return null;
  return pairs;
}

function textContainsManpowerRollcall(text) {
  const lines = String(text || "").split(/\r?\n/);
  for (const ln of lines) {
    const p = parseManpowerRollcallLine(ln);
    if (p && p.length >= 2) return true;
  }
  const oneLine = stripRollcallPreamble(
    sanitizeCell(String(text || "").replace(/\r?\n/g, " "))
  );
  const flat = parseManpowerRollcallLine(oneLine);
  return flat != null && flat.length >= 2;
}

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Text after the last valid roll-call "Trade N" token (for single-line SMS: counts + narratives).
 */
function tailAfterManpowerRollcall(flatText) {
  const lineTrim = stripRollcallPreamble(
    sanitizeCell(String(flatText || "").replace(/\r?\n/g, " "))
  );
  const pairRe =
    /\b([A-Z][A-Za-z0-9'./\-]*(?:\s+[A-Za-z][a-zA-Z0-9'./\-]*){0,2})\s+(\d{1,3})\b/g;
  let lastEnd = 0;
  let m;
  while ((m = pairRe.exec(lineTrim)) !== null) {
    const trade = sanitizeCell(m[1]);
    const num = parseInt(m[2], 10);
    if (!isRollcallTradeToken(trade)) continue;
    if (!Number.isFinite(num) || num < 1 || num > 500) continue;
    lastEnd = m.index + m[0].length;
  }
  return lineTrim.slice(lastEnd).trim();
}

module.exports = {
  parseManpowerRollcallLine,
  textContainsManpowerRollcall,
  tailAfterManpowerRollcall,
  escapeRegExp,
};
