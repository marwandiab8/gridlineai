const { dateKeyEastern, extractExplicitReportDate } = require("./logClassifier");
const { normalizeProjectSlug } = require("./projectAccess");

const COL_LABOURERS = "labourers";
const COL_LABOUR_ENTRIES = "labourEntries";
const LABOUR_PAY_PERIOD_ANCHOR = "2026-04-26"; // Sunday anchor date.

function normalizeLabourerName(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 120);
}

function normalizeLabourEntryText(value) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim()
    .slice(0, 2000);
}

function normalizeLabourerPhone(value) {
  return String(value || "").trim();
}

function parseDateKey(value) {
  const raw = String(value || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  const date = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(date.getTime())) return null;
  return date;
}

function formatDateKey(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  return date.toISOString().slice(0, 10);
}

function monthKeyFromDateKey(dateKey) {
  const raw = String(dateKey || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw.slice(0, 7) : "";
}

function shiftDateKey(dateKey, deltaDays) {
  const date = parseDateKey(dateKey);
  if (!date || !Number.isFinite(deltaDays)) return "";
  date.setUTCDate(date.getUTCDate() + Number(deltaDays));
  return formatDateKey(date);
}

function startOfWeekFromDateKey(dateKey) {
  const date = parseDateKey(dateKey);
  if (!date) return "";
  const day = date.getUTCDay();
  const mondayOffset = day === 0 ? 6 : day - 1;
  return shiftDateKey(dateKey, -mondayOffset);
}

function weeklyKeyFromDateKey(dateKey) {
  return startOfWeekFromDateKey(dateKey);
}

function dayMultiplierFromDateKey(dateKey) {
  const date = parseDateKey(dateKey);
  if (!date) return 1;
  const day = date.getUTCDay();
  if (day === 6) return 1.5; // Saturday
  if (day === 0) return 2; // Sunday
  return 1;
}

function biweeklyPayPeriodStartKeyFromDateKey(dateKey) {
  const date = parseDateKey(dateKey);
  const anchor = parseDateKey(LABOUR_PAY_PERIOD_ANCHOR);
  if (!date || !anchor) return "";
  const deltaDays = Math.floor((date.getTime() - anchor.getTime()) / 86400000);
  const periodIndex = Math.floor(deltaDays / 14);
  const start = new Date(anchor.getTime());
  start.setUTCDate(start.getUTCDate() + periodIndex * 14);
  return formatDateKey(start);
}

function endOfMonthFromDateKey(dateKey) {
  const date = parseDateKey(dateKey);
  if (!date) return "";
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth();
  return formatDateKey(new Date(Date.UTC(year, month + 1, 0)));
}

function parseLabourHoursCommand(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;

  const cleaned = extractExplicitReportDate(raw);
  const body = String(cleaned.cleanedText || raw).trim();
  const segmented = body.match(/^(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)\b\s+([\s\S]+)$/i);
  if (segmented) {
    const declaredHours = Number(segmented[1]);
    const tail = String(segmented[2] || "").trim();
    const parts = [];
    const partRe = /(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)\s+([\s\S]*?)(?=(?:\s+\d+(?:\.\d+)?\s*(?:hours?|hrs?|h)\b)|$)/gi;
    let m;
    while ((m = partRe.exec(tail))) {
      const h = Number(m[1]);
      const task = normalizeLabourEntryText(m[2]);
      if (!Number.isFinite(h) || h <= 0 || !task) continue;
      parts.push({ hours: Math.round(h * 100) / 100, task });
    }
    if (parts.length) {
      const sum = parts.reduce((total, p) => total + p.hours, 0);
      const normalizedSum = Math.round(sum * 100) / 100;
      const effectiveHours =
        Number.isFinite(declaredHours) && declaredHours > 0 && Math.abs(normalizedSum - declaredHours) <= 0.25
          ? Math.round(declaredHours * 100) / 100
          : normalizedSum;
      const workOnSource = parts.map((p) => `${p.hours}h ${p.task}`).join(" - ");
      const workOnDate = extractExplicitReportDate(workOnSource);
      const workOn = normalizeLabourEntryText(String(workOnDate.cleanedText || workOnSource).trim());
      if (workOn && effectiveHours > 0) {
        return {
          hours: effectiveHours,
          workOn,
          reportDateKey: cleaned.reportDateKey || workOnDate.reportDateKey || null,
          rawText: raw,
        };
      }
    }
  }
  const candidates = [
    // Natural field text, e.g. "Hi Sunday April 26- total 9 H . Pumping water… Thanks Wael"
    body.match(
      /^[\s\S]*?\btotal\s+(?:of\s+)?(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)\b[.\s:,-]*([\s\S]+)$/i,
    ),
    body.match(/^(?:labour|labor|hours?|time)\s*[:\-–—]?\s*(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)?\s*(.+)$/i),
    body.match(/^(?:worked|work|labor(?:ed)?)\s*(?:for\s*)?(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)?\s*(?:on\s+)?(.+)$/i),
    body.match(/^(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)\s*(?:on\s+)?(.+)$/i),
  ];

  for (const match of candidates) {
    if (!match) continue;
    const hours = Number(match[1]);
    if (!Number.isFinite(hours) || hours <= 0) continue;
    const workOnSource = normalizeLabourEntryText(match[2]);
    const workOnDate = extractExplicitReportDate(workOnSource);
    const workOnCleaned = String(workOnDate.cleanedText || workOnSource)
      .replace(/\(\s*\d{4}-\d{2}(?:-?\d{2,3})\s*\)/g, " ")
      .replace(/\b(?:for|on|dated|date)\s+\d{4}-\d{2}(?:-?\d{2,3})\b/gi, " ")
      .replace(/\s+/g, " ")
      .trim();
    const workOn = normalizeLabourEntryText(workOnCleaned);
    if (!workOn) continue;
    return {
      hours: Math.round(hours * 100) / 100,
      workOn,
      reportDateKey: cleaned.reportDateKey || workOnDate.reportDateKey || null,
      rawText: raw,
    };
  }

  return null;
}

/**
 * Detects when a message is asking for a logged-hours total (not submitting hours).
 * Returns a range, or null to fall through to other handlers / AI.
 */
function parseLabourHoursBalanceQuery(text) {
  const raw = String(text || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!raw) return null;
  if (parseLabourHoursCommand(text)) return null;

  const lower = raw.toLowerCase();

  const wantsTotal =
    /\?[\s!]*$/.test(raw) ||
    /\b(how\s+many|how\s+much)\s+hours?\b/i.test(raw) ||
    /^hours?\s+for\s+/i.test(raw) ||
    /^(time|my\s+hours?)\s+for\s+/i.test(raw) ||
    /^what(?:'s|s| is)\s+my\s+(total\s+)?(hours?|time)\b/i.test(raw) ||
    /\bmy\s+hours?\b/i.test(raw) ||
    /\b(tally|check|see|show|show\s+me|text\s+me|tell\s+me|give\s+me)\b.*\b(hours?|tally|time|pay)\b/i.test(lower) ||
    /\b(hours?|time)\b.*\b(today|so\s*far|this\s*week|this\s*pay|this\s*month|the\s*week|pay|payroll|pay\s*period)\b/i.test(lower);

  if (!wantsTotal) return null;
  if (!/\b(hours?|hrs?|h\b|time|tally|pay|worked|logged)\b/i.test(lower)) {
    if (!/^(?:what|how|hours)/i.test(raw)) return null;
  }

  if (/\b(today|so\s*far\s*today|this\s*day|right\s*now|right\s*now\s*today)\b/i.test(lower)) {
    return { range: "today" };
  }
  if (/\b(this|the)\s+week\b|weekly|since\s+monday|for\s+the\s+week\b/i.test(lower)) {
    return { range: "week" };
  }
  if (/\b(this|the)\s+month\b|monthly|for\s+the\s+month\b/i.test(lower)) {
    return { range: "month" };
  }
  if (
    /\bthis\s*pay\b|for\s*this\s*pay|for\s*pay|the\s*pay\s*period|pay\s*period|current\s*pay|bi-?weekly|payroll|paycheck|cheque/i.test(
      lower
    )
  ) {
    return { range: "pay" };
  }
  if (/\bmy\s+hours?\b/i.test(lower)) return { range: "pay" };
  if (/\b(how\s+many|how\s+much)\s+hours?\b/i.test(raw)) return { range: "pay" };

  return null;
}

function getDateKeyRangeForBalanceQuery(range, now = new Date()) {
  const d = now instanceof Date && !Number.isNaN(now.getTime()) ? now : new Date();
  const todayKey = dateKeyEastern(d);
  if (range === "today") {
    return { startKey: todayKey, endKey: todayKey, label: "today" };
  }
  if (range === "week") {
    const wk = startOfWeekFromDateKey(todayKey) || todayKey;
    const wEnd = shiftDateKey(wk, 6) || todayKey;
    return { startKey: wk, endKey: wEnd, label: "this week" };
  }
  if (range === "pay") {
    const start = biweeklyPayPeriodStartKeyFromDateKey(todayKey) || todayKey;
    const pEnd = shiftDateKey(start, 13) || start;
    return { startKey: start, endKey: pEnd, label: "this pay period" };
  }
  if (range === "month") {
    const mk = monthKeyFromDateKey(todayKey);
    const mStart = mk ? `${mk}-01` : todayKey;
    const mEnd = endOfMonthFromDateKey(mStart) || todayKey;
    return { startKey: mStart, endKey: mEnd, label: "this month" };
  }
  return null;
}

function formatLabourBalanceReply({
  labourerName,
  rangeLabel,
  startKey,
  endKey,
  totalHours,
  totalPaidHours,
  totalEntries,
}) {
  const w = Math.round((Number(totalHours) || 0) * 100) / 100;
  const p = Math.round((Number(totalPaidHours) || 0) * 100) / 100;
  const who = String(labourerName || "You")
    .replace(/\s+/g, " ")
    .trim() || "You";
  const rangeBits =
    startKey && endKey
      ? startKey === endKey
        ? startKey
        : `${startKey} to ${endKey}`
      : "";
  if (!Number(totalEntries) || totalEntries < 1) {
    return `${who}: no hours logged for ${rangeLabel} (${rangeBits}) yet. Text: labour 8.0 your task.`;
  }
  const same = Math.abs(w - p) < 0.01;
  const body = same ? `${w}h` : `${w}h on site, ${p}h paid (Sat 1.5x / Sun 2x)`;
  const entryWord = totalEntries === 1 ? "entry" : "entries";
  return `${who} — ${rangeLabel} (${rangeBits}): ${body} · ${totalEntries} ${entryWord}.`;
}

function buildLabourEntryDoc(input) {
  const hours = Number(input && input.hours);
  if (!Number.isFinite(hours) || hours <= 0) {
    throw new Error("hours is required.");
  }
  const workOn = normalizeLabourEntryText(input && input.workOn);
  if (!workOn) {
    throw new Error("workOn is required.");
  }
  const reportDateKey = String(input && input.reportDateKey || "").trim() || dateKeyEastern(new Date());
  const labourerName = normalizeLabourerName(input && input.labourerName);
  const labourerPhone = normalizeLabourerPhone(input && input.labourerPhone);
  return {
    labourerName: labourerName || null,
    labourerPhone: labourerPhone || null,
    projectSlug: normalizeProjectSlug(input && input.projectSlug) || null,
    reportDateKey,
    hours: Math.round(hours * 100) / 100,
    workOn,
    notes: normalizeLabourEntryText(input && input.notes) || "",
    source: String(input && input.source || "dashboard").trim() || "dashboard",
    enteredByEmail: String(input && input.enteredByEmail || "").trim() || null,
    enteredByPhone: String(input && input.enteredByPhone || "").trim() || null,
  };
}

async function writeLabourEntry(db, FieldValue, input) {
  const doc = buildLabourEntryDoc(input);
  const ref = await db.collection(COL_LABOUR_ENTRIES).add({
    ...doc,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });
  return { labourEntryId: ref.id, ...doc };
}

async function loadLabourEntries(db, filters = {}) {
  const { startKey, endKey, labourerPhone, labourerName, projectSlug } = filters;
  let query = db.collection(COL_LABOUR_ENTRIES);
  const hasRange = /^\d{4}-\d{2}-\d{2}$/.test(String(startKey || "")) || /^\d{4}-\d{2}-\d{2}$/.test(String(endKey || ""));
  const start = /^\d{4}-\d{2}-\d{2}$/.test(String(startKey || "")) ? String(startKey) : null;
  const end = /^\d{4}-\d{2}-\d{2}$/.test(String(endKey || "")) ? String(endKey) : null;

  if (labourerPhone) query = query.where("labourerPhone", "==", normalizeLabourerPhone(labourerPhone));
  if (labourerName) query = query.where("labourerName", "==", normalizeLabourerName(labourerName));
  if (projectSlug) query = query.where("projectSlug", "==", normalizeProjectSlug(projectSlug));
  if (start) query = query.where("reportDateKey", ">=", start);
  if (end) query = query.where("reportDateKey", "<=", end);
  if (hasRange || start || end) query = query.orderBy("reportDateKey", "asc").orderBy("createdAt", "asc");
  else query = query.orderBy("createdAt", "desc");

  const snap = await query.limit(5000).get();
  return snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
}

function sumHours(entries) {
  return (entries || []).reduce((total, item) => total + (Number(item && item.hours) || 0), 0);
}

function groupEntriesByKey(entries, keyFn) {
  const groups = new Map();
  for (const entry of entries || []) {
    const key = keyFn(entry);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  }
  return groups;
}

function normalizeLabourRangeKeys(startKey, endKey) {
  const start = String(startKey || "").trim();
  const end = String(endKey || "").trim();
  return {
    startKey: /^\d{4}-\d{2}-\d{2}$/.test(start) ? start : null,
    endKey: /^\d{4}-\d{2}-\d{2}$/.test(end) ? end : null,
  };
}

function buildLabourRollup(entries) {
  const sorted = [...(entries || [])].sort((a, b) => {
    const aKey = String(a.reportDateKey || "");
    const bKey = String(b.reportDateKey || "");
    if (aKey !== bKey) return aKey < bKey ? -1 : 1;
    const aCreated = a && a.createdAt && typeof a.createdAt.seconds === "number" ? a.createdAt.seconds : 0;
    const bCreated = b && b.createdAt && typeof b.createdAt.seconds === "number" ? b.createdAt.seconds : 0;
    return aCreated - bCreated;
  });
  const byDay = groupEntriesByKey(sorted, (e) => String(e.reportDateKey || dateKeyEastern(new Date())));
  const dailyTotals = [...byDay.entries()].map(([reportDateKey, dayEntries]) => ({
    reportDateKey,
    totalHours: sumHours(dayEntries),
    totalPaidHours: dayEntries.reduce((total, item) => {
      const hours = Number(item && item.hours) || 0;
      return total + hours * dayMultiplierFromDateKey(reportDateKey);
    }, 0),
    entries: dayEntries,
  }));

  const byWeek = groupEntriesByKey(sorted, (e) => {
    const key = String(e.reportDateKey || dateKeyEastern(new Date()));
    return startOfWeekFromDateKey(key) || key;
  });
  const weeklyTotals = [...byWeek.entries()]
    .map(([weekStartKey, weekEntries]) => ({
      weekStartKey,
      weekEndKey: shiftDateKey(weekStartKey, 6) || weekStartKey,
      totalHours: sumHours(weekEntries),
      entries: weekEntries,
    }))
    .sort((a, b) => (a.weekStartKey < b.weekStartKey ? -1 : a.weekStartKey > b.weekStartKey ? 1 : 0));

  const byMonth = groupEntriesByKey(sorted, (e) => String(monthKeyFromDateKey(e.reportDateKey || "")));
  const monthlyTotals = [...byMonth.entries()]
    .map(([monthKey, monthEntries]) => ({
      monthKey,
      monthStartKey: monthKey ? `${monthKey}-01` : "",
      monthEndKey: endOfMonthFromDateKey(monthKey ? `${monthKey}-01` : "") || monthKey,
      totalHours: sumHours(monthEntries),
      entries: monthEntries,
    }))
    .sort((a, b) => (a.monthKey < b.monthKey ? -1 : a.monthKey > b.monthKey ? 1 : 0));

  const byLabourer = groupEntriesByKey(sorted, (e) => String(e.labourerName || e.labourerPhone || "Unknown"));
  const labourerTotals = [...byLabourer.entries()].map(([labourer, labourerEntries]) => ({
    labourer,
    totalHours: sumHours(labourerEntries),
    totalPaidHours: labourerEntries.reduce((total, item) => {
      const hours = Number(item && item.hours) || 0;
      const reportDateKey = String(item?.reportDateKey || "");
      return total + hours * dayMultiplierFromDateKey(reportDateKey);
    }, 0),
    entries: labourerEntries,
  }));

  const byPayPeriod = groupEntriesByKey(sorted, (e) => {
    const key = String(e.reportDateKey || dateKeyEastern(new Date()));
    return biweeklyPayPeriodStartKeyFromDateKey(key) || key;
  });
  const paidPeriodTotals = [...byPayPeriod.entries()]
    .map(([periodStartKey, periodEntries]) => {
      let saturdayHours = 0;
      let sundayHours = 0;
      let weekdayHours = 0;
      let totalPaidHours = 0;
      for (const entry of periodEntries) {
        const reportDateKey = String(entry?.reportDateKey || "");
        const hours = Number(entry?.hours) || 0;
        const date = parseDateKey(reportDateKey);
        const day = date ? date.getUTCDay() : -1;
        if (day === 6) saturdayHours += hours;
        else if (day === 0) sundayHours += hours;
        else weekdayHours += hours;
        totalPaidHours += hours * dayMultiplierFromDateKey(reportDateKey);
      }
      return {
        periodStartKey,
        periodEndKey: shiftDateKey(periodStartKey, 13) || periodStartKey,
        totalHours: sumHours(periodEntries),
        totalPaidHours,
        saturdayHours,
        sundayHours,
        weekdayHours,
        entries: periodEntries,
      };
    })
    .sort((a, b) => (a.periodStartKey < b.periodStartKey ? -1 : a.periodStartKey > b.periodStartKey ? 1 : 0));

  const totalPaidHours = sorted.reduce((total, entry) => {
    const reportDateKey = String(entry?.reportDateKey || "");
    const hours = Number(entry?.hours) || 0;
    return total + hours * dayMultiplierFromDateKey(reportDateKey);
  }, 0);

  return {
    totalHours: sumHours(sorted),
    totalPaidHours,
    totalEntries: sorted.length,
    dailyTotals,
    weeklyTotals,
    monthlyTotals,
    paidPeriodTotals,
    labourerTotals,
    entries: sorted,
  };
}

module.exports = {
  COL_LABOURERS,
  COL_LABOUR_ENTRIES,
  normalizeLabourerName,
  normalizeLabourEntryText,
  normalizeLabourerPhone,
  parseLabourHoursCommand,
  parseLabourHoursBalanceQuery,
  getDateKeyRangeForBalanceQuery,
  formatLabourBalanceReply,
  buildLabourEntryDoc,
  writeLabourEntry,
  loadLabourEntries,
  sumHours,
  buildLabourRollup,
  normalizeLabourRangeKeys,
  monthKeyFromDateKey,
  weeklyKeyFromDateKey,
  biweeklyPayPeriodStartKeyFromDateKey,
  dayMultiplierFromDateKey,
  startOfWeekFromDateKey,
  endOfMonthFromDateKey,
};
