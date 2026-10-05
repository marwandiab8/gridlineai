// What happens when the supervisor approves (or un-approves) a labour entry:
//  - that labourer's Daily Summary PDF for the day is (re)built and stored as a management-only report;
//  - the project's running Winter Heat workbook is rebuilt from every approved entry of the season.
// Both live in `labourBillingReports`, which only management can read: they carry pay hours and owner billing.
const ExcelJS = require("exceljs");
const { LABOUR_CATEGORIES, getLabourActivity, getLabourCategory } = require("./labourActivityCodes");
const { buildCodedLabourReport, renderCodedLabourReportPdf } = require("./labourCodedReportPdf");
const { labourLinesForEntry, loadLabourEntries } = require("./labourRepository");

const COL_LABOUR_BILLING_REPORTS = "labourBillingReports";
// Winter heat season the workbook covers; entries before this date are not in it. Extra work started Oct 2, 2026.
const WINTER_HEAT_SEASON_START = "2026-10-02";
const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function safeName(value) {
  return String(value || "").replace(/[^A-Za-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "Labourer";
}

function hours(minutes) {
  return Math.round((Number(minutes) || 0) / 60 * 100) / 100;
}

function dailySummaryId(entryId) {
  return `daily-${entryId}`;
}

function workbookId(projectSlug) {
  return `winter-heat-${projectSlug || "all"}`;
}

/** Who approved: their name when known, else their email. */
function approverName(review, approverNames = {}) {
  if (!review) return "";
  const email = String(review.byEmail || "");
  return String(review.byName || approverNames[email.toLowerCase()] || email).trim();
}

/** Builds and stores the Daily Summary PDF for one approved entry. Returns the report record. */
async function publishLabourDailySummary({ db, bucket, FieldValue, entryId, entry, supervisor = "", approvedByEmail = null }) {
  const report = buildCodedLabourReport([{ ...entry, id: entryId }]);
  if (report.sheets.length !== 1) throw new Error("The entry is not approved and fully coded.");
  const sheet = report.sheets[0];
  const bytes = await renderCodedLabourReportPdf(report, { supervisor });
  const fileName = `Log_${safeName(sheet.labourer)}_${sheet.dateKey}.pdf`;
  const storagePath = `labour-daily/${entry.projectSlug || "unassigned"}/${sheet.dateKey}/${entryId}/${fileName}`;
  await bucket.file(storagePath).save(Buffer.from(bytes), {
    contentType: "application/pdf",
    contentDisposition: `attachment; filename="${fileName}"`,
  });
  const record = {
    type: "labourDailySummary",
    entryId,
    labourerName: sheet.labourer,
    labourerPhone: entry.labourerPhone || null,
    projectSlug: entry.projectSlug || null,
    dateKey: sheet.dateKey,
    totalHours: hours(sheet.minutes),
    hoursByCategory: Object.fromEntries(LABOUR_CATEGORIES.map((c) => [c.id, hours(sheet.byCategory.get(c.id))])),
    winterHeatHours: hours(sheet.byCategory.get("winter-heat")),
    supervisor: supervisor || null,
    approvedByEmail,
    storagePath,
    reportFileName: fileName,
    updatedAt: FieldValue.serverTimestamp(),
  };
  const ref = db.collection(COL_LABOUR_BILLING_REPORTS).doc(dailySummaryId(entryId));
  const previous = await ref.get();
  await ref.set({ ...record, createdAt: previous.exists ? previous.get("createdAt") : FieldValue.serverTimestamp() });
  return record;
}

/** Removes the Daily Summary of an entry that is no longer approved, so no stale sheet stays listed. */
async function withdrawLabourDailySummary({ db, bucket, entryId }) {
  const ref = db.collection(COL_LABOUR_BILLING_REPORTS).doc(dailySummaryId(entryId));
  const snap = await ref.get();
  if (!snap.exists) return false;
  const storagePath = snap.get("storagePath");
  await ref.delete();
  if (storagePath) await bucket.file(storagePath).delete({ ignoreNotFound: true });
  return true;
}

function headerRow(sheet, values) {
  const row = sheet.addRow(values);
  row.font = { bold: true };
  row.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFE8ECF2" } };
  row.alignment = { vertical: "middle", wrapText: true };
  return row;
}

function timestampText(value) {
  if (!value) return "";
  const date = typeof value.toDate === "function" ? value.toDate() : value instanceof Date ? value : null;
  if (!date) return "";
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(date).replace(",", "");
}

/**
 * The Winter Heat verification workbook. Every number on "Daily Winter Heat" and "By Labourer" is a SUMIFS
 * formula over the "Detail" sheet (with its value cached), so a reviewer can trace each hour to a line.
 */
// `approverNames` (email -> name) names the approver on entries approved before the name was kept on the review.
async function buildWinterHeatWorkbook(entries, { projectSlug = "", seasonStartKey = WINTER_HEAT_SEASON_START, now = new Date(), approverNames = {} } = {}) {
  const inSeason = (entries || []).filter((e) => e && e.review && String(e.reportDateKey || "") >= seasonStartKey);
  const approvedIds = new Set();
  const details = [];
  for (const entry of inSeason) {
    // Same test as the PDF: approved, every line coded, lines adding up to the entry's hours.
    const sheetMatch = buildCodedLabourReport([entry]).sheets[0];
    if (!sheetMatch) continue;
    approvedIds.add(entry.id);
    for (const line of labourLinesForEntry(entry)) {
      const activity = getLabourActivity(line.code);
      const category = getLabourCategory(activity.category);
      details.push({
        date: entry.reportDateKey,
        labourer: sheetMatch.labourer,
        category: category.label,
        chargeable: category.chargeable ? "Yes" : "No",
        keyword: activity.keyword,
        code: activity.code,
        activity: activity.label,
        description: activity.description,
        location: String(line.location || ""),
        note: String(line.text || ""),
        hours: hours(line.minutes),
        approvedBy: approverName(entry.review, approverNames),
        approvedAt: timestampText(entry.review && entry.review.at),
        entryId: entry.id,
      });
    }
  }
  details.sort((a, b) => a.date.localeCompare(b.date) || a.labourer.localeCompare(b.labourer));
  const pending = inSeason.filter((e) => !approvedIds.has(e.id))
    .sort((a, b) => String(a.reportDateKey).localeCompare(String(b.reportDateKey)));

  const wb = new ExcelJS.Workbook();
  wb.creator = "GridlineAI";
  wb.created = now;
  // Cached values are stored with each formula, but zeros are not kept by the writer: recalculate on open.
  wb.calcProperties.fullCalcOnLoad = true;
  const labourers = [...new Set(details.map((d) => d.labourer))].sort();
  const dates = [...new Set(details.map((d) => d.date))].sort();
  const n = details.length;
  const last = Math.max(n + 1, 2);
  const col = (letter) => `Detail!$${letter}$2:$${letter}$${last}`;
  const DATE = col("A");
  const WHO = col("B");
  const CAT = col("C");
  const HRS = col("K");

  // Daily Winter Heat (first sheet: what the owner's reviewer opens).
  const daily = wb.addWorksheet("Daily Winter Heat", { views: [{ state: "frozen", ySplit: 4 }] });
  daily.addRow([`Winter Heat (extra) - ${projectSlug || "all projects"}`]).font = { bold: true, size: 14 };
  daily.addRow([`Approved hours from ${seasonStartKey}. Updated ${timestampText(now)}. Each hour traces to a line on the Detail sheet.`]);
  daily.addRow([]);
  headerRow(daily, ["Date", "Day", ...labourers, "Day total", "Running total"]);
  const firstLabourerCol = 3;
  const dayTotalCol = firstLabourerCol + labourers.length;
  const colLetter = (i) => daily.getColumn(i).letter;
  let running = 0;
  dates.forEach((date, i) => {
    const r = 5 + i;
    const perLabourer = labourers.map((who) => details.filter((d) => d.date === date && d.labourer === who && d.category === "Winter Heat").reduce((s, d) => s + d.hours, 0));
    const dayTotal = Math.round(perLabourer.reduce((s, v) => s + v, 0) * 100) / 100;
    running = Math.round((running + dayTotal) * 100) / 100;
    const [y, m, d] = date.split("-").map(Number);
    const row = daily.addRow([
      date,
      WEEKDAYS[new Date(Date.UTC(y, m - 1, d)).getUTCDay()],
      ...labourers.map((who, k) => ({
        formula: `SUMIFS(${HRS},${DATE},$A${r},${WHO},${colLetter(firstLabourerCol + k)}$4,${CAT},"Winter Heat")`,
        result: Math.round(perLabourer[k] * 100) / 100,
      })),
      { formula: `SUM(${colLetter(firstLabourerCol)}${r}:${colLetter(dayTotalCol - 1)}${r})`, result: dayTotal },
      { formula: i === 0 ? `${colLetter(dayTotalCol)}${r}` : `${colLetter(dayTotalCol + 1)}${r - 1}+${colLetter(dayTotalCol)}${r}`, result: running },
    ]);
    row.getCell(dayTotalCol).font = { bold: true };
  });
  const totalRowNumber = 5 + dates.length;
  if (dates.length) {
    const sumOf = (c) => ({ formula: `SUM(${colLetter(c)}5:${colLetter(c)}${totalRowNumber - 1})`, result: Math.round(details.filter((d) => d.category === "Winter Heat" && (c === dayTotalCol || d.labourer === labourers[c - firstLabourerCol])).reduce((s, d) => s + d.hours, 0) * 100) / 100 });
    const totals = daily.addRow(["Total", "", ...labourers.map((_, k) => sumOf(firstLabourerCol + k)), sumOf(dayTotalCol), ""]);
    totals.font = { bold: true };
  } else {
    daily.addRow(["No approved hours yet."]);
  }
  daily.getColumn(1).width = 12;
  daily.getColumn(2).width = 6;
  for (let c = firstLabourerCol; c <= dayTotalCol + 1; c += 1) {
    daily.getColumn(c).width = 16;
    daily.getColumn(c).numFmt = "0.00";
  }

  // By Labourer: season totals per category.
  const byLabourer = wb.addWorksheet("By Labourer", { views: [{ state: "frozen", ySplit: 1 }] });
  headerRow(byLabourer, ["Labourer", ...LABOUR_CATEGORIES.map((c) => `${c.label} (${c.note})`), "Total"]);
  labourers.forEach((who, i) => {
    const r = i + 2;
    const values = LABOUR_CATEGORIES.map((c) => Math.round(details.filter((d) => d.labourer === who && d.category === c.label).reduce((s, d) => s + d.hours, 0) * 100) / 100);
    byLabourer.addRow([
      who,
      ...LABOUR_CATEGORIES.map((c, k) => ({ formula: `SUMIFS(${HRS},${WHO},$A${r},${CAT},"${c.label}")`, result: values[k] })),
      { formula: `SUM(B${r}:${byLabourer.getColumn(LABOUR_CATEGORIES.length + 1).letter}${r})`, result: Math.round(values.reduce((s, v) => s + v, 0) * 100) / 100 },
    ]);
  });
  byLabourer.getColumn(1).width = 22;
  for (let c = 2; c <= LABOUR_CATEGORIES.length + 2; c += 1) {
    byLabourer.getColumn(c).width = 22;
    byLabourer.getColumn(c).numFmt = "0.00";
  }

  // Detail: one row per approved line.
  const detail = wb.addWorksheet("Detail", { views: [{ state: "frozen", ySplit: 1 }] });
  headerRow(detail, ["Date", "Labourer", "Category", "Extra", "Keyword", "Code", "Activity", "Description", "Location", "Note", "Hours", "Approved by", "Approved at", "Entry id"]);
  for (const d of details) {
    detail.addRow([d.date, d.labourer, d.category, d.chargeable, d.keyword, d.code, d.activity, d.description, d.location, d.note, d.hours, d.approvedBy, d.approvedAt, d.entryId]);
  }
  [11, 18, 18, 9, 18, 18, 30, 60, 28, 24, 8, 26, 17, 22].forEach((w, i) => { detail.getColumn(i + 1).width = w; });
  detail.getColumn(8).alignment = { wrapText: true, vertical: "top" };
  detail.getColumn(11).numFmt = "0.00";
  if (n) detail.autoFilter = { from: "A1", to: `N${n + 1}` };

  // Waiting for review: not counted anywhere above.
  const waiting = wb.addWorksheet("Waiting for Review", { views: [{ state: "frozen", ySplit: 1 }] });
  headerRow(waiting, ["Date", "Labourer", "Hours", "What they wrote", "Entry id"]);
  for (const e of pending) {
    waiting.addRow([e.reportDateKey, String(e.labourerName || e.labourerPhone || ""), hours(e.minutesWorked), String(e.workOn || ""), e.id]);
  }
  [11, 18, 8, 80, 22].forEach((w, i) => { waiting.getColumn(i + 1).width = w; });

  const buffer = await wb.xlsx.writeBuffer();
  return {
    buffer: Buffer.from(buffer),
    winterHeatHours: Math.round(details.filter((d) => d.category === "Winter Heat").reduce((s, d) => s + d.hours, 0) * 100) / 100,
    approvedEntries: approvedIds.size,
    pendingEntries: pending.length,
    days: dates.length,
  };
}

/** Rebuilds the project's season workbook from Firestore and stores it. */
async function rebuildWinterHeatWorkbook({ db, bucket, FieldValue, projectSlug, now = new Date() }) {
  const entries = await loadLabourEntries(db, { startKey: WINTER_HEAT_SEASON_START, endKey: "9999-12-31", projectSlug: projectSlug || null });
  // Names the Daily Summaries already hold, for entries approved before the review kept the name.
  const approverNames = {};
  const summaries = await db.collection(COL_LABOUR_BILLING_REPORTS).where("type", "==", "labourDailySummary").get();
  summaries.forEach((doc) => {
    const d = doc.data() || {};
    if (d.approvedByEmail && d.supervisor && !String(d.supervisor).includes("@")) approverNames[String(d.approvedByEmail).toLowerCase()] = d.supervisor;
  });
  const built = await buildWinterHeatWorkbook(entries, { projectSlug, now, approverNames });
  const fileName = `Winter_Heat_${safeName(projectSlug || "all")}.xlsx`;
  const storagePath = `labour-winter-heat/${projectSlug || "all"}/${fileName}`;
  await bucket.file(storagePath).save(built.buffer, {
    contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    contentDisposition: `attachment; filename="${fileName}"`,
  });
  await db.collection(COL_LABOUR_BILLING_REPORTS).doc(workbookId(projectSlug)).set({
    type: "winterHeatWorkbook",
    projectSlug: projectSlug || null,
    seasonStartKey: WINTER_HEAT_SEASON_START,
    winterHeatHours: built.winterHeatHours,
    approvedEntries: built.approvedEntries,
    pendingEntries: built.pendingEntries,
    days: built.days,
    storagePath,
    reportFileName: fileName,
    updatedAt: FieldValue.serverTimestamp(),
    createdAt: FieldValue.serverTimestamp(),
  });
  return built;
}

module.exports = {
  COL_LABOUR_BILLING_REPORTS,
  WINTER_HEAT_SEASON_START,
  buildWinterHeatWorkbook,
  dailySummaryId,
  publishLabourDailySummary,
  rebuildWinterHeatWorkbook,
  withdrawLabourDailySummary,
  workbookId,
};
