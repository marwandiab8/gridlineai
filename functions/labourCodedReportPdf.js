// Labour hours by activity code: a "Daily Summary" sheet per labourer per day (hours by category, then the
// detailed breakdown), with a summary page first when the report covers more than one sheet. Only entries the
// supervisor has approved are included; the rest are listed as left out, so nothing is billed unreviewed.
const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");
const { LABOUR_CATEGORIES, getLabourActivity, getLabourCategory } = require("./labourActivityCodes");
const { labourLinesForEntry } = require("./labourRepository");

function minutesOf(entry) {
  const minutes = Number(entry && entry.minutesWorked);
  if (Number.isFinite(minutes) && minutes > 0) return Math.round(minutes);
  const hours = Number(entry && entry.hours);
  return Number.isFinite(hours) && hours > 0 ? Math.round(hours * 60) : 0;
}

function hoursText(minutes) {
  return ((Number(minutes) || 0) / 60).toFixed(2);
}

function labourerLabel(entry) {
  return String((entry && (entry.labourerName || entry.labourerPhone)) || "Unknown").trim();
}

/** Everything the PDF shows, worked out from the entries (no drawing), so it can be checked in tests. */
function buildCodedLabourReport(entries) {
  const sheets = [];
  const pending = [];
  const totals = new Map(LABOUR_CATEGORIES.map((c) => [c.id, 0]));
  for (const entry of entries || []) {
    const minutes = minutesOf(entry);
    const lines = labourLinesForEntry(entry);
    const approved = entry && entry.review && entry.review.status === "approved";
    const uncoded = lines.some((line) => !getLabourActivity(line.code));
    const linesMinutes = lines.reduce((sum, line) => sum + (Math.round(Number(line.minutes)) || 0), 0);
    if (!approved || uncoded || linesMinutes !== minutes) {
      pending.push({ labourer: labourerLabel(entry), dateKey: entry.reportDateKey || "", minutes });
      continue;
    }
    const byCategory = new Map(LABOUR_CATEGORIES.map((c) => [c.id, 0]));
    const rows = lines.map((line) => {
      const activity = getLabourActivity(line.code);
      byCategory.set(activity.category, byCategory.get(activity.category) + line.minutes);
      return {
        category: getLabourCategory(activity.category).label,
        activity: activity.label,
        code: activity.code,
        description: activity.description,
        note: String(line.text || "").trim(),
        minutes: line.minutes,
      };
    });
    for (const [id, value] of byCategory) totals.set(id, totals.get(id) + value);
    sheets.push({
      labourer: labourerLabel(entry),
      phone: entry.labourerPhone || "",
      dateKey: entry.reportDateKey || "",
      projectSlug: entry.projectSlug || "",
      minutes,
      byCategory,
      rows,
    });
  }
  sheets.sort((a, b) => a.dateKey.localeCompare(b.dateKey) || a.labourer.localeCompare(b.labourer));
  pending.sort((a, b) => a.dateKey.localeCompare(b.dateKey) || a.labourer.localeCompare(b.labourer));
  const totalMinutes = sheets.reduce((sum, sheet) => sum + sheet.minutes, 0);
  return { sheets, pending, totals, totalMinutes };
}

// The PDF's built-in fonts only cover Latin-1; swap typographic marks and drop anything else (emoji etc.).
function pdfSafe(value) {
  return String(value == null ? "" : value)
    .replace(/[\u2018\u2019]/g, "'").replace(/[\u201c\u201d]/g, '"').replace(/[\u2013\u2014]/g, "-")
    .replace(/[^\x20-\x7e\xa0-\xff]/g, "");
}

function wrap(text, font, size, width) {
  const words = pdfSafe(text).split(/\s+/).filter(Boolean);
  const lines = [];
  let line = "";
  for (const word of words) {
    const next = line ? `${line} ${word}` : word;
    if (!line || font.widthOfTextAtSize(next, size) <= width) line = next;
    else {
      lines.push(line);
      line = word;
    }
  }
  if (line) lines.push(line);
  return lines.length ? lines : [""];
}

async function renderCodedLabourReportPdf(report, { title = "Labour Hours by Activity", rangeLabel = "", projectLabel = "", supervisor = "" } = {}) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const ink = rgb(0.1, 0.11, 0.14);
  const muted = rgb(0.4, 0.43, 0.48);
  const ruleColor = rgb(0.8, 0.82, 0.86);
  const headFill = rgb(0.93, 0.94, 0.96);
  const W = 612;
  const H = 792;
  const M = 48;
  let page;
  let y;

  function newPage() {
    page = pdf.addPage([W, H]);
    y = H - M;
  }
  function text(value, { size = 10, f = font, color = ink, x = M } = {}) {
    page.drawText(pdfSafe(value), { x, y, size, font: f, color });
  }
  function line(label, value) {
    text(label, { f: bold, size: 11 });
    text(value, { size: 11, x: M + bold.widthOfTextAtSize(label, 11) + 4 });
    y -= 17;
  }
  // columns: [{ title, width, align }]; rows: arrays of cell strings (a cell may be { main, sub }).
  function table(columns, rows, { totalRow = null } = {}) {
    const pad = 5;
    const size = 9.5;
    const drawRow = (cells, { header = false, strong = false } = {}) => {
      const wrapped = cells.map((cell, i) => {
        const main = typeof cell === "object" && cell ? cell.main : cell;
        const sub = typeof cell === "object" && cell ? cell.sub : "";
        const w = columns[i].width - pad * 2;
        return {
          main: wrap(main, header || strong ? bold : font, size, w),
          sub: sub ? wrap(sub, font, size - 1, w) : [],
        };
      });
      const height = Math.max(...wrapped.map((c) => c.main.length * (size + 3) + c.sub.length * (size + 2))) + pad * 2;
      if (y - height < M) {
        newPage();
        if (!header) drawRow(columns.map((c) => c.title), { header: true });
      }
      let x = M;
      if (header) page.drawRectangle({ x: M, y: y - height, width: columns.reduce((s, c) => s + c.width, 0), height, color: headFill });
      wrapped.forEach((cell, i) => {
        const col = columns[i];
        let ty = y - pad - size;
        for (const l of cell.main) {
          const f = header || strong ? bold : font;
          const tx = col.align === "right" ? x + col.width - pad - f.widthOfTextAtSize(l, size) : x + pad;
          page.drawText(l, { x: tx, y: ty, size, font: f, color: ink });
          ty -= size + 3;
        }
        for (const l of cell.sub) {
          page.drawText(l, { x: x + pad, y: ty, size: size - 1, font, color: muted });
          ty -= size + 2;
        }
        x += col.width;
      });
      y -= height;
      page.drawLine({ start: { x: M, y }, end: { x: W - M, y }, thickness: 0.5, color: ruleColor });
    };
    drawRow(columns.map((c) => c.title), { header: true });
    rows.forEach((row) => drawRow(row));
    if (totalRow) drawRow(totalRow, { strong: true });
    y -= 14;
  }

  const contentW = W - M * 2;
  const chargeableNote = LABOUR_CATEGORIES.map((c) => `${c.label}: ${c.note}`).join("   ·   ");

  if (report.sheets.length > 1 || !report.sheets.length) {
    newPage();
    text(title, { f: bold, size: 18 });
    y -= 24;
    if (rangeLabel) line("Dates:", rangeLabel);
    if (projectLabel) line("Project:", projectLabel);
    if (supervisor) line("Supervisor:", supervisor);
    y -= 6;
    const catW = 82;
    const columns = [
      { title: "Date", width: 76 },
      { title: "Labourer", width: contentW - 76 - catW * 3 - 56 },
      ...LABOUR_CATEGORIES.map((c) => ({ title: c.label, width: catW, align: "right" })),
      { title: "Total", width: 56, align: "right" },
    ];
    table(
      columns,
      report.sheets.map((s) => [s.dateKey, s.labourer, ...LABOUR_CATEGORIES.map((c) => hoursText(s.byCategory.get(c.id))), hoursText(s.minutes)]),
      { totalRow: ["Total", "", ...LABOUR_CATEGORIES.map((c) => hoursText(report.totals.get(c.id))), hoursText(report.totalMinutes)] },
    );
    for (const l of wrap(chargeableNote, font, 9, contentW)) {
      text(l, { size: 9, color: muted });
      y -= 13;
    }
    if (report.pending.length) {
      y -= 6;
      text("Not included - waiting for supervisor review:", { f: bold, size: 10 });
      y -= 15;
      for (const p of report.pending) {
        if (y < M) newPage();
        text(`${p.dateKey}  ${p.labourer}  ${hoursText(p.minutes)} h`, { size: 9.5, color: muted });
        y -= 13;
      }
    }
  }

  for (const sheet of report.sheets) {
    newPage();
    text("Daily Summary", { f: bold, size: 18 });
    y -= 26;
    line("Labourer:", sheet.labourer);
    line("Date:", sheet.dateKey);
    if (supervisor) line("Supervisor:", supervisor);
    if (sheet.projectSlug) line("Project:", sheet.projectSlug);
    y -= 8;
    text("Hours by Category", { f: bold, size: 13 });
    y -= 10;
    table(
      [{ title: "Category", width: contentW - 90 }, { title: "Hours", width: 90, align: "right" }],
      LABOUR_CATEGORIES.filter((c) => sheet.byCategory.get(c.id) > 0).map((c) => [`${c.label} (${c.note})`, hoursText(sheet.byCategory.get(c.id))]),
    );
    text(`Total Daily Hours: ${hoursText(sheet.minutes)}`, { f: bold, size: 12 });
    y -= 26;
    text("Detailed Breakdown", { f: bold, size: 13 });
    y -= 10;
    table(
      [
        { title: "Category", width: 108 },
        { title: "Activity", width: 140 },
        { title: "Description", width: contentW - 108 - 140 - 60 },
        { title: "Hours", width: 60, align: "right" },
      ],
      sheet.rows.map((r) => [r.category, { main: r.activity, sub: r.code }, { main: r.description, sub: r.note }, hoursText(r.minutes)]),
    );
  }

  return pdf.save();
}

module.exports = { buildCodedLabourReport, pdfSafe, renderCodedLabourReportPdf };
