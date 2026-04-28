const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");

function wrapText(text, font, size, maxWidth) {
  const raw = String(text || "").trim();
  if (!raw) return [""];
  const words = raw.split(/\s+/);
  const lines = [];
  let line = "";
  for (const word of words) {
    const candidate = line ? `${line} ${word}` : word;
    if (font.widthOfTextAtSize(candidate, size) <= maxWidth || !line) {
      line = candidate;
      continue;
    }
    lines.push(line);
    line = word;
  }
  if (line) lines.push(line);
  return lines.length ? lines : [""];
}

function formatHours(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0.00";
  return n % 1 === 0 ? String(n) : n.toFixed(2).replace(/0+$/, "").replace(/\.$/, "");
}

function formatRangeLabel(startKey, endKey) {
  if (!startKey && !endKey) return "All time";
  if (startKey && endKey && startKey === endKey) return startKey;
  if (startKey && endKey) return `${startKey} to ${endKey}`;
  return startKey || endKey || "All time";
}

function formatPeriodLabel(startKey, endKey) {
  return formatRangeLabel(startKey, endKey);
}

function formatMultiplierBreakdown(hours, multiplier) {
  const h = Number(hours) || 0;
  const m = Number(multiplier) || 1;
  if (!h) return `0h x ${m} = 0 paid`;
  const paid = h * m;
  return `${formatHours(h)}h x ${m} = ${formatHours(paid)} paid`;
}

function monthKeyFromDateKey(dateKey) {
  const raw = String(dateKey || "").trim();
  return raw ? raw.slice(0, 7) : "";
}

function weeklyKeyFromDateKey(dateKey) {
  const raw = String(dateKey || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return "";
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  if (Number.isNaN(date.getTime())) return "";
  const day = date.getUTCDay();
  const mondayOffset = day === 0 ? 6 : day - 1;
  date.setUTCDate(date.getUTCDate() - mondayOffset);
  return date.toISOString().slice(0, 10);
}

function dayMultiplierForReportDateKey(dateKey) {
  const raw = String(dateKey || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return 1;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  if (Number.isNaN(date.getTime())) return 1;
  const day = date.getUTCDay();
  if (day === 6) return 1.5;
  if (day === 0) return 2;
  return 1;
}

async function generateLabourReportPdf({
  pdfTitle,
  subtitle,
  summary,
  entries,
  storageBucket,
  storagePath,
  downloadToken,
}) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const pageW = 612;
  const pageH = 792;
  const margin = 44;
  const contentW = pageW - margin * 2;
  const lineH = 12;
  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - margin;

  const colors = {
    ink: rgb(0.14, 0.15, 0.2),
    muted: rgb(0.42, 0.45, 0.5),
    rule: rgb(0.78, 0.8, 0.84),
    accent: rgb(0.28, 0.36, 0.5),
  };

  function newPage() {
    page = pdf.addPage([pageW, pageH]);
    y = pageH - margin;
  }

  function ensure(space) {
    if (y - space < margin) newPage();
  }

  function draw(text, size = 10, bold = false, color = colors.ink, x = margin, width = contentW) {
    const lines = wrapText(text, bold ? fontBold : font, size, width);
    for (const line of lines) {
      ensure(size + 6);
      page.drawText(line, { x, y, size, font: bold ? fontBold : font, color });
      y -= size + 4;
    }
  }

  function rule() {
    ensure(10);
    page.drawLine({
      start: { x: margin, y: y - 2 },
      end: { x: pageW - margin, y: y - 2 },
      thickness: 0.6,
      color: colors.rule,
    });
    y -= 10;
  }

  function section(title) {
    y -= 4;
    rule();
    draw(title, 13, true, colors.ink);
    y -= 2;
  }

  function drawKeyValue(label, value) {
    ensure(18);
    page.drawText(label, { x: margin, y, size: 8.5, font: fontBold, color: colors.accent });
    draw(value || "—", 9.5, false, colors.ink, margin + 110, contentW - 110);
  }

  draw(pdfTitle, 18, true, colors.ink);
  if (subtitle) draw(subtitle, 10.5, false, colors.muted);
  y -= 4;
  drawKeyValue("Range", formatRangeLabel(summary.startKey, summary.endKey));
  drawKeyValue("Total hours", formatHours(summary.totalHours));
  drawKeyValue("Total paid hours", formatHours(summary.totalPaidHours));
  drawKeyValue("Total entries", String(summary.totalEntries || 0));
  y -= 6;

  section("Daily Totals");
  for (const day of summary.dailyTotals || []) {
    ensure(20);
    page.drawText(`${day.reportDateKey}`, { x: margin, y, size: 10, font: fontBold, color: colors.ink });
    page.drawText(`${formatHours(day.totalHours)} hours`, {
      x: pageW - margin - 100,
      y,
      size: 10,
      font: fontBold,
      color: colors.accent,
    });
    y -= 14;
    for (const item of day.entries || []) {
      const line = [
        item.labourerName || item.labourerPhone || "Unknown",
        `${formatHours(item.hours)}h`,
        item.workOn || "",
      ].filter(Boolean).join(" · ");
      draw(line, 9.2, false, colors.muted, margin + 10, contentW - 12);
    }
    y -= 4;
  }

  section("Paid Period Totals (Biweekly)");
  draw("Pay cycle anchor: Sunday 2026-04-26 · Saturday=1.5x · Sunday=2x", 9, false, colors.muted);
  for (const period of summary.paidPeriodTotals || []) {
    ensure(24);
    page.drawText(
      formatPeriodLabel(period.periodStartKey, period.periodEndKey),
      { x: margin, y, size: 10, font: fontBold, color: colors.ink }
    );
    page.drawText(`${formatHours(period.totalPaidHours)} paid`, {
      x: pageW - margin - 100,
      y,
      size: 10,
      font: fontBold,
      color: colors.accent,
    });
    y -= 14;
    draw(
      [
        `Base: ${formatHours(period.totalHours)}h`,
        `Weekday: ${formatHours(period.weekdayHours)}h`,
        `Saturday: ${formatMultiplierBreakdown(period.saturdayHours, 1.5)}`,
        `Sunday: ${formatMultiplierBreakdown(period.sundayHours, 2)}`,
      ].join(" · "),
      9.2,
      false,
      colors.muted,
      margin + 10,
      contentW - 12
    );
    y -= 4;
  }

  section("Weekly Totals");
  for (const week of summary.weeklyTotals || []) {
    ensure(20);
    page.drawText(
      formatPeriodLabel(week.weekStartKey, week.weekEndKey),
      { x: margin, y, size: 10, font: fontBold, color: colors.ink }
    );
    page.drawText(`${formatHours(week.totalHours)} hours`, {
      x: pageW - margin - 100,
      y,
      size: 10,
      font: fontBold,
      color: colors.accent,
    });
    y -= 14;
    draw(`${String((week.entries || []).length || 0)} entries in this week`, 9.2, false, colors.muted, margin + 10, contentW - 12);
    y -= 4;
  }

  section("Monthly Totals");
  for (const month of summary.monthlyTotals || []) {
    ensure(20);
    page.drawText(
      formatPeriodLabel(month.monthStartKey, month.monthEndKey),
      { x: margin, y, size: 10, font: fontBold, color: colors.ink }
    );
    page.drawText(`${formatHours(month.totalHours)} hours`, {
      x: pageW - margin - 100,
      y,
      size: 10,
      font: fontBold,
      color: colors.accent,
    });
    y -= 14;
    draw(`${String((month.entries || []).length || 0)} entries in this month`, 9.2, false, colors.muted, margin + 10, contentW - 12);
    y -= 4;
  }

  section("Labourers");
  for (const labourer of summary.labourerTotals || []) {
    ensure(20);
    page.drawText(labourer.labourer, { x: margin, y, size: 10, font: fontBold, color: colors.ink });
    page.drawText(`${formatHours(labourer.totalPaidHours || labourer.totalHours)} paid`, {
      x: pageW - margin - 100,
      y,
      size: 10,
      font: fontBold,
      color: colors.accent,
    });
    y -= 14;
    for (const item of labourer.entries || []) {
      const multiplier = dayMultiplierForReportDateKey(item.reportDateKey || "");
      const line = `${item.reportDateKey || "-"} · ${formatHours(item.hours)}h (${formatMultiplierBreakdown(item.hours, multiplier)}) · ${item.projectSlug || "-"} · ${item.workOn || ""}`;
      draw(line, 9.2, false, colors.muted, margin + 10, contentW - 12);
    }
    y -= 4;
  }

  section("Detailed Entries");
  for (const item of entries || []) {
    ensure(26);
    draw(
      `${item.reportDateKey || "-"} · ${item.labourerName || item.labourerPhone || "Unknown"} · ${formatHours(item.hours)}h`,
      9.5,
      true,
      colors.ink
    );
    draw(`${item.projectSlug || "-"} · ${item.workOn || ""}`, 9.5, false, colors.muted);
    if (item.notes) draw(item.notes, 9.1, false, colors.muted);
    y -= 4;
  }

  const bytes = await pdf.save();
  const file = storageBucket.file(storagePath);
  await file.save(Buffer.from(bytes), {
    contentType: "application/pdf",
    metadata: {
      metadata: { firebaseStorageDownloadTokens: downloadToken },
    },
  });

  return {
    storagePath,
    downloadURL: `https://firebasestorage.googleapis.com/v0/b/${encodeURIComponent(
      storageBucket.name
    )}/o/${encodeURIComponent(storagePath)}?alt=media&token=${encodeURIComponent(downloadToken)}`,
  };
}

module.exports = {
  generateLabourReportPdf,
  formatRangeLabel,
  monthKeyFromDateKey,
  weeklyKeyFromDateKey,
};
