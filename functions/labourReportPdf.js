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

function parseDateKey(dateKey) {
  const raw = String(dateKey || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  const d = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function formatLongDateFromKey(dateKey) {
  const date = parseDateKey(dateKey);
  if (!date) return String(dateKey || "").trim() || "-";
  const weekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
  const months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ];
  const dow = weekdays[date.getUTCDay()] || "";
  const mon = months[date.getUTCMonth()] || "";
  const day = date.getUTCDate();
  const year = date.getUTCFullYear();
  return `${dow} ${mon} ${day} ${year}`.trim();
}

function shiftDateKey(dateKey, deltaDays) {
  const date = parseDateKey(dateKey);
  if (!date || !Number.isFinite(deltaDays)) return "";
  date.setUTCDate(date.getUTCDate() + Number(deltaDays));
  return date.toISOString().slice(0, 10);
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

function dayMultiplierForReportDateKey() {
  // Premium rules are summarized in the weekly/pay-period breakdown, not per-day here.
  return 1;
}

function isSundayDateKey(dateKey) {
  const d = parseDateKey(dateKey);
  return !!(d && d.getUTCDay && d.getUTCDay() === 0);
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
  const margin = 44;
  const portrait = { width: 612, height: 792 };
  const landscape = { width: 792, height: 612 };
  let pageW = portrait.width;
  let pageH = portrait.height;
  let contentW = pageW - margin * 2;
  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - margin;
  let pageLayout = "portrait";

  const colors = {
    ink: rgb(0.14, 0.15, 0.2),
    muted: rgb(0.42, 0.45, 0.5),
    rule: rgb(0.78, 0.8, 0.84),
    accent: rgb(0.28, 0.36, 0.5),
  };

  function applyLayout(layout = "portrait") {
    pageLayout = layout === "landscape" ? "landscape" : "portrait";
    const dims = pageLayout === "landscape" ? landscape : portrait;
    pageW = dims.width;
    pageH = dims.height;
    contentW = pageW - margin * 2;
  }

  function newPage(layout = pageLayout) {
    applyLayout(layout);
    page = pdf.addPage([pageW, pageH]);
    y = pageH - margin;
  }

  function ensure(space) {
    if (y - space < margin) newPage();
  }

  function draw(text, size = 10, bold = false, color = colors.ink, x = margin, width = contentW) {
    const lines = wrapText(text, bold ? fontBold : font, size, width);
    for (const line of lines) {
      ensure(size + 10);
      page.drawText(line, { x, y, size, font: bold ? fontBold : font, color });
      y -= size + 6;
    }
  }

  function rule() {
    ensure(16);
    page.drawLine({
      start: { x: margin, y: y - 6 },
      end: { x: pageW - margin, y: y - 6 },
      thickness: 0.6,
      color: colors.rule,
    });
    y -= 16;
  }

  function section(title) {
    y -= 8;
    rule();
    draw(title, 13, true, colors.ink);
    y -= 6;
  }

  function compactDateParts(dateKey) {
    const date = parseDateKey(dateKey);
    if (!date) return ["-", String(dateKey || "").trim() || "-"];
    const weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(date.getUTCDate()).padStart(2, "0");
    return [weekdays[date.getUTCDay()] || "-", `${mm}-${dd}`];
  }

  function drawPayPeriodHoursTable(periodStartKey, periodEndKey, periodEntries) {
    const startKey = String(periodStartKey || "").trim();
    const endKey = String(periodEndKey || "").trim();
    if (!startKey || !endKey) return;

    const dayKeys = [];
    for (let i = 0; i < 14; i += 1) {
      const k = shiftDateKey(startKey, i);
      if (k) dayKeys.push(k);
    }
    if (dayKeys.length < 1) return;

    const labourerRows = new Map();
    const totalByDay = new Map();
    for (const entry of periodEntries || []) {
      const who = String(entry?.labourerName || entry?.labourerPhone || "Unknown").trim() || "Unknown";
      const dateKey = String(entry?.reportDateKey || "").trim();
      const hours = Number(entry?.hours) || 0;
      if (!dateKey || !Number.isFinite(hours) || hours <= 0) continue;
      if (!labourerRows.has(who)) labourerRows.set(who, new Map());
      const byDay = labourerRows.get(who);
      byDay.set(dateKey, (Number(byDay.get(dateKey)) || 0) + hours);
      totalByDay.set(dateKey, (Number(totalByDay.get(dateKey)) || 0) + hours);
    }

    const labourers = [...labourerRows.keys()].sort((a, b) => a.localeCompare(b));
    if (!labourers.length) {
      draw("No labour entries in this pay period.", 9.2, false, colors.muted);
      y -= 2;
      return;
    }

    newPage("landscape");
    draw("Labour Hours Matrix", 16, true, colors.ink);
    draw(`Pay Period Starting ${formatLongDateFromKey(startKey)}`, 10.5, false, colors.muted);
    draw(formatRangeLabel(startKey, endKey), 9.2, false, colors.muted);
    y -= 8;

    const headerH = 40;
    const rowH = 28;
    const colNameW = 168;
    const totalColW = 52;
    const dayColW = Math.floor((contentW - colNameW - totalColW) / dayKeys.length);
    const tableW = colNameW + totalColW + dayColW * dayKeys.length;
    const x0 = margin;
    const gridColor = colors.rule;
    const headerBg = rgb(0.965, 0.97, 0.98);
    const totalBg = rgb(0.948, 0.956, 0.97);
    const zebraBg = rgb(0.985, 0.988, 0.994);

    function drawHeader(tableTopY) {
      page.drawRectangle({
        x: x0,
        y: tableTopY - headerH,
        width: tableW,
        height: headerH,
        color: headerBg,
        borderColor: gridColor,
        borderWidth: 0.6,
      });

      for (let i = 0; i <= dayKeys.length; i += 1) {
        const x = x0 + colNameW + i * dayColW;
        page.drawLine({
          start: { x, y: tableTopY },
          end: { x, y: tableTopY - headerH },
          thickness: 0.6,
          color: gridColor,
        });
      }
      const totalX = x0 + colNameW + dayKeys.length * dayColW;
      page.drawLine({
        start: { x: totalX, y: tableTopY },
        end: { x: totalX, y: tableTopY - headerH },
        thickness: 0.6,
        color: gridColor,
      });
      page.drawLine({
        start: { x: x0 + tableW, y: tableTopY },
        end: { x: x0 + tableW, y: tableTopY - headerH },
        thickness: 0.6,
        color: gridColor,
      });

      page.drawText("Labourer", {
        x: x0 + 6,
        y: tableTopY - 24,
        size: 8.8,
        font: fontBold,
        color: colors.ink,
      });
      page.drawText("Total", {
        x: totalX + 10,
        y: tableTopY - 24,
        size: 8.8,
        font: fontBold,
        color: colors.ink,
      });

      for (let i = 0; i < dayKeys.length; i += 1) {
        const [dow, md] = compactDateParts(dayKeys[i]);
        const colX = x0 + colNameW + i * dayColW;
        const dowW = fontBold.widthOfTextAtSize(dow, 7.1);
        const mdW = font.widthOfTextAtSize(md, 6.9);
        page.drawText(dow, {
          x: colX + Math.max(2, (dayColW - dowW) / 2),
          y: tableTopY - 16,
          size: 7.1,
          font: fontBold,
          color: colors.ink,
        });
        page.drawText(md, {
          x: colX + Math.max(2, (dayColW - mdW) / 2),
          y: tableTopY - 28,
          size: 6.9,
          font,
          color: colors.muted,
        });
      }
    }

    function drawRow(tableTopY, rowIndex, labourer) {
      const rowTop = tableTopY - headerH - rowIndex * rowH;
      const rowBottom = rowTop - rowH;
      const isTotal = labourer === "Total";
      const rowBg = isTotal ? totalBg : rowIndex % 2 === 0 ? zebraBg : null;

      if (rowBg) {
        page.drawRectangle({
          x: x0,
          y: rowBottom,
          width: tableW,
          height: rowH,
          color: rowBg,
        });
      }
      page.drawRectangle({
        x: x0,
        y: rowBottom,
        width: tableW,
        height: rowH,
        borderColor: gridColor,
        borderWidth: 0.6,
      });

      for (let i = 0; i <= dayKeys.length; i += 1) {
        const x = x0 + colNameW + i * dayColW;
        page.drawLine({
          start: { x, y: rowTop },
          end: { x, y: rowBottom },
          thickness: 0.6,
          color: gridColor,
        });
      }
      const totalX = x0 + colNameW + dayKeys.length * dayColW;
      page.drawLine({
        start: { x: totalX, y: rowTop },
        end: { x: totalX, y: rowBottom },
        thickness: 0.6,
        color: gridColor,
      });
      page.drawLine({
        start: { x: x0 + tableW, y: rowTop },
        end: { x: x0 + tableW, y: rowBottom },
        thickness: 0.6,
        color: gridColor,
      });

      const nameLines = wrapText(labourer, isTotal ? fontBold : font, 8.7, colNameW - 10).slice(0, 2);
      const nameBlockH = nameLines.length * 9;
      const nameY = rowBottom + (rowH + nameBlockH) / 2 - 10;
      for (let i = 0; i < nameLines.length; i += 1) {
        page.drawText(nameLines[i], {
          x: x0 + 6,
          y: nameY - i * 9,
          size: 8.4,
          font: isTotal ? fontBold : font,
          color: isTotal ? colors.accent : colors.ink,
        });
      }

      const byDay = getRowDayMap(labourer);
      let rowTotal = 0;
      for (let i = 0; i < dayKeys.length; i += 1) {
        const k = dayKeys[i];
        const hours = Number(byDay.get(k)) || 0;
        rowTotal += hours;
        if (!hours) continue;
        const value = formatHours(hours);
        const textW = fontBold.widthOfTextAtSize(value, 7.8);
        const colX = x0 + colNameW + i * dayColW;
        page.drawText(value, {
          x: colX + Math.max(2, (dayColW - textW) / 2),
          y: rowBottom + 10,
          size: 7.8,
          font: fontBold,
          color: isTotal ? colors.ink : colors.accent,
        });
      }
      if (rowTotal) {
        const value = formatHours(rowTotal);
        const textW = fontBold.widthOfTextAtSize(value, 8.2);
        page.drawText(value, {
          x: totalX + Math.max(4, (totalColW - textW) / 2),
          y: rowBottom + 10,
          size: 8.2,
          font: fontBold,
          color: colors.ink,
        });
      }
    }

    const rows = ["Total", ...labourers];

    function getRowDayMap(labourer) {
      if (labourer === "Total") return totalByDay;
      return labourerRows.get(labourer) || new Map();
    }

    let rowCursor = 0;
    while (rowCursor < rows.length) {
      const remaining = rows.length - rowCursor;
      const minTableH = headerH + rowH + 10;
      ensure(minTableH);

      const availableH = y - margin;
      const maxRowsThisPage = Math.max(1, Math.floor((availableH - headerH - 10) / rowH));
      const rowsThisPage = Math.min(remaining, maxRowsThisPage);

      const tableTopY = y;
      drawHeader(tableTopY);

      for (let i = 0; i < rowsThisPage; i += 1) {
        const labourer = rows[rowCursor + i];
        drawRow(tableTopY, i, labourer);
      }

      // Bottom border line.
      const tableBottomY = tableTopY - headerH - rowsThisPage * rowH;
      page.drawLine({
        start: { x: x0, y: tableBottomY },
        end: { x: x0 + tableW, y: tableBottomY },
        thickness: 0.6,
        color: gridColor,
      });

      y = tableBottomY - 14;
      rowCursor += rowsThisPage;
      if (rowCursor < rows.length) newPage("landscape");
    }
    newPage("portrait");
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
  drawKeyValue("Total entries", String(summary.totalEntries || 0));
  drawKeyValue("Total hours", `${formatHours(summary.totalHours)}h`);
  drawKeyValue("Paid units", `${formatHours(summary.totalPayUnits)}h`);
  y -= 6;

  if (!Array.isArray(entries) || entries.length < 1) {
    section("Report Status");
    draw(
      "No labour entries were found for the selected filters and date range. Check the start/end dates, labourer filter, and project filter.",
      10,
      false,
      colors.muted
    );
  }

  section("Daily Totals");
  if (!summary.dailyTotals || summary.dailyTotals.length < 1) {
    draw("No daily totals in this range.", 9.2, false, colors.muted, margin + 10, contentW - 12);
    y -= 4;
  } else {
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
  }

  section("Paid Period Totals (Biweekly)");
  draw(
    "Pay cycle anchor: Saturday 2026-04-25 · 44h/week · after 12h/day @ 1.5x · Saturday/public holiday @ 1.5x · Sunday @ 2x · no pyramiding",
    9,
    false,
    colors.muted
  );
  if (!summary.paidPeriodTotals || summary.paidPeriodTotals.length < 1) {
    draw("No paid-period totals in this range.", 9.2, false, colors.muted, margin + 10, contentW - 12);
    y -= 4;
  } else {
    for (const period of summary.paidPeriodTotals || []) {
      ensure(24);
      page.drawText(
        formatPeriodLabel(period.periodStartKey, period.periodEndKey),
        { x: margin, y, size: 10, font: fontBold, color: colors.ink }
      );
      y -= 14;

      // Pay-stub style 3-row breakdown (hours only; rates/amounts are not known here).
      const rows = [
        ["Straight Time", `${formatHours(period.regularHours)}h`],
        ["Time-and-a-Half", `${formatHours(period.overtimeHours)}h`],
        ["Double Time", `${formatHours(period.doubleTimeHours)}h`],
      ];
      const xDesc = margin + 10;
      const xHrs = pageW - margin - 130;
      ensure(14);
      page.drawText("Description", { x: xDesc, y, size: 8.6, font: fontBold, color: colors.accent });
      page.drawText("Hrs/Units", { x: xHrs, y, size: 8.6, font: fontBold, color: colors.accent });
      y -= 12;
      for (const [desc, hrs] of rows) {
        ensure(12);
        page.drawText(String(desc), { x: xDesc, y, size: 9.2, font, color: colors.muted });
        page.drawText(String(hrs), { x: xHrs, y, size: 9.2, font: fontBold, color: colors.ink });
        y -= 12;
      }
      y -= 4;

      // Pay-period table by labourer/day.
      drawPayPeriodHoursTable(period.periodStartKey, period.periodEndKey, period.entries);
    }
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
    page.drawText(`${formatHours(labourer.totalHours)} hours`, {
      x: pageW - margin - 100,
      y,
      size: 10,
      font: fontBold,
      color: colors.accent,
    });
    y -= 14;
    for (const item of labourer.entries || []) {
      const dateKey = item.reportDateKey || "-";
      const tag = isSundayDateKey(dateKey) ? " · DOUBLE" : "";
      const line = `${dateKey} · ${formatHours(item.hours)}h${tag} · ${item.projectSlug || "-"} · ${item.workOn || ""}`;
      draw(line, 9.2, false, colors.muted, margin + 10, contentW - 12);
    }
    y -= 4;
  }

  section("Detailed Entries");
  for (const item of entries || []) {
    ensure(26);
    const dateKey = item.reportDateKey || "-";
    const tag = isSundayDateKey(dateKey) ? " · DOUBLE" : "";
    draw(
      `${dateKey} · ${item.labourerName || item.labourerPhone || "Unknown"} · ${formatHours(item.hours)}h${tag}`,
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
