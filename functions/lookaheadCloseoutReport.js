const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");
const { FieldValue, Timestamp } = require("firebase-admin/firestore");
const { sanitizePdfText } = require("./pdfWinAnsiText");

function safeDateKey(value) {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
}

function addDays(dateKey, days) {
  const d = new Date(`${dateKey}T12:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCDate(d.getUTCDate() + Number(days || 0));
  return d.toISOString().slice(0, 10);
}

function toDate(dateKey) {
  const safe = safeDateKey(dateKey);
  return safe ? new Date(`${safe}T12:00:00Z`) : null;
}

function formatDayShort(dateKey) {
  const d = toDate(dateKey);
  if (!d) return dateKey || "?";
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  }).format(d);
}

function formatWeekLabel(startDateKey, endDateKey) {
  const start = toDate(startDateKey);
  const end = toDate(endDateKey || startDateKey);
  if (!start || !end) return `${startDateKey || "?"}-${endDateKey || startDateKey || "?"}`;
  const month = new Intl.DateTimeFormat("en-US", { month: "short" });
  const day = new Intl.DateTimeFormat("en-US", { day: "numeric" });
  const ms = month.format(start);
  const me = month.format(end);
  const ds = day.format(start);
  const de = day.format(end);
  return ms === me ? `${ms} ${ds}-${de}` : `${ms} ${ds}-${me} ${de}`;
}

function normalizeText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeTaskKey(task) {
  return [
    normalizeText(task && task.section).toLowerCase(),
    normalizeText(task && task.activity).toLowerCase(),
    normalizeText(task && task.actionBy).toLowerCase(),
  ].join("|");
}

function sortDateKeys(values) {
  return [...new Set((values || []).map((value) => safeDateKey(value)).filter(Boolean))].sort();
}

function taskStart(task) {
  const dates = sortDateKeys(task && task.scheduledDateKeys);
  if (dates.length) return dates[0];
  return safeDateKey(task && task.startDate);
}

function taskEnd(task) {
  const dates = sortDateKeys(task && task.scheduledDateKeys);
  if (dates.length) return dates[dates.length - 1];
  return safeDateKey(task && task.finishDate) || safeDateKey(task && task.startDate);
}

function intersectsWeek(task, weekStart, weekEnd) {
  const dates = sortDateKeys(task && task.scheduledDateKeys);
  if (dates.some((dateKey) => dateKey >= weekStart && dateKey <= weekEnd)) return true;
  const start = taskStart(task);
  const end = taskEnd(task) || start;
  if (!start || !end) return false;
  return start <= weekEnd && end >= weekStart;
}

function classifyLookaheadDelta(previousSnapshot, currentParsed) {
  const prevStart = safeDateKey(previousSnapshot && previousSnapshot.window && previousSnapshot.window.startDateKey);
  const prevWeekEnd = prevStart ? addDays(prevStart, 4) : null;
  const prevTasks = Array.isArray(previousSnapshot && previousSnapshot.tasks) ? previousSnapshot.tasks : [];
  const currentTasks = Array.isArray(currentParsed && currentParsed.tasks) ? currentParsed.tasks : [];
  const reviewTasks = prevStart && prevWeekEnd
    ? prevTasks.filter((task) => intersectsWeek(task, prevStart, prevWeekEnd))
    : prevTasks;

  const currentByKey = new Map();
  for (const task of currentTasks) {
    currentByKey.set(normalizeTaskKey(task), task);
  }

  const completed = [];
  const ongoing = [];
  const delayed = [];

  for (const task of reviewTasks) {
    const key = normalizeTaskKey(task);
    const nextTask = currentByKey.get(key) || null;
    const prevTaskEnd = taskEnd(task);
    const currentTaskStart = taskStart(nextTask);
    const plannedWithinWeek = prevTaskEnd && prevWeekEnd ? prevTaskEnd <= prevWeekEnd : true;
    const summary = {
      task,
      nextTask,
      trade: normalizeText(task.actionBy) || "General",
      section: normalizeText(task.section) || "General",
      label: normalizeText(task.activity) || "(Untitled activity)",
      priorRange: `${formatDayShort(taskStart(task))}${taskEnd(task) && taskEnd(task) !== taskStart(task) ? ` to ${formatDayShort(taskEnd(task))}` : ""}`,
      nextRange:
        nextTask && taskStart(nextTask)
          ? `${formatDayShort(taskStart(nextTask))}${taskEnd(nextTask) && taskEnd(nextTask) !== taskStart(nextTask) ? ` to ${formatDayShort(taskEnd(nextTask))}` : ""}`
          : "",
    };

    if (!nextTask) {
      completed.push(summary);
      continue;
    }

    if (!plannedWithinWeek) {
      ongoing.push(summary);
      continue;
    }

    if (currentTaskStart && prevTaskEnd && currentTaskStart > prevTaskEnd) {
      delayed.push(summary);
      continue;
    }

    ongoing.push(summary);
  }

  const byTrade = new Map();
  for (const item of reviewTasks) {
    const trade = normalizeText(item.actionBy) || "General";
    if (!byTrade.has(trade)) byTrade.set(trade, { planned: 0, completed: 0, ongoing: 0 });
    byTrade.get(trade).planned += 1;
  }
  for (const item of completed) {
    byTrade.get(item.trade).completed += 1;
  }
  for (const item of ongoing) {
    byTrade.get(item.trade).ongoing += 1;
  }

  const totalPlanned = reviewTasks.length;
  const completedOrOnTrack = completed.length + ongoing.length;
  const progressPct = totalPlanned ? Math.round((completedOrOnTrack / totalPlanned) * 100) : 0;
  const daysCovered = prevStart && prevWeekEnd ? 5 : 0;

  return {
    previousWeekStart: prevStart,
    previousWeekEnd: prevWeekEnd,
    totalPlanned,
    completed,
    ongoing,
    delayed,
    completedOrOnTrack,
    progressPct,
    daysCovered,
    tradeStats: Array.from(byTrade.entries())
      .map(([trade, stats]) => ({
        trade,
        planned: stats.planned,
        completedOrOnTrack: stats.completed + stats.ongoing,
        pct: stats.planned ? Math.round(((stats.completed + stats.ongoing) / stats.planned) * 100) : 0,
      }))
      .sort((a, b) => a.trade.localeCompare(b.trade)),
  };
}

function buildCloseoutNarrative(model) {
  const total = model.totalPlanned || 0;
  const completed = model.completed.length;
  const ongoing = model.ongoing.length;
  const delayed = model.delayed.length;
  const bits = [];
  bits.push(
    `${completed + ongoing} of ${total} planned tasks were completed or remained on track (${model.progressPct}%).`
  );
  if (delayed) {
    bits.push(`${delayed} task${delayed === 1 ? "" : "s"} rolled into the next lookahead.`);
  }
  if (completed) {
    bits.push(`${completed} task${completed === 1 ? "" : "s"} dropped out of the next schedule and were treated as completed.`);
  }
  return bits.join(" ");
}

function makeBullet(summary, mode) {
  const base = `${summary.label} (${summary.trade})`;
  if (mode === "completed") {
    return `${base} - planned ${summary.priorRange}; no carry-forward found in the next lookahead.`;
  }
  if (mode === "ongoing") {
    return `${base} - planned ${summary.priorRange}; continuing as ${summary.nextRange || "ongoing"} in the next lookahead.`;
  }
  return `${base} - planned ${summary.priorRange}; pushed to ${summary.nextRange || "later dates"} in the next lookahead.`;
}

function wrapText(text, maxWidth, font, size) {
  const words = sanitizePdfText(text).split(/\s+/).filter(Boolean);
  if (!words.length) return [""];
  const lines = [];
  let current = words[0];
  for (let i = 1; i < words.length; i += 1) {
    const candidate = `${current} ${words[i]}`;
    if (font.widthOfTextAtSize(candidate, size) <= maxWidth) current = candidate;
    else {
      lines.push(current);
      current = words[i];
    }
  }
  lines.push(current);
  return lines;
}

async function renderCloseoutPdf({ title, companyName, projectName, checkedInBy, model }) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const pageWidth = 612;
  const pageHeight = 792;
  const left = 42;
  const right = 42;
  const contentWidth = pageWidth - left - right;
  const palette = {
    ink: rgb(0.08, 0.11, 0.16),
    muted: rgb(0.38, 0.43, 0.51),
    line: rgb(0.84, 0.87, 0.9),
    shell: rgb(0.96, 0.97, 0.98),
    brand: rgb(0.09, 0.35, 0.67),
    success: rgb(0.12, 0.54, 0.29),
    warning: rgb(0.73, 0.47, 0.04),
    danger: rgb(0.79, 0.14, 0.17),
  };
  let page = pdf.addPage([pageWidth, pageHeight]);
  let y = pageHeight - 40;

  function newPage() {
    page = pdf.addPage([pageWidth, pageHeight]);
    y = pageHeight - 40;
  }
  function ensureSpace(height) {
    if (y - height < 40) newPage();
  }
  function drawWrapped(text, x, topY, width, size = 10, bold = false, color = palette.ink) {
    const lines = wrapText(text, width, bold ? fontBold : font, size);
    let cursorY = topY;
    for (const line of lines) {
      page.drawText(line, { x, y: cursorY, size, font: bold ? fontBold : font, color });
      cursorY -= size + 2;
    }
    return topY - cursorY;
  }
  function drawCard(titleText, items, accent) {
    const lineHeight = 12;
    const itemHeights = items.map((item) => wrapText(item, contentWidth - 40, font, 10).length * lineHeight + 4);
    const height = 30 + itemHeights.reduce((a, b) => a + b, 0) + 12;
    ensureSpace(height + 10);
    page.drawRectangle({ x: left, y: y - height, width: contentWidth, height, color: rgb(1, 1, 1), borderColor: palette.line, borderWidth: 1 });
    page.drawRectangle({ x: left, y: y - height, width: 10, height, color: accent });
    page.drawText(sanitizePdfText(titleText), { x: left + 18, y: y - 18, size: 12, font: fontBold, color: palette.ink });
    let cursorY = y - 36;
    for (const item of items) {
      page.drawCircle({ x: left + 22, y: cursorY + 3, size: 2.1, color: accent });
      const used = drawWrapped(item, left + 30, cursorY, contentWidth - 40, 10, false, palette.ink);
      cursorY -= used + 4;
    }
    y -= height + 10;
  }

  ensureSpace(120);
  page.drawRectangle({ x: left, y: y - 94, width: contentWidth, height: 94, color: palette.shell, borderColor: palette.line, borderWidth: 1 });
  page.drawRectangle({ x: left, y: y - 94, width: 12, height: 94, color: palette.brand });
  drawWrapped(title, left + 24, y - 26, contentWidth - 40, 18, true);
  drawWrapped(`Company ${companyName}   Project ${projectName}`, left + 24, y - 54, contentWidth - 40, 10, false, palette.muted);
  y -= 112;

  const status = model.delayed.length ? "Goal Not Met" : "On Track";
  const weekLabel = formatWeekLabel(model.previousWeekStart, model.previousWeekEnd);
  drawCard("CLOSEOUT SUMMARY", [
    `Activity - Week of ${weekLabel}`,
    `Progress - ${model.completedOrOnTrack} / ${model.totalPlanned} tasks (${model.progressPct}%)`,
    `Days - ${model.daysCovered} / ${model.daysCovered} days (100%)`,
    `Status - ${status}`,
    buildCloseoutNarrative(model),
    checkedInBy ? `Checked in by ${checkedInBy}` : "",
  ].filter(Boolean), palette.brand);

  drawCard(`COMPLETED (${model.completed.length})`, model.completed.length ? model.completed.map((item) => makeBullet(item, "completed")) : ["No completed tasks were inferred from the lookahead comparison."], palette.success);
  drawCard(`ONGOING (${model.ongoing.length})`, model.ongoing.length ? model.ongoing.map((item) => makeBullet(item, "ongoing")) : ["No ongoing tasks were identified."], palette.warning);
  drawCard(`DELAYED (${model.delayed.length})`, model.delayed.length ? model.delayed.map((item) => makeBullet(item, "delayed")) : ["No delayed tasks were identified."], palette.danger);
  drawCard("WEEK COMPLETION BY TRADE", model.tradeStats.map((row) => `${row.trade}: ${row.pct}% (${row.completedOrOnTrack}/${row.planned})`), palette.brand);

  return Buffer.from(await pdf.save());
}

function buildReportTitle(startDateKey, endDateKey) {
  return `Lookahead Closeout from ${startDateKey || "?"} to ${endDateKey || startDateKey || "?"}`;
}

function dateKeyToFilenameSegment(dateKey) {
  return safeDateKey(dateKey) ? dateKey.replace(/-/g, "_") : "unknown";
}

async function createLookaheadCloseoutReportPdf({
  db,
  bucket,
  phoneE164,
  companyName,
  projectName,
  checkedInBy,
  previousSnapshot,
  currentParsed,
}) {
  const model = classifyLookaheadDelta(previousSnapshot, currentParsed);
  const title = buildReportTitle(model.previousWeekStart, model.previousWeekEnd);
  const fileName = `Lookahead_Closeout_${dateKeyToFilenameSegment(model.previousWeekStart)}_to_${dateKeyToFilenameSegment(model.previousWeekEnd)}.pdf`;
  const storagePath = `dailyReports/${encodeURIComponent(phoneE164)}/${model.previousWeekStart || "unknown"}_closeout/${fileName}`;
  const buffer = await renderCloseoutPdf({
    title,
    companyName,
    projectName,
    checkedInBy,
    model,
  });
  const file = bucket.file(storagePath);
  await file.save(buffer, {
    contentType: "application/pdf",
    contentDisposition: `attachment; filename="${fileName}"`,
  });
  let downloadURL = null;
  try {
    [downloadURL] = await file.getSignedUrl({
      action: "read",
      expires: Date.now() + 1000 * 60 * 60 * 24 * 365,
      responseDisposition: `attachment; filename="${fileName}"`,
    });
  } catch (_) {
    downloadURL = null;
  }
  const reportRef = await db.collection("dailyReports").add({
    phoneE164,
    projectId: previousSnapshot.projectSlug || null,
    projectName: projectName || null,
    reportType: "lookaheadCloseout",
    reportTitle: title,
    reportFileName: fileName,
    dateKey: model.previousWeekStart || null,
    dateRangeStartKey: model.previousWeekStart || null,
    dateRangeEndKey: model.previousWeekEnd || null,
    storagePath,
    downloadURL,
    logEntryCount: model.totalPlanned,
    createdAt: FieldValue.serverTimestamp(),
  });
  return {
    reportId: reportRef.id,
    reportTitle: title,
    storagePath,
    downloadURL,
    reportFileName: fileName,
    summary: {
      totalPlanned: model.totalPlanned,
      completed: model.completed.length,
      ongoing: model.ongoing.length,
      delayed: model.delayed.length,
      progressPct: model.progressPct,
    },
  };
}

module.exports = {
  classifyLookaheadDelta,
  createLookaheadCloseoutReportPdf,
};
