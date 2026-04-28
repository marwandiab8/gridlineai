const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");
const { FieldValue, Timestamp } = require("firebase-admin/firestore");
const { fetchWeatherRangeSummary } = require("./dailyReportWeather");
const { sanitizePdfText } = require("./pdfWinAnsiText");

function safeDateKey(value) {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
}

function dateKeyToFilenameSegment(dateKey) {
  const safe = safeDateKey(dateKey);
  if (!safe) return "unknown";
  return safe.replace(/-/g, "_");
}

function buildReportTitle(startDateKey, endDateKey) {
  const from = startDateKey || "?";
  const to = endDateKey || from;
  return `Activities from ${from} to ${to}`;
}

function makeStorageDownloadToken() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 18)}`;
}

function buildActivitiesReportSequenceDocId(phoneE164, startDateKey, endDateKey) {
  const phone = encodeURIComponent(String(phoneE164 || "").trim());
  const start = String(startDateKey || "unknown").trim();
  const end = String(endDateKey || startDateKey || "unknown").trim();
  return `${phone}__${start}__${end}`;
}

function allocateActivitiesReportSequence(db, phoneE164, startDateKey, endDateKey) {
  const seqRef = db
    .collection("activitiesReportSequences")
    .doc(buildActivitiesReportSequenceDocId(phoneE164, startDateKey, endDateKey));
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(seqRef);
    const current = snap.exists ? Number(snap.data().lastSequence || 0) : 0;
    const next = Number.isFinite(current) && current > 0 ? current + 1 : 1;
    tx.set(
      seqRef,
      {
        phoneE164,
        startDateKey: startDateKey || null,
        endDateKey: endDateKey || null,
        lastSequence: next,
        updatedAt: Timestamp.now(),
      },
      { merge: true }
    );
    return next;
  });
}

function buildFirebaseDownloadUrl(bucketName, storagePath, token) {
  return `https://firebasestorage.googleapis.com/v0/b/${encodeURIComponent(
    bucketName
  )}/o/${encodeURIComponent(storagePath)}?alt=media&token=${encodeURIComponent(token)}`;
}

function toDateForLabel(dateKey) {
  const safe = safeDateKey(dateKey);
  if (!safe) return null;
  return new Date(`${safe}T12:00:00Z`);
}

function formatDayShort(dateKey) {
  const d = toDateForLabel(dateKey);
  if (!d) return dateKey || "?";
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  }).format(d);
}

function formatWeekWindowLabel(startDateKey, endDateKey) {
  const start = toDateForLabel(startDateKey);
  const end = toDateForLabel(endDateKey || startDateKey);
  if (!start || !end) return `${startDateKey || "?"} to ${endDateKey || startDateKey || "?"}`;
  const monthStart = new Intl.DateTimeFormat("en-US", { month: "short" }).format(start);
  const monthEnd = new Intl.DateTimeFormat("en-US", { month: "short" }).format(end);
  const dayStart = new Intl.DateTimeFormat("en-US", { day: "numeric" }).format(start);
  const dayEnd = new Intl.DateTimeFormat("en-US", { day: "numeric" }).format(end);
  return monthStart === monthEnd
    ? `${monthStart} ${dayStart}-${dayEnd}`
    : `${monthStart} ${dayStart}-${monthEnd} ${dayEnd}`;
}

function formatDateRangeLabel(startDateKey, endDateKey) {
  const start = safeDateKey(startDateKey);
  const end = safeDateKey(endDateKey || startDateKey);
  if (start && end && start !== end) return `${formatDayShort(start)}-${formatDayShort(end)}`;
  if (start) return formatDayShort(start);
  return "Date TBD";
}

function sortDateKeys(values) {
  return [...new Set((values || []).filter((value) => safeDateKey(value)))].sort();
}

function compressDateRange(task) {
  const dates = sortDateKeys(task && task.scheduledDateKeys);
  if (dates.length >= 2) return `${formatDayShort(dates[0])}-${formatDayShort(dates[dates.length - 1])}`;
  if (dates.length === 1) return formatDayShort(dates[0]);
  return formatDateRangeLabel(task && task.startDate, task && task.finishDate);
}

function normalizeActivityLabel(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\s*[-:]\s*$/, "")
    .slice(0, 180);
}

function cleanSectionLabel(value) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text || "General";
}

function cleanTradeLabel(value) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text || "";
}

function sentenceCase(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function taskMatches(task, pattern) {
  const haystack = [
    task && task.activity,
    task && task.section,
    task && task.actionBy,
  ]
    .filter(Boolean)
    .join(" ");
  return pattern.test(haystack);
}

function taskFlag(task) {
  if (taskMatches(task, /\bpour concrete\b/i)) return "critical";
  if (taskMatches(task, /\b(erecting steel|steel erection|start erecting steel)\b/i)) return "milestone";
  if (taskMatches(task, /\b(start|begins?|starting)\b/i)) return "milestone";
  return "";
}

function taskFlagBadge(flag) {
  if (flag === "critical") return "[CRITICAL]";
  if (flag === "milestone") return "[MILESTONE]";
  return "";
}

function buildTaskDisplayLine(task) {
  const label = normalizeActivityLabel(task.activity) || "(Untitled activity)";
  const range = compressDateRange(task);
  const trade = cleanTradeLabel(task.actionBy);
  const badge = taskFlagBadge(taskFlag(task));
  const parts = [`${label} [${range}]`];
  if (trade) parts.push(`(${trade})`);
  if (badge) parts.push(badge);
  return parts.join(" ");
}

function summarizeActivityHeadline(tasks, window) {
  const startDateKey = safeDateKey(window && window.startDateKey);
  const endDateKey = safeDateKey(window && window.endDateKey) || startDateKey;
  const weekLabel = formatWeekWindowLabel(startDateKey, endDateKey);
  const phrases = [];
  if ((tasks || []).some((task) => taskMatches(task, /\b(erecting steel|steel erection)\b/i))) {
    phrases.push("Steel Erection Begins");
  }
  if ((tasks || []).some((task) => taskMatches(task, /\bpour concrete\b/i))) {
    phrases.push("Critical Concrete Pours");
  }
  if ((tasks || []).some((task) => taskMatches(task, /\b(footing|mud slab)\b/i))) {
    phrases.push("Footings Advance");
  }
  if ((tasks || []).some((task) => taskMatches(task, /\bwaterproof/i))) {
    phrases.push("Waterproofing / Backfill Sequence");
  }
  if (!phrases.length) {
    phrases.push("Lookahead Activities");
  }
  return `Week of ${weekLabel}: ${phrases.slice(0, 3).join(", ")}`;
}

function buildCriticalPathItems(tasks) {
  return (tasks || [])
    .filter((task) => taskMatches(task, /\bpour concrete\b/i))
    .sort((a, b) => compressDateRange(a).localeCompare(compressDateRange(b)))
    .map((task) => {
      const label = normalizeActivityLabel(task.activity);
      const range = compressDateRange(task);
      const section = cleanSectionLabel(task.section);
      return `${range}: ${label}${section !== "General" ? ` - ${section}` : ""}`;
    });
}

function buildWeatherHighlights(window, weatherSummary) {
  if (weatherSummary && Array.isArray(weatherSummary.summaryItems) && weatherSummary.summaryItems.length) {
    return weatherSummary.summaryItems;
  }
  const startDateKey = safeDateKey(window && window.startDateKey);
  const endDateKey = safeDateKey(window && window.endDateKey) || startDateKey;
  return [
    `Weather data was not available for ${formatWeekWindowLabel(startDateKey, endDateKey)}.`,
    "Review the site forecast manually before distributing the report.",
  ];
}

function groupTasksBySection(tasks) {
  const grouped = new Map();
  for (const task of tasks || []) {
    const section = cleanSectionLabel(task.section);
    if (!grouped.has(section)) grouped.set(section, []);
    grouped.get(section).push(task);
  }
  return Array.from(grouped.entries()).map(([section, sectionTasks]) => ({
    section,
    items: sectionTasks.map((task) => buildTaskDisplayLine(task)),
  }));
}

function buildWeekGoalItems(tasks) {
  const counts = new Map();
  for (const task of tasks || []) {
    const trade = cleanTradeLabel(task.actionBy) || "General";
    counts.set(trade, (counts.get(trade) || 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([trade, count]) => `${count} ${trade} task${count === 1 ? "" : "s"}`);
}

function buildCoordinationNotes(tasks, window) {
  const notes = [];
  const steelErection = (tasks || []).find((task) =>
    taskMatches(task, /\b(erecting steel|steel erection|start erecting steel)\b/i)
  );
  if (steelErection) {
    notes.push(
      `Steel erection starts ${compressDateRange(steelErection)}. Keep crane access and adjacent work fronts clear.`
    );
  }

  const criticalPour = (tasks || []).find((task) => taskMatches(task, /\bpour concrete\b/i));
  if (criticalPour) {
    notes.push(
      `Critical concrete sequence runs through ${compressDateRange(criticalPour)}. Protect forms, rebar, close-out, and pour handoff.`
    );
  }

  const waterproofCount = (tasks || []).filter((task) => taskMatches(task, /\bwaterproof/i)).length;
  if (waterproofCount) {
    notes.push(
      `Waterproofing drives follow-on work this week. Confirm inspection and substrate readiness before backfill crews move in.`
    );
  }

  const excavationCount = (tasks || []).filter((task) => taskMatches(task, /\b(excavate|excavation|backfill)\b/i)).length;
  if (excavationCount >= 2) {
    notes.push(
      `Earthworks are running on multiple fronts through ${formatDayShort(window && window.endDateKey)}. Watch site access and haul routes daily.`
    );
  }

  if (!notes.length) {
    notes.push("Review trade handoffs daily and protect milestone work from access conflicts.");
  }

  return notes.slice(0, 5);
}

function buildActivitiesReportModel({ companyName, projectName, window, tasks, weatherSummary = null }) {
  const rows = Array.isArray(tasks) ? tasks : [];
  const startDateKey = safeDateKey(window && window.startDateKey);
  const endDateKey = safeDateKey(window && window.endDateKey) || startDateKey;
  const uniqueDays = new Set();
  for (const task of rows) {
    for (const d of task.scheduledDateKeys || []) {
      const safe = safeDateKey(d);
      if (safe) uniqueDays.add(safe);
    }
  }

  return {
    companyName: companyName || "Matheson",
    projectName: projectName || "Docksteader Paramedic Station",
    activityHeadline: summarizeActivityHeadline(rows, { startDateKey, endDateKey }),
    progressLine: `0 / ${rows.length} Tasks (0%)`,
    daysLine: `0 / ${uniqueDays.size || 0} days (0%)`,
    status: "On Track",
    criticalPathTitle: "CRITICAL PATH POURS",
    criticalPathItems: buildCriticalPathItems(rows),
    weatherTitle: "WEATHER HIGHLIGHTS",
    weatherItems: buildWeatherHighlights({ startDateKey, endDateKey }, weatherSummary),
    locationTitle: "BY LOCATION & TRADE",
    sections: groupTasksBySection(rows),
    weekGoalTitle: `WEEK GOAL: ${rows.length} TASKS`,
    weekGoalItems: buildWeekGoalItems(rows),
    coordinationTitle: "COORDINATION NOTES",
    coordinationItems: buildCoordinationNotes(rows, { startDateKey, endDateKey }),
  };
}

function splitWords(text) {
  return sanitizePdfText(text).split(/\s+/).filter(Boolean);
}

function wrapText(text, maxWidth, font, size) {
  const words = splitWords(text);
  if (!words.length) return [""];
  const lines = [];
  let current = words[0];
  for (let i = 1; i < words.length; i += 1) {
    const candidate = `${current} ${words[i]}`;
    if (font.widthOfTextAtSize(candidate, size) <= maxWidth) {
      current = candidate;
    } else {
      lines.push(current);
      current = words[i];
    }
  }
  lines.push(current);
  return lines;
}

function drawWrappedText(page, text, x, y, width, options) {
  const lines = wrapText(sanitizePdfText(text), width, options.font, options.size);
  let cursorY = y;
  for (const line of lines) {
    page.drawText(line, {
      x,
      y: cursorY,
      size: options.size,
      font: options.font,
      color: options.color || rgb(0, 0, 0),
    });
    cursorY -= options.lineHeight;
  }
  return {
    lines,
    height: lines.length * options.lineHeight,
    nextY: cursorY,
  };
}

async function renderActivitiesPdf({ title, companyName, projectName, window, tasks, weatherSummary }) {
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
    critical: rgb(0.79, 0.14, 0.17),
    gold: rgb(0.76, 0.54, 0.05),
    green: rgb(0.1, 0.49, 0.28),
  };

  const model = buildActivitiesReportModel({ companyName, projectName, window, tasks, weatherSummary });

  let page = pdf.addPage([pageWidth, pageHeight]);
  let y = pageHeight - 40;

  function newPage() {
    page = pdf.addPage([pageWidth, pageHeight]);
    y = pageHeight - 40;
  }

  function ensureSpace(heightNeeded) {
    if (y - heightNeeded < 40) newPage();
  }

  function drawHeader() {
    page.drawRectangle({
      x: left,
      y: y - 92,
      width: contentWidth,
      height: 92,
      color: palette.shell,
      borderColor: palette.line,
      borderWidth: 1,
    });
    page.drawRectangle({
      x: left,
      y: y - 92,
      width: 12,
      height: 92,
      color: palette.brand,
    });
    drawWrappedText(page, sanitizePdfText(title), left + 24, y - 26, contentWidth - 36, {
      font: fontBold,
      size: 19,
      lineHeight: 22,
      color: palette.ink,
    });
    drawWrappedText(page, sanitizePdfText(model.activityHeadline), left + 24, y - 54, contentWidth - 36, {
      font,
      size: 11,
      lineHeight: 14,
      color: palette.muted,
    });
    y -= 112;
  }

  function drawSummaryGrid() {
    const top = y;
    const labelW = 74;
    const gap = 10;
    const colW = (contentWidth - gap) / 2;
    const cellTextSize = 9;
    const cellLineHeight = 11;
    const rowPaddingTop = 8;
    const rowPaddingBottom = 7;
    const rows = [
      ["Company", model.companyName, "Project", model.projectName],
      ["Activity", model.activityHeadline, "Status", model.status],
      ["Progress", model.progressLine, "Days", model.daysLine],
    ];

    const rowHeights = rows.map(([l1, v1, l2, v2]) => {
      const leftLines = l1
        ? wrapText(sanitizePdfText(v1), colW - labelW - 16, font, cellTextSize).length
        : 0;
      const rightLines = l2
        ? wrapText(sanitizePdfText(v2), colW - labelW - 16, font, cellTextSize).length
        : 0;
      const lineCount = Math.max(1, leftLines, rightLines);
      return rowPaddingTop + lineCount * cellLineHeight + rowPaddingBottom;
    });
    const estimatedHeight = rowHeights.reduce((sum, value) => sum + value, 0) + 18;
    ensureSpace(estimatedHeight);

    let cursorTop = top;
    for (let i = 0; i < rows.length; i += 1) {
      const rowH = rowHeights[i];
      const rowTop = cursorTop;
      const rowBottom = rowTop - rowH;
      page.drawRectangle({
        x: left,
        y: rowBottom,
        width: contentWidth,
        height: rowH,
        color: i % 2 === 0 ? rgb(1, 1, 1) : rgb(0.985, 0.987, 0.99),
        borderColor: palette.line,
        borderWidth: 0.7,
      });
      page.drawLine({
        start: { x: left + colW, y: rowBottom },
        end: { x: left + colW, y: rowTop },
        thickness: 0.7,
        color: palette.line,
      });

      const [l1, v1, l2, v2] = rows[i];
      const textY = rowTop - rowPaddingTop - cellTextSize;
      if (l1) {
        page.drawText(sanitizePdfText(`${l1}`), {
          x: left + 10,
          y: textY,
          size: cellTextSize,
          font: fontBold,
          color: palette.muted,
        });
        drawWrappedText(page, sanitizePdfText(v1), left + 10 + labelW, textY, colW - labelW - 16, {
          font,
          size: cellTextSize,
          lineHeight: cellLineHeight,
          color: palette.ink,
        });
      }
      if (l2) {
        page.drawText(sanitizePdfText(`${l2}`), {
          x: left + colW + 10,
          y: textY,
          size: cellTextSize,
          font: fontBold,
          color: palette.muted,
        });
        drawWrappedText(
          page,
          sanitizePdfText(v2),
          left + colW + 10 + labelW,
          textY,
          colW - labelW - 16,
          {
            font,
            size: cellTextSize,
            lineHeight: cellLineHeight,
            color: palette.ink,
          }
        );
      }
      cursorTop = rowBottom;
    }

    y = cursorTop - 16;
  }

  function measureBulletItems(items, width, fontRef, size, lineHeight) {
    let height = 0;
    for (const item of items) {
      const lines = wrapText(sanitizePdfText(item), width - 18, fontRef, size);
      height += Math.max(1, lines.length) * lineHeight + 4;
    }
    return height;
  }

  function drawSectionCard(titleText, items, options = {}) {
    const accent = options.accent || palette.brand;
    const titleSize = options.titleSize || 12;
    const itemSize = options.itemSize || 10;
    const itemLineHeight = options.itemLineHeight || 12;
    const introText = options.introText || "";
    const introHeight = introText
      ? wrapText(introText, contentWidth - 28, font, 10).length * 12 + 4
      : 0;
    const itemsHeight = measureBulletItems(items, contentWidth - 28, font, itemSize, itemLineHeight);
    const height = 24 + introHeight + itemsHeight + 16;

    ensureSpace(height + 8);
    page.drawRectangle({
      x: left,
      y: y - height,
      width: contentWidth,
      height,
      color: rgb(1, 1, 1),
      borderColor: palette.line,
      borderWidth: 1,
    });
    page.drawRectangle({
      x: left,
      y: y - height,
      width: 10,
      height,
      color: accent,
    });
    page.drawText(sanitizePdfText(titleText), {
      x: left + 18,
      y: y - 18,
      size: titleSize,
      font: fontBold,
      color: palette.ink,
    });

    let cursorY = y - 36;
    if (introText) {
      const intro = drawWrappedText(page, sanitizePdfText(introText), left + 18, cursorY, contentWidth - 28, {
        font,
        size: 10,
        lineHeight: 12,
        color: palette.muted,
      });
      cursorY -= intro.height + 2;
    }

    for (const item of items) {
      page.drawCircle({
        x: left + 21,
        y: cursorY + 3,
        size: 2.2,
        color: accent,
      });
      const result = drawWrappedText(page, sanitizePdfText(item), left + 30, cursorY, contentWidth - 40, {
        font,
        size: itemSize,
        lineHeight: itemLineHeight,
        color: palette.ink,
      });
      cursorY -= result.height + 4;
    }

    y -= height + 10;
  }

  function drawLocationSections() {
    drawSectionCard(model.locationTitle, [], {
      accent: palette.brand,
      introText: "Grouped by workbook section so field teams can scan the plan by work area and handoff sequence.",
    });
    y += 10;
    for (const section of model.sections) {
      drawSectionCard(section.section, section.items, {
        accent: palette.shell,
        titleSize: 11,
        itemSize: 10,
        itemLineHeight: 12,
      });
    }
  }

  drawHeader();
  drawSummaryGrid();
  drawSectionCard(
    model.criticalPathTitle,
    model.criticalPathItems.length ? model.criticalPathItems : ["No concrete pours identified in this window."],
    {
      accent: palette.critical,
    }
  );
  drawSectionCard(model.weatherTitle, model.weatherItems, {
    accent: palette.gold,
  });
  drawLocationSections();
  drawSectionCard(model.weekGoalTitle, model.weekGoalItems, {
    accent: palette.green,
  });
  drawSectionCard(model.coordinationTitle, model.coordinationItems, {
    accent: palette.brand,
  });

  const bytes = await pdf.save();
  return Buffer.from(bytes);
}

async function createLookaheadActivitiesReportPdf({
  db,
  bucket,
  phoneE164,
  projectSlug,
  companyName,
  projectName,
  projectLocation,
  parsed,
  logger,
  runId,
}) {
  const startDateKey = safeDateKey(parsed && parsed.window && parsed.window.startDateKey);
  const endDateKey = safeDateKey(parsed && parsed.window && parsed.window.endDateKey) || startDateKey;
  const title = buildReportTitle(startDateKey, endDateKey);
  const fromSeg = dateKeyToFilenameSegment(startDateKey);
  const toSeg = dateKeyToFilenameSegment(endDateKey || startDateKey);
  const sequence = await allocateActivitiesReportSequence(db, phoneE164, startDateKey, endDateKey);
  const seqStr = String(Math.max(1, Number(sequence) || 1)).padStart(3, "0");
  const fileName = `Construction_Activities_from_${fromSeg}_to_${toSeg}_${seqStr}.pdf`;
  const storagePath = `dailyReports/${encodeURIComponent(phoneE164)}/${startDateKey || "unknown"}_to_${endDateKey || "unknown"}/activities/${fileName}`;

  const weatherSummary =
    startDateKey && endDateKey
      ? await fetchWeatherRangeSummary({
          addressLine: projectLocation || projectName || "",
          startDateKey,
          endDateKey,
          logger,
          runId,
        })
      : null;
  if (logger) {
    logger.info("lookaheadActivitiesPdf: weather summary prepared", {
      runId,
      startDateKey,
      endDateKey,
      projectLocation: projectLocation || null,
      weatherOk: weatherSummary ? weatherSummary.ok !== false : null,
      weatherSummaryItems:
        weatherSummary && Array.isArray(weatherSummary.summaryItems)
          ? weatherSummary.summaryItems
          : [],
    });
  }

  const buffer = await renderActivitiesPdf({
    title,
    companyName,
    projectName,
    window: (parsed && parsed.window) || {},
    tasks: (parsed && parsed.tasks) || [],
    weatherSummary,
  });

  const file = bucket.file(storagePath);
  const downloadToken = makeStorageDownloadToken();
  await file.save(buffer, {
    contentType: "application/pdf",
    contentDisposition: `attachment; filename="${fileName}"`,
    metadata: {
      metadata: {
        phoneE164,
        reportType: "activities",
        reportTitle: title,
        reportFileName: fileName,
        reportStartDateKey: startDateKey || "",
        reportEndDateKey: endDateKey || "",
        firebaseStorageDownloadTokens: downloadToken,
      },
    },
  });

  let downloadURL = null;
  let downloadUrlError = null;
  try {
    const [url] = await file.getSignedUrl({
      action: "read",
      expires: Date.now() + 1000 * 60 * 60 * 24 * 365,
      responseDisposition: `attachment; filename="${fileName}"`,
    });
    downloadURL = url || null;
  } catch (signErr) {
    downloadUrlError = String(signErr.message || signErr).slice(0, 500);
    downloadURL = buildFirebaseDownloadUrl(bucket.name, storagePath, downloadToken);
  }

  const reportRef = await db.collection("dailyReports").add({
    phoneE164,
    projectId: projectSlug || null,
    projectName: projectName || null,
    reportType: "activities",
    reportTitle: title,
    reportFileName: fileName,
    reportDate: null,
    dateKey: startDateKey || null,
    dateRangeStartKey: startDateKey || null,
    dateRangeEndKey: endDateKey || null,
    storagePath,
    downloadURL,
    downloadUrlError: downloadUrlError || null,
    messageCount: 0,
    logEntryCount: Array.isArray(parsed && parsed.tasks) ? parsed.tasks.length : 0,
    mediaCount: 0,
    unifiedDayLog: false,
    aiNarrative: false,
    createdAt: FieldValue.serverTimestamp(),
  });

  return {
    reportId: reportRef.id,
    reportTitle: title,
    storagePath,
    downloadURL,
    downloadUrlError: downloadUrlError || null,
    reportFileName: fileName,
    window: {
      startDateKey: startDateKey || null,
      endDateKey: endDateKey || null,
    },
    taskCount: Array.isArray(parsed && parsed.tasks) ? parsed.tasks.length : 0,
    weatherSummary: weatherSummary
      ? {
          ok: weatherSummary.ok !== false,
          locationQuery: weatherSummary.locationQuery || null,
          resolvedLabel: weatherSummary.resolvedLabel || null,
          summaryItems: Array.isArray(weatherSummary.summaryItems)
            ? weatherSummary.summaryItems.slice(0, 4)
            : [],
        }
      : null,
  };
}

module.exports = {
  createLookaheadActivitiesReportPdf,
  buildReportTitle,
  buildActivitiesReportModel,
};
