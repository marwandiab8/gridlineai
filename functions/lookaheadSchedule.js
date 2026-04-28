const fs = require("fs");
const path = require("path");
const ExcelJS = require("exceljs");

function asTrimmedText(value) {
  if (value == null) return "";
  if (typeof value === "string") return value.replace(/\s+/g, " ").trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value).trim();
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") {
    if (typeof value.text === "string") return value.text.replace(/\s+/g, " ").trim();
    if (value.richText && Array.isArray(value.richText)) {
      return value.richText.map((part) => String(part.text || "")).join("").replace(/\s+/g, " ").trim();
    }
    if (value.result != null) return asTrimmedText(value.result);
    if (typeof value.formula === "string" && value.formula.trim() !== "") return "";
  }
  return String(value).replace(/\s+/g, " ").trim();
}

function parseExcelDate(value) {
  if (value == null || value === "") return null;
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : new Date(value.getTime());
  }
  if (typeof value === "object" && value.result != null) {
    return parseExcelDate(value.result);
  }
  const text = asTrimmedText(value);
  if (!text) return null;
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function toDateKey(value) {
  const date = parseExcelDate(value);
  return date ? date.toISOString().slice(0, 10) : null;
}

function normalizeDateRange(startDateKey, endDateKey, fallbackStart, fallbackEnd) {
  const start = startDateKey || fallbackStart || null;
  const end = endDateKey || fallbackEnd || null;
  if (!start || !end) return { startDateKey: start, endDateKey: end };
  return start <= end
    ? { startDateKey: start, endDateKey: end }
    : { startDateKey: end, endDateKey: start };
}

function valueLooksComplete(raw) {
  return asTrimmedText(raw).toLowerCase() === "complete";
}

function extractCellResult(cellValue) {
  if (cellValue == null) return "";
  if (typeof cellValue === "object" && cellValue.result != null) {
    return asTrimmedText(cellValue.result);
  }
  return asTrimmedText(cellValue);
}

function inferDateColumns(worksheet) {
  const out = [];
  const headerRow = worksheet.getRow(3);
  for (let col = 7; col <= worksheet.columnCount; col += 1) {
    const dateKey = toDateKey(headerRow.getCell(col).value);
    if (!dateKey) continue;
    out.push({
      columnNumber: col,
      dateKey,
      weekdayLabel: asTrimmedText(worksheet.getRow(1).getCell(col).value),
      shortLabel: asTrimmedText(worksheet.getRow(4).getCell(col).value),
    });
  }
  return out;
}

function rowIsSectionHeading({ activity, actionBy, durationText, startDate, finishDate }) {
  return Boolean(activity) && !actionBy && !durationText && !startDate && !finishDate;
}

function rowIsTemplateHeader({ activity, actionBy, durationText, startDate, finishDate }) {
  const activityLower = String(activity || "").trim().toLowerCase();
  const actionLower = String(actionBy || "").trim().toLowerCase();
  const durationLower = String(durationText || "").trim().toLowerCase();
  if (activityLower !== "activities") return false;
  if (actionLower === "action by") return true;
  if (durationLower === "duration") return true;
  return !startDate && !finishDate;
}

function overlapsWindow(taskStart, taskFinish, startDateKey, endDateKey) {
  if (!startDateKey || !endDateKey) return true;
  const start = taskStart || "";
  const finish = taskFinish || taskStart || "";
  if (!start && !finish) return false;
  const effectiveStart = start || finish;
  const effectiveFinish = finish || start;
  return effectiveStart <= endDateKey && effectiveFinish >= startDateKey;
}

function buildScheduledDates(row, dateColumns, startDateKey, endDateKey) {
  const out = [];
  for (const col of dateColumns) {
    if (startDateKey && col.dateKey < startDateKey) continue;
    if (endDateKey && col.dateKey > endDateKey) continue;
    const marker = extractCellResult(row.getCell(col.columnNumber).value);
    if (!marker) continue;
    out.push({
      dateKey: col.dateKey,
      marker,
      weekdayLabel: col.weekdayLabel,
      shortLabel: col.shortLabel,
    });
  }
  return out;
}

function parseLookaheadWorksheet(worksheet, options = {}) {
  const dateColumns = inferDateColumns(worksheet);
  const window = normalizeDateRange(
    options.startDateKey || null,
    options.endDateKey || null,
    dateColumns[0] ? dateColumns[0].dateKey : null,
    dateColumns.length ? dateColumns[dateColumns.length - 1].dateKey : null
  );
  const includeHidden = options.includeHidden !== false;
  const includeCompleted = options.includeCompleted === true;
  const tasks = [];
  let currentSection = "";

  for (let rowNumber = 5; rowNumber <= worksheet.rowCount; rowNumber += 1) {
    const row = worksheet.getRow(rowNumber);
    const activity = asTrimmedText(row.getCell(1).value);
    const completionFlag = asTrimmedText(row.getCell(2).value);
    const actionBy = asTrimmedText(row.getCell(3).value);
    const durationText = asTrimmedText(row.getCell(4).value);
    const startDate = toDateKey(row.getCell(5).value);
    const finishDate = toDateKey(row.getCell(6).value);

    if (!activity) continue;
    if (rowIsTemplateHeader({ activity, actionBy, durationText, startDate, finishDate })) {
      continue;
    }

    if (rowIsSectionHeading({ activity, actionBy, durationText, startDate, finishDate })) {
      currentSection = activity;
      continue;
    }

    if (!includeHidden && row.hidden) continue;
    if (!includeCompleted && valueLooksComplete(completionFlag)) continue;

    const scheduledDates = buildScheduledDates(
      row,
      dateColumns,
      window.startDateKey,
      window.endDateKey
    );
    const intersectsWindow =
      scheduledDates.length > 0 ||
      overlapsWindow(startDate, finishDate, window.startDateKey, window.endDateKey);

    if (!intersectsWindow) continue;

    tasks.push({
      rowNumber,
      section: currentSection || null,
      activity,
      actionBy: actionBy || null,
      durationDays: durationText ? Number(durationText) || null : null,
      durationText: durationText || null,
      startDate,
      finishDate,
      hidden: row.hidden === true,
      completed: valueLooksComplete(completionFlag),
      scheduledDates,
      scheduledDateKeys: scheduledDates.map((item) => item.dateKey),
    });
  }

  return {
    worksheetName: worksheet.name,
    window,
    dateColumns,
    taskCount: tasks.length,
    tasks,
  };
}

async function parseLookaheadWorkbookFile(filePath, options = {}) {
  const resolvedPath = path.resolve(filePath);
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(resolvedPath);
  return parseLookaheadWorkbook(workbook, {
    sourcePath: resolvedPath,
    fileName: path.basename(resolvedPath),
    ...options,
  });
}

function parseLookaheadWorkbook(workbook, options = {}) {
  const worksheetName = options.worksheetName || "3 Weeks Look Ahead Schedule";
  const worksheet = workbook.getWorksheet(worksheetName) || workbook.worksheets[0];
  if (!worksheet) {
    throw new Error(`No worksheet found in workbook`);
  }
  const parsed = parseLookaheadWorksheet(worksheet, options);
  return {
    sourcePath: options.sourcePath || "",
    fileName: options.fileName || "",
    ...parsed,
  };
}

async function parseLookaheadWorkbookBuffer(buffer, options = {}) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);
  return parseLookaheadWorkbook(workbook, options);
}

function formatTaskSummary(parsed) {
  const lines = [];
  lines.push(`Source: ${parsed.fileName || parsed.sourcePath || parsed.worksheetName}`);
  lines.push(
    `Window: ${parsed.window.startDateKey || "?"} to ${parsed.window.endDateKey || "?"}`
  );
  lines.push(`Tasks: ${parsed.taskCount}`);
  let previousSection = null;
  for (const task of parsed.tasks) {
    const section = task.section || "Uncategorized";
    if (section !== previousSection) {
      lines.push("");
      lines.push(section);
      previousSection = section;
    }
    const who = task.actionBy ? ` | ${task.actionBy}` : "";
    const span =
      task.startDate || task.finishDate
        ? ` | ${task.startDate || "?"} -> ${task.finishDate || task.startDate || "?"}`
        : "";
    const days = task.scheduledDateKeys.length ? ` | ${task.scheduledDateKeys.join(", ")}` : "";
    lines.push(`- ${task.activity}${who}${span}${days}`);
  }
  return lines.join("\n");
}

function formatDayLabel(dateKey) {
  const date =
    typeof dateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(dateKey)
      ? new Date(`${dateKey}T12:00:00Z`)
      : parseExcelDate(dateKey);
  if (!date) return dateKey || "";
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  }).format(date);
}

function formatWeekLabel(startDateKey, endDateKey) {
  const start =
    typeof startDateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(startDateKey)
      ? new Date(`${startDateKey}T12:00:00Z`)
      : parseExcelDate(startDateKey);
  const end =
    typeof endDateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(endDateKey)
      ? new Date(`${endDateKey}T12:00:00Z`)
      : parseExcelDate(endDateKey);
  if (!start || !end) return `${startDateKey || "?"}–${endDateKey || "?"}`;
  const startMonth = new Intl.DateTimeFormat("en-US", {
    month: "short",
  }).format(start);
  const endMonth = new Intl.DateTimeFormat("en-US", {
    month: "short",
  }).format(end);
  const startDay = new Intl.DateTimeFormat("en-US", {
    day: "numeric",
  }).format(start);
  const endDay = new Intl.DateTimeFormat("en-US", {
    day: "numeric",
  }).format(end);
  return startMonth === endMonth
    ? `${startMonth} ${startDay}–${endDay}`
    : `${startMonth} ${startDay}–${endMonth} ${endDay}`;
}

function compressScheduledDates(task) {
  const dates = Array.isArray(task.scheduledDateKeys) ? task.scheduledDateKeys : [];
  if (!dates.length) return "";
  if (dates.length === 1) return `[${formatDayLabel(dates[0])}]`;
  const start = dates[0];
  const end = dates[dates.length - 1];
  const consecutive = dates.every((dateKey, index) => {
    if (index === 0) return true;
    const prev = parseExcelDate(dates[index - 1]);
    const current = parseExcelDate(dateKey);
    if (!prev || !current) return false;
    const deltaDays = Math.round((current.getTime() - prev.getTime()) / 86400000);
    return deltaDays === 1;
  });
  if (consecutive) {
    return `[${formatDayLabel(start)}–${formatDayLabel(end)}]`;
  }
  if (dates.length === 2) {
    return `[${formatDayLabel(start)} & ${formatDayLabel(end)}]`;
  }
  return `[starts ${formatDayLabel(start)}]`;
}

function normalizeActivityLabel(text) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function taskMatches(task, pattern) {
  const haystack = `${task.section || ""} ${task.activity || ""} ${task.actionBy || ""}`.toLowerCase();
  return pattern.test(haystack);
}

function buildCoordinationNotes(tasks, window) {
  const notes = [];
  const steelErection = tasks.find((task) => taskMatches(task, /\berecting steel\b/));
  if (steelErection) {
    notes.push(
      `Steel erection begins ${compressScheduledDates(steelErection).replace(/^\[|\]$/g, "")} — coordinate crane and concurrent work fronts.`
    );
  }
  const fridayPour = tasks.find((task) =>
    /\bpour concrete\b/i.test(task.activity || "") &&
    Array.isArray(task.scheduledDateKeys) &&
    task.scheduledDateKeys.includes("2026-04-24")
  );
  if (fridayPour) {
    notes.push(
      `FW & Piers pour on ${formatDayLabel("2026-04-24")} is the critical path sequence: forms, rebar, close, then pour.`
    );
  }
  const srwpCount = tasks.filter((task) => String(task.actionBy || "").toUpperCase() === "SRWP").length;
  if (srwpCount) {
    notes.push(
      `SRWP has ${srwpCount} waterproofing task${srwpCount === 1 ? "" : "s"} early in the week; backfills depend on those completions.`
    );
  }
  const coreydaleCount = tasks.filter((task) => String(task.actionBy || "").toLowerCase() === "coreydale").length;
  if (coreydaleCount) {
    notes.push(
      `Coreydale is split across excavation and backfill fronts through ${formatDayLabel(window.endDateKey)}.`
    );
  }
  return notes;
}

function formatCrewscopeStyleSummary(parsed, options = {}) {
  const companyName = options.companyName || "Matheson";
  const projectName = options.projectName || "Docksteader Paramedic Station";
  const weekLabel = formatWeekLabel(parsed.window.startDateKey, parsed.window.endDateKey);
  const taskCount = parsed.taskCount || 0;
  const uniqueDays = new Set();
  for (const task of parsed.tasks || []) {
    for (const dateKey of task.scheduledDateKeys || []) {
      uniqueDays.add(dateKey);
    }
  }
  const grouped = new Map();
  const tradeCounts = new Map();
  for (const task of parsed.tasks || []) {
    const section = task.section || "General";
    if (!grouped.has(section)) grouped.set(section, []);
    grouped.get(section).push(task);
    const trade = task.actionBy || companyName;
    tradeCounts.set(trade, (tradeCounts.get(trade) || 0) + 1);
  }

  const criticalTasks = (parsed.tasks || []).filter((task) => /\bpour concrete\b/i.test(task.activity || ""));
  const coordinationNotes = buildCoordinationNotes(parsed.tasks || [], parsed.window);

  const lines = [];
  lines.push(`Progress Update: Week of ${weekLabel}`);
  lines.push("");
  lines.push("Check-in:");
  lines.push("");
  lines.push(`Company\t${companyName}`);
  lines.push(`Project\t${projectName}`);
  lines.push(`Activity\tWeek of ${weekLabel}`);
  lines.push(`Progress\t0 / ${taskCount} Tasks (0%)`);
  lines.push(`Days\t0 / ${uniqueDays.size} days (0%)`);
  lines.push("Status\tOn Track");
  lines.push("Note\tHere is next week's plan.");
  lines.push("");
  if (criticalTasks.length) {
    lines.push("CRITICAL PATH");
    for (const task of criticalTasks) {
      lines.push(`${compressScheduledDates(task)} ${normalizeActivityLabel(task.activity)}${task.section ? ` – ${task.section}` : ""}`);
    }
    lines.push("");
  }
  lines.push("BY LOCATION & TRADE");
  lines.push("");
  for (const [section, tasks] of grouped.entries()) {
    lines.push(`${section}:`);
    for (const task of tasks) {
      const trade = task.actionBy ? ` (${task.actionBy})` : "";
      lines.push(`- ${normalizeActivityLabel(task.activity)} ${compressScheduledDates(task)}${trade}`.replace(/\s+/g, " ").trim());
    }
  }
  lines.push("");
  lines.push(`WEEK GOAL: ${taskCount} TASKS`);
  for (const [trade, count] of Array.from(tradeCounts.entries()).sort((a, b) => a[0].localeCompare(b[0]))) {
    lines.push(`- ${count} ${trade} task${count === 1 ? "" : "s"}`);
  }
  if (coordinationNotes.length) {
    lines.push("");
    lines.push("COORDINATION NOTES:");
    for (const note of coordinationNotes) {
      lines.push(`- ${note}`);
    }
  }
  return lines.join("\n");
}

function parseCliArgs(argv) {
  const out = {
    filePath: "",
    format: "text",
    includeHidden: true,
    includeCompleted: false,
    startDateKey: "",
    endDateKey: "",
    companyName: "",
    projectName: "",
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg) continue;
    if (!out.filePath && !arg.startsWith("--")) {
      out.filePath = arg;
      continue;
    }
    if (arg === "--json") {
      out.format = "json";
      continue;
    }
    if (arg === "--crewscope") {
      out.format = "crewscope";
      continue;
    }
    if (arg === "--visible-only") {
      out.includeHidden = false;
      continue;
    }
    if (arg === "--include-completed") {
      out.includeCompleted = true;
      continue;
    }
    if (arg === "--start" && argv[i + 1]) {
      out.startDateKey = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--end" && argv[i + 1]) {
      out.endDateKey = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--company" && argv[i + 1]) {
      out.companyName = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--project" && argv[i + 1]) {
      out.projectName = argv[i + 1];
      i += 1;
      continue;
    }
  }
  return out;
}

async function runCli(argv, io = process) {
  const args = parseCliArgs(argv);
  if (!args.filePath) {
    io.stderr.write(
      "Usage: node extractLookaheadSchedule.js <xlsx-file> [--json|--crewscope] [--visible-only] [--include-completed] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--company NAME] [--project NAME]\n"
    );
    return 1;
  }
  if (!fs.existsSync(args.filePath)) {
    io.stderr.write(`File not found: ${args.filePath}\n`);
    return 1;
  }
  const parsed = await parseLookaheadWorkbookFile(args.filePath, args);
  if (args.format === "json") {
    io.stdout.write(`${JSON.stringify(parsed, null, 2)}\n`);
  } else if (args.format === "crewscope") {
    io.stdout.write(`${formatCrewscopeStyleSummary(parsed, args)}\n`);
  } else {
    io.stdout.write(`${formatTaskSummary(parsed)}\n`);
  }
  return 0;
}

module.exports = {
  asTrimmedText,
  parseExcelDate,
  toDateKey,
  inferDateColumns,
  parseLookaheadWorksheet,
  parseLookaheadWorkbookFile,
  parseLookaheadWorkbookBuffer,
  formatTaskSummary,
  formatCrewscopeStyleSummary,
  parseCliArgs,
  runCli,
};
