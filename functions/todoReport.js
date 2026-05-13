const ExcelJS = require("exceljs");
const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");
const { sanitizePdfText } = require("./pdfWinAnsiText");

const REPORT_TIME_ZONE = "America/New_York";
const REPORT_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: REPORT_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

function coerceDate(value) {
  if (!value) return null;
  try {
    if (typeof value.toDate === "function") return value.toDate();
    if (value.seconds) return new Date(value.seconds * 1000);
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) return date;
  } catch (_) {}
  return null;
}

function formatTimestamp(value) {
  const date = coerceDate(value);
  if (date) return REPORT_DATE_TIME_FORMATTER.format(date);
  return String(value || "").trim();
}

function normalizeTodoRow(todoSnap) {
  const todo = todoSnap || {};
  return {
    ...todo,
    subTodos: Array.isArray(todo.subTodos) ? todo.subTodos : [],
    comments: Array.isArray(todo.comments) ? todo.comments : [],
    labels: Array.isArray(todo.labels) ? todo.labels : [],
    tags: Array.isArray(todo.tags) ? todo.tags : [],
    reminders: Array.isArray(todo.reminders) ? todo.reminders : [],
    dependencies: Array.isArray(todo.dependencies) ? todo.dependencies : [],
    recurrence:
      todo.recurrence && typeof todo.recurrence === "object"
        ? todo.recurrence
        : { mode: "none", customText: "" },
  };
}

function normalizeStatus(status) {
  return String(status || "open").trim().toLowerCase();
}

function partitionTodos(todos) {
  const active = [];
  const completed = [];
  for (const rawTodo of Array.isArray(todos) ? todos : []) {
    const todo = normalizeTodoRow(rawTodo);
    if (normalizeStatus(todo.status) === "completed") completed.push(todo);
    else active.push(todo);
  }
  return { active, completed };
}

function summarizeTodos(todos) {
  return (Array.isArray(todos) ? todos : []).reduce(
    (acc, rawTodo) => {
      const todo = normalizeTodoRow(rawTodo);
      acc.totalTodos += 1;
      const status = normalizeStatus(todo.status);
      if (status === "completed") acc.completedTodos += 1;
      else if (status === "inprogress") acc.inProgressTodos += 1;
      else acc.openTodos += 1;
      acc.totalSubTodos += todo.subTodos.length;
      acc.totalComments += todo.comments.length;
      for (const subTodo of todo.subTodos) {
        const comments = Array.isArray(subTodo?.comments) ? subTodo.comments : [];
        acc.totalComments += comments.length;
      }
      return acc;
    },
    {
      totalTodos: 0,
      openTodos: 0,
      inProgressTodos: 0,
      completedTodos: 0,
      totalSubTodos: 0,
      totalComments: 0,
    }
  );
}

function formatList(values, prefix = "") {
  return (Array.isArray(values) ? values : [])
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .map((item) => `${prefix}${item}`)
    .join(", ");
}

function formatRecurrence(recurrence) {
  const mode = String(recurrence?.mode || "none").trim();
  const customText = String(recurrence?.customText || "").trim();
  if (mode === "custom" && customText) return `custom: ${customText}`;
  return mode || "none";
}

async function saveReportBuffer({
  storageBucket,
  storagePath,
  buffer,
  contentType,
  downloadToken,
}) {
  const file = storageBucket.file(storagePath);
  const downloadFileName = String(storagePath || "").split("/").pop() || "todo_report";
  await file.save(buffer, {
    contentType,
    contentDisposition: `attachment; filename="${downloadFileName}"`,
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

async function generateTodoReportExcel({
  reportTitle,
  projectSlug,
  todos,
  storageBucket,
  storagePath,
  downloadToken,
}) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "Gridline AI";
  workbook.created = new Date();
  const todoGroups = partitionTodos(todos);

  const todoSheet = workbook.addWorksheet("Todos");
  todoSheet.columns = [
    { header: "Section", key: "section", width: 18 },
    { header: "Todo ID", key: "id", width: 20 },
    { header: "Task", key: "taskText", width: 42 },
    { header: "Status", key: "status", width: 14 },
    { header: "Priority", key: "priority", width: 12 },
    { header: "Due By", key: "dueBy", width: 22 },
    { header: "Started At", key: "startedAt", width: 22 },
    { header: "Finished At", key: "finishedAt", width: 22 },
    { header: "Labels", key: "labels", width: 24 },
    { header: "Tags", key: "tags", width: 24 },
    { header: "Dependencies", key: "dependencies", width: 24 },
    { header: "Reminders", key: "reminders", width: 28 },
    { header: "Recurrence", key: "recurrence", width: 24 },
    { header: "Comments", key: "commentCount", width: 12 },
    { header: "Sub-tasks", key: "subTodoCount", width: 12 },
    { header: "Created By", key: "createdBy", width: 24 },
    { header: "Created At", key: "createdAt", width: 22 },
    { header: "Updated At", key: "updatedAt", width: 22 },
    { header: "Source", key: "source", width: 14 },
  ];

  const subTodoSheet = workbook.addWorksheet("SubTodos");
  subTodoSheet.columns = [
    { header: "Parent Todo ID", key: "parentId", width: 20 },
    { header: "Sub-todo ID", key: "id", width: 20 },
    { header: "Text", key: "text", width: 42 },
    { header: "Status", key: "status", width: 14 },
    { header: "Priority", key: "priority", width: 12 },
    { header: "Due By", key: "dueBy", width: 22 },
    { header: "Started At", key: "startedAt", width: 22 },
    { header: "Finished At", key: "finishedAt", width: 22 },
    { header: "Labels", key: "labels", width: 24 },
    { header: "Tags", key: "tags", width: 24 },
    { header: "Dependencies", key: "dependencies", width: 24 },
    { header: "Reminders", key: "reminders", width: 28 },
    { header: "Recurrence", key: "recurrence", width: 24 },
    { header: "Comments", key: "commentCount", width: 12 },
    { header: "Created By", key: "createdBy", width: 24 },
    { header: "Created At", key: "createdAt", width: 22 },
    { header: "Updated At", key: "updatedAt", width: 22 },
  ];

  const commentsSheet = workbook.addWorksheet("Comments");
  commentsSheet.columns = [
    { header: "Parent Todo ID", key: "parentId", width: 20 },
    { header: "Scope", key: "scope", width: 12 },
    { header: "Sub-todo ID", key: "subTodoId", width: 20 },
    { header: "Comment", key: "text", width: 60 },
    { header: "Created By", key: "createdBy", width: 24 },
    { header: "Created At", key: "createdAt", width: 22 },
  ];

  const summarySheet = workbook.addWorksheet("Summary");
  const summary = summarizeTodos(todos);
  summarySheet.addRows([
    ["Report", reportTitle || "Todo Report"],
    ["Project", projectSlug || "home"],
    ["Total todos", summary.totalTodos],
    ["Open", summary.openTodos],
    ["In progress", summary.inProgressTodos],
    ["Completed", summary.completedTodos],
    ["Sub-tasks", summary.totalSubTodos],
    ["Comments", summary.totalComments],
    ["Generated at", formatTimestamp(new Date())],
  ]);
  summarySheet.columns = [{ width: 24 }, { width: 40 }];

  const todoSectionHeaderFill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFD9EAF7" },
  };
  const completedSectionHeaderFill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFDFF2DF" },
  };
  const completedRowFill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFF3FBF3" },
  };

  function addTodoSectionRow(label, fill) {
    const row = todoSheet.addRow({
      section: label,
      id: "",
      taskText: "",
      status: "",
      priority: "",
      dueBy: "",
      startedAt: "",
      finishedAt: "",
      labels: "",
      tags: "",
      dependencies: "",
      reminders: "",
      recurrence: "",
      commentCount: "",
      subTodoCount: "",
      createdBy: "",
      createdAt: "",
      updatedAt: "",
      source: "",
    });
    row.font = { bold: true };
    row.fill = fill;
    return row;
  }

  function addTodoRow(todo, sectionLabel) {
    const row = todoSheet.addRow({
      section: sectionLabel,
      id: todo.id || "",
      taskText: todo.taskText || "",
      status: todo.status || "open",
      priority: todo.priority || "",
      dueBy: formatTimestamp(todo.dueBy),
      startedAt: formatTimestamp(todo.startedAt),
      finishedAt: formatTimestamp(todo.finishedAt),
      labels: formatList(todo.labels),
      tags: formatList(todo.tags, "@"),
      dependencies: formatList(todo.dependencies),
      reminders: formatList(todo.reminders),
      recurrence: formatRecurrence(todo.recurrence),
      commentCount: todo.comments.length,
      subTodoCount: todo.subTodos.length,
      createdBy: todo.createdByName || todo.createdByEmail || todo.createdByPhone || "",
      createdAt: formatTimestamp(todo.createdAt),
      updatedAt: formatTimestamp(todo.updatedAt),
      source: todo.source || "",
    });
    if (normalizeStatus(todo.status) === "completed") {
      row.fill = completedRowFill;
      row.font = { color: { argb: "FF2D6A2D" } };
    }
  }

  function appendTodoSection(todosInSection, sectionLabel, fill) {
    if (!todosInSection.length) return;
    addTodoSectionRow(sectionLabel, fill);
    for (const todo of todosInSection) {
      addTodoRow(todo, sectionLabel);
    }
  }

  appendTodoSection(todoGroups.active, "Active Todos", todoSectionHeaderFill);
  if (todoGroups.active.length && todoGroups.completed.length) {
    todoSheet.addRow({});
  }
  appendTodoSection(todoGroups.completed, "Completed Todos", completedSectionHeaderFill);

  for (const todo of [...todoGroups.active, ...todoGroups.completed]) {
    for (const comment of todo.comments) {
      commentsSheet.addRow({
        parentId: todo.id || "",
        scope: "todo",
        subTodoId: "",
        text: comment?.text || "",
        createdBy: comment?.createdByName || comment?.createdByEmail || "",
        createdAt: formatTimestamp(comment?.createdAt),
      });
    }

    for (const subTodo of todo.subTodos) {
      const comments = Array.isArray(subTodo?.comments) ? subTodo.comments : [];
      subTodoSheet.addRow({
        parentId: todo.id || "",
        id: subTodo?.id || "",
        text: subTodo?.text || "",
        status: subTodo?.status || "open",
        priority: subTodo?.priority || "",
        dueBy: formatTimestamp(subTodo?.dueBy),
        startedAt: formatTimestamp(subTodo?.startedAt),
        finishedAt: formatTimestamp(subTodo?.finishedAt),
        labels: formatList(subTodo?.labels),
        tags: formatList(subTodo?.tags, "@"),
        dependencies: formatList(subTodo?.dependencies),
        reminders: formatList(subTodo?.reminders),
        recurrence: formatRecurrence(subTodo?.recurrence),
        commentCount: comments.length,
        createdBy: subTodo?.createdByName || subTodo?.createdByEmail || "",
        createdAt: formatTimestamp(subTodo?.createdAt),
        updatedAt: formatTimestamp(subTodo?.updatedAt),
      });

      for (const comment of comments) {
        commentsSheet.addRow({
          parentId: todo.id || "",
          scope: "subtodo",
          subTodoId: subTodo?.id || "",
          text: comment?.text || "",
          createdBy: comment?.createdByName || comment?.createdByEmail || "",
          createdAt: formatTimestamp(comment?.createdAt),
        });
      }
    }
  }

  [todoSheet, subTodoSheet, commentsSheet].forEach((sheet) => {
    sheet.getRow(1).font = { bold: true };
    sheet.views = [{ state: "frozen", ySplit: 1 }];
  });

  const buf = await workbook.xlsx.writeBuffer();
  return saveReportBuffer({
    storageBucket,
    storagePath,
    buffer: Buffer.from(buf),
    contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    downloadToken,
  });
}

async function generateTodoReportPdf({
  reportTitle,
  projectSlug,
  todos,
  storageBucket,
  storagePath,
  downloadToken,
}) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const todoGroups = partitionTodos(todos);
  const pageW = 612;
  const pageH = 792;
  const margin = 42;
  const maxW = pageW - margin * 2;
  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - margin;

  const colors = {
    ink: rgb(0.12, 0.12, 0.15),
    muted: rgb(0.38, 0.4, 0.45),
    rule: rgb(0.82, 0.84, 0.88),
  };

  function ensure(space = 18) {
    if (y - space < margin) {
      page = pdf.addPage([pageW, pageH]);
      y = pageH - margin;
    }
  }

  function wrap(text, size = 10, bold = false, x = margin, width = maxW, color = colors.ink) {
    const source = sanitizePdfText(String(text || "").trim());
    if (!source) return;
    const words = source.split(/\s+/);
    let line = "";
    for (const word of words) {
      const candidate = line ? `${line} ${word}` : word;
      const usedFont = bold ? fontBold : font;
      if (usedFont.widthOfTextAtSize(candidate, size) <= width || !line) {
        line = candidate;
        continue;
      }
      ensure(size + 8);
      page.drawText(line, { x, y, size, font: usedFont, color });
      y -= size + 5;
      line = word;
    }
    if (line) {
      ensure(size + 8);
      page.drawText(line, { x, y, size, font: bold ? fontBold : font, color });
      y -= size + 5;
    }
  }

  function rule() {
    ensure(16);
    page.drawLine({
      start: { x: margin, y: y - 4 },
      end: { x: pageW - margin, y: y - 4 },
      thickness: 0.7,
      color: colors.rule,
    });
    y -= 14;
  }

  const summary = summarizeTodos(todos);
  const sectionedTodos = [
    { title: "Open / In Progress Todos", items: todoGroups.active },
    { title: "Completed Todos", items: todoGroups.completed },
  ];
  wrap(reportTitle || "Todo Report", 18, true);
  wrap(`Project: ${projectSlug || "home"}`, 10, false, margin, maxW, colors.muted);
  wrap(`Generated: ${formatTimestamp(new Date())}`, 10, false, margin, maxW, colors.muted);
  y -= 4;
  rule();
  wrap(
    `Todos ${summary.totalTodos} · Open ${summary.openTodos} · In progress ${summary.inProgressTodos} · Completed ${summary.completedTodos} · Sub-tasks ${summary.totalSubTodos} · Comments ${summary.totalComments}`,
    10,
    false
  );
  y -= 6;
  rule();

  let index = 0;
  for (const section of sectionedTodos) {
    ensure(24);
    wrap(`${section.title} (${section.items.length})`, 12, true);
    if (!section.items.length) {
      wrap("None.", 9.5, false, margin, maxW, colors.muted);
      y -= 2;
      rule();
      continue;
    }
    for (const todo of section.items) {
      index += 1;
      ensure(80);
      wrap(`${index}. ${todo.taskText || "(untitled todo)"}`, 13, true);
      wrap(
        `Status: ${todo.status || "open"} · Priority: ${todo.priority || "-"} · Due: ${formatTimestamp(todo.dueBy) || "-"} · Created by: ${
          todo.createdByName || todo.createdByEmail || todo.createdByPhone || "-"
        }`,
        9.5,
        false,
        margin,
        maxW,
        colors.muted
      );
      wrap(
        `Labels: ${formatList(todo.labels) || "-"} · Tags: ${formatList(todo.tags, "@") || "-"} · Dependencies: ${formatList(
          todo.dependencies
        ) || "-"}`,
        9,
        false,
        margin,
        maxW,
        colors.muted
      );
      wrap(
        `Recurrence: ${formatRecurrence(todo.recurrence)} · Reminders: ${formatList(todo.reminders) || "-"} · Sub-tasks: ${todo.subTodos.length} · Comments: ${todo.comments.length}`,
        9,
        false,
        margin,
        maxW,
        colors.muted
      );
      if (todo.comments.length) {
        wrap("Comments:", 9.5, true);
        for (const comment of todo.comments.slice(-3)) {
          wrap(
            `- ${comment?.text || ""} (${comment?.createdByName || comment?.createdByEmail || "-"} · ${formatTimestamp(
              comment?.createdAt
            ) || "-"})`,
            9
          );
        }
      }
      if (todo.subTodos.length) {
        wrap("Sub-tasks:", 9.5, true);
        for (const subTodo of todo.subTodos) {
          const comments = Array.isArray(subTodo?.comments) ? subTodo.comments : [];
          wrap(
            `- ${subTodo?.text || ""} · ${subTodo?.status || "open"} · Priority ${subTodo?.priority || "-"} · Due ${
              formatTimestamp(subTodo?.dueBy) || "-"
            }`,
            9
          );
          if (comments.length) {
            for (const comment of comments.slice(-2)) {
              wrap(
                `  Comment: ${comment?.text || ""} (${comment?.createdByName || comment?.createdByEmail || "-"} · ${formatTimestamp(
                  comment?.createdAt
                ) || "-"})`,
                8.5
              );
            }
          }
        }
      }
      y -= 2;
      rule();
    }
  }

  const bytes = await pdf.save();
  return saveReportBuffer({
    storageBucket,
    storagePath,
    buffer: Buffer.from(bytes),
    contentType: "application/pdf",
    downloadToken,
  });
}

module.exports = {
  generateTodoReportExcel,
  generateTodoReportPdf,
  partitionTodos,
  summarizeTodos,
};
