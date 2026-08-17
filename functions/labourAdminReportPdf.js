const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");
const {
  LABOUR_QUERY_TIME_ZONE,
  formatHours,
  formatHoursMinutes,
} = require("./labourAdminQuery");

const ADMIN_LABOUR_REPORT_COLLECTION = "adminLabourReports";

function canAccessAdminLabourReportMetadata(access) {
  return Boolean(access && String(access.role || "").trim().toLowerCase() === "admin");
}

function formatTorontoTimestamp(value = new Date()) {
  const date = value instanceof Date && !Number.isNaN(value.getTime()) ? value : new Date();
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: LABOUR_QUERY_TIME_ZONE,
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(date);
}

function buildAdminLabourPdfModel(result, generatedAt = new Date()) {
  if (!result || typeof result !== "object" || !result.request) {
    throw new Error("Canonical labour aggregation result is required.");
  }
  return {
    title: result.request.projectName
      ? `${result.request.projectName} Labour Report`
      : "GridlineAI Labour Report",
    periodLabel: result.request.periodLabel || "Selected period",
    startKey: result.request.startKey || null,
    endKey: result.request.endKey || null,
    generatedAtToronto: formatTorontoTimestamp(generatedAt),
    totalMinutes: result.totalMinutes,
    totalHours: formatHours(result.totalMinutes),
    totalHoursMinutes: formatHoursMinutes(result.totalMinutes),
    workerCount: result.workerCount,
    entryCount: result.entryCount,
    projectCount: result.projectCount,
    excludedCount: result.excludedCount,
    excludedReasons: { ...(result.excludedReasons || {}) },
    auditFlags: { ...(result.auditFlags || {}) },
    sourceStatement: "Totals were calculated from current canonical labourEntries using integer minutes.",
    sections: (result.sections || []).map((section) => ({
      projectSlug: section.projectSlug,
      projectName: section.projectName,
      totalMinutes: section.totalMinutes,
      totalHours: formatHours(section.totalMinutes),
      totalHoursMinutes: formatHoursMinutes(section.totalMinutes),
      workerCount: section.workerCount,
      entryCount: section.entryCount,
      workerTotals: (section.workerTotals || []).map((row) => ({
        workerId: row.workerId,
        workerName: row.workerName,
        totalMinutes: row.totalMinutes,
        totalHours: formatHours(row.totalMinutes),
      })),
      dayTotals: (section.dayTotals || []).map((row) => ({
        reportDateKey: row.reportDateKey,
        totalMinutes: row.totalMinutes,
        totalHours: formatHours(row.totalMinutes),
      })),
      workerDayTotals: (section.workerDayTotals || []).map((row) => ({
        workerId: row.workerId,
        workerName: row.workerName,
        reportDateKey: row.reportDateKey,
        totalMinutes: row.totalMinutes,
        totalHours: formatHours(row.totalMinutes),
      })),
    })),
  };
}

function wrapText(text, font, size, maxWidth) {
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  if (!words.length) return [""];
  const lines = [];
  let current = "";
  for (const word of words) {
    const candidate = current ? `${current} ${word}` : word;
    if (!current || font.widthOfTextAtSize(candidate, size) <= maxWidth) {
      current = candidate;
    } else {
      lines.push(current);
      current = word;
    }
  }
  if (current) lines.push(current);
  return lines;
}

async function renderAdminLabourReportPdf(model) {
  const pdf = await PDFDocument.create();
  const regular = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const pageSize = [612, 792];
  const margin = 44;
  const colors = {
    ink: rgb(0.12, 0.14, 0.18),
    muted: rgb(0.4, 0.43, 0.48),
    accent: rgb(0.18, 0.33, 0.52),
    line: rgb(0.82, 0.84, 0.87),
  };
  let page;
  let y;

  const newPage = () => {
    page = pdf.addPage(pageSize);
    y = pageSize[1] - margin;
  };
  const ensure = (height) => {
    if (y - height < margin) newPage();
  };
  const draw = (text, { size = 9.5, isBold = false, color = colors.ink, indent = 0 } = {}) => {
    const font = isBold ? bold : regular;
    const width = pageSize[0] - margin * 2 - indent;
    for (const line of wrapText(text, font, size, width)) {
      ensure(size + 8);
      page.drawText(line, { x: margin + indent, y, size, font, color });
      y -= size + 5;
    }
  };
  const rule = () => {
    ensure(14);
    page.drawLine({
      start: { x: margin, y: y - 4 },
      end: { x: pageSize[0] - margin, y: y - 4 },
      thickness: 0.7,
      color: colors.line,
    });
    y -= 14;
  };
  const section = (title) => {
    ensure(28);
    y -= 4;
    draw(title, { size: 13, isBold: true, color: colors.accent });
    rule();
  };

  newPage();
  draw(model.title, { size: 19, isBold: true });
  draw(`${model.periodLabel}: ${model.startKey || "-"} to ${model.endKey || "-"}`, {
    size: 10.5,
    color: colors.muted,
  });
  draw(`Generated ${model.generatedAtToronto}`, { size: 9.5, color: colors.muted });
  rule();
  draw(`Grand total: ${model.totalHours} hours · ${model.totalHoursMinutes} · ${model.totalMinutes.toLocaleString("en-CA")} minutes`, {
    size: 12,
    isBold: true,
  });
  draw(`${model.workerCount} labourers · ${model.entryCount} canonical entries · ${model.projectCount} projects`, {
    size: 10,
  });
  draw(model.sourceStatement, { size: 9, color: colors.muted });
  if (model.excludedCount) {
    draw(`Warning: ${model.excludedCount} invalid ${model.excludedCount === 1 ? "entry was" : "entries were"} excluded and ${model.excludedCount === 1 ? "requires" : "require"} review.`, {
      size: 9.5,
      isBold: true,
      color: rgb(0.65, 0.22, 0.16),
    });
  }
  const legacyConflictCount = Number(model.auditFlags?.contradictory_legacy_hours_ignored || 0);
  if (legacyConflictCount) {
    draw(`Warning: ${legacyConflictCount} conflicting legacy hours ${legacyConflictCount === 1 ? "value was" : "values were"} ignored and ${legacyConflictCount === 1 ? "requires" : "require"} review.`, {
      size: 9.5,
      isBold: true,
      color: rgb(0.65, 0.22, 0.16),
    });
  }

  for (const project of model.sections) {
    section(project.projectName);
    draw(`Project subtotal: ${project.totalHours} hours · ${project.totalHoursMinutes}`, {
      size: 11,
      isBold: true,
    });
    draw(`${project.workerCount} labourers · ${project.entryCount} canonical entries`, {
      size: 9.5,
      color: colors.muted,
    });

    draw("Per-worker totals", { size: 10.5, isBold: true });
    for (const worker of project.workerTotals) {
      draw(`${worker.workerName}: ${worker.totalHours} hours (${worker.totalMinutes} minutes)`, {
        size: 9.2,
        indent: 12,
      });
    }

    draw("Per-day totals", { size: 10.5, isBold: true });
    for (const day of project.dayTotals) {
      draw(`${day.reportDateKey}: ${day.totalHours} hours (${day.totalMinutes} minutes)`, {
        size: 9.2,
        indent: 12,
      });
    }

    draw("Worker-by-day breakdown", { size: 10.5, isBold: true });
    for (const row of project.workerDayTotals) {
      draw(`${row.reportDateKey} · ${row.workerName}: ${row.totalHours} hours (${row.totalMinutes} minutes)`, {
        size: 9.2,
        indent: 12,
      });
    }
  }

  return Buffer.from(await pdf.save());
}

async function generateAdminLabourReportPdf({ result, generatedAt, storageBucket, storagePath }) {
  if (!storageBucket || typeof storageBucket.file !== "function") {
    throw new Error("Storage bucket is required.");
  }
  const path = String(storagePath || "").trim();
  if (!/^adminLabourReports\/[a-f0-9]{40}\.pdf$/.test(path)) {
    throw new Error("Admin labour report storage path is invalid.");
  }
  const model = buildAdminLabourPdfModel(result, generatedAt);
  const bytes = await renderAdminLabourReportPdf(model);
  await storageBucket.file(path).save(bytes, {
    contentType: "application/pdf",
    contentDisposition: 'attachment; filename="GridlineAI_Labour_Report.pdf"',
    metadata: {
      cacheControl: "private, max-age=0, no-store",
    },
    resumable: false,
  });
  return { storagePath: path, byteLength: bytes.length, model };
}

module.exports = {
  ADMIN_LABOUR_REPORT_COLLECTION,
  buildAdminLabourPdfModel,
  canAccessAdminLabourReportMetadata,
  formatTorontoTimestamp,
  generateAdminLabourReportPdf,
  renderAdminLabourReportPdf,
};
