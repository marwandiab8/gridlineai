/**
 * Callable export: Excel / PDF for issue lists or a single issue (authenticated).
 */

const ExcelJS = require("exceljs");
const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");
const https = require("https");
const http = require("http");
const { URL } = require("url");
const { HttpsError } = require("firebase-functions/v2/https");
const {
  COLLECTION_BY_TYPE,
  ALL_ISSUE_COLLECTIONS,
  TYPE_BY_COLLECTION,
} = require("./issueConstants");
const { sanitizePdfText } = require("./pdfWinAnsiText");
const { assertAuthenticated } = require("./authz");

function formatTs(ts) {
  if (!ts) return "";
  try {
    if (typeof ts.toDate === "function") return ts.toDate().toISOString();
    if (ts.seconds != null) return new Date(ts.seconds * 1000).toISOString();
  } catch (_) {}
  return String(ts);
}

function fetchBuffer(url) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === "https:" ? https : http;
    const req = lib.request(
      {
        hostname: u.hostname,
        port: u.port || (u.protocol === "https:" ? 443 : 80),
        path: u.pathname + u.search,
        method: "GET",
      },
      (res) => {
        if (res.statusCode && res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode}`));
          res.resume();
          return;
        }
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => resolve(Buffer.concat(chunks)));
      }
    );
    req.on("error", reject);
    req.end();
  });
}

function applyFilters(rows, f) {
  let out = rows;
  if (f.projectSlug) {
    out = out.filter((r) => r.projectId === f.projectSlug);
  }
  if (f.status) {
    out = out.filter((r) => r.status === f.status);
  }
  if (f.assignedTo) {
    out = out.filter(
      (r) => (r.assignedTo || "").toLowerCase() === f.assignedTo.toLowerCase()
    );
  }
  if (f.issueType && f.issueType !== "all") {
    out = out.filter((r) => r.issueType === f.issueType);
  }
  if (f.dateFrom) {
    const t = new Date(f.dateFrom).getTime();
    out = out.filter((r) => {
      const c = r.createdAt;
      const ms =
        c && typeof c.toMillis === "function"
          ? c.toMillis()
          : c && c.seconds
            ? c.seconds * 1000
            : 0;
      return ms >= t;
    });
  }
  if (f.dateTo) {
    const t = new Date(f.dateTo).getTime() + 86400000;
    out = out.filter((r) => {
      const c = r.createdAt;
      const ms =
        c && typeof c.toMillis === "function"
          ? c.toMillis()
          : c && c.seconds
            ? c.seconds * 1000
            : 0;
      return ms <= t;
    });
  }
  if (f.search) {
    const s = f.search.toLowerCase();
    out = out.filter((r) => {
      const blob = [
        r.title,
        r.description,
        r.location,
        r.area,
        r.trade,
        r.reference,
        r.requestedAction,
        r.reportedByPhone,
        r.assignedTo,
        r.issueId,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return blob.includes(s);
    });
  }
  return out;
}

async function loadIssuesFromFirestore(db, { scope, issueType, maxPerCollection }) {
  const cap = Math.min(Math.max(maxPerCollection || 800, 1), 3000);
  const cols =
    scope === "all"
      ? ALL_ISSUE_COLLECTIONS
      : [COLLECTION_BY_TYPE[issueType] || COLLECTION_BY_TYPE.general];

  const merged = [];
  for (const col of cols) {
    const snap = await db
      .collection(col)
      .orderBy("createdAt", "desc")
      .limit(cap)
      .get();
    for (const doc of snap.docs) {
      merged.push({
        id: doc.id,
        issueCollection: col,
        issueType: TYPE_BY_COLLECTION[col] || "general",
        ...doc.data(),
      });
    }
  }
  merged.sort((a, b) => {
    const ma = a.createdAt && a.createdAt.toMillis ? a.createdAt.toMillis() : 0;
    const mb = b.createdAt && b.createdAt.toMillis ? b.createdAt.toMillis() : 0;
    return mb - ma;
  });
  return merged;
}

async function buildExcelBuffer(issues, { title, filterDescription }) {
  const wb = new ExcelJS.Workbook();
  wb.creator = "Construction SMS Assistant";
  const headerRow = filterDescription ? 3 : 1;
  const ws = wb.addWorksheet("Issues", {
    views: [{ state: "frozen", ySplit: headerRow }],
  });

  if (filterDescription) {
    ws.mergeCells(1, 1, 1, 19);
    ws.getCell(1, 1).value = filterDescription;
    ws.getCell(1, 1).font = { italic: true, size: 10 };
  }
  const headers = [
    "issueId",
    "issueType",
    "projectId",
    "projectName",
    "title",
    "description",
    "location",
    "area",
    "trade",
    "reference",
    "requestedAction",
    "status",
    "priority",
    "assignedTo",
    "createdAt",
    "updatedAt",
    "closedAt",
    "source",
    "reportedByPhone",
    "reportedByName",
    "dueDate",
    "photoCount",
    "photoLinks",
  ];

  headers.forEach((h, i) => {
    const cell = ws.getCell(headerRow, i + 1);
    cell.value = h;
    cell.font = { bold: true };
    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFE7E7E7" },
    };
  });

  let r = headerRow + 1;
  for (const issue of issues) {
    const photos = Array.isArray(issue.photos) ? issue.photos : [];
    const photoLinks = photos
      .map((p) => p.downloadURL || p.storagePath || "")
      .filter(Boolean)
      .join(" | ");

    const row = [
      issue.issueId || issue.id,
      issue.issueType || "",
      issue.projectId || "",
      issue.projectName || "",
      issue.title || "",
      issue.description || "",
      issue.location || "",
      issue.area || "",
      issue.trade || "",
      issue.reference || "",
      issue.requestedAction || "",
      issue.status || "",
      issue.priority || "",
      issue.assignedTo || "",
      formatTs(issue.createdAt),
      formatTs(issue.updatedAt),
      formatTs(issue.closedAt),
      issue.source || "",
      issue.reportedByPhone || "",
      issue.reportedByName || "",
      formatTs(issue.dueDate),
      photos.length,
      photoLinks,
    ];
    row.forEach((v, i) => {
      ws.getCell(r, i + 1).value = v;
    });
    r += 1;
  }

  ws.columns = headers.map((h, i) => ({
    key: h,
    width: i === 5 || i === 21 ? 40 : 16,
  }));

  const buf = await wb.xlsx.writeBuffer();
  return Buffer.from(buf);
}

async function buildListPdfBuffer(issues, { title, filterDescription, companyName }) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const pageW = 612;
  const pageH = 792;
  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - 48;
  const margin = 48;
  const lineH = 13;
  const maxW = pageW - margin * 2;

  function drawHeader() {
    page.drawText(sanitizePdfText(companyName || "Construction — Issue log"), {
      x: margin,
      y,
      size: 16,
      font: fontBold,
      color: rgb(0.15, 0.15, 0.18),
    });
    y -= 22;
    page.drawText(sanitizePdfText(title || "Issue export"), {
      x: margin,
      y,
      size: 11,
      font,
      color: rgb(0.2, 0.2, 0.25),
    });
    y -= 16;
    if (filterDescription) {
      const words = sanitizePdfText(filterDescription).split(/\s+/);
      let line = "";
      for (const w of words) {
        const test = line ? `${line} ${w}` : w;
        const width = font.widthOfTextAtSize(test, 9);
        if (width > maxW && line) {
          page.drawText(line, { x: margin, y, size: 9, font, color: rgb(0.35, 0.35, 0.4) });
          y -= 11;
          line = w;
        } else {
          line = test;
        }
      }
      if (line) {
        page.drawText(line, { x: margin, y, size: 9, font, color: rgb(0.35, 0.35, 0.4) });
        y -= 14;
      }
    }
    y -= 10;
  }

  drawHeader();

  function ensureSpace(linesNeeded) {
    if (y < margin + linesNeeded * lineH + 40) {
      page = pdf.addPage([pageW, pageH]);
      y = pageH - 48;
    }
  }

  for (const issue of issues) {
    const block = [
      `${issue.issueType || "?"} · ${issue.status || ""} · ${issue.priority || ""}`,
      `${issue.title || "(no title)"}`,
      `Project: ${issue.projectName || issue.projectId || "—"} · ID: ${issue.issueId || issue.id}`,
      `${issue.description || ""}`.slice(0, 500),
      `Created: ${formatTs(issue.createdAt)} · Updated: ${formatTs(issue.updatedAt)}`,
      `Source: ${issue.source || ""} · Reporter: ${issue.reportedByPhone || ""}`,
      "—".repeat(72),
    ];
    const need = block.length + 1;
    ensureSpace(need);
    for (const line of block) {
      page.drawText(sanitizePdfText(line).slice(0, 120), {
        x: margin,
        y,
        size: 10,
        font: line.startsWith("—") ? font : font,
        color: rgb(0.12, 0.12, 0.14),
      });
      y -= lineH;
    }
    y -= 6;
  }

  const bytes = await pdf.save();
  return Buffer.from(bytes);
}

function chunkTextLine(text, maxLen) {
  const t = text || "";
  if (t.length <= maxLen) return [t];
  const out = [];
  for (let i = 0; i < t.length; i += maxLen) {
    out.push(t.slice(i, i + maxLen));
  }
  return out;
}

async function embedImageIfPossible(pdf, buf) {
  try {
    if (buf[0] === 0xff && buf[1] === 0xd8) return await pdf.embedJpg(buf);
    return await pdf.embedPng(buf);
  } catch (_) {
    return null;
  }
}

async function buildSingleIssuePdfBuffer(issue, { companyName }) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const pageW = 612;
  const pageH = 792;
  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - 48;
  const margin = 48;
  const lineH = 14;

  page.drawText(sanitizePdfText(companyName || "Construction — Issue report"), {
    x: margin,
    y,
    size: 15,
    font: fontBold,
    color: rgb(0.12, 0.12, 0.15),
  });
  y -= 26;

  const lines = [
    `Title: ${issue.title || ""}`,
    `Type: ${issue.issueType || ""} · Status: ${issue.status || ""} · Priority: ${issue.priority || ""}`,
    `Project: ${issue.projectName || issue.projectId || "—"}`,
    `Location: ${issue.location || "—"}`,
    `Assigned: ${issue.assignedTo || "—"} · Due: ${formatTs(issue.dueDate) || "—"}`,
    `Created: ${formatTs(issue.createdAt)} · Updated: ${formatTs(issue.updatedAt)}`,
    `Closed: ${formatTs(issue.closedAt) || "—"}`,
    `Source: ${issue.source || ""} · Reporter: ${issue.reportedByPhone || ""} ${issue.reportedByName || ""}`,
    "",
    "Description:",
    (issue.description || "").slice(0, 3500),
  ];

  if (issue.aiSummary) {
    lines.push("", "AI summary:", String(issue.aiSummary).slice(0, 1500));
  }

  const hist = Array.isArray(issue.history) ? issue.history : [];
  if (hist.length) {
    lines.push("", "History:");
    for (const h of hist.slice(-30)) {
      const line = `- ${formatTs(h.at)} ${h.action || ""} ${h.note || ""} ${h.field ? `${h.field}: ${h.oldValue} -> ${h.newValue}` : ""}`;
      lines.push(line.slice(0, 500));
    }
  }

  for (const line of lines) {
    if (line === "") {
      if (y < margin + 60) {
        page = pdf.addPage([pageW, pageH]);
        y = pageH - 48;
      }
      y -= 8;
      continue;
    }
    const chunks = chunkTextLine(line, 95);
    for (const chunk of chunks) {
      if (y < margin + 60) {
        page = pdf.addPage([pageW, pageH]);
        y = pageH - 48;
      }
      page.drawText(sanitizePdfText(chunk), {
        x: margin,
        y,
        size: chunk.startsWith("Title:") ? 12 : 10,
        font: chunk.startsWith("Title:") ? fontBold : font,
        color: rgb(0.1, 0.1, 0.12),
      });
      y -= chunk.startsWith("Title:") ? 18 : lineH;
    }
  }

  const photos = Array.isArray(issue.photos) ? issue.photos : [];
  for (let i = 0; i < Math.min(photos.length, 6); i++) {
    const p = photos[i];
    const url = p.downloadURL;
    if (!url) continue;
    let buf;
    try {
      buf = await fetchBuffer(url);
    } catch (_) {
      continue;
    }
    const img = await embedImageIfPossible(pdf, buf);
    if (!img) continue;

    if (y < 200) {
      page = pdf.addPage([pageW, pageH]);
      y = pageH - 48;
    }
    const w = Math.min(280, img.width);
    const scale = w / img.width;
    const h = img.height * scale;
    page.drawImage(img, {
      x: margin,
      y: y - h,
      width: w,
      height: h,
    });
    y -= h + 24;
  }

  const bytes = await pdf.save();
  return Buffer.from(bytes);
}

function isAllowedIssueCollection(value) {
  return ALL_ISSUE_COLLECTIONS.includes(String(value || "").trim());
}

async function buildActionableListPdfBuffer(issues, { title, filterDescription, companyName }) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const pageW = 612;
  const pageH = 792;
  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - 48;
  const margin = 48;
  const lineH = 13;
  const maxW = pageW - margin * 2;

  function drawWrappedLine(text, size = 10, bold = false, color = rgb(0.12, 0.12, 0.14)) {
    const chunks = chunkTextLine(text, 102);
    for (const chunk of chunks) {
      if (y < margin + 60) {
        page = pdf.addPage([pageW, pageH]);
        y = pageH - 48;
      }
      page.drawText(sanitizePdfText(chunk), {
        x: margin,
        y,
        size,
        font: bold ? fontBold : font,
        color,
      });
      y -= lineH;
    }
  }

  function drawHeader() {
    page.drawText(sanitizePdfText(companyName || "Construction - Issue log"), {
      x: margin,
      y,
      size: 16,
      font: fontBold,
      color: rgb(0.15, 0.15, 0.18),
    });
    y -= 22;
    page.drawText(sanitizePdfText(title || "Issue export"), {
      x: margin,
      y,
      size: 11,
      font,
      color: rgb(0.2, 0.2, 0.25),
    });
    y -= 16;
    if (filterDescription) {
      const words = sanitizePdfText(filterDescription).split(/\s+/);
      let line = "";
      for (const w of words) {
        const test = line ? `${line} ${w}` : w;
        const width = font.widthOfTextAtSize(test, 9);
        if (width > maxW && line) {
          page.drawText(line, { x: margin, y, size: 9, font, color: rgb(0.35, 0.35, 0.4) });
          y -= 11;
          line = w;
        } else {
          line = test;
        }
      }
      if (line) {
        page.drawText(line, { x: margin, y, size: 9, font, color: rgb(0.35, 0.35, 0.4) });
        y -= 14;
      }
    }
    y -= 10;
  }

  drawHeader();

  for (const issue of issues) {
    const photos = Array.isArray(issue.photos) ? issue.photos : [];
    const lines = [
      `${issue.issueType || "?"} - ${issue.status || ""} - ${issue.priority || ""}`,
      `${issue.title || "(no title)"}`,
      `Project: ${issue.projectName || issue.projectId || "-"} - ID: ${issue.issueId || issue.id}`,
      `Location: ${issue.location || "-"} - Area: ${issue.area || "-"}`,
      `Trade: ${issue.trade || "-"} - Reference: ${issue.reference || "-"}`,
      `Required action: ${issue.requestedAction || "-"}`,
      `Assigned: ${issue.assignedTo || "-"} - Due: ${formatTs(issue.dueDate) || "-"}`,
      `${issue.description || ""}`.slice(0, 500),
      `Created: ${formatTs(issue.createdAt)} - Updated: ${formatTs(issue.updatedAt)}`,
      `Source: ${issue.source || ""} - Reporter: ${issue.reportedByPhone || ""} - Photos: ${photos.length}`,
      "-".repeat(72),
    ];
    for (let idx = 0; idx < lines.length; idx += 1) {
      const line = lines[idx];
      drawWrappedLine(line, 10, idx === 1);
    }
    y -= 6;
  }

  const bytes = await pdf.save();
  return Buffer.from(bytes);
}

async function buildActionableSingleIssuePdfBuffer(issue, { companyName }) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const pageW = 612;
  const pageH = 792;
  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - 48;
  const margin = 48;
  const lineH = 14;
  const photoCount = Array.isArray(issue.photos) ? issue.photos.length : 0;

  page.drawText(sanitizePdfText(companyName || "Construction - Issue report"), {
    x: margin,
    y,
    size: 15,
    font: fontBold,
    color: rgb(0.12, 0.12, 0.15),
  });
  y -= 26;

  const lines = [
    `Title: ${issue.title || ""}`,
    `Type: ${issue.issueType || ""} - Status: ${issue.status || ""} - Priority: ${issue.priority || ""}`,
    `Project: ${issue.projectName || issue.projectId || "-"}`,
    `Location: ${issue.location || "-"}`,
    `Area: ${issue.area || "-"}`,
    `Trade: ${issue.trade || "-"}`,
    `Reference: ${issue.reference || "-"}`,
    `Required action: ${issue.requestedAction || "-"}`,
    `Assigned: ${issue.assignedTo || "-"} - Due: ${formatTs(issue.dueDate) || "-"}`,
    `Created: ${formatTs(issue.createdAt)} - Updated: ${formatTs(issue.updatedAt)}`,
    `Closed: ${formatTs(issue.closedAt) || "-"}`,
    `Source: ${issue.source || ""} - Reporter: ${issue.reportedByPhone || ""} ${issue.reportedByName || ""}`,
    `Photos: ${photoCount}`,
    "",
    "Description:",
    (issue.description || "").slice(0, 3500),
  ];

  if (issue.aiSummary) {
    lines.push("", "AI summary:", String(issue.aiSummary).slice(0, 1500));
  }

  const hist = Array.isArray(issue.history) ? issue.history : [];
  if (hist.length) {
    lines.push("", "History:");
    for (const h of hist.slice(-30)) {
      const line = `- ${formatTs(h.at)} ${h.action || ""} ${h.note || ""} ${h.field ? `${h.field}: ${h.oldValue} -> ${h.newValue}` : ""}`;
      lines.push(line.slice(0, 500));
    }
  }

  for (const line of lines) {
    if (line === "") {
      if (y < margin + 60) {
        page = pdf.addPage([pageW, pageH]);
        y = pageH - 48;
      }
      y -= 8;
      continue;
    }
    const chunks = chunkTextLine(line, 95);
    for (const chunk of chunks) {
      if (y < margin + 60) {
        page = pdf.addPage([pageW, pageH]);
        y = pageH - 48;
      }
      page.drawText(sanitizePdfText(chunk), {
        x: margin,
        y,
        size: chunk.startsWith("Title:") ? 12 : 10,
        font: chunk.startsWith("Title:") ? fontBold : font,
        color: rgb(0.1, 0.1, 0.12),
      });
      y -= chunk.startsWith("Title:") ? 18 : lineH;
    }
  }

  const photos = Array.isArray(issue.photos) ? issue.photos : [];
  for (let i = 0; i < Math.min(photos.length, 6); i += 1) {
    const p = photos[i];
    const url = p.downloadURL;
    if (!url) continue;
    let buf;
    try {
      buf = await fetchBuffer(url);
    } catch (_) {
      continue;
    }
    const img = await embedImageIfPossible(pdf, buf);
    if (!img) continue;

    if (y < 200) {
      page = pdf.addPage([pageW, pageH]);
      y = pageH - 48;
    }
    const w = Math.min(280, img.width);
    const scale = w / img.width;
    const h = img.height * scale;
    page.drawImage(img, {
      x: margin,
      y: y - h,
      width: w,
      height: h,
    });
    y -= h + 24;
  }

  const bytes = await pdf.save();
  return Buffer.from(bytes);
}

/**
 * @param {{ db: import('firebase-admin').firestore.Firestore, request: import('firebase-functions/v2/https').CallableRequest }} ctx
 */
async function runIssueExport(ctx) {
  const { db, request, access, projectSlugs } = ctx;
  assertAuthenticated(request);

  const data = request.data || {};
  const format = data.format === "pdf" ? "pdf" : "xlsx";
  const mode = data.mode === "single" ? "single" : "list";
  const filters = data.filters && typeof data.filters === "object" ? data.filters : {};
  const companyName = data.companyName || "";

  if (mode === "single") {
    const issueCollection = data.issueCollection;
    const issueId = data.issueId;
    if (!issueCollection || !issueId) {
      throw new HttpsError("invalid-argument", "issueCollection and issueId required.");
    }
    if (!isAllowedIssueCollection(issueCollection)) {
      throw new HttpsError("permission-denied", "Unsupported issue collection.");
    }
    const snap = await db.collection(issueCollection).doc(issueId).get();
    if (!snap.exists) {
      throw new HttpsError("not-found", "Issue not found.");
    }
    const issue = {
      id: snap.id,
      issueCollection,
      issueType: TYPE_BY_COLLECTION[issueCollection] || "general",
      ...snap.data(),
    };
    if (
      access &&
      access.allProjects !== true &&
      String(access.role || "").trim().toLowerCase() !== "admin"
    ) {
      const allowed = new Set(Array.isArray(projectSlugs) ? projectSlugs : []);
      if (!allowed.has(String(issue.projectId || "").trim().toLowerCase())) {
        throw new HttpsError("permission-denied", "You cannot export this issue.");
      }
    }

    let buffer;
    let mime;
    let filename;
    if (format === "pdf") {
      buffer = await buildActionableSingleIssuePdfBuffer(issue, { companyName });
      mime = "application/pdf";
      filename = `issue-${issue.issueId || issueId}.pdf`;
    } else {
      buffer = await buildExcelBuffer([issue], {
        title: "Single issue",
        filterDescription: `Single issue export · ${issue.issueId}`,
      });
      mime =
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
      filename = `issue-${issue.issueId || issueId}.xlsx`;
    }

    return {
      fileBase64: buffer.toString("base64"),
      mimeType: mime,
      filename,
    };
  }

  const scope = data.scope === "all" ? "all" : "typed";
  const issueTypeKey = data.issueType || "general";
  const raw = await loadIssuesFromFirestore(db, {
    scope,
    issueType: issueTypeKey,
    maxPerCollection: data.maxPerCollection,
  });
  const scopedRows =
    access &&
    access.allProjects !== true &&
    String(access.role || "").trim().toLowerCase() !== "admin"
      ? raw.filter((row) =>
          new Set(Array.isArray(projectSlugs) ? projectSlugs : []).has(
            String(row.projectId || "").trim().toLowerCase()
          )
        )
      : raw;
  const filtered = applyFilters(scopedRows, filters);

  const filterParts = [];
  if (filters.projectSlug) filterParts.push(`project=${filters.projectSlug}`);
  if (filters.status) filterParts.push(`status=${filters.status}`);
  if (filters.assignedTo) filterParts.push(`assigned=${filters.assignedTo}`);
  if (filters.dateFrom) filterParts.push(`from=${filters.dateFrom}`);
  if (filters.dateTo) filterParts.push(`to=${filters.dateTo}`);
  if (filters.search) filterParts.push(`search=${filters.search}`);
  const filterDescription =
    filterParts.length > 0
      ? `Filters: ${filterParts.join(" · ")} · rows=${filtered.length}`
      : `Exported rows=${filtered.length}`;

  const title =
    scope === "all"
      ? "All issue types"
      : `${issueTypeKey} issues`;

  let buffer;
  let mime;
  let filename;
  if (format === "pdf") {
    buffer = await buildActionableListPdfBuffer(filtered, {
      title,
      filterDescription,
      companyName,
    });
    mime = "application/pdf";
    filename = `issues-${scope === "all" ? "all" : issueTypeKey}.pdf`;
  } else {
    buffer = await buildExcelBuffer(filtered, {
      title,
      filterDescription: `${title} · ${filterDescription}`,
    });
    mime =
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    filename = `issues-${scope === "all" ? "all" : issueTypeKey}.xlsx`;
  }

  return {
    fileBase64: buffer.toString("base64"),
    mimeType: mime,
    filename,
  };
}

module.exports = {
  runIssueExport,
  buildExcelBuffer,
  applyFilters,
  loadIssuesFromFirestore,
  isAllowedIssueCollection,
};
