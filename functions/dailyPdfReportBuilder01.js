/**
 * Formal Daily Site Log PDF — wrapped tables, cover block, inline photos by log entry.
 */

const { rgb } = require("pdf-lib");
const { sanitizePdfText } = require("./pdfWinAnsiText");
const { parsePipeRows, reportLineText, lineText } = require("./dailyReportContent");
const { formatWallDateTimeEt } = require("./logClassifier");
const {
  prepareMergedForPdf,
  collectIssuePhotoMediaIds,
  filterPhotosNotForIssues,
  dedupePhotosByMediaId,
  capPhotosBySimilarCaption,
  sortPhotosForRender,
  refineCaptionForPdf,
  issueChunkLineRedundant,
  stripSourceLogArtifacts,
  filterManpowerChunksForPdf,
  buildCaptionDedupHaystack,
} = require("./dailyPdfCompact");

const LEADING = 3;
const CELL_PAD = 4;
const TABLE_FONT = 8;
const TABLE_HEADER_FONT = 9;
const MAX_CELL_LINES = 8;
const MAX_PHOTOS_WEATHER = 4;
const MAX_PHOTOS_MANPOWER = 5;
const MAX_PHOTOS_WORK = 6;
const MAX_PHOTOS_ISSUES = 5;
const MAX_PHOTOS_CONCRETE = 5;
const MAX_PHOTOS_INSPECTION = 5;
const MAX_PHOTOS_SITE = 12;
/** Slightly tighter photos for grid-friendly layout */
const PHOTO_MAX_H = 142;
const PHOTO_MAX_W = 265;

function wrapToLines(text, font, size, maxWidth) {
  const s = sanitizePdfText(String(text || "").trim());
  if (!s) return [""];
  const words = s.split(/\s+/);
  const lines = [];
  let cur = "";
  for (const w of words) {
    const tryLine = cur ? `${cur} ${w}` : w;
    if (font.widthOfTextAtSize(tryLine, size) <= maxWidth) {
      cur = tryLine;
    } else {
      if (cur) lines.push(cur);
      if (font.widthOfTextAtSize(w, size) <= maxWidth) cur = w;
      else {
        let chunk = "";
        for (const ch of w) {
          const t = chunk + ch;
          if (font.widthOfTextAtSize(t, size) > maxWidth && chunk) {
            lines.push(chunk);
            chunk = ch;
          } else {
            chunk = t;
          }
        }
        cur = chunk;
      }
    }
  }
  if (cur) lines.push(cur);
  return lines.length ? lines : [""];
}

async function embedImageIfPossible(pdf, buf) {
  try {
    if (!buf || buf.length < 2) return null;
    if (buf[0] === 0xff && buf[1] === 0xd8) return await pdf.embedJpg(buf);
    return await pdf.embedPng(buf);
  } catch (_) {
    return null;
  }
}

/**
 * @param {object} opts
 */
async function renderDailySiteLogPdf(opts) {
  const {
    pdf,
    font,
    fontBold,
    storageBucket,
    pageW = 612,
    pageH = 792,
    margin = 50,
    titleStr,
    footerBrand,
    coverTitle,
    coverMeta,
    merged,
    model,
    concreteLabel,
    logger,
    runId,
    logoStoragePath,
  } = opts;

  const contentW = pageW - 2 * margin;
  const footerReserve = 42;
  const headerReserve2 = 36;
  /** Professional construction-report palette — navy/slate, restrained */
  const C = {
    ink: rgb(0.1, 0.14, 0.24),
    inkBody: rgb(0.12, 0.15, 0.21),
    inkMuted: rgb(0.42, 0.45, 0.5),
    rule: rgb(0.38, 0.46, 0.56),
    ruleLight: rgb(0.78, 0.82, 0.88),
    tableHdr: rgb(0.76, 0.82, 0.91),
    tableHdrBorder: rgb(0.52, 0.6, 0.72),
    tableHdrText: rgb(0.08, 0.12, 0.22),
    rowA: rgb(0.99, 0.995, 1),
    rowB: rgb(0.93, 0.96, 0.99),
    rowBorder: rgb(0.74, 0.79, 0.86),
    subhead: rgb(0.24, 0.36, 0.5),
    trade: rgb(0.12, 0.22, 0.36),
    coverRule: rgb(0.5, 0.58, 0.68),
  };
  let page = pdf.addPage([pageW, pageH]);
  let pageIndex = 0;
  let y = pageH - margin - 14;

  function ensureSectionOpening(minBottom = 130) {
    if (y - margin - footerReserve < minBottom) newPage();
  }

  function newPage() {
    page = pdf.addPage([pageW, pageH]);
    pageIndex++;
    y = pageH - margin - headerReserve2;
  }

  function ensureSpace(need) {
    if (y - need < margin + footerReserve) newPage();
  }

  function drawLineFull(yy, thickness, color) {
    page.drawLine({
      start: { x: margin, y: yy },
      end: { x: pageW - margin, y: yy },
      thickness,
      color: color || rgb(0.78, 0.8, 0.84),
    });
  }

  function drawParagraph(text, size, bold, color) {
    drawParagraphInColumn(margin, contentW, text, size, bold, color);
  }

  function drawParagraphInColumn(leftX, maxW, text, size, bold, color) {
    const f = bold ? fontBold : font;
    const lines = wrapToLines(String(text || ""), f, size, maxW);
    const lh = size + LEADING;
    for (const line of lines) {
      ensureSpace(lh);
      page.drawText(sanitizePdfText(line), {
        x: leftX,
        y,
        size,
        font: f,
        color: color || C.inkBody,
      });
      y -= lh;
    }
  }

  /** Rule + heading + spacing — must match ensureSpaceForSectionTitleAndTableBlock reserve (allows 1–2 title lines) */
  const SECTION_TITLE_BLOCK_RESERVE = 124;

  function drawSectionTitle(title) {
    ensureSectionOpening(118);
    y -= 14;
    ensureSpace(44);
    drawLineFull(y + 3, 1.05, C.rule);
    y -= 12;
    drawParagraph(title, 12.5, true, C.ink);
    y -= 10;
  }

  function drawSubheading(t) {
    y -= 2;
    drawParagraph(t, 9.5, true, C.subhead);
    y -= 4;
  }

  /** Visual break so narrative after a table is not read as part of the grid. */
  function drawPostTableDivider() {
    y -= 4;
    drawLineFull(y + 2, 0.35, C.ruleLight);
    y -= 12;
  }

  function computeWrappedTableRowHeights(rows, colWidths) {
    const safeRows = rows || [];
    return safeRows.map((row) => {
      let maxLines = 1;
      for (let c = 0; c < colWidths.length; c++) {
        const raw = String((row || [])[c] ?? "");
        const lines = wrapToLines(raw, font, TABLE_FONT, colWidths[c] - CELL_PAD * 2).slice(0, MAX_CELL_LINES);
        maxLines = Math.max(maxLines, lines.length);
      }
      return maxLines * (TABLE_FONT + 2) + CELL_PAD * 2;
    });
  }

  /**
   * Keep section heading + table header + first row chunk on one page when practical
   * (avoids a section title alone with the table starting on the next page).
   * @param {number} [extraBeforeTable] — e.g. intro paragraph height before the table
   */
  function ensureSpaceForSectionTitleAndTableBlock(rows, colWidths, extraBeforeTable = 0) {
    const headerH = 20;
    const rowHeights = computeWrappedTableRowHeights(rows, colWidths);
    const firstRowH = rowHeights[0] || 28;
    const minTable = headerH + 8 + firstRowH + 20;
    const need = SECTION_TITLE_BLOCK_RESERVE + extraBeforeTable + minTable;
    if (y - margin - footerReserve < need) {
      newPage();
    }
  }

  function estimateWrappedTextHeight(text, size, maxW) {
    const t = String(text || "").trim();
    if (!t) return 0;
    const lines = wrapToLines(t, font, size, maxW);
    return lines.length * (size + LEADING) + 6;
  }

  /**
   * Multi-line cells; paginates with repeated headers when a row would orphan.
   */
  function drawWrappedTable(headers, rows, colWidths) {
    const safeRows = rows || [];
    const totalW = colWidths.reduce((a, b) => a + b, 0);
    const headerH = 20;
    const x0 = margin;

    const rowHeights = computeWrappedTableRowHeights(safeRows, colWidths);

    const firstRowH = rowHeights[0] || 28;
    const minBlock = headerH + 8 + firstRowH + 16;
    if (y - margin - footerReserve < minBlock) {
      newPage();
    }

    const drawHeader = (continued) => {
      ensureSpace(headerH + 8);
      page.drawRectangle({
        x: x0,
        y: y - headerH,
        width: totalW,
        height: headerH,
        color: C.tableHdr,
        borderColor: C.tableHdrBorder,
        borderWidth: 0.85,
      });
      let cx = x0 + CELL_PAD;
      for (let i = 0; i < headers.length; i++) {
        const rawH = String(headers[i] ?? "");
        const hlab = continued && i === 0 && rawH ? `${rawH} (cont.)` : rawH;
        const lines = wrapToLines(hlab, fontBold, TABLE_HEADER_FONT, colWidths[i] - CELL_PAD * 2).slice(0, 4);
        let ly = y - headerH + 6;
        for (const ln of lines) {
          page.drawText(sanitizePdfText(ln), {
            x: cx,
            y: ly,
            size: TABLE_HEADER_FONT,
            font: fontBold,
            color: C.tableHdrText,
          });
          ly -= TABLE_HEADER_FONT + 2;
        }
        cx += colWidths[i];
      }
      y -= headerH + 4;
    };

    drawHeader(false);

    for (let r = 0; r < safeRows.length; r++) {
      const row = safeRows[r];
      const cellLinesArr = [];
      let maxLines = 1;
      for (let c = 0; c < colWidths.length; c++) {
        const raw = String(row[c] ?? "");
        const lines = wrapToLines(raw, font, TABLE_FONT, colWidths[c] - CELL_PAD * 2).slice(0, MAX_CELL_LINES);
        cellLinesArr.push(lines);
        maxLines = Math.max(maxLines, lines.length);
      }
      const rowH = maxLines * (TABLE_FONT + 2) + CELL_PAD * 2;
      if (y - margin - footerReserve < rowH + 8) {
        newPage();
        drawHeader(true);
      }
      const fill = r % 2 === 0 ? C.rowA : C.rowB;
      page.drawRectangle({
        x: x0,
        y: y - rowH,
        width: totalW,
        height: rowH,
        color: fill,
        borderColor: C.rowBorder,
        borderWidth: 0.5,
      });
      let cx = x0 + CELL_PAD;
      for (let c = 0; c < colWidths.length; c++) {
        let ly = y - CELL_PAD - TABLE_FONT;
        for (const ln of cellLinesArr[c]) {
          page.drawText(sanitizePdfText(ln), {
            x: cx,
            y: ly,
            size: TABLE_FONT,
            font,
            color: C.inkBody,
          });
          ly -= TABLE_FONT + 2;
        }
        cx += colWidths[c];
      }
      y -= rowH;
    }
    y -= 16;
  }

  /** @returns {Promise<{ bottomY: number, leftW: number }>} */
  async function drawCoverLogoBand(topY) {
    const boxW = 132;
    const maxH = 52;
    const leftX = margin;
    if (logoStoragePath && storageBucket) {
      let buf;
      try {
        [buf] = await storageBucket.file(logoStoragePath).download();
      } catch (_) {
        buf = null;
      }
      const img = buf ? await embedImageIfPossible(pdf, buf) : null;
      if (img) {
        const scale = boxW / img.width;
        const h = Math.min(img.height * scale, maxH);
        const w = img.width * (h / img.height);
        page.drawImage(img, { x: leftX, y: topY - h, width: w, height: h });
        return { bottomY: topY - h - 10, leftW: boxW };
      }
    }
    const h = 52;
    page.drawRectangle({
      x: leftX,
      y: topY - h,
      width: boxW,
      height: h,
      borderWidth: 1,
      borderColor: rgb(0.68, 0.7, 0.76),
      color: rgb(0.96, 0.97, 0.99),
    });
    page.drawLine({
      start: { x: leftX + 4, y: topY - 8 },
      end: { x: leftX + boxW - 4, y: topY - h + 8 },
      thickness: 0.35,
      color: rgb(0.88, 0.89, 0.92),
    });
    page.drawText(sanitizePdfText("Company logo"), {
      x: leftX + 10,
      y: topY - h + 20,
      size: 7.5,
      font: fontBold,
      color: rgb(0.48, 0.5, 0.55),
    });
    page.drawText(sanitizePdfText("Optional — set in project settings"), {
      x: leftX + 10,
      y: topY - h + 8,
      size: 6.5,
      font,
      color: rgb(0.58, 0.6, 0.64),
    });
    return { bottomY: topY - h - 10, leftW: boxW };
  }

  function drawCoverTitleBlock(leftX, maxW, topY, titleMain, titleDate, projectLine, brandLine) {
    let yy = topY;
    if (brandLine) {
      page.drawText(sanitizePdfText(String(brandLine).slice(0, 80)), {
        x: leftX,
        y: yy,
        size: 8,
        font,
        color: C.inkMuted,
      });
      yy -= 9 + LEADING;
    }
    const main = String(titleMain || "").trim() || String(coverTitle || "");
    const linesMain = wrapToLines(main, fontBold, 18, maxW);
    const lh = 18 + LEADING;
    for (const line of linesMain) {
      page.drawText(sanitizePdfText(line), {
        x: leftX,
        y: yy,
        size: 18,
        font: fontBold,
        color: C.ink,
      });
      yy -= lh;
    }
    if (titleDate) {
      page.drawText(sanitizePdfText(String(titleDate)), {
        x: leftX,
        y: yy,
        size: 11,
        font,
        color: C.subhead,
      });
      yy -= 11 + LEADING + 2;
    }
    if (projectLine) {
      page.drawText(sanitizePdfText(String(projectLine).slice(0, 120)), {
        x: leftX,
        y: yy,
        size: 10.5,
        font: fontBold,
        color: C.inkBody,
      });
      yy -= 10.5 + LEADING + 4;
    }
    return yy;
  }

  function drawCoverMetaGrid(meta) {
    if (!meta) return;
    const rows = meta.grid || [];
    const labelW = 96;
    const gap = 10;
    const valX = margin + labelW + gap;
    const valW = Math.max(120, contentW - labelW - gap);
    for (const row of rows) {
      const lab = String(row.label || "");
      let val = String(row.value ?? "");
      const trimmed = val.trim();
      const isPlaceholder = !trimmed || trimmed === "—";
      if (isPlaceholder) val = "Not specified";
      const valColor = isPlaceholder ? C.inkMuted : C.inkBody;
      const lines = wrapToLines(val, font, 9.5, valW);
      const lh = 9.5 + 2;
      const rowH = Math.max(20, lines.length * lh + 10);
      ensureSpace(rowH);
      const ty = y - 12;
      page.drawText(sanitizePdfText(lab), {
        x: margin,
        y: ty,
        size: 8.5,
        font: fontBold,
        color: C.subhead,
      });
      let vy = ty;
      for (const ln of lines) {
        page.drawText(sanitizePdfText(ln), {
          x: valX,
          y: vy,
          size: 9.5,
          font,
          color: valColor,
        });
        vy -= lh;
      }
      y = vy - 8;
    }
    if (meta.lines && meta.lines.length) {
      y -= 2;
      for (const ln of meta.lines) {
        drawParagraphInColumn(margin, contentW, ln, 8.5, false, C.inkMuted);
      }
    }
  }

  function drawTradeHeading(label) {
    ensureSectionOpening(96);
    y -= 8;
    ensureSpace(30);
    drawParagraphInColumn(margin + 2, contentW - 4, label, 10.5, true, C.trade);
    drawLineFull(y + 2, 0.55, C.ruleLight);
    y -= 8;
  }

  async function drawPhotoBlock(photo, opts = {}) {
    const indent = Number(opts.indent) || 0;
    const left = opts.left != null ? opts.left : margin + indent;
    const maxW = Math.min(
      opts.maxBoxW != null ? opts.maxBoxW : Math.min(PHOTO_MAX_W, contentW - indent),
      contentW - (left - margin)
    );
    const maxH = opts.maxBoxH != null ? opts.maxBoxH : PHOTO_MAX_H;
    const captionContext = opts.captionContext || "";
    let buf;
    try {
      [buf] = await storageBucket.file(photo.storagePath).download();
    } catch (e) {
      if (logger)
        logger.warn("dailyPdfReportBuilder: photo download failed", {
          runId,
          path: photo.storagePath,
          message: e.message,
        });
      drawParagraphInColumn(left, maxW, `(Photo unavailable)`, 8, false, rgb(0.45, 0.22, 0.22));
      return;
    }
    const img = await embedImageIfPossible(pdf, buf);
    if (!img) {
      drawParagraphInColumn(left, maxW, `(Unsupported image format)`, 8, false);
      return;
    }
    const scale = Math.min(maxW / img.width, maxH / img.height);
    const w = img.width * scale;
    const h = img.height * scale;
    const need = h + 44;
    ensureSpace(need);
    page.drawImage(img, {
      x: left,
      y: y - h,
      width: w,
      height: h,
    });
    y -= h + 6;
    let ts = "";
    try {
      if (photo.createdAt && typeof photo.createdAt.toDate === "function") {
        ts = formatWallDateTimeEt(photo.createdAt.toDate());
      }
    } catch (_) {}
    let capBody = photo.captionText ? sanitizePdfText(photo.captionText).slice(0, 220) : "";
    const m = opts.model;
    if (!String(capBody || "").trim() && m && m.entryById && photo.linkedLogEntryId) {
      const e = m.entryById.get(String(photo.linkedLogEntryId).trim());
      if (e) {
        const dk = m.reportDateKey;
        capBody = dk ? reportLineText(e, dk) : lineText(e);
        capBody = sanitizePdfText(String(capBody || "").slice(0, 220));
      }
    }
    capBody = refineCaptionForPdf(capBody, captionContext, captionHayNorm);
    const cap = [ts, capBody].filter(Boolean).join(" — ");
    drawParagraphInColumn(left, maxW, cap || `Ref ${photo.mediaId || ""}`, 8, false, C.inkMuted);
    y -= 4;
  }

  async function drawPhotoList(list, opts = {}) {
    const {
      indent = 0,
      model: mdl,
      globalSeen,
      issueMediaIds: issueIds,
      forWorkSection,
      maxPhotos = 8,
      captionContext,
      maxBoxH,
      maxBoxW,
      captionKeyMax = 2,
    } = opts;
    let photos = dedupePhotosByMediaId(list || []);
    photos = photos.filter((p) => p && (globalSeen ? !globalSeen.has(String(p.mediaId)) : true));
    if (forWorkSection && issueIds && issueIds.size) {
      photos = filterPhotosNotForIssues(photos, issueIds);
    }
    photos = capPhotosBySimilarCaption(sortPhotosForRender(photos), captionKeyMax);
    photos = photos.slice(0, maxPhotos);
    if (photos.length) {
      const est = Math.min(500, photos.length * 158);
      if (y - margin - footerReserve < est) {
        newPage();
      }
    }
    for (const p of photos) {
      await drawPhotoBlock(p, {
        indent,
        model: mdl || model,
        captionContext,
        maxBoxH,
        maxBoxW,
      });
      if (globalSeen && p.mediaId != null) globalSeen.add(String(p.mediaId));
    }
  }

  function collectPlacedPhotoIds(st) {
    const ids = new Set();
    function add(arr) {
      for (const p of arr || []) {
        if (p && p.mediaId != null) ids.add(String(p.mediaId));
      }
    }
    for (const ch of st.weatherChunks || []) add(ch.photos);
    for (const ch of st.manpowerChunks || []) add(ch.photos);
    for (const ch of st.inspectionChunks || []) add(ch.photos);
    for (const ch of st.concreteChunks || []) add(ch.photos);
    for (const ch of st.issueChunks || []) add(ch.photos);
    for (const b of st.workBlocks || []) {
      for (const r of b.rows || []) add(r.photos);
    }
    return ids;
  }

  /** --- Formal cover (logo left, title + project right, labeled meta rows) --- */
  const coverTopY = y;
  const gap = 14;
  const logoBand = await drawCoverLogoBand(coverTopY);
  const textLeft = margin + logoBand.leftW + gap;
  const textW = pageW - margin - textLeft;
  const titleMain = (coverMeta && coverMeta.titleMain) || coverTitle;
  const titleDate = (coverMeta && coverMeta.titleDate) || "";
  const projectHeadline = (coverMeta && coverMeta.projectHeadline) || "";
  const brandLine = (coverMeta && coverMeta.brandLine) || "";
  const titleEndY = drawCoverTitleBlock(
    textLeft,
    textW,
    coverTopY,
    titleMain,
    titleDate,
    projectHeadline,
    brandLine
  );
  y = Math.min(logoBand.bottomY, titleEndY) - 10;
  drawLineFull(y + 5, 1, C.coverRule);
  y -= 14;
  drawCoverMetaGrid(coverMeta || {});
  y -= 6;
  drawLineFull(y + 3, 0.55, C.ruleLight);
  y -= 16;

  const det = model.deterministic;
  const st = model.structured;
  prepareMergedForPdf(merged, det);
  const captionHayNorm = buildCaptionDedupHaystack(merged);
  const issueMediaIds = collectIssuePhotoMediaIds(st);
  const renderedMediaIds = new Set();

  function pdfDisplayLine(s) {
    return stripSourceLogArtifacts(String(s || ""));
  }

  const execSummary = merged.execSummary && String(merged.execSummary).trim();
  if (execSummary) {
    drawSectionTitle("Executive summary");
    drawParagraph(execSummary, 10, false);
    y -= 10;
  }

  /** Weather — report day only (no weekly table) */
  drawSectionTitle("Weather");
  const wd = merged.weatherDaily;
  const snap = wd && wd.snapshot;
  drawSubheading("Report day conditions");
  if (snap && snap.ok) {
    if (snap.resolvedLabel) {
      let locLine;
      if (snap.usedFallbackLocation && snap.siteAddressUnresolved) {
        locLine = `Location (approx., Brampton area): ${snap.resolvedLabel}`;
      } else if (snap.usedFallbackLocation) {
        locLine = `Location: ${snap.resolvedLabel}`;
      } else {
        locLine = `Location (geocoded): ${snap.resolvedLabel}`;
      }
      drawParagraph(locLine, 9.5, false, rgb(0.18, 0.2, 0.24));
    }
    if (snap.usedFallbackLocation) {
      const note = snap.siteAddressUnresolved
        ? "Forecast uses Brampton-area coordinates; the weather geocoder did not match the full street address to a point."
        : "Forecast location: Brampton, Ontario, Canada (no project site address on file).";
      drawParagraph(note, 8.5, false, rgb(0.32, 0.33, 0.38));
    }
    drawParagraph(snap.summaryLine || "—", 10, false);
  } else {
    const msg =
      (snap && snap.message) ||
      "Automated weather was not available for this report (service error or network).";
    drawParagraph(msg, 9.5, false, rgb(0.35, 0.28, 0.22));
  }
  const fieldWx = pdfDisplayLine(String((wd && wd.narrativeFromLog) || "").trim());
  if (fieldWx && !/^not stated in field messages/i.test(fieldWx)) {
    drawSubheading("Field notes (from log)");
    drawParagraph(fieldWx, 10, false);
  }
  if (st.weatherChunks && st.weatherChunks.length) {
    drawSubheading("Weather log lines & photos");
    const wxLines = [];
    for (const ch of st.weatherChunks) {
      const wln = pdfDisplayLine(ch.text);
      if (wln) {
        drawParagraph(`- ${wln}`, 10, false, C.inkBody);
        wxLines.push(wln);
      }
    }
    const wxPhotos = (st.weatherChunks || []).flatMap((ch) => ch.photos || []);
    if (wxPhotos.length) {
      await drawPhotoList(wxPhotos, {
        indent: 14,
        model,
        globalSeen: renderedMediaIds,
        maxPhotos: MAX_PHOTOS_WEATHER,
        captionContext: wxLines[0] || "",
      });
    }
  }
  y -= 6;

  /** Workforce */
  const manpowerColWidths = [120, 100, 56, contentW - 276];
  ensureSpaceForSectionTitleAndTableBlock(det.manpowerRows, manpowerColWidths);
  drawSectionTitle("Workforce Summary");
  drawWrappedTable(["Trade", "Foreman", "Workers", "Notes"], det.manpowerRows, manpowerColWidths);
  const manNarRaw = String(merged.manpowerNarrative || "").trim();
  const manpowerChunksPdf = filterManpowerChunksForPdf(st.manpowerChunks);
  const hasMpNarrative = manNarRaw && manNarRaw !== "—";
  const mpPhotos = manpowerChunksPdf.flatMap((ch) => ch.photos || []);
  if (hasMpNarrative || mpPhotos.length) {
    drawPostTableDivider();
  }
  if (hasMpNarrative) {
    drawSubheading("Narrative");
    drawParagraph(merged.manpowerNarrative, 10, false, C.inkBody);
  }
  if (mpPhotos.length) {
    drawSubheading("Supporting photos");
    await drawPhotoList(mpPhotos, {
      model,
      globalSeen: renderedMediaIds,
      maxPhotos: MAX_PHOTOS_MANPOWER,
      captionContext: manNarRaw || (manpowerChunksPdf[0] && manpowerChunksPdf[0].text) || "",
    });
  }

  /** Work */
  drawSectionTitle("Work Completed / In Progress");
  function normTradeLabel(s) {
    return String(s || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");
  }
  function collectPhotosForStructuredTrade(workBlocks, tradeLabel) {
    const t = normTradeLabel(tradeLabel);
    if (!workBlocks || !t) return [];
    for (const b of workBlocks) {
      const bt = normTradeLabel(b.trade);
      if (bt === t || bt.includes(t) || t.includes(bt)) {
        return b.rows.flatMap((r) => r.photos || []);
      }
    }
    return [];
  }

  if (
    merged.useStructuredWorkLayout &&
    merged.workSectionsAi &&
    merged.workSectionsAi.length
  ) {
    const stitched = String((model.deterministic && model.deterministic.workNarrativeBlock) || "").trim();
    const summ = String(merged.workNarrative || "").trim();
    const summaryDistinct = summ && summ !== stitched;
    if (
      summaryDistinct &&
      summ !== "—" &&
      !/^Not stated in log entries/i.test(summ)
    ) {
      drawSubheading("Superintendent summary");
      drawParagraph(summ.slice(0, 1600), 9.5, false, rgb(0.2, 0.22, 0.26));
    }
    drawSubheading("Field activities by trade / scope");
    for (const sec of merged.workSectionsAi) {
      drawTradeHeading(sec.trade);
      for (const it of sec.items || []) {
        const lit = pdfDisplayLine(it);
        if (lit) drawParagraph(`  - ${lit}`, 10, false, C.inkBody);
      }
      const photos = collectPhotosForStructuredTrade(st.workBlocks, sec.trade);
      await drawPhotoList(photos, {
        indent: 18,
        model,
        globalSeen: renderedMediaIds,
        issueMediaIds,
        forWorkSection: true,
        maxPhotos: MAX_PHOTOS_WORK,
        captionContext: (sec.items && sec.items[0]) || "",
      });
    }
  } else if (st.workBlocks && st.workBlocks.length) {
    const stitched = String((model.deterministic && model.deterministic.workNarrativeBlock) || "").trim();
    const summ = String(merged.workNarrative || "").trim();
    const summaryDistinct = summ && summ !== stitched;
    if (
      summaryDistinct &&
      summ !== "—" &&
      !/^Not stated in log entries/i.test(summ)
    ) {
      drawSubheading("Superintendent summary");
      drawParagraph(summ.slice(0, 1600), 9.5, false, rgb(0.2, 0.22, 0.26));
    }
    drawSubheading("Field activities by trade / scope");
    for (const block of st.workBlocks) {
      drawTradeHeading(block.trade);
      const blockPhotos = [];
      for (const row of block.rows) {
        drawParagraph(`  - ${pdfDisplayLine(row.text)}`, 10, false, C.inkBody);
        blockPhotos.push(...(row.photos || []));
      }
      if (blockPhotos.length) {
        await drawPhotoList(blockPhotos, {
          indent: 18,
          model,
          globalSeen: renderedMediaIds,
          issueMediaIds,
          forWorkSection: true,
          maxPhotos: MAX_PHOTOS_WORK,
          captionContext: (block.rows[0] && block.rows[0].text) || "",
        });
      }
    }
  } else {
    drawParagraph(merged.workNarrative, 10, false);
  }

  /** Issues */
  drawSectionTitle("Issues & Deficiencies");
  drawParagraph(merged.issuesText, 10, false, C.inkBody);
  if (st.issueChunks && st.issueChunks.length) {
    for (const ch of st.issueChunks) {
      const shown = pdfDisplayLine(ch.text);
      if (shown && !issueChunkLineRedundant(shown, merged.issuesText)) {
        drawParagraph(`- ${shown}`, 10, false, C.inkBody);
      }
    }
    const issuePhotos = (st.issueChunks || []).flatMap((ch) => ch.photos || []);
    if (issuePhotos.length) {
      drawSubheading("Photos");
      await drawPhotoList(issuePhotos, {
        indent: 14,
        model,
        globalSeen: renderedMediaIds,
        maxPhotos: MAX_PHOTOS_ISSUES,
        captionContext: String(merged.issuesText || "").slice(0, 280),
        captionKeyMax: 1,
      });
    }
  }

  /** Inspections — omit empty placeholder-only block */
  const insTextRaw = String(merged.inspectionText || "").trim();
  const insPlaceholder =
    !insTextRaw ||
    insTextRaw === "—" ||
    /^not stated in field messages\.?$/i.test(insTextRaw);
  const hasInsChunks = st.inspectionChunks && st.inspectionChunks.length;
  if (!insPlaceholder || hasInsChunks) {
    drawSectionTitle("Inspections");
    if (!insPlaceholder) {
      drawParagraph(merged.inspectionText, 10, false);
    }
    if (hasInsChunks) {
      for (const ch of st.inspectionChunks) {
        const iln = pdfDisplayLine(ch.text);
        if (iln) {
          drawParagraph(`- ${iln}`, 10, false, C.inkBody);
        }
      }
      const insPhotos = (st.inspectionChunks || []).flatMap((ch) => ch.photos || []);
      if (insPhotos.length) {
        drawSubheading("Photos");
        await drawPhotoList(insPhotos, {
          indent: 14,
          model,
          globalSeen: renderedMediaIds,
          maxPhotos: MAX_PHOTOS_INSPECTION,
          captionContext: merged.inspectionText || "",
        });
      }
    }
  }

  /** Concrete */
  const concreteColWidths = [contentW * 0.5, contentW * 0.22, contentW * 0.28];
  const concreteRowsForPdf = det.concreteRows.map((r) => {
    const x = [...r];
    while (x.length < 3) x.push("—");
    return x.slice(0, 3);
  });
  ensureSpaceForSectionTitleAndTableBlock(concreteRowsForPdf, concreteColWidths);
  drawSectionTitle(`Concrete Summary — ${concreteLabel}`);
  drawWrappedTable(["Pour location / scope", "Volume", "Status"], concreteRowsForPdf, concreteColWidths);
  const concNar = String(merged.concreteNarrative || "").trim();
  const concPhotos = (st.concreteChunks || []).flatMap((ch) => ch.photos || []);
  const hasConcNarrative = concNar && concNar !== "—";
  if (hasConcNarrative || concPhotos.length) {
    drawPostTableDivider();
  }
  if (hasConcNarrative) {
    drawSubheading("Notes");
    drawParagraph(merged.concreteNarrative, 10, false, C.inkBody);
  }
  if (concPhotos.length) {
    drawSubheading("Photos");
    await drawPhotoList(concPhotos, {
      indent: 14,
      model,
      globalSeen: renderedMediaIds,
      maxPhotos: MAX_PHOTOS_CONCRETE,
      captionContext: concNar || (st.concreteChunks && st.concreteChunks[0] && st.concreteChunks[0].text) || "",
      captionKeyMax: 1,
    });
  }

  /** Open items */
  const openIntroRaw = String(merged.openIntro || "").trim();
  const showOpenIntro =
    openIntroRaw &&
    openIntroRaw !== "—" &&
    !/^not stated in field messages\.?$/i.test(openIntroRaw);
  const introExtra = showOpenIntro ? estimateWrappedTextHeight(openIntroRaw, 10, contentW) : 0;
  const aiOpen = merged.openItemsTableRaw ? parsePipeRows(merged.openItemsTableRaw, 4) : [];
  const openItemColWidths = [30, contentW - 30 - 96 - 86, 96, 86];
  const openRows =
    aiOpen.length > 0
      ? aiOpen.map((r) => {
          const x = [...r];
          while (x.length < 4) x.push("");
          return x.slice(0, 4);
        })
      : det.openItemRows.length
        ? det.openItemRows
        : [["—", "No open items flagged in log entries.", "—", "—"]];
  ensureSpaceForSectionTitleAndTableBlock(openRows, openItemColWidths, introExtra);
  drawSectionTitle("Open Items / Action Required");
  if (showOpenIntro) {
    drawParagraph(merged.openIntro, 10, false);
  }
  drawWrappedTable(["#", "Action item", "Responsible", "Status"], openRows, openItemColWidths);

  /** Site photos: any project media not already drawn in a section above */
  const placed = collectPlacedPhotoIds(st);
  const remainingPhotos = (model.photos || []).filter((p) => {
    if (!p) return false;
    const id = String(p.mediaId);
    if (renderedMediaIds.has(id)) return false;
    return !placed.has(id);
  });
  if (remainingPhotos.length) {
    drawSectionTitle("Site Photos");
    drawParagraph(
      "Images from this project for the report period (including items not matched to a specific section).",
      8.5,
      false,
      rgb(0.35, 0.36, 0.4)
    );
    await drawPhotoList(remainingPhotos, {
      model,
      globalSeen: renderedMediaIds,
      maxPhotos: MAX_PHOTOS_SITE,
    });
  }

  /** Footers */
  const pages = pdf.getPages();
  const totalPages = pages.length;
  const headerTxt = sanitizePdfText(titleStr);
  const footL = sanitizePdfText(`Powered by ${footerBrand}`);
  for (let i = 0; i < totalPages; i++) {
    const pg = pages[i];
    const fr = `Page ${i + 1} of ${totalPages}`;
    const fw = font.widthOfTextAtSize(fr, 8);
    pg.drawText(footL, { x: margin, y: 16, size: 8, font, color: C.inkMuted });
    pg.drawText(sanitizePdfText(fr), {
      x: pageW - margin - fw,
      y: 16,
      size: 8,
      font,
      color: C.inkMuted,
    });
    if (i > 0) {
      pg.drawText(headerTxt, {
        x: margin,
        y: pageH - 28,
        size: 9,
        font: fontBold,
        color: C.ink,
      });
    }
  }
}

module.exports = {
  renderDailySiteLogPdf,
  wrapToLines,
};
