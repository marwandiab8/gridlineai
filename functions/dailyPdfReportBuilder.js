/**
 * Formal Daily Site Log PDF â€” wrapped tables, cover block, inline photos by log entry.
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
/** Slightly tighter photos for grid-friendly layout */
const PHOTO_MAX_H = 320;
const PHOTO_MAX_W = 440;

function splitOversizeToken(token, font, size, maxWidth) {
  const raw = sanitizePdfText(String(token || "").trim());
  if (!raw) return [""];
  if (font.widthOfTextAtSize(raw, size) <= maxWidth) return [raw];

  const parts = [];
  let rest = raw;
  while (rest && font.widthOfTextAtSize(rest, size) > maxWidth) {
    let splitAt = -1;
    for (let i = rest.length - 1; i > 0; i--) {
      const ch = rest[i];
      const left = rest.slice(0, i + 1);
      if (!/[-/_,.;:)]/.test(ch)) continue;
      if (font.widthOfTextAtSize(left, size) <= maxWidth) {
        splitAt = i + 1;
        break;
      }
    }

    if (splitAt > 0) {
      parts.push(rest.slice(0, splitAt));
      rest = rest.slice(splitAt);
      continue;
    }

    let chunk = "";
    for (const ch of rest) {
      const next = chunk + ch;
      if (font.widthOfTextAtSize(next, size) > maxWidth && chunk) break;
      chunk = next;
    }
    if (!chunk) chunk = rest.slice(0, 1);

    const needsSoftHyphen = /^[a-z0-9]+$/i.test(chunk) && chunk.length < rest.length;
    if (needsSoftHyphen) {
      let hyphenated = chunk;
      while (hyphenated.length > 2 && font.widthOfTextAtSize(`${hyphenated}-`, size) > maxWidth) {
        hyphenated = hyphenated.slice(0, -1);
      }
      chunk = hyphenated;
      parts.push(`${chunk}-`);
    } else {
      parts.push(chunk);
    }
    rest = rest.slice(chunk.length);
  }
  if (rest) parts.push(rest);
  return parts.filter(Boolean);
}

const COVER_BRAND_FONT = 8;
const COVER_TITLE_FONT = 18;
const COVER_BRAND_TITLE_GAP = 9;

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
      if (font.widthOfTextAtSize(w, size) <= maxWidth) {
        cur = w;
      } else {
        const chunks = splitOversizeToken(w, font, size, maxWidth);
        if (chunks.length > 1) {
          lines.push(...chunks.slice(0, -1));
          cur = chunks[chunks.length - 1];
        } else {
          cur = chunks[0] || "";
        }
      }
    }
  }
  if (cur) lines.push(cur);
  return lines.length ? lines : [""];
}

function selectRemainingSitePhotos(photos, renderedMediaIds) {
  const seen = renderedMediaIds instanceof Set ? renderedMediaIds : new Set(renderedMediaIds || []);
  return (photos || []).filter((p) => {
    if (!p || p.mediaId == null) return false;
    return !seen.has(String(p.mediaId));
  });
}

function selectRemainingJournalPhotos(photos, renderedMediaIds) {
  const seen = renderedMediaIds instanceof Set ? renderedMediaIds : new Set(renderedMediaIds || []);
  return (photos || []).filter((p) => {
    if (!p || p.mediaId == null) return false;
    return !seen.has(String(p.mediaId));
  });
}

function parseWorkerCount(value) {
  const text = String(value == null ? "" : value).trim();
  if (!text || text === "â€”") return 0;
  const match = text.match(/\d+(?:\.\d+)?/);
  if (!match) return 0;
  const num = Number(match[0]);
  return Number.isFinite(num) ? num : 0;
}

function buildManpowerRowsWithTotal(rows) {
  const safeRows = Array.isArray(rows) ? rows.map((row) => [...row]) : [];
  const totalWorkers = safeRows.reduce((sum, row) => sum + parseWorkerCount(row && row[2]), 0);
  if (!safeRows.length || totalWorkers <= 0) {
    return { rows: safeRows, totalWorkers };
  }
  return {
    totalWorkers,
    rows: [
      ...safeRows,
      ["TOTAL WORKERS", "â€”", String(totalWorkers), "Total workforce on site"],
    ],
  };
}

function shouldRenderWorkSummary(execSummary, workSummary, stitchedNarrative) {
  const exec = String(execSummary || "").trim();
  const summ = String(workSummary || "").trim();
  const stitched = String(stitchedNarrative || "").trim();
  if (!summ || summ === "â€”" || /^Not stated in log entries/i.test(summ)) return false;
  if (exec) return false;
  return summ !== stitched;
}

function shouldRenderProjectNotes(projectNotes) {
  const text = String(projectNotes || "").trim();
  if (!text) return false;
  if (text === "â€”") return false;
  if (/^not specified\.?$/i.test(text)) return false;
  if (/^(standard\s+)?ppe\s+required(?:\s+only)?\.?$/i.test(text)) return false;
  return true;
}

function formatAttributedUpdate(authorLabel, text) {
  const author = String(authorLabel || "").trim();
  const body = String(text || "").trim();
  if (!author) return body;
  if (!body) return author;
  return `${author}: ${body}`;
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
    projectNotes,
    merged,
    model,
    concreteLabel,
    logger,
    runId,
    logoStoragePath,
  } = opts;

  const contentW = pageW - 2 * margin;
  const footerReserve = 42;
  const headerReserve2 = 54;
  /** Professional construction-report palette â€” navy/slate, restrained */
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

  /** Rule + heading + spacing â€” must match ensureSpaceForSectionTitleAndTableBlock reserve (allows 1â€“2 title lines) */
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
   * @param {number} [extraBeforeTable] â€” e.g. intro paragraph height before the table
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
    page.drawText(sanitizePdfText("Optional â€” set in project settings"), {
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
        size: COVER_BRAND_FONT,
        font,
        color: C.inkMuted,
      });
      yy -= COVER_BRAND_FONT + LEADING + COVER_BRAND_TITLE_GAP;
    }
    const main = String(titleMain || "").trim() || String(coverTitle || "");
    const linesMain = wrapToLines(main, fontBold, COVER_TITLE_FONT, maxW);
    const lh = COVER_TITLE_FONT + LEADING;
    for (const line of linesMain) {
      page.drawText(sanitizePdfText(line), {
        x: leftX,
        y: yy,
        size: COVER_TITLE_FONT,
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
      const isPlaceholder = !trimmed || trimmed === "â€”";
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

  function shouldRenderIntroText(text) {
    const raw = String(text || "").trim();
    if (!raw) return false;
    if (/^not stated in field messages\.?$/i.test(raw)) return false;
    const safe = sanitizePdfText(raw).replace(/\s+/g, "").trim();
    if (!safe) return false;
    if (/^(?:-+|_+|[?"]+)$/.test(safe)) return false;
    return true;
  }

  async function drawPhotoBlock(photo, opts = {}) {
    const indent = Number(opts.indent) || 0;
    const left = opts.left != null ? opts.left : margin + indent;
    const availableW = Math.max(120, contentW - (left - margin));
    const maxW = Math.min(
      opts.maxBoxW != null ? opts.maxBoxW : Math.min(PHOTO_MAX_W, availableW),
      availableW
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
    const cap = [ts, capBody].filter(Boolean).join(" - ");
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
      maxPhotos = null,
      captionContext,
      maxBoxH,
      maxBoxW,
      captionKeyMax = 0,
    } = opts;
    let photos = dedupePhotosByMediaId(list || []);
    photos = photos.filter((p) => p && (globalSeen ? !globalSeen.has(String(p.mediaId)) : true));
    if (forWorkSection && issueIds && issueIds.size) {
      photos = filterPhotosNotForIssues(photos, issueIds);
    }
    photos = sortPhotosForRender(photos);
    if (Number.isFinite(captionKeyMax) && captionKeyMax > 0) {
      photos = capPhotosBySimilarCaption(photos, captionKeyMax);
    }
    if (Number.isFinite(maxPhotos) && maxPhotos > 0) {
      photos = photos.slice(0, maxPhotos);
    }
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
  y = Math.min(logoBand.bottomY, titleEndY) - 18;
  drawLineFull(y + 7, 1, C.coverRule);
  y -= 20;
  drawCoverMetaGrid(coverMeta || {});
  y -= 10;
  drawLineFull(y + 3, 0.55, C.ruleLight);
  y -= 18;

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

  if (shouldRenderProjectNotes(projectNotes)) {
    drawSectionTitle("Project Notes");
    drawParagraph(projectNotes, 10, false, C.inkBody);
    y -= 8;
  }

  /** Weather â€” report day only (no weekly table) */
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
    drawParagraph(snap.summaryLine || "â€”", 10, false);
  } else {
    const msg =
      (snap && snap.message) ||
      "Automated weather was not available for this report (service error or network).";
    drawParagraph(msg, 9.5, false, rgb(0.35, 0.28, 0.22));
  }
  y -= 6;

  /** Workforce */
  const manpowerColWidths = [120, 100, 56, contentW - 276];
  const manpowerTable = buildManpowerRowsWithTotal(det.manpowerRows);
  ensureSpaceForSectionTitleAndTableBlock(manpowerTable.rows, manpowerColWidths);
  drawSectionTitle("Workforce Summary");
  drawWrappedTable(["Trade", "Foreman", "Workers", "Notes"], manpowerTable.rows, manpowerColWidths);
  const manNarRaw = String(merged.manpowerNarrative || "").trim();
  const manpowerChunksPdf = filterManpowerChunksForPdf(st.manpowerChunks);
  const hasMpNarrative = manNarRaw && manNarRaw !== "â€”";
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
    if (shouldRenderWorkSummary(merged.execSummary, summ, stitched)) {
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
      const sourceBlock = (st.workBlocks || []).find((b) => {
        const bt = normTradeLabel(b.trade);
        const t = normTradeLabel(sec.trade);
        return bt === t || bt.includes(t) || t.includes(bt);
      });
      if (sourceBlock && sourceBlock.rows && sourceBlock.rows.length) {
        drawSubheading("Source updates");
        for (const row of sourceBlock.rows) {
          const attributed = formatAttributedUpdate(row.authorLabel, pdfDisplayLine(row.text));
          if (attributed) drawParagraph(`  - ${attributed}`, 9.5, false, C.inkBody);
        }
      }
      const photos = collectPhotosForStructuredTrade(st.workBlocks, sec.trade);
      await drawPhotoList(photos, {
        indent: 18,
        model,
        globalSeen: renderedMediaIds,
        issueMediaIds,
        forWorkSection: true,
        captionContext: (sec.items && sec.items[0]) || "",
      });
    }
  } else if (st.workBlocks && st.workBlocks.length) {
    const stitched = String((model.deterministic && model.deterministic.workNarrativeBlock) || "").trim();
    const summ = String(merged.workNarrative || "").trim();
    if (shouldRenderWorkSummary(merged.execSummary, summ, stitched)) {
      drawSubheading("Superintendent summary");
      drawParagraph(summ.slice(0, 1600), 9.5, false, rgb(0.2, 0.22, 0.26));
    }
    drawSubheading("Field activities by trade / scope");
    for (const block of st.workBlocks) {
      drawTradeHeading(block.trade);
      const blockPhotos = [];
      for (const row of block.rows) {
        drawParagraph(
          `  - ${formatAttributedUpdate(row.authorLabel, pdfDisplayLine(row.text))}`,
          10,
          false,
          C.inkBody
        );
        blockPhotos.push(...(row.photos || []));
      }
      if (blockPhotos.length) {
        await drawPhotoList(blockPhotos, {
          indent: 18,
          model,
          globalSeen: renderedMediaIds,
          issueMediaIds,
          forWorkSection: true,
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
      const shown = formatAttributedUpdate(ch.authorLabel, pdfDisplayLine(ch.text));
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
        captionContext: String(merged.issuesText || "").slice(0, 280),
      });
    }
  }

  /** Inspections â€” omit empty placeholder-only block */
  const insTextRaw = String(merged.inspectionText || "").trim();
  const insPlaceholder =
    !insTextRaw ||
    insTextRaw === "â€”" ||
    /^not stated in field messages\.?$/i.test(insTextRaw);
  const hasInsChunks = st.inspectionChunks && st.inspectionChunks.length;
  if (!insPlaceholder || hasInsChunks) {
    drawSectionTitle("Inspections");
    if (!insPlaceholder) {
      drawParagraph(merged.inspectionText, 10, false);
    }
    if (hasInsChunks) {
      for (const ch of st.inspectionChunks) {
        const iln = formatAttributedUpdate(ch.authorLabel, pdfDisplayLine(ch.text));
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
          captionContext: merged.inspectionText || "",
        });
      }
    }
  }

  /** Concrete */
  const concreteColWidths = [contentW * 0.5, contentW * 0.22, contentW * 0.28];
  const concreteRowsForPdf = det.concreteRows.map((r) => {
    const x = [...r];
    while (x.length < 3) x.push("â€”");
    return x.slice(0, 3);
  });
  ensureSpaceForSectionTitleAndTableBlock(concreteRowsForPdf, concreteColWidths);
  drawSectionTitle(`Concrete Summary â€” ${concreteLabel}`);
  drawWrappedTable(["Pour location / scope", "Volume", "Status"], concreteRowsForPdf, concreteColWidths);
  const concNar = String(merged.concreteNarrative || "").trim();
  const concPhotos = (st.concreteChunks || []).flatMap((ch) => ch.photos || []);
  const hasConcNarrative = concNar && concNar !== "â€”";
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
      captionContext: concNar || (st.concreteChunks && st.concreteChunks[0] && st.concreteChunks[0].text) || "",
    });
  }

  /** Open items */
  const openIntroRaw = String(merged.openIntro || "").trim();
  const showOpenIntro = shouldRenderIntroText(openIntroRaw);
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
        : [["â€”", "No open items flagged in log entries.", "â€”", "â€”"]];
  ensureSpaceForSectionTitleAndTableBlock(openRows, openItemColWidths, introExtra);
  drawSectionTitle("Open Items / Action Required");
  if (showOpenIntro) {
    drawParagraph(merged.openIntro, 10, false);
  }
  drawWrappedTable(["#", "Action item", "Responsible", "Status"], openRows, openItemColWidths);

  /** Site photos: any project media not already drawn in a section above */
  const remainingPhotos = selectRemainingSitePhotos(model.photos || [], renderedMediaIds);
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

async function renderJournalPdf(opts) {
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
    coverMeta,
    logoStoragePath,
    projectNotes,
    merged,
    model,
    logger,
    runId,
  } = opts;

  const contentW = pageW - 2 * margin;
  const footerReserve = 42;
  const headerReserve = 44;
  const C = {
    ink: rgb(0.14, 0.15, 0.2),
    body: rgb(0.17, 0.18, 0.22),
    muted: rgb(0.43, 0.45, 0.5),
    rule: rgb(0.78, 0.8, 0.84),
    accent: rgb(0.3, 0.37, 0.48),
  };

  let page = pdf.addPage([pageW, pageH]);
  let y = pageH - margin - 8;

  function newPage() {
    page = pdf.addPage([pageW, pageH]);
    y = pageH - margin - headerReserve;
  }

  function ensureSpace(need) {
    if (y - need < margin + footerReserve) newPage();
  }

  function drawLine(yy, thickness = 0.6, color = C.rule) {
    page.drawLine({
      start: { x: margin, y: yy },
      end: { x: pageW - margin, y: yy },
      thickness,
      color,
    });
  }

  function drawParagraph(text, size = 10, bold = false, color = C.body, left = margin, maxW = contentW) {
    const f = bold ? fontBold : font;
    const lines = wrapToLines(String(text || "").trim(), f, size, maxW);
    const lh = size + LEADING;
    for (const line of lines) {
      ensureSpace(lh + 2);
      page.drawText(sanitizePdfText(line), {
        x: left,
        y,
        size,
        font: f,
        color,
      });
      y -= lh;
    }
  }

  function drawSectionTitle(title) {
    y -= 10;
    ensureSpace(34);
    drawLine(y + 4, 0.8, C.rule);
    y -= 12;
    drawParagraph(title, 12, true, C.ink);
    y -= 6;
  }

  function drawBullets(items, color = C.body) {
    for (const item of items || []) {
      const lines = wrapToLines(String(item || "").trim(), font, 10, contentW - 18);
      if (!lines.length) continue;
      ensureSpace(lines.length * (10 + LEADING) + 2);
      page.drawText("•", {
        x: margin,
        y,
        size: 10,
        font: fontBold,
        color,
      });
      let bulletY = y;
      for (const line of lines) {
        page.drawText(sanitizePdfText(line), {
          x: margin + 14,
          y: bulletY,
          size: 10,
          font,
          color,
        });
        bulletY -= 10 + LEADING;
      }
      y = bulletY - 2;
    }
  }

  function drawJournalTimelineEntry(row) {
    const timeLabel = String(row && row.time ? row.time : "").trim();
    const authorLabel = String(row && row.authorLabel ? row.authorLabel : "").trim();
    const text = String(row && row.text ? row.text : "").trim();
    if (!timeLabel && !text) return;
    const metaLabel = [timeLabel, authorLabel].filter(Boolean).join(" · ");
    if (metaLabel) {
      drawParagraph(metaLabel, 8.5, true, C.accent);
    }
    if (text) {
      drawParagraph(text, 10, false, C.body);
    }
    y -= 4;
  }

  function drawMetaGrid(grid) {
    for (const row of grid || []) {
      const label = String(row.label || "").trim();
      const value = String(row.value || "").trim();
      if (!label && !value) continue;
      ensureSpace(26);
      page.drawText(sanitizePdfText(label), {
        x: margin,
        y,
        size: 8.5,
        font: fontBold,
        color: C.accent,
      });
      drawParagraph(value || "Not specified", 9.5, false, value ? C.body : C.muted, margin + 100, contentW - 100);
      y -= 4;
    }
  }

  async function drawJournalCoverLogo(topY) {
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
    return { bottomY: topY, leftW: 0 };
  }

  async function drawJournalPhoto(photo, captionContext) {
    let buf;
    try {
      [buf] = await storageBucket.file(photo.storagePath).download();
    } catch (e) {
      if (logger) {
        logger.warn("dailyPdfReportBuilder: journal photo download failed", {
          runId,
          path: photo.storagePath,
          message: e.message,
        });
      }
      drawParagraph("(Photo unavailable)", 8.5, false, C.muted);
      return;
    }
    const img = await embedImageIfPossible(pdf, buf);
    if (!img) {
      drawParagraph("(Unsupported image format)", 8.5, false, C.muted);
      return;
    }
    const maxW = contentW;
    const maxH = 420;
    const scale = Math.min(maxW / img.width, maxH / img.height);
    const w = img.width * scale;
    const h = img.height * scale;
    ensureSpace(h + 40);
    page.drawImage(img, {
      x: margin,
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
    const body = refineCaptionForPdf(
      String(photo.captionText || "").trim() || String(captionContext || "").trim(),
      captionContext || "",
      ""
    );
    const cap = [ts, body].filter(Boolean).join(" - ");
    if (cap) {
      drawParagraph(cap, 8.5, false, C.muted);
    }
    y -= 4;
  }

  const overview = String(merged.overview || model.deterministic.overview || "").trim();
  const keyMoments = Array.isArray(merged.keyMoments) && merged.keyMoments.length
    ? merged.keyMoments
    : model.deterministic.keyMoments || [];
  const reflections = Array.isArray(merged.reflections) && merged.reflections.length
    ? merged.reflections
    : model.deterministic.reflections || [];
  const closingNote = String(merged.closingNote || model.deterministic.closingNote || "").trim();

  const coverTopY = y;
  const logoBand = await drawJournalCoverLogo(coverTopY);
  const gap = logoBand.leftW > 0 ? 18 : 0;
  const textLeft = margin + logoBand.leftW + gap;
  const textWidth = pageW - margin - textLeft;
  const drawJournalCoverText = (text, size, bold, color) => {
    drawParagraph(text, size, bold, color, textLeft, textWidth);
  };

  drawJournalCoverText(coverMeta.brandLine || `Personal daily journal - ${footerBrand}`, 8.5, false, C.muted);
  y -= 10;
  drawJournalCoverText(coverMeta.titleMain || titleStr, 20, true, C.ink);
  if (coverMeta.titleDate) {
    y -= 2;
    drawJournalCoverText(coverMeta.titleDate, 11, false, C.accent);
  }
  y = Math.min(y, logoBand.bottomY) - 4;
  drawLine(y + 4, 1, C.rule);
  y -= 12;
  drawMetaGrid(coverMeta.grid || []);
  if (coverMeta.lines && coverMeta.lines.length) {
    for (const line of coverMeta.lines) {
      drawParagraph(line, 8.5, false, C.muted);
    }
  }
  y -= 8;

  if (overview) {
    drawSectionTitle("Day Overview");
    drawParagraph(overview, 10, false, C.body);
  }

  if (shouldRenderProjectNotes(projectNotes)) {
    drawSectionTitle("Project Notes");
    drawParagraph(projectNotes, 10, false, C.body);
  }

  const renderedPhotoIds = new Set();
  const timelineRows = Array.isArray(model.timeline) ? model.timeline : [];
  if (timelineRows.length) {
    drawSectionTitle("Chronological Journal");
    for (const row of timelineRows) {
      drawJournalTimelineEntry(row);
      const linkedPhotos = Array.isArray(row.photos) ? row.photos : [];
      for (const photo of linkedPhotos) {
        if (!photo || renderedPhotoIds.has(String(photo.mediaId))) continue;
        renderedPhotoIds.add(String(photo.mediaId));
        await drawJournalPhoto(photo, row.text || "");
      }
    }
  }

  if (keyMoments.length) {
    drawSectionTitle("Key Moments");
    drawBullets(keyMoments, C.body);
  }

  if (reflections.length) {
    drawSectionTitle("Reflections");
    drawBullets(reflections, C.body);
  }

  const photos = Array.isArray(model.photos) ? model.photos : [];
  const remainingPhotos = selectRemainingJournalPhotos(photos, renderedPhotoIds);
  if (remainingPhotos.length) {
    drawSectionTitle("Additional Photos");
    for (const photo of remainingPhotos) {
      if (!photo || renderedPhotoIds.has(String(photo.mediaId))) continue;
      renderedPhotoIds.add(String(photo.mediaId));
      const linkedMoment = (model.timeline || []).find(
        (row) => String(row.entryId || "") === String(photo.linkedLogEntryId || "")
      );
      await drawJournalPhoto(photo, linkedMoment ? linkedMoment.text : "");
    }
  }

  if (closingNote) {
    drawSectionTitle("Closing Note");
    drawParagraph(closingNote, 10, false, C.body);
  }

  const pages = pdf.getPages();
  const totalPages = pages.length;
  const headerTxt = sanitizePdfText(titleStr);
  const footL = sanitizePdfText(`Powered by ${footerBrand}`);
  for (let i = 0; i < totalPages; i++) {
    const pg = pages[i];
    const fr = `Page ${i + 1} of ${totalPages}`;
    const fw = font.widthOfTextAtSize(fr, 8);
    pg.drawText(footL, { x: margin, y: 16, size: 8, font, color: C.muted });
    pg.drawText(sanitizePdfText(fr), {
      x: pageW - margin - fw,
      y: 16,
      size: 8,
      font,
      color: C.muted,
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
  renderJournalPdf,
  selectRemainingSitePhotos,
  selectRemainingJournalPhotos,
  wrapToLines,
  buildManpowerRowsWithTotal,
  shouldRenderWorkSummary,
  shouldRenderProjectNotes,
};

