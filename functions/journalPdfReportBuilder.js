const { rgb } = require("pdf-lib");
const { sanitizePdfText } = require("./pdfWinAnsiText");
const { formatWallDateTimeEt } = require("./logClassifier");
const { refineCaptionForPdf } = require("./dailyPdfCompact");
const {
  wrapToLines,
  selectRemainingJournalPhotos,
} = require("./dailyPdfReportBuilderLegacy");

const LEADING = 3;
const HIDDEN_SHORTCUT_EVENT_TYPES = new Set([
  "start_spotify",
  "arrive_home",
  "leave_home",
  "leave_location",
]);

function normalizeJournalKey(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isRawShortcutTrackingText(value) {
  const text = String(value || "").trim();
  if (!text) return false;
  return (
    /iOS Shortcuts tracking event/i.test(text) ||
    /\bEvent type:\s*[a-z_]+/i.test(text) ||
    /\bTimezone:\s*[A-Za-z_]+\/[A-Za-z_]+/i.test(text) ||
    /\bCoordinates:\s*-?\d+(?:\.\d+)?\s*,\s*-?\d+(?:\.\d+)?/i.test(text) ||
    /\bDevice:\s*iPhone\b/i.test(text)
  );
}

function extractEventTypeFromText(value) {
  const match = String(value || "").match(/\bEvent type:\s*([a-z0-9_]+)/i);
  return match ? String(match[1] || "").trim().toLowerCase() : "";
}

function extractLocationFromText(value) {
  const match = String(value || "").match(/\bLocation:\s*([^\n]+?)(?=\.\s+(?:Device|Coordinates|Notes|Project|Timezone|Event)|\.$|$)/i);
  return match ? String(match[1] || "").trim() : "";
}

function humanizeShortcutLocation(value) {
  const raw = String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[.]+$/, "");
  if (!raw) return "";

  if (/^costco(?:\s+in)?\s+vaughn?$/i.test(raw)) return "Costco Vaughan";
  if (/^vr\s+zero\s+latenc(?:y|y's?)$/i.test(raw)) return "Zero Latency VR";
  if (/^jack\s+astors?(?:\s+restaurant)?$/i.test(raw)) return "Jack Astor’s";
  if (/^sky\s+zone(?:\s+in)?\s+vaughan$/i.test(raw)) return "Sky Zone Vaughan";
  return raw.replace(/\bVaughn\b/gi, "Vaughan");
}

function shortcutMetaForRow(row, model) {
  const entry = model && model.entryById instanceof Map
    ? model.entryById.get(String((row && row.entryId) || ""))
    : null;
  const rowText = String((row && row.text) || "");
  const source = String((entry && entry.source) || "").trim();
  const eventType = String((entry && entry.shortcutEventType) || extractEventTypeFromText(rowText))
    .trim()
    .toLowerCase();
  const location = String(
    (entry && entry.shortcutLocationLabel) || extractLocationFromText(rowText) || ""
  ).trim();
  const isTracking = source === "ios_shortcuts" || Boolean(eventType) || isRawShortcutTrackingText(rowText);
  return { entry, eventType, location, isTracking };
}

function formatShortcutTimelineRow(row, model) {
  const meta = shortcutMetaForRow(row, model);
  if (!meta.isTracking) return { ...row, isTracking: false, compactTracking: false };

  const eventType = meta.eventType;
  if (!eventType || HIDDEN_SHORTCUT_EVENT_TYPES.has(eventType)) return null;

  const location = humanizeShortcutLocation(meta.location);
  let text = "";
  switch (eventType) {
    case "arrive_location":
      text = location || "Arrived at location";
      break;
    case "arrive_work":
      text = location || "Arrived at work";
      break;
    case "leave_work":
      text = "Left work";
      break;
    case "arrive_gym":
      text = location || "Arrived at the gym";
      break;
    case "leave_gym":
      text = "Left the gym";
      break;
    case "start_workout":
      text = "Workout started";
      break;
    case "finish_workout":
      text = "Workout finished";
      break;
    default:
      return null;
  }

  return {
    ...row,
    text,
    authorLabel: "",
    isTracking: true,
    compactTracking: true,
    shortcutEventType: eventType,
    shortcutLocationLabel: location,
  };
}

function prepareJournalTimeline(model) {
  const rows = Array.isArray(model && model.timeline) ? model.timeline : [];
  const out = [];
  const seenTracking = new Set();

  for (const row of rows) {
    const prepared = formatShortcutTimelineRow(row, model);
    if (!prepared) continue;
    if (prepared.isTracking) {
      const key = [
        prepared.shortcutEventType || "tracking",
        String(prepared.time || "").trim(),
        normalizeJournalKey(prepared.shortcutLocationLabel || prepared.text),
      ].join("|");
      if (seenTracking.has(key)) continue;
      seenTracking.add(key);
    }
    out.push(prepared);
  }
  return out;
}

function filterJournalNarrativeItems(items, seen = new Set()) {
  const out = [];
  for (const item of Array.isArray(items) ? items : []) {
    const text = String(item || "").trim();
    if (!text || isRawShortcutTrackingText(text)) continue;
    const key = normalizeJournalKey(text);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(text);
  }
  return out;
}

function cleanJournalNarrativeText(value, seen = null) {
  const text = String(value || "").trim();
  if (!text || isRawShortcutTrackingText(text)) return "";
  const key = normalizeJournalKey(text);
  if (seen && key && seen.has(key)) return "";
  if (seen && key) seen.add(key);
  return text;
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
      page.drawText(sanitizePdfText(line), { x: left, y, size, font: f, color });
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
      page.drawText("•", { x: margin, y, size: 10, font: fontBold, color });
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
    const timeLabel = String((row && row.time) || "").trim();
    const authorLabel = String((row && row.authorLabel) || "").trim();
    const text = String((row && row.text) || "").trim();
    if (!timeLabel && !text) return;

    if (row && row.compactTracking) {
      const line = [timeLabel, text].filter(Boolean).join(" — ");
      if (line) drawParagraph(line, 9.5, false, C.body);
      y -= 3;
      return;
    }

    const metaLabel = [timeLabel, authorLabel].filter(Boolean).join(" · ");
    if (metaLabel) drawParagraph(metaLabel, 8.5, true, C.accent);
    if (text) drawParagraph(text, 10, false, C.body);
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
        logger.warn("journalPdfReportBuilder: journal photo download failed", {
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
    page.drawImage(img, { x: margin, y: y - h, width: w, height: h });
    y -= h + 6;

    let ts = "";
    try {
      if (photo.createdAt && typeof photo.createdAt.toDate === "function") {
        ts = formatWallDateTimeEt(photo.createdAt.toDate());
      }
    } catch (_) {}
    const safeContext = isRawShortcutTrackingText(captionContext) ? "" : String(captionContext || "").trim();
    const body = refineCaptionForPdf(
      String(photo.captionText || "").trim() || safeContext,
      safeContext,
      ""
    );
    const cap = [ts, body].filter(Boolean).join(" - ");
    if (cap) drawParagraph(cap, 8.5, false, C.muted);
    y -= 4;
  }

  const timelineRows = prepareJournalTimeline(model || {});
  const seenNarrative = new Set();
  for (const row of timelineRows) {
    const key = normalizeJournalKey(row && row.text);
    if (key) seenNarrative.add(key);
  }

  const overviewRaw = merged && merged.overview
    ? merged.overview
    : model && model.deterministic && model.deterministic.overview;
  const overview = cleanJournalNarrativeText(overviewRaw);
  const keyMomentsRaw = Array.isArray(merged && merged.keyMoments) && merged.keyMoments.length
    ? merged.keyMoments
    : (model && model.deterministic && model.deterministic.keyMoments) || [];
  const reflectionsRaw = Array.isArray(merged && merged.reflections) && merged.reflections.length
    ? merged.reflections
    : (model && model.deterministic && model.deterministic.reflections) || [];
  const keyMoments = filterJournalNarrativeItems(keyMomentsRaw, seenNarrative);
  const reflections = filterJournalNarrativeItems(reflectionsRaw, seenNarrative);
  const closingRaw = (merged && merged.closingNote) ||
    (model && model.deterministic && model.deterministic.closingNote) || "";
  const closingNote = cleanJournalNarrativeText(closingRaw, seenNarrative);

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
    for (const line of coverMeta.lines) drawParagraph(line, 8.5, false, C.muted);
  }
  y -= 8;

  if (overview) {
    drawSectionTitle("Day Overview");
    drawParagraph(overview, 10, false, C.body);
  }

  const renderedPhotoIds = new Set();
  if (timelineRows.length) {
    drawSectionTitle("Chronological Journal");
    for (const row of timelineRows) {
      drawJournalTimelineEntry(row);
      const linkedPhotos = Array.isArray(row.photos) ? row.photos : [];
      for (const photo of linkedPhotos) {
        if (!photo || renderedPhotoIds.has(String(photo.mediaId))) continue;
        renderedPhotoIds.add(String(photo.mediaId));
        await drawJournalPhoto(photo, row.isTracking ? "" : row.text || "");
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

  const photos = Array.isArray(model && model.photos) ? model.photos : [];
  const remainingPhotos = selectRemainingJournalPhotos(photos, renderedPhotoIds);
  if (remainingPhotos.length) {
    drawSectionTitle("Additional Photos");
    for (const photo of remainingPhotos) {
      if (!photo || renderedPhotoIds.has(String(photo.mediaId))) continue;
      renderedPhotoIds.add(String(photo.mediaId));
      const linkedMoment = timelineRows.find(
        (row) => String(row.entryId || "") === String(photo.linkedLogEntryId || "")
      );
      await drawJournalPhoto(photo, linkedMoment && !linkedMoment.isTracking ? linkedMoment.text : "");
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
  renderJournalPdf,
  isRawShortcutTrackingText,
  humanizeShortcutLocation,
  shortcutMetaForRow,
  formatShortcutTimelineRow,
  prepareJournalTimeline,
  filterJournalNarrativeItems,
  cleanJournalNarrativeText,
};
