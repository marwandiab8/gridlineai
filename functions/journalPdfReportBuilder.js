const { rgb } = require("pdf-lib");
const { sanitizePdfText } = require("./pdfWinAnsiText");
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

// Each contributor gets their own accent colour so their chapter, photo captions and
// notes are recognisable at a glance - "who wrote what" without reading a single label.
const STORYLINE_COLORS = [
  rgb(0.2, 0.38, 0.62),
  rgb(0.66, 0.28, 0.44),
  rgb(0.2, 0.52, 0.42),
  rgb(0.62, 0.42, 0.14),
  rgb(0.42, 0.32, 0.62),
  rgb(0.36, 0.44, 0.5),
];

function firstName(author) {
  const name = String(author || "").trim();
  if (!name || /^unknown/i.test(name)) return "";
  return name.split(/\s+/)[0];
}

function possessive(name) {
  const value = String(name || "").trim();
  if (!value) return "";
  return /s$/i.test(value) ? `${value}'` : `${value}'s`;
}

function joinNames(names) {
  const list = (names || []).filter(Boolean);
  if (list.length <= 1) return list[0] || "";
  return `${list.slice(0, -1).join(", ")} & ${list[list.length - 1]}`;
}

function sameText(a, b) {
  const left = normalizeJournalKey(a);
  return Boolean(left) && left === normalizeJournalKey(b);
}

function photoTimeLabel(photo) {
  try {
    const created = photo && photo.createdAt;
    const date = created && typeof created.toDate === "function" ? created.toDate() : created ? new Date(created) : null;
    if (!date || !Number.isFinite(date.getTime())) return "";
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Toronto",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
      timeZoneName: "short",
    }).format(date);
  } catch (_) {
    return "";
  }
}

/** The notes a person wrote, minus raw tracking telemetry and exact repeats. */
function storylineNotes(line) {
  const seen = new Set();
  return (Array.isArray(line && line.notes) ? line.notes : []).filter((note) => {
    const text = String((note && note.text) || "").trim();
    if (!text && !(note && note.photos && note.photos.length)) return false;
    if (text && isRawShortcutTrackingText(text)) return false;
    const key = normalizeJournalKey(text);
    if (key && seen.has(key)) return false;
    if (key) seen.add(key);
    return true;
  });
}

async function renderJournalPdf(opts) {
  const {
    pdf,
    font,
    fontBold,
    fontItalic: fontItalicOpt,
    storageBucket,
    pageW = 612,
    pageH = 792,
    margin = 54,
    titleStr,
    footerBrand,
    coverMeta = {},
    logoStoragePath,
    merged = {},
    logger,
    runId,
  } = opts;
  const fontItalic = fontItalicOpt || font;

  const contentW = pageW - 2 * margin;
  const footerReserve = 42;
  const headerReserve = 44;
  const C = {
    ink: rgb(0.13, 0.14, 0.18),
    body: rgb(0.2, 0.21, 0.25),
    muted: rgb(0.45, 0.47, 0.52),
    rule: rgb(0.82, 0.83, 0.86),
    card: rgb(0.965, 0.965, 0.975),
    good: rgb(0.18, 0.5, 0.34),
    hard: rgb(0.7, 0.4, 0.12),
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

  function drawRule(yy, thickness = 0.6, color = C.rule, left = margin, width = contentW) {
    page.drawLine({ start: { x: left, y: yy }, end: { x: left + width, y: yy }, thickness, color });
  }

  function linesFor(text, f, size, maxW) {
    return wrapToLines(String(text || "").trim(), f, size, maxW);
  }

  function drawText(text, { size = 10, f = font, color = C.body, left = margin, maxW = contentW, leading = LEADING } = {}) {
    const lh = size + leading;
    for (const line of linesFor(text, f, size, maxW)) {
      ensureSpace(lh + 2);
      page.drawText(sanitizePdfText(line), { x: left, y, size, font: f, color });
      y -= lh;
    }
  }

  function drawLabel(text, color = C.muted) {
    y -= 4;
    ensureSpace(24);
    drawText(String(text || "").toUpperCase(), { size: 7.5, f: fontBold, color, leading: 4 });
    y -= 2;
  }

  function drawBulletList(items, color) {
    for (const item of items || []) {
      const lines = linesFor(item, font, 10, contentW - 16);
      if (!lines.length) continue;
      ensureSpace(lines.length * (10 + LEADING) + 2);
      page.drawCircle({ x: margin + 3, y: y + 3.2, size: 1.8, color });
      let lineY = y;
      for (const line of lines) {
        page.drawText(sanitizePdfText(line), { x: margin + 12, y: lineY, size: 10, font, color: C.body });
        lineY -= 10 + LEADING;
      }
      y = lineY - 2;
    }
  }

  async function drawCoverLogo(topY) {
    const boxW = 120;
    const maxH = 48;
    if (!logoStoragePath || !storageBucket) return { bottomY: topY, leftW: 0 };
    let buf = null;
    try {
      [buf] = await storageBucket.file(logoStoragePath).download();
    } catch (_) {}
    const img = buf ? await embedImageIfPossible(pdf, buf) : null;
    if (!img) return { bottomY: topY, leftW: 0 };
    const h = Math.min(img.height * (boxW / img.width), maxH);
    const w = img.width * (h / img.height);
    page.drawImage(img, { x: margin, y: topY - h, width: w, height: h });
    return { bottomY: topY - h - 10, leftW: boxW };
  }

  /**
   * A photo with its caption card directly underneath: who sent it and when, then the
   * caption in their words. The card is always drawn, so every picture is attributed
   * even when it has no caption of its own.
   */
  async function drawPhotoWithCaption(photo, { author, color, caption, time }) {
    let buf = null;
    try {
      if (storageBucket) [buf] = await storageBucket.file(photo.storagePath).download();
    } catch (e) {
      if (logger) {
        logger.warn("journalPdfReportBuilder: journal photo download failed", {
          runId,
          path: photo.storagePath,
          message: e.message,
        });
      }
    }
    const img = buf ? await embedImageIfPossible(pdf, buf) : null;

    const captionText = String(caption || "").trim();
    const metaText = [author, time || photoTimeLabel(photo)].filter(Boolean).join(" · ");
    const pad = 8;
    const textW = contentW - pad * 2 - 4;
    const metaLines = metaText ? linesFor(metaText, fontBold, 8, textW) : [];
    const captionLines = captionText ? linesFor(captionText, fontItalic, 10, textW) : [];
    const cardH = pad * 2 + metaLines.length * 11 + captionLines.length * 13.5 + (metaLines.length && captionLines.length ? 2 : 0);

    let imgW = 0;
    let imgH = 0;
    if (img) {
      const scale = Math.min((contentW * 0.85) / img.width, 290 / img.height, 1.5);
      imgW = img.width * scale;
      imgH = img.height * scale;
    }
    const placeholderH = img ? 0 : 22;
    ensureSpace(imgH + placeholderH + cardH + 12);

    if (img) {
      page.drawImage(img, { x: margin + (contentW - imgW) / 2, y: y - imgH, width: imgW, height: imgH });
      y -= imgH;
    } else {
      page.drawRectangle({ x: margin, y: y - placeholderH, width: contentW, height: placeholderH, color: C.card });
      page.drawText(sanitizePdfText("Photo unavailable"), { x: margin + pad, y: y - 14, size: 8.5, font: fontItalic, color: C.muted });
      y -= placeholderH;
    }

    if (cardH > pad * 2) {
      page.drawRectangle({ x: margin, y: y - cardH, width: contentW, height: cardH, color: C.card });
      page.drawRectangle({ x: margin, y: y - cardH, width: 3, height: cardH, color });
      let lineY = y - pad - 7;
      for (const line of metaLines) {
        page.drawText(sanitizePdfText(line), { x: margin + pad + 4, y: lineY, size: 8, font: fontBold, color });
        lineY -= 11;
      }
      if (metaLines.length && captionLines.length) lineY -= 2;
      for (const line of captionLines) {
        page.drawText(sanitizePdfText(line), { x: margin + pad + 4, y: lineY, size: 10, font: fontItalic, color: C.ink });
        lineY -= 13.5;
      }
      y -= cardH;
    }
    y -= 14;
  }

  function photoCaption(photo, fallback) {
    const own = String((photo && photo.captionText) || "").trim();
    const context = String(fallback || "").trim();
    const safeContext = isRawShortcutTrackingText(context) ? "" : context;
    return refineCaptionForPdf(own || safeContext, safeContext, "");
  }

  function drawChapterHeading(title, color) {
    y -= 14;
    ensureSpace(60);
    page.drawRectangle({ x: margin, y: y - 6, width: 4, height: 22, color });
    page.drawText(sanitizePdfText(title), { x: margin + 12, y, size: 15, font: fontBold, color: C.ink });
    y -= 26;
  }

  async function drawStoryline(line, color) {
    const name = firstName(line.author) || line.author || "Someone";
    drawChapterHeading(`${possessive(line.author || "Someone")} day`, color);

    if (line.headline) {
      drawText(line.headline, { size: 11.5, f: fontItalic, color: C.ink, leading: 4 });
      y -= 6;
    }
    for (const paragraph of line.story || []) {
      drawText(paragraph, { size: 10.5, color: C.body, leading: 5 });
      y -= 7;
    }

    if ((line.highs || []).length) {
      drawLabel("The good", C.good);
      drawBulletList(line.highs, C.good);
    }
    if ((line.struggles || []).length) {
      drawLabel("The hard parts", C.hard);
      drawBulletList(line.struggles, C.hard);
    }

    const activities = (line.activities || []).filter((row) => row && row.text);
    if (activities.length) {
      drawLabel("Day at a glance");
      for (const row of activities) {
        drawText(row.text, { size: 8.5, color: C.muted, leading: 3 });
        y -= 1;
      }
    }

    const notes = storylineNotes(line);
    const shownPhotoIds = new Set();
    if (notes.length) {
      drawLabel(`In ${name}'s own words`, color);
      for (const note of notes) {
        const photos = (note.photos || []).filter((photo) => photo && !shownPhotoIds.has(String(photo.mediaId)));
        const firstCaption = photos.length ? String(photos[0].captionText || "").trim() : "";
        // When a note came with a photo, the note is that photo's caption: show it once, under the picture.
        const noteIsCaption = photos.length > 0 && (!firstCaption || sameText(firstCaption, note.text));
        if (!noteIsCaption) {
          ensureSpace(30);
          drawText(note.time || "", { size: 8, f: fontBold, color, leading: 3 });
          if (note.text) {
            drawText(note.text, { size: 10, color: C.body, leading: 4 });
            y -= 4;
          }
        }
        for (let i = 0; i < photos.length; i += 1) {
          const photo = photos[i];
          shownPhotoIds.add(String(photo.mediaId));
          const caption = i === 0 && noteIsCaption ? note.text : photoCaption(photo, "");
          await drawPhotoWithCaption(photo, { author: line.author, color, caption, time: note.time });
        }
        y -= 4;
      }
    }

    const morePhotos = (line.photos || []).filter((photo) => photo && !shownPhotoIds.has(String(photo.mediaId)));
    if (morePhotos.length) {
      ensureSpace(330); // keep the label on the same page as the first photo
      drawLabel(`More from ${name}`, color);
      for (const photo of morePhotos) {
        shownPhotoIds.add(String(photo.mediaId));
        const linkedNote = (line.notes || []).find((note) => String(note.entryId || "") === String(photo.linkedLogEntryId || ""));
        await drawPhotoWithCaption(photo, { author: line.author, color, caption: photoCaption(photo, linkedNote && linkedNote.text) });
      }
    }
  }

  // ----- Cover -----
  const storylines = Array.isArray(merged.storylines) ? merged.storylines : [];
  const logoBand = await drawCoverLogo(y);
  const textLeft = margin + (logoBand.leftW ? logoBand.leftW + 18 : 0);
  const textWidth = pageW - margin - textLeft;
  const coverText = (text, size, f, color, leading = LEADING) =>
    drawText(text, { size, f, color, left: textLeft, maxW: textWidth, leading });

  coverText(coverMeta.brandLine || `Shared daily journal - ${footerBrand}`, 8.5, font, C.muted);
  y -= 8;
  coverText(coverMeta.titleMain || titleStr, 22, fontBold, C.ink);
  if (coverMeta.titleDate) {
    y -= 2;
    coverText(coverMeta.titleDate, 11, font, C.muted);
  }
  const dayTitle = String(merged.dayTitle || coverMeta.dayTitle || "").trim();
  if (dayTitle) {
    y -= 8;
    coverText(dayTitle, 16, fontItalic, C.ink, 5);
  }
  const toldBy = joinNames(storylines.map((line) => line.author));
  if (toldBy) {
    y -= 4;
    coverText(`Told by ${toldBy}`, 9.5, font, C.muted);
  }
  y = Math.min(y, logoBand.bottomY) - 6;
  drawRule(y + 4, 0.8);
  y -= 10;
  for (const row of coverMeta.grid || []) {
    const label = String(row.label || "").trim();
    const value = String(row.value || "").trim();
    if (!label && !value) continue;
    ensureSpace(18);
    page.drawText(sanitizePdfText(label), { x: margin, y, size: 8, font: fontBold, color: C.muted });
    drawText(value || "Not specified", { size: 8.5, color: C.muted, left: margin + 90, maxW: contentW - 90 });
    y -= 2;
  }

  // ----- Storylines -----
  if (!storylines.length) {
    y -= 16;
    drawText("No journal notes were captured for this day.", { size: 10.5, f: fontItalic, color: C.muted });
  }
  for (let i = 0; i < storylines.length; i += 1) {
    await drawStoryline(storylines[i], STORYLINE_COLORS[i % STORYLINE_COLORS.length]);
  }

  if (merged.sharedThread) {
    drawChapterHeading("Where our days met", C.muted);
    drawText(merged.sharedThread, { size: 10.5, f: fontItalic, color: C.ink, leading: 5 });
  }

  const orphanPhotos = Array.isArray(merged.orphanPhotos) ? merged.orphanPhotos : [];
  if (orphanPhotos.length) {
    ensureSpace(360);
    drawChapterHeading("More photos from the day", C.muted);
    for (const photo of orphanPhotos) {
      await drawPhotoWithCaption(photo, { author: "", color: C.muted, caption: photoCaption(photo, "") });
    }
  }

  if (merged.closingNote) {
    y -= 10;
    ensureSpace(50);
    drawRule(y + 6, 0.6);
    y -= 12;
    drawText(merged.closingNote, { size: 11, f: fontItalic, color: C.ink, leading: 5 });
  }

  // ----- Header / footer -----
  const pages = pdf.getPages();
  const totalPages = pages.length;
  const headerTxt = sanitizePdfText(titleStr);
  const footL = sanitizePdfText(`Powered by ${footerBrand}`);
  for (let i = 0; i < totalPages; i++) {
    const pg = pages[i];
    const fr = `Page ${i + 1} of ${totalPages}`;
    const fw = font.widthOfTextAtSize(fr, 8);
    pg.drawText(footL, { x: margin, y: 16, size: 8, font, color: C.muted });
    pg.drawText(sanitizePdfText(fr), { x: pageW - margin - fw, y: 16, size: 8, font, color: C.muted });
    if (i > 0) {
      pg.drawText(headerTxt, { x: margin, y: pageH - 28, size: 9, font: fontBold, color: C.ink });
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
