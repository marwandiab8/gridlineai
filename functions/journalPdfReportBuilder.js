const { rgb } = require("pdf-lib");
const { sanitizePdfText } = require("./pdfWinAnsiText");
const { drawRichText } = require("./pdfEmoji");
const { refineCaptionForPdf } = require("./dailyPdfCompact");
const { wrapToLines, shrinkPhotoForPdf } = require("./dailyPdfReportBuilderLegacy");
const { formatSetParts, formatTimeRange, formatMinutes } = require("./gymK2Workouts");
const {
  formatHours,
  describeProcoreLogFacts,
  describeProcoreWeather,
  describeProcoreVisitor,
  describeProcoreInspection,
} = require("./procoreDailyLog");
const {
  describeProcorePhotoSet,
  procorePhotoCaption,
  photoTimeLabel: procorePhotoTime,
} = require("./procoreDailyLogPhotos");

const LEADING = 3;
// A photo is shrunk to fit what's left of a page down to this height (about 2 inches), never smaller.
const MIN_PHOTO_H = 150;
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
// The Procore daily log is a record, not anyone's story: a plain slate of its own.
const PROCORE_COLOR = rgb(0.3, 0.35, 0.42);

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

  // A section label waits for its first piece of content and goes on whichever page that lands on, so a label is
  // never left alone at the bottom of a page.
  const LABEL_H = 20;
  let pendingLabel = null;

  function ensureSpace(need) {
    const label = pendingLabel;
    if (y - need - (label ? LABEL_H : 0) < margin + footerReserve) newPage();
    if (label) {
      pendingLabel = null;
      y -= 4;
      drawText(String(label.text || "").toUpperCase(), { size: 7.5, f: fontBold, color: label.color, leading: 4 });
      y -= 2;
    }
  }

  /** Room left on this page below `y`, after a waiting label. */
  function roomLeft() {
    return y - (margin + footerReserve) - (pendingLabel ? LABEL_H : 0);
  }

  /** Room on a fresh page, after a waiting label. */
  function fullPageRoom() {
    return pageH - margin - headerReserve - (margin + footerReserve) - (pendingLabel ? LABEL_H : 0);
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
      drawRichText(page, line, { x: left, y, size, font: f, color });
      y -= lh;
    }
  }

  function drawLabel(text, color = C.muted) {
    pendingLabel = { text, color };
  }

  function drawBulletList(items, color) {
    for (const item of items || []) {
      const lines = linesFor(item, font, 10, contentW - 16);
      if (!lines.length) continue;
      ensureSpace(lines.length * (10 + LEADING) + 2);
      page.drawCircle({ x: margin + 3, y: y + 3.2, size: 1.8, color });
      let lineY = y;
      for (const line of lines) {
        drawRichText(page, line, { x: margin + 12, y: lineY, size: 10, font, color: C.body });
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
    const tail = photo === lastPhoto ? lastPhotoTailH : 0;
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
      const scale = Math.min((contentW * 0.8) / img.width, 250 / img.height, 1.5);
      imgW = img.width * scale;
      imgH = img.height * scale;
      // Shrink a little rather than leave the rest of the page empty.
      const room = roomLeft() - cardH - 12 - tail;
      if (imgH > room && room >= MIN_PHOTO_H) {
        imgW *= room / imgH;
        imgH = room;
      }
    }
    const placeholderH = img ? 0 : 22;
    ensureSpace(imgH + placeholderH + cardH + 12);

    if (img) {
      page.drawImage(img, { x: margin + (contentW - imgW) / 2, y: y - imgH, width: imgW, height: imgH });
      y -= imgH;
    } else {
      page.drawRectangle({ x: margin, y: y - placeholderH, width: contentW, height: placeholderH, color: C.card });
      drawRichText(page, "Photo unavailable", { x: margin + pad, y: y - 14, size: 8.5, font: fontItalic, color: C.muted });
      y -= placeholderH;
    }

    if (cardH > pad * 2) {
      page.drawRectangle({ x: margin, y: y - cardH, width: contentW, height: cardH, color: C.card });
      page.drawRectangle({ x: margin, y: y - cardH, width: 3, height: cardH, color });
      let lineY = y - pad - 7;
      for (const line of metaLines) {
        drawRichText(page, line, { x: margin + pad + 4, y: lineY, size: 8, font: fontBold, color });
        lineY -= 11;
      }
      if (metaLines.length && captionLines.length) lineY -= 2;
      for (const line of captionLines) {
        drawRichText(page, line, { x: margin + pad + 4, y: lineY, size: 10, font: fontItalic, color: C.ink });
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

  /** Text that ends at `right` (a column of numbers). */
  function drawRightAt(text, size, f, color, right, yy) {
    const value = sanitizePdfText(text);
    const width = f.widthOfTextAtSize(value, size);
    page.drawText(value, { x: right - width, y: yy, size, font: f, color });
    return width;
  }

  function drawRightText(text, size, f, color, yy) {
    return drawRightAt(text, size, f, color, margin + contentW, yy);
  }

  /** "80 lb × 30", "Bodyweight × 6 wide handle". */
  function setInline(set, unit) {
    const { weight, reps } = formatSetParts(set, unit);
    const count = String(reps || "").replace(/^(\d+) reps?$/, "$1");
    return count ? `${weight} × ${count}` : weight;
  }

  /** Every set on one line: "80 lb × 30 · 180 lb × 11", or "135 lb × 8 (3 sets)" when they were all the same. */
  function setsInline(sets, unit) {
    const parts = sets.map((set) => setInline(set, unit));
    if (parts.length > 1 && parts.every((part) => part === parts[0])) return `${parts[0]} (${parts.length} sets)`;
    return parts.join("  ·  ");
  }

  /**
   * One workout: the routine with its time and focus, then a row per exercise - its name, every set
   * (weight × reps) and when it was worked on, with the exercise's note under its sets.
   */
  function drawWorkout(workout, color) {
    const range = formatTimeRange(workout.startMs, workout.endMs);
    const minutes = formatMinutes(workout.startMs, workout.endMs);
    const nameW = 168;
    const timeW = 84;
    const setsX = margin + nameW + 16;
    const setsW = contentW - nameW - 16 - timeW - 8;
    const rows = workout.exercises.map((exercise) => ({
      nameLines: linesFor(exercise.name, fontBold, 9.5, nameW),
      setLines: exercise.sets.length ? linesFor(setsInline(exercise.sets, workout.unit), font, 9, setsW) : [],
      noteLines: exercise.note ? linesFor(exercise.note, fontItalic, 8.5, setsW) : [],
      time: formatTimeRange(exercise.startMs, exercise.endMs),
    }));
    const rowH = (row) => Math.max(row.nameLines.length * 12, row.setLines.length * 12 + row.noteLines.length * 11, 12) + 8;
    const focusText = workout.focus.length ? `Focus: ${workout.focus.join(", ")}` : "";
    const introH =
      15 +
      (focusText ? linesFor(focusText, font, 8.5, contentW).length * 11.5 : 0) +
      (workout.notes ? linesFor(workout.notes, fontItalic, 9.5, contentW).length * 12.5 : 0) +
      2;
    const totalH = introH + rows.reduce((sum, row) => sum + rowH(row), 0) + 8;

    // A workout is kept in one piece: if it fits on a page but not in what's left of this one, it starts on the next.
    if (totalH > roomLeft() && totalH <= fullPageRoom()) newPage();
    ensureSpace(introH + (rows.length ? rowH(rows[0]) : 0));
    drawRichText(page, workout.routineName, { x: margin, y, size: 12.5, font: fontBold, color: C.ink });
    drawRightText([range, minutes].filter(Boolean).join(" · "), 8.5, font, C.muted, y + 1);
    y -= 15;
    if (focusText) drawText(focusText, { size: 8.5, color: C.muted, leading: 3 });
    if (workout.notes) drawText(workout.notes, { size: 9.5, f: fontItalic, color: C.body, leading: 3 });
    y -= 2;

    rows.forEach((row, index) => {
      const h = rowH(row);
      ensureSpace(h);
      const top = y + 9;
      if (index % 2 === 0) page.drawRectangle({ x: margin, y: top - h, width: contentW, height: h, color: C.card });
      const firstBaseline = top - 4 - 8.5;
      row.nameLines.forEach((line, i) => {
        drawRichText(page, line, { x: margin + 8, y: firstBaseline - i * 12, size: 9.5, font: fontBold, color: C.ink });
      });
      row.setLines.forEach((line, i) => {
        drawRichText(page, line, { x: setsX, y: firstBaseline - i * 12, size: 9, font, color: C.ink });
      });
      row.noteLines.forEach((line, i) => {
        drawRichText(page, line, { x: setsX, y: firstBaseline - row.setLines.length * 12 - i * 11, size: 8.5, font: fontItalic, color: C.muted });
      });
      if (row.time) drawRightText(row.time, 8, font, C.muted, firstBaseline);
      y = top - h - 9;
    });
    y -= 8;
  }

  function drawChapterHeading(title, color) {
    y -= 14;
    ensureSpace(60);
    page.drawRectangle({ x: margin, y: y - 6, width: 4, height: 22, color });
    drawRichText(page, title, { x: margin + 12, y, size: 15, font: fontBold, color: C.ink });
    y -= 26;
  }

  /** The photos each note shows (each picture once) and the ones left over for "More from ...". */
  function storylinePhotoPlan(line) {
    const shown = new Set();
    const notes = storylineNotes(line);
    const notePhotos = notes.map((note) =>
      (note.photos || []).filter((photo) => {
        if (!photo || shown.has(String(photo.mediaId))) return false;
        shown.add(String(photo.mediaId));
        return true;
      })
    );
    const morePhotos = (line.photos || []).filter((photo) => photo && !shown.has(String(photo.mediaId)));
    return { notes, notePhotos, morePhotos };
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

    const workouts = Array.isArray(line.workouts) ? line.workouts : [];
    if (workouts.length) {
      drawLabel(workouts.length > 1 ? "Workouts" : "Workout", color);
      for (const workout of workouts) drawWorkout(workout, color);
    }

    const activities = (line.activities || []).filter((row) => row && row.text);
    if (activities.length) {
      drawLabel("Day at a glance");
      for (const row of activities) {
        drawText(row.text, { size: 8.5, color: C.muted, leading: 3 });
        y -= 1;
      }
    }

    const { notes, notePhotos, morePhotos } = storylinePhotoPlan(line);
    if (notes.length) {
      drawLabel(`In ${name}'s own words`, color);
      for (const [noteIndex, note] of notes.entries()) {
        const photos = notePhotos[noteIndex];
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
          const caption = i === 0 && noteIsCaption ? note.text : photoCaption(photo, "");
          await drawPhotoWithCaption(photo, { author: line.author, color, caption, time: note.time });
        }
        y -= 4;
      }
    }

    if (morePhotos.length) {
      drawLabel(`More from ${name}`, color);
      for (const photo of morePhotos) {
        const linkedNote = (line.notes || []).find((note) => String(note.entryId || "") === String(photo.linkedLogEntryId || ""));
        await drawPhotoWithCaption(photo, { author: line.author, color, caption: photoCaption(photo, linkedNote && linkedNote.text) });
      }
    }
    pendingLabel = null;
  }

  /**
   * A row per company on site: the company, its workers and hours (man-hours) as numbers, and what it did in the
   * log's own words; a total row under them. The column heads are repeated at the top of each page the table runs on.
   */
  function drawManpowerTable(rows, totals) {
    const pad = 8;
    const companyW = 150;
    const numW = 42;
    const companyX = margin + pad;
    const workersRight = companyX + companyW + numW;
    const hoursRight = workersRight + numW;
    const workX = hoursRight + 14;
    const workW = margin + contentW - pad - workX;
    const lineH = 11.5;
    const headH = 14;
    const shaped = rows.map((row) => ({
      companyLines: linesFor(row.company, fontBold, 9, companyW),
      workLines: row.notes ? linesFor(row.notes, font, 9, workW) : [],
      workers: String(row.workers),
      hours: formatHours(row.manHours),
    }));
    const rowH = (row) => Math.max(row.companyLines.length, row.workLines.length, 1) * lineH + 8;
    let headPage = null;
    shaped.forEach((row, index) => {
      const h = rowH(row);
      ensureSpace(h + (headPage === page ? 0 : headH));
      if (headPage !== page) {
        drawRichText(page, "Company", { x: companyX, y, size: 7.5, font: fontBold, color: C.muted });
        drawRightAt("Workers", 7.5, fontBold, C.muted, workersRight, y);
        drawRightAt("Hours", 7.5, fontBold, C.muted, hoursRight, y);
        drawRichText(page, "Work", { x: workX, y, size: 7.5, font: fontBold, color: C.muted });
        y -= headH;
        headPage = page;
      }
      const top = y + 9;
      if (index % 2 === 0) page.drawRectangle({ x: margin, y: top - h, width: contentW, height: h, color: C.card });
      const firstBaseline = top - 4 - 8;
      row.companyLines.forEach((line, i) => {
        drawRichText(page, line, { x: companyX, y: firstBaseline - i * lineH, size: 9, font: fontBold, color: C.ink });
      });
      drawRightAt(row.workers, 9, font, C.ink, workersRight, firstBaseline);
      drawRightAt(row.hours, 9, font, C.ink, hoursRight, firstBaseline);
      row.workLines.forEach((line, i) => {
        drawRichText(page, line, { x: workX, y: firstBaseline - i * lineH, size: 9, font, color: C.body });
      });
      y = top - h - 9;
    });
    ensureSpace(22);
    drawRule(y + 9, 0.6);
    y -= 4;
    drawRichText(page, "Total", { x: companyX, y, size: 9, font: fontBold, color: C.ink });
    drawRightAt(String(totals.workers), 9, fontBold, C.ink, workersRight, y);
    drawRightAt(formatHours(totals.manHours), 9, fontBold, C.ink, hoursRight, y);
    y -= 22;
  }

  /**
   * Photos side by side, `perRow` to a row, each scaled into its cell (at most `maxH` tall) and sitting on the row's
   * bottom line, with a short caption centred under it. A row is never split over two pages.
   */
  async function drawPhotoGrid(photos, { perRow, gapX, maxH, caption }) {
    const cellW = (contentW - gapX * (perRow - 1)) / perRow;
    const captionSize = 7.5;
    const captionLH = captionSize + 2;
    for (let k = 0; k < photos.length; k += perRow) {
      const cells = [];
      for (const photo of photos.slice(k, k + perRow)) {
        let img = null;
        try {
          if (storageBucket) {
            const [buf] = await storageBucket.file(photo.storagePath).download();
            img = await embedImageIfPossible(pdf, await shrinkPhotoForPdf(buf));
          }
        } catch (e) {
          if (logger && logger.warn) logger.warn("journalPdfReportBuilder: Procore photo download failed", { runId, path: photo.storagePath, message: e.message });
        }
        const scale = img ? Math.min(cellW / img.width, maxH / img.height) : 0;
        const text = img ? caption(photo) : "Photo unavailable";
        cells.push({ img, w: img ? img.width * scale : 0, h: img ? img.height * scale : 0, lines: linesFor(text, font, captionSize, cellW).slice(0, 2) });
      }
      const imagesH = Math.max(...cells.map((cell) => cell.h));
      const rowH = imagesH + 4 + Math.max(...cells.map((cell) => cell.lines.length)) * captionLH + (perRow > 2 ? 8 : 12);
      ensureSpace(rowH);
      const top = y;
      cells.forEach((cell, i) => {
        const left = margin + i * (cellW + gapX);
        if (cell.img) page.drawImage(cell.img, { x: left + (cellW - cell.w) / 2, y: top - imagesH, width: cell.w, height: cell.h });
        let cy = top - imagesH - 4;
        for (const line of cell.lines) {
          const width = font.widthOfTextAtSize(sanitizePdfText(line), captionSize);
          drawRichText(page, line, { x: left + (cellW - width) / 2, y: cy - captionSize, size: captionSize, font, color: C.muted });
          cy -= captionLH;
        }
      });
      y = top - rowH;
    }
  }

  /**
   * The day's Procore daily log (procoreDailyLog.js), after the journal itself: who completed it and the day's totals,
   * the observed weather, the companies on site, then visitors, inspections and the log's other entries, and last some
   * of the day's photos (procoreDailyLogPhotos.js, the same picks as the site log's): a few large, two to a row with
   * when and who took them, then the small ones six to a row with the time.
   */
  async function drawProcoreDailyLog(log, photos) {
    const color = PROCORE_COLOR;
    drawChapterHeading("Procore daily log", color);
    drawText(describeProcoreLogFacts(log), { size: 9, color: C.muted, leading: 3 });
    y -= 6;
    const weather = describeProcoreWeather(log.weather);
    if (weather.summary) {
      drawLabel("Weather", color);
      drawText(weather.summary, { size: 10, color: C.body, leading: 4 });
      if (weather.hourly) drawText(weather.hourly, { size: 8.5, color: C.muted, leading: 3 });
      y -= 8;
    }
    if (log.manpower.length) {
      drawLabel("On site", color);
      drawManpowerTable(log.manpower, log.totals);
    }
    if (log.visitors.length) {
      drawLabel(log.visitors.length > 1 ? "Visitors" : "Visitor", color);
      drawBulletList(log.visitors.map(describeProcoreVisitor), color);
      y -= 6;
    }
    if (log.inspections.length) {
      drawLabel(log.inspections.length > 1 ? "Inspections" : "Inspection", color);
      drawBulletList(log.inspections.map(describeProcoreInspection), color);
      y -= 6;
    }
    for (const section of log.sections || []) {
      drawLabel(section.title, color);
      drawBulletList(section.entries, color);
      y -= 6;
    }
    if (photos && (photos.large.length || photos.thumbs.length)) {
      drawLabel("Photos", color);
      drawText(describeProcorePhotoSet(photos), { size: 9, color: C.muted, leading: 3 });
      y -= 6;
      await drawPhotoGrid(photos.large, { perRow: 2, gapX: 14, maxH: 200, caption: procorePhotoCaption });
      await drawPhotoGrid(photos.thumbs, { perRow: 6, gapX: 8, maxH: 62, caption: (p) => procorePhotoTime(p.takenAt) });
    }
    pendingLabel = null;
  }

  // ----- Cover -----
  const storylines = Array.isArray(merged.storylines) ? merged.storylines : [];
  const orphanPhotos = Array.isArray(merged.orphanPhotos) ? merged.orphanPhotos : [];

  // When the journal ends on a photo, that photo shrinks a little if that keeps the closing note on its page
  // rather than leaving the note alone on a new one.
  let lastPhoto = null;
  let lastPhotoTailH = 0;
  if (merged.closingNote) {
    const closingLines = linesFor(merged.closingNote, fontItalic, 11, contentW).length;
    lastPhotoTailH = 18 + Math.max(50, 14 + closingLines * 16);
    if (orphanPhotos.length) {
      lastPhoto = orphanPhotos[orphanPhotos.length - 1];
    } else if (!merged.sharedThread && storylines.length) {
      const plan = storylinePhotoPlan(storylines[storylines.length - 1]);
      const lastNotePhotos = plan.notePhotos[plan.notePhotos.length - 1] || [];
      lastPhoto = plan.morePhotos.length ? plan.morePhotos[plan.morePhotos.length - 1] : lastNotePhotos[lastNotePhotos.length - 1] || null;
    }
  }
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

  if (merged.procoreDailyLog) {
    try {
      await drawProcoreDailyLog(merged.procoreDailyLog, merged.procorePhotos);
    } catch (procoreErr) {
      if (logger && logger.warn) logger.warn("journalPdfReportBuilder: Procore daily log not drawn", { runId, message: procoreErr.message });
    }
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
    drawRichText(pg, fr, { x: pageW - margin - fw, y: 16, size: 8, font, color: C.muted });
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
