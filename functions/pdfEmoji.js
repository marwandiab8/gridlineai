/**
 * Emojis in the PDF reports. pdf-lib's StandardFonts (Helvetica) can't draw them, so sanitizePdfText drops them;
 * here they are drawn as small colour pictures (Twemoji, https://github.com/jdecked/twemoji - graphics CC-BY 4.0)
 * in line with the text.
 *
 * - prepareEmojiImages(pdf, sources) runs once per report, before drawing: it finds every emoji in the report's
 *   text, fetches each picture once (kept in memory between reports) and embeds it in that PDF.
 * - wrap with richTextWidth (an emoji is a fixed square), and draw a line with drawRichText. A line without emoji
 *   is drawn exactly as before (sanitizePdfText + drawText); an emoji whose picture couldn't be fetched leaves a gap.
 */
const { sanitizePdfText } = require("./pdfWinAnsiText");

const EMOJI_RE = /\p{RGI_Emoji}/gv;
const TWEMOJI_BASE = "https://cdn.jsdelivr.net/gh/jdecked/twemoji@15.1.0/assets/72x72";
const EMOJI_SCALE = 1.05; // picture height, in font sizes
const EMOJI_ADVANCE = 1.2; // space an emoji takes on the line, in font sizes
const EMOJI_DROP = 0.18; // how far the picture sits below the baseline, in font sizes
const MAX_EMOJIS = 300; // distinct emojis per report
const FETCH_TIMEOUT_MS = 5000;
const CREDIT = "Emoji graphics: Twemoji (CC-BY 4.0)";

const imagesByDoc = new WeakMap(); // PDFDocument -> Map(emoji -> PDFImage)
const pngCache = new Map(); // emoji -> PNG bytes, or null when it couldn't be fetched

const hasEmoji = (text) => /\p{RGI_Emoji}/v.test(String(text ?? ""));

/** Text and emojis in order: [{ text } | { emoji }]. */
function emojiRuns(text) {
  const s = String(text ?? "");
  const runs = [];
  let last = 0;
  for (const m of s.matchAll(EMOJI_RE)) {
    if (m.index > last) runs.push({ text: s.slice(last, m.index) });
    runs.push({ emoji: m[0] });
    last = m.index + m[0].length;
  }
  if (last < s.length) runs.push({ text: s.slice(last) });
  return runs;
}

/** Twemoji's file name for an emoji: its code points in hex, without U+FE0F unless it's a ZWJ sequence. */
function twemojiCode(emoji) {
  const s = emoji.includes("‍") ? emoji : emoji.replace(/️/g, "");
  return Array.from(s, (ch) => ch.codePointAt(0).toString(16)).join("-");
}

/** Width of a line on the page: its text (as Helvetica draws it) plus a square per emoji. */
function richTextWidth(font, text, size) {
  let width = 0;
  for (const run of emojiRuns(text)) {
    width += run.emoji ? size * EMOJI_ADVANCE : font.widthOfTextAtSize(sanitizePdfText(run.text), size);
  }
  return width;
}

/** Draws one line: page.drawText for the text, the emoji pictures in between. */
function drawRichText(page, text, opts) {
  const s = String(text ?? "");
  if (!hasEmoji(s)) {
    page.drawText(sanitizePdfText(s), opts);
    return;
  }
  const { x, y, size, font } = opts;
  const images = (page.doc && imagesByDoc.get(page.doc)) || new Map();
  let cx = x;
  for (const run of emojiRuns(s)) {
    if (run.emoji) {
      const img = images.get(run.emoji);
      if (img) {
        const h = size * EMOJI_SCALE;
        page.drawImage(img, { x: cx + (size * EMOJI_ADVANCE - h) / 2, y: y - size * EMOJI_DROP, width: h, height: h });
      }
      cx += size * EMOJI_ADVANCE;
    } else {
      const t = sanitizePdfText(run.text);
      if (t) page.drawText(t, { ...opts, x: cx });
      cx += font.widthOfTextAtSize(t, size);
    }
  }
}

/** Every string in a value (objects and arrays, nested), for finding the emojis a report will draw. */
function collectStrings(value, out = [], depth = 0) {
  if (value == null || depth > 12 || out.length > 20000) return out;
  if (typeof value === "string") out.push(value);
  else if (Array.isArray(value)) for (const v of value) collectStrings(v, out, depth + 1);
  else if (typeof value === "object" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
    for (const v of Object.values(value)) collectStrings(v, out, depth + 1);
  }
  return out;
}

async function fetchPng(emoji, fetchImpl) {
  if (pngCache.has(emoji)) return pngCache.get(emoji);
  let bytes = null;
  try {
    const res = await fetchImpl(`${TWEMOJI_BASE}/${twemojiCode(emoji)}.png`, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
    if (res.ok) bytes = new Uint8Array(await res.arrayBuffer());
  } catch (_) {
    bytes = null;
  }
  // Only a picture that exists is remembered; a failed fetch is tried again next report.
  if (bytes) pngCache.set(emoji, bytes);
  return bytes;
}

/**
 * Finds the emojis in `sources` (any nested strings), fetches their pictures and embeds them in `pdf` for
 * drawRichText. Returns how many were embedded. Credits Twemoji in the document's keywords when it drew any.
 */
async function prepareEmojiImages(pdf, sources, { fetchImpl = fetch, logger = null } = {}) {
  const emojis = new Set();
  for (const s of collectStrings(sources)) {
    if (!hasEmoji(s)) continue;
    for (const m of s.matchAll(EMOJI_RE)) {
      if (emojis.size >= MAX_EMOJIS) break;
      emojis.add(m[0]);
    }
  }
  if (!emojis.size) return 0;
  const images = new Map();
  const list = [...emojis];
  const missing = [];
  for (let i = 0; i < list.length; i += 8) {
    await Promise.all(list.slice(i, i + 8).map(async (emoji) => {
      const bytes = await fetchPng(emoji, fetchImpl);
      if (!bytes) return missing.push(emoji);
      try {
        images.set(emoji, await pdf.embedPng(bytes));
      } catch (_) {
        missing.push(emoji);
      }
    }));
  }
  imagesByDoc.set(pdf, images);
  if (images.size) pdf.setKeywords([CREDIT]);
  if (missing.length && logger) logger.warn("pdfEmoji: some emoji pictures were unavailable", { missing: missing.length, embedded: images.size });
  return images.size;
}

module.exports = {
  CREDIT,
  EMOJI_ADVANCE,
  drawRichText,
  emojiRuns,
  hasEmoji,
  prepareEmojiImages,
  richTextWidth,
  twemojiCode,
};
