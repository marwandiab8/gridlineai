const test = require("node:test");
const assert = require("node:assert/strict");
const { PDFDocument, StandardFonts } = require("pdf-lib");

const { CREDIT, EMOJI_ADVANCE, drawRichText, emojiRuns, prepareEmojiImages, richTextWidth, twemojiCode } = require("./pdfEmoji");
const { wrapToLines } = require("./dailyPdfReportBuilderLegacy");

// A 1x1 transparent PNG, standing in for an emoji picture.
const PNG = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=", "base64");
function fakeFetch({ missing = [] } = {}) {
  const urls = [];
  const fetchImpl = async (url) => {
    urls.push(url);
    if (missing.some((code) => url.endsWith(`/${code}.png`))) return { ok: false, status: 404 };
    return { ok: true, status: 200, arrayBuffer: async () => PNG.buffer.slice(PNG.byteOffset, PNG.byteOffset + PNG.length) };
  };
  return { urls, fetchImpl };
}
// Helvetica stand-in: every character is half the font size wide.
const fakeFont = { widthOfTextAtSize: (t, size) => String(t).length * size * 0.5 };

test("text is split into words and whole emojis, family, flag, skin tone and keycap included", () => {
  assert.deepEqual(emojiRuns("David went climbing 🧗 today"), [{ text: "David went climbing " }, { emoji: "🧗" }, { text: " today" }]);
  assert.deepEqual(emojiRuns("👨‍👩‍👧🇨🇦👍🏽1️⃣").map((r) => r.emoji), ["👨‍👩‍👧", "🇨🇦", "👍🏽", "1️⃣"]);
  assert.deepEqual(emojiRuns("no emoji here"), [{ text: "no emoji here" }]);
  assert.deepEqual(emojiRuns(""), []);
});

test("each emoji's picture file is named as Twemoji names it", () => {
  assert.equal(twemojiCode("🧗"), "1f9d7");
  assert.equal(twemojiCode("❤️"), "2764", "the variation selector is dropped");
  assert.equal(twemojiCode("👨‍👩‍👧"), "1f468-200d-1f469-200d-1f467", "zero-width joiners stay");
  assert.equal(twemojiCode("🇨🇦"), "1f1e8-1f1e6");
  assert.equal(twemojiCode("👍🏽"), "1f44d-1f3fd");
  assert.equal(twemojiCode("1️⃣"), "31-20e3");
});

test("an emoji takes a fixed square on the line; text is measured as Helvetica draws it", () => {
  assert.equal(richTextWidth(fakeFont, "abc", 10), 15);
  assert.equal(richTextWidth(fakeFont, "ab 🧗", 10), 15 + 10 * EMOJI_ADVANCE);
  assert.equal(richTextWidth(fakeFont, "", 10), 0);
});

test("wrapping keeps emojis, measures them, and never cuts one in half", async () => {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const lines = wrapToLines("David went climbing 🧗 and finished the route ❤️ — great day", font, 10, 120);
  assert.ok(lines.length > 1);
  assert.ok(lines.join(" ").includes("🧗") && lines.join(" ").includes("❤️"), lines.join(" | "));
  for (const line of lines) assert.ok(richTextWidth(font, line, 10) <= 120 + 0.01, line);
  const long = wrapToLines("👨‍👩‍👧".repeat(30), font, 10, 60);
  for (const line of long) assert.match(line, /^(👨‍👩‍👧)+$/u, "a long run of family emojis breaks between emojis");
  assert.deepEqual(wrapToLines("plain text only", font, 10, 500), ["plain text only"], "text without emoji wraps as before");
});

function fakePage(images = null) {
  const calls = [];
  const doc = {};
  return {
    calls,
    doc,
    page: { doc, drawText: (t, o) => calls.push(["text", t, o.x]), drawImage: (img, o) => calls.push(["image", img, o.x, o.width]) },
  };
}

test("a line without emoji is drawn exactly as before", () => {
  const { page, calls } = fakePage();
  drawRichText(page, "Pour got pushed – again", { x: 10, y: 100, size: 10, font: fakeFont });
  assert.deepEqual(calls, [["text", "Pour got pushed - again", 10]], "one drawText of the cleaned text");
});

test("a line with emojis draws the text around their pictures; a missing picture leaves a gap", async () => {
  const pdf = await PDFDocument.create();
  const { fetchImpl } = fakeFetch({ missing: ["1f600"] });
  assert.equal(await prepareEmojiImages(pdf, ["went climbing 🧗", "😀"], { fetchImpl }), 1);
  const page = pdf.addPage();
  const calls = [];
  const realText = page.drawText.bind(page);
  const realImage = page.drawImage.bind(page);
  page.drawText = (t, o) => { calls.push(["text", t, o.x]); realText(t, o); };
  page.drawImage = (img, o) => { calls.push(["image", o.x, o.width]); realImage(img, o); };
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  drawRichText(page, "Hi 🧗 ok 😀 end", { x: 10, y: 100, size: 10, font });
  const kinds = calls.map((c) => c[0]);
  assert.deepEqual(kinds, ["text", "image", "text", "text"], "the 😀 picture was unavailable, so only its space is kept");
  const xHi = calls[0][2];
  const xOk = calls[2][2];
  assert.equal(xOk, xHi + font.widthOfTextAtSize("Hi ", 10) + 10 * EMOJI_ADVANCE);
  assert.equal(calls[3][2], xOk + font.widthOfTextAtSize(" ok ", 10) + 10 * EMOJI_ADVANCE, "text after the gap carries on");
});

test("pictures are fetched once per emoji, embedded, and Twemoji is credited", async () => {
  const pdf = await PDFDocument.create();
  const { urls, fetchImpl } = fakeFetch();
  const n = await prepareEmojiImages(pdf, { a: "🎉 party", nested: [{ b: "🎉 again and 🚀" }] }, { fetchImpl });
  assert.equal(n, 2);
  assert.deepEqual(urls.map((u) => u.split("/").pop()).sort(), ["1f389.png", "1f680.png"]);
  assert.match(urls[0], /^https:\/\/cdn\.jsdelivr\.net\/gh\/jdecked\/twemoji@15\.1\.0\/assets\/72x72\//);
  assert.equal(pdf.getKeywords(), CREDIT);

  const again = fakeFetch();
  await prepareEmojiImages(await PDFDocument.create(), ["🎉"], { fetchImpl: again.fetchImpl });
  assert.equal(again.urls.length, 0, "a picture already fetched is reused");

  const none = fakeFetch();
  const plain = await PDFDocument.create();
  assert.equal(await prepareEmojiImages(plain, ["no emoji"], { fetchImpl: none.fetchImpl }), 0);
  assert.equal(none.urls.length, 0);
  assert.equal(plain.getKeywords(), undefined, "no credit when no emoji was drawn");
});

test("a journal with emojis renders them as pictures", async () => {
  const { renderJournalPdf } = require("./journalPdfReportBuilder");
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const fontItalic = await pdf.embedFont(StandardFonts.HelveticaOblique);
  const merged = {
    dayTitle: "Climbing day 🧗",
    storylines: [{ author: "Marwan Diab", headline: "David's big climb 🧗", story: ["David went climbing 🧗 and finished the routes he'd been trying 💪."], highs: ["Sent it 🎯"], struggles: [], activities: [], notes: [{ time: "7:10 PM EDT", text: "So proud ❤️", photos: [] }], photos: [] }],
    orphanPhotos: [],
    sharedThread: "",
    closingNote: "Good night 🌙",
  };
  const { fetchImpl } = fakeFetch();
  const embedded = await prepareEmojiImages(pdf, [merged], { fetchImpl });
  assert.equal(embedded, 5);
  const { PDFPage } = require("pdf-lib");
  const realDrawImage = PDFPage.prototype.drawImage;
  let drawn = 0;
  PDFPage.prototype.drawImage = function (...args) { drawn += 1; return realDrawImage.apply(this, args); };
  try {
  await renderJournalPdf({
    pdf, font, fontBold, fontItalic,
    storageBucket: { file: () => ({ download: async () => { throw new Error("not found"); } }) },
    titleStr: "Daily Journal - Thu, Sep 24, 2026",
    footerBrand: "Gridline",
    coverMeta: { titleMain: "Daily Journal", titleDate: "Thu, Sep 24, 2026", grid: [] },
    merged,
    logger: { warn() {} },
    runId: "test",
  });
  } finally {
    PDFPage.prototype.drawImage = realDrawImage;
  }
  assert.ok(drawn >= 6, `each emoji is drawn where it's used (${drawn} drawn)`);
  const bytes = Buffer.from(await pdf.save({ useObjectStreams: false })).toString("latin1");
  assert.ok((bytes.match(/\/Subtype \/Image/g) || []).length >= 5, "each emoji picture is in the PDF");
});
