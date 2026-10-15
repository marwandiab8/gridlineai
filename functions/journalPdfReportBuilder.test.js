const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  isRawShortcutTrackingText,
  humanizeShortcutLocation,
  prepareJournalTimeline,
  filterJournalNarrativeItems,
  cleanJournalNarrativeText,
} = require("./journalPdfReportBuilder");

test("raw iOS Shortcut telemetry is recognized without hiding normal journal text", () => {
  assert.equal(
    isRawShortcutTrackingText(
      "iOS Shortcuts tracking event - Arrived at location. Event type: arrive_location. Timezone: America/Toronto. Device: iPhone. Coordinates: 43.7, -79.5."
    ),
    true
  );
  assert.equal(isRawShortcutTrackingText("Boys are playing VR zero latency"), false);
});

test("known location labels are presented as readable journal places", () => {
  assert.equal(humanizeShortcutLocation("Costco in Vaughn"), "Costco Vaughan");
  assert.equal(humanizeShortcutLocation("VR zero latency's"), "Zero Latency VR");
  assert.equal(humanizeShortcutLocation("Jack astors restaurant"), "Jack Astor’s");
  assert.equal(humanizeShortcutLocation("Sky zone in Vaughan"), "Sky Zone Vaughan");
});

test("journal timeline hides routine tracking and keeps concise meaningful locations", () => {
  const entries = new Map([
    ["spotify", { source: "ios_shortcuts", shortcutEventType: "start_spotify", shortcutLocationLabel: "Centre Wellington" }],
    ["home", { source: "ios_shortcuts", shortcutEventType: "arrive_home" }],
    ["costco", { source: "ios_shortcuts", shortcutEventType: "arrive_location", shortcutLocationLabel: "Costco in Vaughn" }],
    ["vr", { source: "ios_shortcuts", shortcutEventType: "arrive_location", shortcutLocationLabel: "VR zero latency's" }],
    ["note", { source: "sms" }],
  ]);
  const model = {
    entryById: entries,
    timeline: [
      { entryId: "spotify", time: "10:13 AM EDT", authorLabel: "Marwan", text: "raw spotify telemetry" },
      { entryId: "home", time: "1:02 PM EDT", authorLabel: "Marwan", text: "raw home telemetry" },
      { entryId: "costco", time: "3:52 PM EDT", authorLabel: "Marwan", text: "raw costco telemetry" },
      { entryId: "vr", time: "5:13 PM EDT", authorLabel: "Marwan", text: "raw vr telemetry" },
      { entryId: "note", time: "5:39 PM EDT", authorLabel: "Marwan Diab", text: "Boys are playing VR zero latency" },
    ],
  };

  const rows = prepareJournalTimeline(model);
  assert.equal(rows.length, 3);
  assert.deepEqual(
    rows.map((row) => ({ time: row.time, text: row.text, compactTracking: row.compactTracking })),
    [
      { time: "3:52 PM EDT", text: "Costco Vaughan", compactTracking: true },
      { time: "5:13 PM EDT", text: "Zero Latency VR", compactTracking: true },
      { time: "5:39 PM EDT", text: "Boys are playing VR zero latency", compactTracking: false },
    ]
  );
  assert.equal(rows.some((row) => /Coordinates|Device:|Timezone:|Event type:/i.test(row.text)), false);
});

test("duplicate tracking events appear only once in the visible timeline", () => {
  const entry = {
    source: "ios_shortcuts",
    shortcutEventType: "arrive_location",
    shortcutLocationLabel: "Jack astors restaurant",
  };
  const model = {
    entryById: new Map([["a", entry], ["b", entry]]),
    timeline: [
      { entryId: "a", time: "6:26 PM EDT", text: "first raw copy" },
      { entryId: "b", time: "6:26 PM EDT", text: "second raw copy" },
    ],
  };
  const rows = prepareJournalTimeline(model);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].text, "Jack Astor’s");
});

test("raw tracking is removed from summary sections and repeated narrative is deduplicated", () => {
  const seen = new Set(["boys are playing vr zero latency"]);
  const items = filterJournalNarrativeItems(
    [
      "iOS Shortcuts tracking event - Arrived at location. Event type: arrive_location.",
      "Boys are playing VR zero latency",
      "$280 dinner at Jack's astors and $165 at VR 280+165=445",
      "$280 dinner at Jack's astors and $165 at VR 280+165=445",
    ],
    seen
  );
  assert.deepEqual(items, ["$280 dinner at Jack's astors and $165 at VR 280+165=445"]);
  assert.equal(
    cleanJournalNarrativeText(
      "Started listening to Spotify. Event type: start_spotify. Timezone: America/Toronto."
    ),
    ""
  );
});

test("renderJournalPdf draws a two-person storytelling journal with captioned photos", async () => {
  const { PDFDocument, StandardFonts } = require("pdf-lib");
  const { renderJournalPdf } = require("./journalPdfReportBuilder");
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const fontItalic = await pdf.embedFont(StandardFonts.HelveticaOblique);
  const photo = { mediaId: "p1", storagePath: "missing.jpg", captionText: "Pasta night" };
  await renderJournalPdf({
    pdf, font, fontBold, fontItalic,
    storageBucket: { file: () => ({ download: async () => { throw new Error("not found"); } }) },
    titleStr: "Daily Journal - Wed, Sep 23, 2026",
    footerBrand: "Gridline",
    coverMeta: { titleMain: "Daily Journal", titleDate: "Wed, Sep 23, 2026", grid: [] },
    merged: {
      dayTitle: "Early iron, late pasta",
      storylines: [
        { author: "Marwan Diab", headline: "A long day", story: ["I lifted, then waited on a pump truck."], highs: ["PR"], struggles: ["Delays"], activities: [{ text: "Was at work from 7:06 AM to 1:13 PM." }], notes: [{ time: "3:30 PM EDT", text: "Pour got pushed again.", photos: [] }], photos: [], workouts: [require("./gymK2Workouts").normalizeGymWorkout("w1", { status: "final", routineName: "Chest", focus: ["Chest"], unit: "lb", startedAt: "2026-09-23T09:17:00Z", finishedAt: "2026-09-23T10:11:00Z", exercises: [{ name: "MTS Chest Press", firstEditTime: 1790155208501, lastEditTime: 1790155647568, sets: [{ weight: "40", reps: "20" }, { weight: "0", reps: "6 wide handle" }] }] })] },
        { author: "Ashley Trower", headline: "", story: [], highs: [], struggles: [], activities: [], notes: [{ time: "7:10 PM EDT", text: "Made pasta.", photos: [photo] }], photos: [] },
      ],
      orphanPhotos: [],
      sharedThread: "They met at dinner.",
      closingNote: "Good night.",
    },
    logger: { warn() {} },
    runId: "test",
  });
  assert.ok(pdf.getPageCount() >= 1);
  assert.ok((await pdf.save()).length > 1000);
});

test("renderJournalPdf never splits a workout across pages and keeps the WORKOUT label with it", async () => {
  const { PDFDocument, PDFPage, StandardFonts } = require("pdf-lib");
  const { renderJournalPdf } = require("./journalPdfReportBuilder");
  const { normalizeGymWorkout } = require("./gymK2Workouts");
  const exercises = ["Back Pulldown Single Arm", "Pull-ups", "T-Bar row", "MTS Front Pulldown", "Abdominal Crunch", "Seated Row"];
  const workout = normalizeGymWorkout("w1", {
    status: "final",
    routineName: "Back",
    focus: ["Back"],
    unit: "lb",
    startedAt: "2026-10-14T09:09:00Z",
    finishedAt: "2026-10-14T10:07:00Z",
    exercises: exercises.map((name, i) => ({ name, note: i === 3 ? "Seat elevation number 6" : "", sets: [{ weight: "140", reps: "5" }, { weight: "140", reps: "5" }, { weight: "140", reps: "4" }] })),
  });
  const realDrawText = PDFPage.prototype.drawText;
  try {
    // Every amount of story before the workout, so the workout lands at every height on the page.
    for (let paragraphs = 0; paragraphs <= 14; paragraphs += 1) {
      const pdf = await PDFDocument.create();
      const pageOf = new Map();
      PDFPage.prototype.drawText = function drawText(text, options) {
        if (!pageOf.has(text)) pageOf.set(text, pdf.getPages().indexOf(this));
        return realDrawText.call(this, text, options);
      };
      await renderJournalPdf({
        pdf,
        font: await pdf.embedFont(StandardFonts.Helvetica),
        fontBold: await pdf.embedFont(StandardFonts.HelveticaBold),
        fontItalic: await pdf.embedFont(StandardFonts.HelveticaOblique),
        storageBucket: null,
        titleStr: "Daily Journal - Wed, Oct 14, 2026",
        footerBrand: "Gridline",
        coverMeta: { titleMain: "Daily Journal" },
        merged: {
          storylines: [{
            author: "Marwan Diab",
            headline: "",
            story: Array.from({ length: paragraphs }, () => "A full paragraph of the day's story that runs across the page and onto a second and third line so it takes real room on the page before the workout."),
            highs: [], struggles: [], activities: [], notes: [], photos: [],
            workouts: [workout],
          }],
        },
        logger: { warn() {} },
        runId: "test",
      });
      const pages = ["WORKOUT", "Back", ...exercises].map((text) => pageOf.get(text));
      assert.ok(pages.every((p) => p === pages[0] && p >= 0), `with ${paragraphs} paragraphs the workout spread over pages ${pages.join(", ")}`);
    }
  } finally {
    PDFPage.prototype.drawText = realDrawText;
  }
});

test("renderJournalPdf keeps the closing note on the page of the journal's last photo", async () => {
  const { PDFDocument, PDFPage, StandardFonts } = require("pdf-lib");
  const sharp = require("sharp");
  const { renderJournalPdf } = require("./journalPdfReportBuilder");
  const jpeg = await sharp({ create: { width: 1200, height: 900, channels: 3, background: { r: 120, g: 90, b: 60 } } }).jpeg().toBuffer();
  const realDrawText = PDFPage.prototype.drawText;
  try {
    for (let paragraphs = 0; paragraphs <= 14; paragraphs += 1) {
      const pdf = await PDFDocument.create();
      const pageOf = new Map();
      PDFPage.prototype.drawText = function drawText(text, options) {
        if (!pageOf.has(text)) pageOf.set(text, pdf.getPages().indexOf(this));
        return realDrawText.call(this, text, options);
      };
      await renderJournalPdf({
        pdf,
        font: await pdf.embedFont(StandardFonts.Helvetica),
        fontBold: await pdf.embedFont(StandardFonts.HelveticaBold),
        fontItalic: await pdf.embedFont(StandardFonts.HelveticaOblique),
        storageBucket: { file: () => ({ download: async () => [jpeg] }) },
        titleStr: "Daily Journal - Wed, Oct 14, 2026",
        footerBrand: "Gridline",
        coverMeta: { titleMain: "Daily Journal" },
        merged: {
          storylines: [{
            author: "Marwan Diab",
            headline: "",
            story: Array.from({ length: paragraphs }, () => "A full paragraph of the day's story that runs across the page and onto a second and third line so it takes real room on the page before the photo."),
            highs: [], struggles: [], activities: [], workouts: [], photos: [],
            notes: [{ time: "11:27 AM EDT", text: "Chuck steak and 3 boiled eggs.", photos: [{ mediaId: "m1", storagePath: "meal.jpg", captionText: "" }] }],
          }],
          closingNote: "Good night.",
        },
        logger: { warn() {} },
        runId: "test",
      });
      const photoPage = pageOf.get("Chuck steak and 3 boiled eggs.");
      const closingPage = pageOf.get("Good night.");
      assert.ok(photoPage >= 0, "the photo caption was drawn");
      assert.equal(closingPage, photoPage, `with ${paragraphs} paragraphs the closing note went to page ${closingPage + 1}, the photo is on ${photoPage + 1}`);
    }
  } finally {
    PDFPage.prototype.drawText = realDrawText;
  }
});
