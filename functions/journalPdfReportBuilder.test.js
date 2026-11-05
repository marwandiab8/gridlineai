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

test("renderJournalPdf ends a project journal with its Procore daily log, repeating the table's heads on a new page", async () => {
  const { PDFDocument, PDFPage, StandardFonts } = require("pdf-lib");
  const { renderJournalPdf } = require("./journalPdfReportBuilder");
  const procoreDailyLog = {
    completedBy: "Pat Lee",
    completedOn: "Tue, November 3, 2026",
    photos: 104,
    weather: { hourly: [{ time: "6 AM", sky: "Partly cloudy", tempC: 9 }], lowC: 8, highC: 17, precipCm: 0, windAvgKmh: 4.4, gustsKmh: 20 },
    manpower: Array.from({ length: 40 }, (_, i) => ({
      company: `Company ${i + 1}`,
      workers: 2,
      manHours: 16,
      notes: "Continuing block wall construction at PH-3 along the north side, then grouting and cleaning down the scaffold.",
    })),
    totals: { workers: 80, manHours: 640 },
    visitors: [{ who: "", from: "12:00 PM", to: "1:00 PM", subject: "", details: "Owner's rep reviewed progress." }],
    inspections: [],
    sections: [{ type: "delivery_logs", title: "Deliveries", entries: ["7:30 AM · Rebar for the pad"] }],
  };
  const realDrawText = PDFPage.prototype.drawText;
  try {
    const pdf = await PDFDocument.create();
    const drawn = [];
    PDFPage.prototype.drawText = function drawText(text, options) {
      drawn.push({ text, page: pdf.getPages().indexOf(this) });
      return realDrawText.call(this, text, options);
    };
    await renderJournalPdf({
      pdf,
      font: await pdf.embedFont(StandardFonts.Helvetica),
      fontBold: await pdf.embedFont(StandardFonts.HelveticaBold),
      fontItalic: await pdf.embedFont(StandardFonts.HelveticaOblique),
      storageBucket: null,
      titleStr: "Daily Journal - Docksteader - Mon, Nov 2, 2026",
      footerBrand: "Gridline",
      coverMeta: { titleMain: "Daily Journal" },
      merged: { storylines: [], orphanPhotos: [], procoreDailyLog },
      logger: { warn() {} },
      runId: "test",
    });
    const pagesOf = (text) => drawn.filter((d) => d.text === text).map((d) => d.page);
    const texts = drawn.map((d) => d.text);
    assert.ok(texts.includes("No journal notes were captured for this day."));
    assert.ok(texts.includes("Procore daily log"));
    assert.ok(texts.includes("Completed by Pat Lee on Tue, November 3, 2026 · 80 workers, 640 hours · 104 photos in Procore"));
    assert.ok(texts.includes("ON SITE") && texts.includes("VISITOR") && texts.includes("DELIVERIES"));
    assert.ok(texts.includes("Company 1") && texts.includes("Company 40") && texts.includes("Total") && texts.includes("640"));
    // 40 rows run over pages: every page of the table starts with the column heads.
    const tablePages = [...new Set(drawn.filter((d) => /^Company \d+$/.test(d.text)).map((d) => d.page))];
    assert.ok(tablePages.length >= 2, `the table should run over pages, got ${tablePages.join(", ")}`);
    assert.deepEqual(pagesOf("Workers"), tablePages);
    assert.deepEqual(pagesOf("Hours"), tablePages);
  } finally {
    PDFPage.prototype.drawText = realDrawText;
  }
});

test("renderJournalPdf ends the Procore daily log with its photos: the large ones two to a row, then the small ones", async () => {
  const sharp = require("sharp");
  const { PDFDocument, PDFPage, StandardFonts } = require("pdf-lib");
  const { renderJournalPdf } = require("./journalPdfReportBuilder");
  const landscape = await sharp({ create: { width: 1600, height: 1200, channels: 3, background: { r: 90, g: 120, b: 150 } } }).jpeg().toBuffer();
  const portrait = await sharp({ create: { width: 270, height: 360, channels: 3, background: { r: 150, g: 120, b: 90 } } }).jpeg().toBuffer();
  const files = new Map();
  const at = (i) => new Date(Date.UTC(2026, 10, 3, 13 + i, 34)).toISOString(); // 8:34 AM in Toronto, then each hour
  const photo = (kind, i, uploader, buf) => {
    const p = { id: `${kind}${i}`, storagePath: `procoreDailyLogs/docksteader/2026-11-03/${kind}-${i}.jpg`, takenAt: at(i), uploader };
    if (buf) files.set(p.storagePath, buf);
    return p;
  };
  const large = [0, 1, 2].map((i) => photo("large", i, "Pat Lee", landscape));
  const thumbs = Array.from({ length: 8 }, (_, i) => photo("small", i, "Sam Roy", i === 7 ? null : portrait));
  const storageBucket = {
    file: (name) => ({
      async download() {
        if (!files.has(name)) throw new Error("No such object");
        return [files.get(name)];
      },
    }),
  };
  const procoreDailyLog = {
    completedBy: "Pat Lee",
    completedOn: "Tue, November 3, 2026",
    photos: 33,
    weather: null,
    manpower: [{ company: "Legacy Masonry", workers: 5, manHours: 50, notes: "Block wall on GL 6.1." }],
    totals: { workers: 5, manHours: 50 },
    visitors: [],
    inspections: [],
    sections: [],
  };
  const realDrawText = PDFPage.prototype.drawText;
  const realDrawImage = PDFPage.prototype.drawImage;
  try {
    const pdf = await PDFDocument.create();
    const drawn = [];
    const images = [];
    PDFPage.prototype.drawText = function drawText(text, options) {
      drawn.push(text);
      return realDrawText.call(this, text, options);
    };
    PDFPage.prototype.drawImage = function drawImage(image, options) {
      images.push({ page: pdf.getPages().indexOf(this), ...options });
      return realDrawImage.call(this, image, options);
    };
    await renderJournalPdf({
      pdf,
      font: await pdf.embedFont(StandardFonts.Helvetica),
      fontBold: await pdf.embedFont(StandardFonts.HelveticaBold),
      fontItalic: await pdf.embedFont(StandardFonts.HelveticaOblique),
      storageBucket,
      titleStr: "Daily Journal - Docksteader - Tue, Nov 3, 2026",
      footerBrand: "Gridline",
      coverMeta: { titleMain: "Daily Journal" },
      merged: { storylines: [], orphanPhotos: [], procoreDailyLog, procorePhotos: { total: 33, starred: false, large, thumbs } },
      logger: { warn() {} },
      runId: "test",
    });
    assert.ok(drawn.includes("PHOTOS"));
    assert.ok(drawn.includes("33 photos in the Procore daily log: 3 shown large and 8 more small, picked through the day."));
    assert.ok(drawn.includes("8:34 AM · Pat Lee") && drawn.includes("10:34 AM · Pat Lee"));
    assert.ok(drawn.includes("1:34 PM"), "a small photo's time");
    assert.equal(drawn.filter((t) => t === "Photo unavailable").length, 1, "the one missing from Storage");
    assert.equal(images.length, 10);
    const [first, second, third] = images;
    assert.ok(first.y === second.y && second.x > first.x + first.width, "the first two large photos side by side");
    assert.ok(third.y < first.y || third.page > first.page, "the third large photo on the next row");
    assert.ok(Math.abs(first.width - 245) < 1 && Math.abs(first.height - 183.75) < 1, "a large photo fills half the width");
    const small = images.slice(3);
    assert.ok(small.every((image) => image.height <= 62 && image.width < 60), "the small photos are thumbnails");
    assert.equal(new Set(small.slice(0, 6).map((image) => `${image.page}:${image.y}`)).size, 1, "six small photos to a row");
  } finally {
    PDFPage.prototype.drawText = realDrawText;
    PDFPage.prototype.drawImage = realDrawImage;
  }
});
