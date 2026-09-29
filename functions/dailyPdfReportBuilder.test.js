const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  wrapToLines,
  selectRemainingSitePhotos,
  selectRemainingJournalPhotos,
  buildManpowerRowsWithTotal,
  shouldRenderWorkSummary,
  shouldRenderProjectNotes,
} = require("./dailyPdfReportBuilder");

const mockFont = {
  widthOfTextAtSize(text) {
    return String(text || "").length;
  },
};

test("wrapToLines prefers clean token breaks before character-level splits", () => {
  const lines = wrapToLines("waterproofing/blindside-membrane completed", mockFont, 10, 15);
  assert.deepEqual(lines, ["waterproofing/", "blindside-", "membrane", "completed"]);
});

test("selectRemainingSitePhotos only removes photos that were actually rendered", () => {
  const photos = [
    { mediaId: "m1", captionText: "Rendered already" },
    { mediaId: "m2", captionText: "Held for fallback" },
    { mediaId: "m3", captionText: "Requested for report", includeInDailyReport: true },
  ];
  const remaining = selectRemainingSitePhotos(photos, new Set(["m1"]));
  assert.deepEqual(
    remaining.map((p) => p.mediaId),
    ["m2", "m3"]
  );
});

test("selectRemainingJournalPhotos returns every unrendered journal photo without a cap", () => {
  const photos = Array.from({ length: 30 }, (_, index) => ({
    mediaId: `m${index + 1}`,
    captionText: `Journal photo ${index + 1}`,
  }));
  const remaining = selectRemainingJournalPhotos(photos, new Set(["m1", "m2"]));

  assert.equal(remaining.length, 28);
  assert.equal(remaining[0].mediaId, "m3");
  assert.equal(remaining[27].mediaId, "m30");
});

test("buildManpowerRowsWithTotal appends a neutral total workers row", () => {
  const result = buildManpowerRowsWithTotal([
    ["Formwork", "Ali", "7", "West side"],
    ["Concrete", "—", "5 workers", "Slab edge"],
    ["Survey", "—", "—", "Layout only"],
  ]);
  assert.equal(result.totalWorkers, 12);
  assert.deepEqual(result.rows[result.rows.length - 1], [
    "TOTAL WORKERS",
    "â€”",
    "12",
    "Total workforce on site",
  ]);
});

test("shouldRenderWorkSummary skips duplicate work summary when executive summary exists", () => {
  assert.equal(
    shouldRenderWorkSummary(
      "Executive summary already covers the main field activities.",
      "Crew completed membrane prep and waterproofing.",
      "Crew completed membrane prep and waterproofing."
    ),
    false
  );
  assert.equal(
    shouldRenderWorkSummary(
      "",
      "Crew completed membrane prep and waterproofing.",
      "Trade bullets were empty."
    ),
    true
  );
});

test("shouldRenderProjectNotes only shows meaningful approved project notes", () => {
  assert.equal(shouldRenderProjectNotes(""), false);
  assert.equal(shouldRenderProjectNotes("  "), false);
  assert.equal(shouldRenderProjectNotes("â€”"), false);
  assert.equal(shouldRenderProjectNotes("Not specified"), false);
  assert.equal(shouldRenderProjectNotes("PPE required"), false);
  assert.equal(shouldRenderProjectNotes("PPE required only."), false);
  assert.equal(
    shouldRenderProjectNotes("Gate code 1842. Protect finished flooring at front entry."),
    true
  );
});

test("isPlaceholderText recognises the fillers used for empty sections", () => {
  const { isPlaceholderText } = require("./dailyPdfReportBuilder");
  for (const empty of ["", "  ", null, undefined, "â€”", "—", "--", "Not stated in field messages.", "Not stated in log entries for this report day.", "No open items flagged in log entries.", "None", "N/A"]) {
    assert.equal(isPlaceholderText(empty), true, `placeholder: ${JSON.stringify(empty)}`);
  }
  for (const real of ["ALC poured Line 4 wall", "3", "None of the pumps worked", "Not started - waiting on rebar"]) {
    assert.equal(isPlaceholderText(real), false, `real: ${real}`);
  }
});

test("hasRealRows is false for placeholder-only tables and true once any cell has content", () => {
  const { hasRealRows } = require("./dailyPdfReportBuilder");
  assert.equal(hasRealRows([["â€”", "â€”", "â€”", "Not stated in log entries for this report day."]]), false);
  assert.equal(hasRealRows([]), false);
  assert.equal(hasRealRows(undefined), false);
  assert.equal(hasRealRows([["â€”", "â€”", "Not stated in log entries."], ["Line 4 wall", "â€”", "Poured"]]), true);
});

test("stripAbsenceSentences drops 'not reported' filler but keeps real news", () => {
  const { stripAbsenceSentences } = require("./dailyPdfReportBuilder");
  assert.equal(
    stripAbsenceSentences("Overcast with a high of 20C. No curated field updates were provided for manpower or issues. Critical next actions were not stated in the field messages."),
    "Overcast with a high of 20C."
  );
  assert.equal(stripAbsenceSentences("ALC poured Line 4. The pour at Stair D was not completed due to rain."), "ALC poured Line 4. The pour at Stair D was not completed due to rain.");
  assert.equal(stripAbsenceSentences(""), "");
});

test("groupByCrew treats O'Connor and O’Connor as one crew, keeping first-seen order", () => {
  const { groupByCrew } = require("./dailyPdfReportBuilder");
  const groups = groupByCrew(
    [
      { company: "Legacy", trade: "Masonry", activity: "a" },
      { company: "O'Connor", trade: "Electrical", activity: "b" },
      { company: "O’Connor ", trade: "electrical", activity: "c" },
      { trade: "", company: "", activity: "d" },
    ],
    "Anyone"
  );
  assert.deepEqual(groups.map((g) => [g.label, g.list.length]), [["Legacy (Masonry)", 1], ["O'Connor (Electrical)", 2], ["Anyone", 1]]);
});

test("tidyManpowerTable drops unused Foreman/Notes columns and moves the Site Logistics note under the table", () => {
  const { tidyManpowerTable } = require("./dailyPdfReportBuilder");
  const shown = tidyManpowerTable(
    [
      ["Superior (Fire protection)", "-", "3", "Site Logistics"],
      ["Legacy (Masonry)", "-", "4", "Site Logistics"],
      ["TOTAL WORKERS", "â€”", "7", "Total workforce on site"],
    ],
    512
  );
  assert.deepEqual(shown.headers, ["Trade", "Workers"]);
  assert.deepEqual(shown.rows[0], ["Superior (Fire protection)", "3"]);
  assert.equal(shown.fromSiteLogistics, true);
  assert.equal(shown.colWidths.reduce((a, b) => a + b, 0), 512);

  const full = tidyManpowerTable([["Formwork", "Ali", "7", "West side (count from Site Logistics)"]], 512);
  assert.deepEqual(full.headers, ["Trade", "Foreman", "Workers", "Notes"]);
  assert.deepEqual(full.rows[0], ["Formwork", "Ali", "7", "West side"]);
  assert.equal(full.colWidths.reduce((a, b) => a + b, 0), 512);
});

test("shrinkPhotoForPdf resizes a full-size photo to at most 1400 px and keeps it a JPEG", async () => {
  const sharp = require("sharp");
  const { shrinkPhotoForPdf } = require("./dailyPdfReportBuilder");
  const big = await sharp({ create: { width: 4032, height: 3024, channels: 3, background: { r: 120, g: 90, b: 60 } } }).jpeg({ quality: 95 }).toBuffer();
  const small = await shrinkPhotoForPdf(big);
  const meta = await sharp(small).metadata();
  assert.equal(meta.format, "jpeg");
  assert.equal(Math.max(meta.width, meta.height), 1400);
  assert.ok(small.length < big.length);
  const notAnImage = Buffer.from("not an image");
  assert.equal(await shrinkPhotoForPdf(notAnImage), notAnImage, "falls back to the original bytes");
});
