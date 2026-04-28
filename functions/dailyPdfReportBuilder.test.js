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
