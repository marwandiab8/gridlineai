const { test } = require("node:test");
const assert = require("node:assert/strict");
const { sortPhotosForRender, prepareMergedForPdf } = require("./dailyPdfCompact");

test("sortPhotosForRender prioritizes explicit include-in-report photos", () => {
  const ordered = sortPhotosForRender([
    { mediaId: "m1", captionText: "generic unlinked" },
    { mediaId: "m2", captionText: "linked photo", linkedLogEntryId: "e2" },
    { mediaId: "m3", captionText: "requested photo", includeInDailyReport: true },
  ]);
  assert.deepEqual(
    ordered.map((p) => p.mediaId),
    ["m3", "m2", "m1"]
  );
});

test("prepareMergedForPdf drops open-item intro that duplicates issue text", () => {
  const merged = {
    execSummary: "",
    issuesText: "Cracked tile remains open at the lobby entry and needs replacement.",
    workNarrative: "",
    concreteNarrative: "",
    openIntro: "Cracked tile remains open at the lobby entry and needs replacement.",
    openItemsTableRaw: "1|Replace cracked tile at lobby entry|Tile crew|Open",
  };
  prepareMergedForPdf(merged, { manpowerRows: [] });
  assert.equal(merged.openIntro, "");
});
