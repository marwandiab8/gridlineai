const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  formatDailyReportPdfFileName,
  buildDailyReportSequenceDocId,
  filterJournalMediaForReport,
} = require("./dailyReportPdf");

test("formatDailyReportPdfFileName prefixes construction reports", () => {
  const fileName = formatDailyReportPdfFileName(new Date("2026-04-14T15:00:00Z"), 1);
  assert.equal(fileName, "Construction_Report_Tuesday_April_14_2026_001.pdf");
});

test("formatDailyReportPdfFileName zero pads later sequence numbers", () => {
  const fileName = formatDailyReportPdfFileName(new Date("2026-04-14T15:00:00Z"), 12);
  assert.equal(fileName, "Construction_Report_Tuesday_April_14_2026_012.pdf");
});

test("formatDailyReportPdfFileName prefixes journals", () => {
  const fileName = formatDailyReportPdfFileName(
    new Date("2026-04-14T15:00:00Z"),
    2,
    "journal"
  );
  assert.equal(fileName, "Journal_Tuesday_April_14_2026_002.pdf");
});

test("buildDailyReportSequenceDocId stays stable per phone and report day", () => {
  assert.equal(
    buildDailyReportSequenceDocId("+14378712424", "2026-04-14"),
    "%2B14378712424__2026-04-14"
  );
});

test("filterJournalMediaForReport keeps linked media even if projectId was saved as _unassigned", () => {
  const curatedEntries = [{ id: "log-1" }];
  const mediaDocs = [
    {
      id: "media-1",
      projectId: "_unassigned",
      linkedLogEntryId: "log-1",
      storagePath: "projects/_unassigned/media/2026-04-18/sid/image-0.jpg",
    },
    {
      id: "media-2",
      projectId: "_unassigned",
      linkedLogEntryId: "other-log",
      storagePath: "projects/_unassigned/media/2026-04-18/sid/image-1.jpg",
    },
    {
      id: "media-3",
      projectId: "home",
      linkedLogEntryId: null,
      storagePath: "projects/home/media/2026-04-18/sid/image-2.jpg",
    },
  ];

  const filtered = filterJournalMediaForReport(mediaDocs, curatedEntries, "home");

  assert.deepEqual(
    filtered.map((row) => row.id),
    ["media-1", "media-3"]
  );
});

test("filterJournalMediaForReport accepts a Set of entry ids so linked photos survive strict journal line filtering", () => {
  const mediaDocs = [
    {
      id: "media-linked",
      projectId: "home",
      linkedLogEntryId: "log-orphan-timeline",
      storagePath: "projects/home/media/2026-04-18/sid/kitchen.jpg",
    },
  ];
  const entryIds = new Set(["log-orphan-timeline"]);
  const filtered = filterJournalMediaForReport(mediaDocs, entryIds, "home");
  assert.deepEqual(
    filtered.map((row) => row.id),
    ["media-linked"]
  );
});

test("filterJournalMediaForReport keeps journal media on the exact Eastern report date only", () => {
  const mediaDocs = [
    {
      id: "same-day-linked",
      dateKey: "2026-04-18",
      projectId: "home",
      linkedLogEntryId: "log-1",
      storagePath: "projects/home/media/2026-04-18/sid/same.jpg",
    },
    {
      id: "previous-day-linked",
      dateKey: "2026-04-17",
      projectId: "home",
      linkedLogEntryId: "log-1",
      storagePath: "projects/home/media/2026-04-17/sid/previous.jpg",
    },
    {
      id: "same-day-unlinked",
      dateKey: "2026-04-18",
      projectId: "home",
      linkedLogEntryId: null,
      storagePath: "projects/home/media/2026-04-18/sid/unlinked.jpg",
    },
  ];

  const filtered = filterJournalMediaForReport(mediaDocs, new Set(["log-1"]), "home", {
    dateKey: "2026-04-18",
  });

  assert.deepEqual(
    filtered.map((row) => row.id),
    ["same-day-linked", "same-day-unlinked"]
  );
});
