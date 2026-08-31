const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  formatDailyReportPdfFileName,
  buildDailyReportSequenceDocId,
  filterJournalMediaForReport,
  filterJournalLogEntriesForProject,
  mediaFallsOnEasternReportDay,
} = require("./dailyReportPdf");
const {
  buildJournalReportModel,
  formatJournalBundleForAi,
} = require("./dailyReportContent");

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

test("mediaFallsOnEasternReportDay keeps only createdAt inside report-day window", () => {
  const dayStart = new Date("2026-04-28T04:00:00.000Z"); // 00:00 ET
  const nextDayStart = new Date("2026-04-29T04:00:00.000Z");
  assert.equal(
    mediaFallsOnEasternReportDay(
      { createdAt: new Date("2026-04-28T16:30:00.000Z"), dateKey: "2026-04-28" },
      dayStart,
      nextDayStart,
      "2026-04-28"
    ),
    true
  );
  assert.equal(
    mediaFallsOnEasternReportDay(
      { createdAt: new Date("2026-04-27T23:30:00.000Z"), dateKey: "2026-04-28" },
      dayStart,
      nextDayStart,
      "2026-04-28"
    ),
    false
  );
});

test("two project journals in one scheduler cycle isolate timeline and AI inputs", () => {
  const createdAt = (iso) => ({
    toDate: () => new Date(iso),
    toMillis: () => new Date(iso).getTime(),
  });
  const rows = [
    {
      id: "home-entry",
      projectSlug: "home",
      projectId: "home",
      source: "ios_shortcuts",
      shortcutEventType: "arrive_home",
      dateKey: "2026-08-16",
      normalizedText: "HOME_ONLY_MARKER",
      includeInDailySummary: true,
      createdAt: createdAt("2026-08-16T14:00:00Z"),
    },
    {
      id: "dock-entry",
      projectSlug: "docksteader",
      projectId: "docksteader",
      source: "ios_shortcuts",
      shortcutEventType: "arrive_work",
      dateKey: "2026-08-16",
      normalizedText: "DOCK_ONLY_MARKER",
      includeInDailySummary: true,
      createdAt: createdAt("2026-08-16T15:00:00Z"),
    },
    {
      id: "dock-legacy-entry",
      projectId: "docksteader",
      source: "sms",
      dateKey: "2026-08-16",
      normalizedText: "DOCK_LEGACY_MARKER",
      includeInDailySummary: true,
      createdAt: createdAt("2026-08-16T16:00:00Z"),
    },
    {
      id: "unassigned-entry",
      source: "ios_shortcuts",
      shortcutEventType: "leave_home",
      dateKey: "2026-08-16",
      normalizedText: "UNASSIGNED_MARKER",
      includeInDailySummary: true,
      createdAt: createdAt("2026-08-16T17:00:00Z"),
    },
    {
      id: "contradictory-entry",
      projectSlug: "home",
      projectId: "docksteader",
      source: "ios_shortcuts",
      shortcutEventType: "arrive_location",
      dateKey: "2026-08-16",
      normalizedText: "CONTRADICTORY_MARKER",
      includeInDailySummary: true,
      createdAt: createdAt("2026-08-16T18:00:00Z"),
    },
    {
      id: "unassigned-sentinel-entry",
      projectSlug: "_unassigned",
      projectId: "_unassigned",
      source: "ios_shortcuts",
      shortcutEventType: "leave_location",
      dateKey: "2026-08-16",
      normalizedText: "UNASSIGNED_SENTINEL_MARKER",
      includeInDailySummary: true,
      createdAt: createdAt("2026-08-16T19:00:00Z"),
    },
  ];

  const homeRows = filterJournalLogEntriesForProject(rows, "home");
  const dockRows = filterJournalLogEntriesForProject(rows, "docksteader");
  assert.deepEqual(homeRows.map((row) => row.id), ["home-entry"]);
  assert.deepEqual(dockRows.map((row) => row.id), ["dock-entry", "dock-legacy-entry"]);

  const homeModel = buildJournalReportModel(homeRows, [], {
    dayStart: new Date("2026-08-16T12:00:00Z"),
    reportDateKey: "2026-08-16",
  });
  const dockModel = buildJournalReportModel(dockRows, [], {
    dayStart: new Date("2026-08-16T12:00:00Z"),
    reportDateKey: "2026-08-16",
  });
  const homeTimeline = homeModel.timeline.map((row) => row.text).join("\n");
  const dockTimeline = dockModel.timeline.map((row) => row.text).join("\n");
  const homeAiInput = formatJournalBundleForAi(homeRows, "2026-08-16");
  const dockAiInput = formatJournalBundleForAi(dockRows, "2026-08-16");

  assert.match(homeTimeline, /arrived (at )?home/i);
  assert.doesNotMatch(homeTimeline, /HOME_ONLY_MARKER/);
  assert.doesNotMatch(homeTimeline, /DOCK_ONLY_MARKER|DOCK_LEGACY_MARKER|UNASSIGNED_MARKER|UNASSIGNED_SENTINEL_MARKER|CONTRADICTORY_MARKER/);
  assert.match(dockTimeline, /arrived at work/i);
  assert.doesNotMatch(dockTimeline, /DOCK_ONLY_MARKER/);
  assert.match(dockTimeline, /DOCK_LEGACY_MARKER/);
  assert.doesNotMatch(dockTimeline, /HOME_ONLY_MARKER|UNASSIGNED_MARKER|UNASSIGNED_SENTINEL_MARKER|CONTRADICTORY_MARKER/);
  assert.match(homeAiInput, /arrived at home/i);
  assert.doesNotMatch(homeAiInput, /HOME_ONLY_MARKER/);
  assert.doesNotMatch(homeAiInput, /DOCK_ONLY_MARKER|DOCK_LEGACY_MARKER|UNASSIGNED_MARKER|UNASSIGNED_SENTINEL_MARKER|CONTRADICTORY_MARKER/);
  assert.match(dockAiInput, /arrived at work/i);
  assert.doesNotMatch(dockAiInput, /DOCK_ONLY_MARKER/);
  assert.match(dockAiInput, /DOCK_LEGACY_MARKER/);
  assert.doesNotMatch(dockAiInput, /HOME_ONLY_MARKER|UNASSIGNED_MARKER|UNASSIGNED_SENTINEL_MARKER|CONTRADICTORY_MARKER/);
});
