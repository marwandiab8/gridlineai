const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  buildDailyReportModel,
  buildJournalReportModel,
  formatJournalBundleForAi,
  filterEntriesForJournalReport,
} = require("./dailyReportContent");

function ts(iso) {
  return {
    toDate() {
      return new Date(iso);
    },
  };
}

test("buildDailyReportModel keeps mixed field notes out of the weather section", () => {
  const reportDateKey = "2026-04-11";
  const model = buildDailyReportModel(
    [
      {
        id: "w1",
        createdAt: ts("2026-04-11T12:00:00Z"),
        category: "weather",
        summaryText: "Light rain through the morning, 12 C, winds 18 km/h from the west.",
        dailySummarySections: ["weather"],
      },
      {
        id: "w2",
        createdAt: ts("2026-04-11T15:00:00Z"),
        category: "journal",
        summaryText: "Weather delay in the morning, then crew completed waterproofing at the north wall.",
        dailySummarySections: ["weather", "workInProgress"],
      },
    ],
    [],
    { dayStart: new Date("2026-04-11T12:00:00Z"), reportDateKey }
  );

  assert.match(model.deterministic.weatherToday, /Light rain/i);
  assert.doesNotMatch(model.deterministic.weatherToday, /waterproofing/i);
  assert.equal(model.structured.weatherChunks.length, 1);
  assert.equal(model.structured.weatherChunks[0].entryId, "w1");
  assert.match(model.deterministic.workNarrativeBlock, /waterproofing/i);
});

test("buildDailyReportModel only promotes actionable items into the open items table", () => {
  const reportDateKey = "2026-04-11";
  const model = buildDailyReportModel(
    [
      {
        id: "i1",
        createdAt: ts("2026-04-11T13:00:00Z"),
        category: "deficiency",
        summaryText: "Deficiency noted at lobby door frame and repaired the same shift.",
        dailySummarySections: ["issues"],
      },
      {
        id: "i2",
        createdAt: ts("2026-04-11T16:00:00Z"),
        category: "deficiency",
        summaryText: "Open item: replace cracked tile at ensuite. Owner: tile crew. Status: open.",
        dailySummarySections: ["issues"],
        assignedTo: "tile crew",
        status: "open",
      },
    ],
    [],
    { dayStart: new Date("2026-04-11T12:00:00Z"), reportDateKey }
  );

  const rows = model.deterministic.openItemRows;
  assert.equal(rows.length, 1);
  assert.match(rows[0][1], /replace cracked tile/i);
  assert.equal(rows[0][2], "tile crew");
});

test("buildDailyReportModel uses iOS Shortcut event time instead of server createdAt", () => {
  const reportDateKey = "2026-07-09";
  const model = buildDailyReportModel(
    [
      {
        id: "shortcut-1",
        createdAt: ts("2026-07-09T19:02:12Z"),
        shortcutEventAtIso: "2026-07-09T12:30:00Z",
        source: "ios_shortcuts",
        category: "note",
        rawText: "Left work.",
        normalizedText: "Left work.",
        shortcutTimezone: "America/Toronto",
        dailySummarySections: ["dayLog"],
      },
    ],
    [],
    { dayStart: new Date("2026-07-09T12:00:00Z"), reportDateKey }
  );

  assert.equal(model.unifiedAppendix[0].time, "8:30 AM EDT");
});

test("buildDailyReportModel converts shortcut UTC timestamp to local event timezone in summer", () => {
  const reportDateKey = "2026-07-13";
  const model = buildDailyReportModel(
    [
      {
        id: "shortcut-1",
        createdAt: ts("2026-07-13T19:29:00Z"),
        shortcutEventAtIso: "2026-07-13T19:29:00Z",
        shortcutTimezone: "America/Toronto",
        source: "ios_shortcuts",
        category: "note",
        rawText: "Left work at end of day.",
        normalizedText: "Left work at end of day.",
        dailySummarySections: ["dayLog"],
      },
    ],
    [],
    { dayStart: new Date("2026-07-13T12:00:00Z"), reportDateKey }
  );

  assert.equal(model.unifiedAppendix[0].time, "3:29 PM EDT");
});

test("buildDailyReportModel rewrites shortcut Event time text to local time", () => {
  const reportDateKey = "2026-07-13";
  const model = buildDailyReportModel(
    [
      {
        id: "shortcut-1",
        createdAt: ts("2026-07-13T19:29:00Z"),
        shortcutEventAtIso: "2026-07-13T19:29:00Z",
        shortcutTimezone: "America/Toronto",
        source: "ios_shortcuts",
        category: "note",
        rawText: "iOS Shortcuts tracking event - Event time: 2026-07-13T19:29:00.000Z.",
        summaryText: "iOS Shortcuts tracking event - Event time: 2026-07-13T19:29:00.000Z.",
        normalizedText: "iOS Shortcuts tracking event - Event time: 2026-07-13T19:29:00.000Z.",
        dailySummarySections: ["dayLog"],
      },
    ],
    [],
    { dayStart: new Date("2026-07-13T12:00:00Z"), reportDateKey }
  );

  assert.match(model.unifiedAppendix[0].text, /Event time:\s*3:29 PM/);
  assert.doesNotMatch(model.unifiedAppendix[0].text, /19:29/);
});

test("buildDailyReportModel rewrites shortcut non-ISO Event time text", () => {
  const reportDateKey = "2026-07-13";
  const model = buildDailyReportModel(
    [
      {
        id: "shortcut-1",
        createdAt: ts("2026-07-13T19:29:00Z"),
        shortcutEventAtIso: "2026-07-13T19:29:00Z",
        shortcutTimezone: "America/Toronto",
        source: "ios_shortcuts",
        category: "note",
        rawText: "iOS Shortcuts tracking event - Event time: 7:29 PM EDT.",
        normalizedText: "iOS Shortcuts tracking event - Event time: 7:29 PM EDT.",
        dailySummarySections: ["dayLog"],
      },
    ],
    [],
    { dayStart: new Date("2026-07-13T12:00:00Z"), reportDateKey }
  );

  assert.match(model.unifiedAppendix[0].text, /Event time:\s*3:29 PM/i);
  assert.ok(!/7:29 PM EDT/i.test(model.unifiedAppendix[0].text));
});

test("buildDailyReportModel rewrites shortcut Time label to local time", () => {
  const reportDateKey = "2026-07-13";
  const model = buildDailyReportModel(
    [
      {
        id: "shortcut-1",
        createdAt: ts("2026-07-13T19:29:00Z"),
        shortcutEventAtIso: "2026-07-13T19:29:00Z",
        shortcutTimezone: "America/Toronto",
        source: "ios_shortcuts",
        category: "note",
        rawText: "iOS Shortcuts tracking event - Time: 7:29 PM EDT.",
        normalizedText: "iOS Shortcuts tracking event - Time: 7:29 PM EDT.",
        dailySummarySections: ["dayLog"],
      },
    ],
    [],
    { dayStart: new Date("2026-07-13T12:00:00Z"), reportDateKey }
  );

  assert.match(model.unifiedAppendix[0].text, /Time:\s*3:29 PM/i);
  assert.ok(!/7:29 PM EDT/i.test(model.unifiedAppendix[0].text));
});

test("buildDailyReportModel includes shortcut location label when not present in text", () => {
  const reportDateKey = "2026-07-13";
  const model = buildDailyReportModel(
    [
      {
        id: "shortcut-1",
        createdAt: ts("2026-07-13T19:29:00Z"),
        shortcutEventAtIso: "2026-07-13T19:29:00Z",
        shortcutTimezone: "America/Toronto",
        shortcutLocationLabel: "Home office",
        source: "ios_shortcuts",
        category: "note",
        rawText: "Arrived at site.",
        normalizedText: "Arrived at site.",
        dailySummarySections: ["dayLog"],
      },
    ],
    [],
    { dayStart: new Date("2026-07-13T12:00:00Z"), reportDateKey }
  );

  assert.match(model.unifiedAppendix[0].text, /Location:\s*Home office/i);
});

test("buildDailyReportModel converts shortcut UTC timestamp to local event timezone in winter", () => {
  const reportDateKey = "2026-01-15";
  const model = buildDailyReportModel(
    [
      {
        id: "shortcut-1",
        createdAt: ts("2026-01-15T19:29:00Z"),
        shortcutEventAtIso: "2026-01-15T19:29:00Z",
        shortcutTimezone: "America/Toronto",
        source: "ios_shortcuts",
        category: "note",
        rawText: "Left work in winter.",
        normalizedText: "Left work in winter.",
        dailySummarySections: ["dayLog"],
      },
    ],
    [],
    { dayStart: new Date("2026-01-15T12:00:00Z"), reportDateKey }
  );

  assert.equal(model.unifiedAppendix[0].time, "2:29 PM EST");
});

test("buildJournalReportModel preserves chronological timeline order and links photos to entries", () => {
  const reportDateKey = "2026-04-18";
  const model = buildJournalReportModel(
    [
      {
        id: "j2",
        createdAt: ts("2026-04-18T15:00:00Z"),
        authorLabel: "Manager B",
        category: "journal",
        rawText: "Second note entered later.",
        normalizedText: "Second note entered later.",
        dailySummarySections: ["journal"],
      },
      {
        id: "j1",
        createdAt: ts("2026-04-18T13:00:00Z"),
        authorLabel: "Manager A",
        category: "journal",
        rawText: "First note entered earlier.",
        normalizedText: "First note entered earlier.",
        dailySummarySections: ["journal"],
      },
    ],
    [
      {
        id: "m1",
        storagePath: "projects/home/media/2026-04-18/sid/image-0.jpg",
        linkedLogEntryId: "j1",
        captionText: "Front yard",
        createdAt: ts("2026-04-18T13:05:00Z"),
      },
    ],
    { dayStart: new Date("2026-04-18T12:00:00Z"), reportDateKey }
  );

  assert.deepEqual(
    model.timeline.map((row) => row.entryId),
    ["j1", "j2"]
  );
  assert.equal(model.timeline[0].authorLabel, "Manager A");
  assert.equal(model.timeline[0].photos.length, 1);
  assert.equal(model.timeline[0].photos[0].mediaId, "m1");
});

test("filterEntriesForJournalReport keeps a note when raw text is diary but another layer echoes report meta", () => {
  const reportDateKey = "2026-04-20";
  const entries = [
    {
      id: "e1",
      createdAt: ts("2026-04-20T12:00:00Z"),
      category: "journal",
      rawText: "Ashley picked up the cake for Saturday.",
      normalizedText: "Ashley picked up the cake for Saturday.",
      summaryText: "User requested to email the daily report PDF.",
      dailySummarySections: ["journal"],
      includeInDailySummary: true,
    },
  ];
  const kept = filterEntriesForJournalReport(entries, reportDateKey);
  assert.equal(kept.length, 1);
  assert.equal(kept[0].id, "e1");
});

test("filterEntriesForJournalReport keeps normal entries and photo placeholders but drops AI chat", () => {
  const reportDateKey = "2026-04-20";
  const entries = [
    {
      id: "photo",
      createdAt: ts("2026-04-20T12:00:00Z"),
      category: "journal",
      rawText: "Photo attachment",
      normalizedText: "Photo attachment",
      dailySummarySections: ["journal"],
      includeInDailySummary: true,
    },
    {
      id: "entry",
      createdAt: ts("2026-04-20T13:00:00Z"),
      category: "journal",
      rawText: "What a day at the park with the kids.",
      normalizedText: "What a day at the park with the kids.",
      dailySummarySections: ["journal"],
      includeInDailySummary: true,
    },
    {
      id: "chat",
      createdAt: ts("2026-04-20T14:00:00Z"),
      category: "journal",
      rawText: "Can you summarize my journal for today?",
      normalizedText: "Can you summarize my journal for today?",
      dailySummarySections: ["journal"],
      includeInDailySummary: true,
    },
  ];

  const kept = filterEntriesForJournalReport(entries, reportDateKey);

  assert.deepEqual(
    kept.map((entry) => entry.id),
    ["photo", "entry"]
  );
});

test("journal AI bundle carries contributor labels for co-authored journals", () => {
  const reportDateKey = "2026-04-24";
  const entries = [
    {
      id: "j1",
      createdAt: ts("2026-04-24T12:00:00Z"),
      authorPhone: "+14370000000",
      senderPhone: "+14370000000",
      authorLabel: "+14370000000",
      category: "journal",
      rawText: "I got to the gym early.",
      normalizedText: "I got to the gym early.",
      dailySummarySections: ["journal"],
    },
    {
      id: "j2",
      createdAt: ts("2026-04-24T14:00:00Z"),
      authorPhone: "+15190000000",
      senderPhone: "+15190000000",
      authorLabel: "Ashley Trower",
      category: "journal",
      rawText: "Bought groceries for the birthday.",
      normalizedText: "Bought groceries for the birthday.",
      dailySummarySections: ["journal"],
    },
    {
      id: "j3",
      createdAt: ts("2026-04-24T22:00:00Z"),
      authorPhone: "+14370000000",
      senderPhone: "+14370000000",
      authorLabel: "Marwan Diab",
      category: "journal",
      rawText: "Got home late from work.",
      normalizedText: "Got home late from work.",
      dailySummarySections: ["journal"],
    },
  ];
  const authorLabelsByIdentity = new Map([
    ["phone:14370000000", "Marwan Diab"],
    ["phone:15190000000", "Ashley Trower"],
  ]);
  const bundle = formatJournalBundleForAi(entries, reportDateKey, {
    authorLabelsByIdentity,
  });
  const model = buildJournalReportModel(entries, [], {
    dayStart: new Date("2026-04-24T12:00:00Z"),
    reportDateKey,
    authorLabelsByIdentity,
  });

  assert.match(bundle, /^Contributors: /m);
  assert.match(bundle, /\[author=Marwan Diab; category=journal\] I got to the gym early\./);
  assert.match(bundle, /\[author=Ashley Trower; category=journal\] Bought groceries/);
  assert.equal(model.isCoauthored, true);
  assert.equal(model.timeline[0].authorLabel, "Marwan Diab");
  assert.match(model.deterministic.overview, /Marwan Diab: I got to the gym early\./);
});

test("buildDailyReportModel carries author labels into source chunks", () => {
  const reportDateKey = "2026-04-18";
  const model = buildDailyReportModel(
    [
      {
        id: "w1",
        createdAt: ts("2026-04-18T13:00:00Z"),
        authorLabel: "Manager A",
        category: "progress",
        rawText: "Electrical rough-in started at level 2 corridor.",
        normalizedText: "Electrical rough-in started at level 2 corridor.",
        dailySummarySections: ["workInProgress"],
      },
      {
        id: "i1",
        createdAt: ts("2026-04-18T14:00:00Z"),
        authorLabel: "Manager B",
        category: "issue",
        rawText: "Issue: damaged frame at east entry requires replacement.",
        normalizedText: "Issue: damaged frame at east entry requires replacement.",
        dailySummarySections: ["issues"],
      },
    ],
    [],
    { dayStart: new Date("2026-04-18T12:00:00Z"), reportDateKey }
  );

  assert.equal(model.structured.workBlocks[0].rows[0].authorLabel, "Manager A");
  assert.equal(model.structured.issueChunks[0].authorLabel, "Manager B");
});
