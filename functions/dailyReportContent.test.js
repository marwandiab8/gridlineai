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

test("journal tracking summaries combine locations, calculate durations, and hide technical events", () => {
  const entries = [
    { id: "arrive-work", source: "ios_shortcuts", shortcutEventType: "arrive_work", shortcutEventAtIso: "2026-08-30T10:57:00Z", authorLabel: "Marwan Diab", projectSlug: "home", includeInDailySummary: true },
    { id: "spotify", source: "ios_shortcuts", shortcutEventType: "start_spotify", shortcutEventAtIso: "2026-08-30T11:00:00Z", authorLabel: "Marwan Diab", projectSlug: "home", includeInDailySummary: true },
    { id: "leave-work", source: "ios_shortcuts", shortcutEventType: "leave_work", shortcutEventAtIso: "2026-08-30T20:12:00Z", authorLabel: "Marwan Diab", projectSlug: "home", includeInDailySummary: true },
  ];
  const model = buildJournalReportModel(entries, [], {
    reportDateKey: "2026-08-30",
    dayStart: new Date("2026-08-30T12:00:00Z"),
  });
  assert.equal(model.timeline.length, 1);
  assert.match(model.timeline[0].text, /was at work from 6:57 AM to 4:12 PM/);
  assert.match(model.timeline[0].text, /\(9 hours and 15 minutes\)/);
  assert.doesNotMatch(formatJournalBundleForAi(entries, "2026-08-30"), /spotify|tracking|event type|coordinates/i);
});

test("journal tracking summaries handle an incomplete event without inventing duration", () => {
  const model = buildJournalReportModel([
    { id: "arrive-home", source: "ios_shortcuts", shortcutEventType: "arrive_home", shortcutEventAtIso: "2026-08-30T21:20:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
  ], [], { reportDateKey: "2026-08-30" });
  assert.equal(model.timeline.length, 1);
  assert.match(model.timeline[0].text, /arrived at home at 5:20 PM/);
  assert.doesNotMatch(model.timeline[0].text, /spent|duration|remained|hours|minutes/i);
});

test("journal tracking summaries combine a gym session and its workout", () => {
  const entries = [
    { id: "gym-in", source: "ios_shortcuts", shortcutEventType: "arrive_gym", shortcutEventAtIso: "2026-08-30T10:00:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "workout-in", source: "ios_shortcuts", shortcutEventType: "start_workout", shortcutEventAtIso: "2026-08-30T10:08:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "workout-out", source: "ios_shortcuts", shortcutEventType: "finish_workout", shortcutEventAtIso: "2026-08-30T11:02:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "gym-out", source: "ios_shortcuts", shortcutEventType: "leave_gym", shortcutEventAtIso: "2026-08-30T11:10:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
  ];
  const model = buildJournalReportModel(entries, [], { reportDateKey: "2026-08-30" });
  assert.equal(model.timeline.length, 1);
  assert.match(model.timeline[0].text, /went to the gym.*completed a workout.*left/i);
  assert.match(model.timeline[0].text, /54 minutes/);
  assert.doesNotMatch(model.timeline[0].text, /start_workout|finish_workout|tracking/i);
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

test("journal tracking summaries use exact times and correct plurals", () => {
  const model = buildJournalReportModel([
    { id: "gym-in", source: "ios_shortcuts", shortcutEventType: "arrive_gym", shortcutEventAtIso: "2026-09-23T08:59:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "w-in", source: "ios_shortcuts", shortcutEventType: "start_workout", shortcutEventAtIso: "2026-09-23T09:10:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "w-out", source: "ios_shortcuts", shortcutEventType: "finish_workout", shortcutEventAtIso: "2026-09-23T10:11:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "gym-out", source: "ios_shortcuts", shortcutEventType: "leave_gym", shortcutEventAtIso: "2026-09-23T10:38:00Z", authorLabel: "Marwan Diab", includeInDailySummary: true },
  ], [], { reportDateKey: "2026-09-23" });
  assert.match(model.timeline[0].text, /lasting 1 hour and 1 minute,/);
  assert.doesNotMatch(model.timeline[0].text, /approximately|1 minutes/);
});

test("journal tracking summaries pair a place visit and a drive", () => {
  const model = buildJournalReportModel([
    { id: "d1", source: "ios_shortcuts", shortcutEventType: "start_drive", shortcutEventAtIso: "2026-09-23T17:15:00Z", shortcutLocationLabel: "Brampton", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "d2", source: "ios_shortcuts", shortcutEventType: "finish_drive", shortcutEventAtIso: "2026-09-23T17:50:00Z", shortcutLocationLabel: "Bolton", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "p1", source: "ios_shortcuts", shortcutEventType: "arrive_location", shortcutEventAtIso: "2026-09-23T17:52:00Z", shortcutLocationLabel: "Quick Oil Change", authorLabel: "Marwan Diab", includeInDailySummary: true },
    { id: "p2", source: "ios_shortcuts", shortcutEventType: "leave_location", shortcutEventAtIso: "2026-09-23T18:38:00Z", shortcutLocationLabel: "quick oil change", authorLabel: "Marwan Diab", includeInDailySummary: true },
  ], [], { reportDateKey: "2026-09-23" });
  const texts = model.timeline.map((row) => row.text);
  assert.equal(texts.length, 2);
  assert.match(texts[0], /drove from Brampton to Bolton at 1:15 PM \(35 minutes\)/);
  assert.match(texts[1], /was at Quick Oil Change from 1:52 PM to 2:38 PM \(46 minutes\)/);
});

test("journal storylines keep each contributor's notes, activities, and photos separate", () => {
  const marwan = { authorLabel: "Marwan Diab", authorPhone: "+15195550101" };
  const ashley = { authorLabel: "Ashley Trower", authorPhone: "+15195550202" };
  const entries = [
    { id: "m-gym", source: "ios_shortcuts", shortcutEventType: "arrive_gym", shortcutEventAtIso: "2026-09-23T09:00:00Z", includeInDailySummary: true, ...marwan },
    { id: "m-note", createdAt: ts("2026-09-23T15:00:00Z"), category: "journal", rawText: "Pour got pushed again.", normalizedText: "Pour got pushed again.", ...marwan },
    { id: "a-note", createdAt: ts("2026-09-23T16:00:00Z"), category: "journal", rawText: "Long shift at the clinic.", normalizedText: "Long shift at the clinic.", ...ashley },
    { id: "a-dinner", createdAt: ts("2026-09-23T23:00:00Z"), category: "journal", rawText: "Made pasta.", normalizedText: "Made pasta.", ...ashley },
  ];
  const media = [
    { id: "linked", storagePath: "a.jpg", contentType: "image/jpeg", linkedLogEntryId: "a-dinner", captionText: "Pasta night", senderPhone: ashley.authorPhone },
    { id: "by-phone", storagePath: "b.jpg", contentType: "image/jpeg", captionText: "Golden hour", senderPhone: "+1 (519) 555-0202" },
    { id: "stranger", storagePath: "c.jpg", contentType: "image/jpeg", captionText: "Who sent this", senderPhone: "+14165559999" },
  ];
  const model = buildJournalReportModel(entries, media, { reportDateKey: "2026-09-23" });
  const [m, a] = model.storylines.lines;
  assert.equal(m.author, "Marwan Diab");
  assert.deepEqual(m.notes.map((note) => note.text), ["Pour got pushed again."]);
  assert.equal(m.activities.length, 1);
  assert.match(m.activities[0].text, /^Arrived at the gym at /);
  assert.equal(m.photos.length, 0);
  assert.equal(a.author, "Ashley Trower");
  assert.deepEqual(a.notes.map((note) => note.text), ["Long shift at the clinic.", "Made pasta."]);
  assert.deepEqual(a.notes[1].photos.map((photo) => photo.mediaId), ["linked"]);
  assert.deepEqual(a.photos.map((photo) => photo.mediaId), ["by-phone"]);
  assert.deepEqual(model.storylines.orphanPhotos.map((photo) => photo.mediaId), ["stranger"]);
});

test("journal AI bundle includes photo captions under the sender's name", () => {
  const entries = [
    { id: "a1", createdAt: ts("2026-09-23T16:00:00Z"), category: "journal", rawText: "Walked the trail.", normalizedText: "Walked the trail.", authorLabel: "Ashley Trower", authorPhone: "+15195550202" },
  ];
  const bundle = formatJournalBundleForAi(entries, "2026-09-23", {
    photos: [{ mediaId: "p", captionText: "Golden hour on the trail", senderPhone: "+15195550202" }],
  });
  assert.match(bundle, /\[photo\] \[author=Ashley Trower\] Golden hour on the trail/);
});
