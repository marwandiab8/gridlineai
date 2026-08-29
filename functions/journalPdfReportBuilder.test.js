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
