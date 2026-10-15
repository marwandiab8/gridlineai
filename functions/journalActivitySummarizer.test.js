const test = require("node:test");
const assert = require("node:assert/strict");

const { summarizeJournalTrackingEntries } = require("./journalActivitySummarizer");

const drive = (id, type, iso, location) => ({
  id,
  source: "ios_shortcuts",
  shortcutEventType: type,
  shortcutEventAtIso: iso,
  shortcutLocationLabel: location,
  authorLabel: "Marwan Diab",
});

const driveText = (from, to) =>
  summarizeJournalTrackingEntries([
    drive("a", "start_drive", "2026-10-14T08:36:00Z", from),
    drive("b", "finish_drive", "2026-10-14T09:04:00Z", to),
  ], { timeZone: "America/Toronto" }).map((e) => e.summaryText);

test("a drive names the places it knows and never shows GPS coordinates", () => {
  assert.deepEqual(driveText("home", "gym"), ["Marwan Diab drove from home to the gym at 4:36 AM (28 minutes)."]);
  assert.deepEqual(driveText("43.705882,-80.392595", "gym"), ["Marwan Diab drove to the gym at 4:36 AM (28 minutes)."]);
  assert.deepEqual(driveText("43.758893, -79.796553", "home"), ["Marwan Diab drove home at 4:36 AM (28 minutes)."]);
  assert.deepEqual(driveText("work", "43.75,-79.79"), ["Marwan Diab drove from work at 4:36 AM (28 minutes)."]);
  assert.deepEqual(driveText("43.70,-80.39", "43.75,-79.79"), ["Marwan Diab drove at 4:36 AM (28 minutes)."]);
  assert.deepEqual(driveText("Costco", "Costco"), ["Marwan Diab drove at 4:36 AM (28 minutes)."]);
});
