const assert = require("node:assert/strict");
const test = require("node:test");

const {
  assertFirebaseProjectId,
  isIosShortcutLegacyLogEntry,
} = require("./policy");

test("accepts the canonical gridlineai Firebase project ID", () => {
  assert.equal(assertFirebaseProjectId("gridlineai"), "gridlineai");
});

test("rejects Firebase web app IDs as project IDs", () => {
  assert.throws(
    () => assertFirebaseProjectId("1:118761010772:web:6eee28ee3c09953de0dfc1"),
    /web app ID/
  );
});

test("rejects missing and malformed project IDs", () => {
  assert.throws(() => assertFirebaseProjectId(""), /required/);
  assert.throws(() => assertFirebaseProjectId("Not A Project"), /not a valid/);
});

test("identifies only iOS Shortcut journal duplicates with a stable event ID", () => {
  assert.equal(
    isIosShortcutLegacyLogEntry({ source: "ios_shortcuts", shortcutEventId: "shortcut-1" }),
    true
  );
  assert.equal(isIosShortcutLegacyLogEntry({ source: "ios_shortcuts" }), false);
  assert.equal(
    isIosShortcutLegacyLogEntry({ source: "manual", shortcutEventId: "shortcut-1" }),
    false
  );
});
