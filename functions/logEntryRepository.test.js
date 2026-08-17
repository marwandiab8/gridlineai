const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  writeLogEntry,
  getLogEntryEffectiveDateKey,
  loadLogEntriesForProjectDay,
  lineText,
} = require("./logEntryRepository");

test("writeLogEntry saves backdated entries against the intended report day", async () => {
  const added = [];
  const db = {
    collection(name) {
      assert.equal(name, "logEntries");
      return {
        async add(doc) {
          added.push(doc);
          return { id: "log-1" };
        },
      };
    },
  };
  const FieldValue = {
    serverTimestamp() {
      return { __type: "serverTimestamp" };
    },
  };

  const result = await writeLogEntry(db, FieldValue, {
    phoneE164: "+14370000000",
    projectSlug: "docksteader",
    reportDateKey: "2026-04-16",
    rawText: "log progress (2026-04-16) Dewatering complete",
    normalizedText: "Dewatering complete",
    category: "progress",
  });

  assert.equal(result.logEntryId, "log-1");
  assert.equal(added.length, 1);
  assert.equal(added[0].dateKey, "2026-04-16");
  assert.equal(added[0].reportDateKey, "2026-04-16");
  assert.equal(added[0].createdAt.__type, "serverTimestamp");
});

test("getLogEntryEffectiveDateKey recovers legacy backdated entries from raw text", () => {
  const key = getLogEntryEffectiveDateKey({
    dateKey: "2026-04-17",
    rawText:
      "log progress (2026-04-16) Coreydale installing culvert under the Dixie entrance",
  });

  assert.equal(key, "2026-04-16");
});

test("loadLogEntriesForProjectDay isolates Shortcut and legacy rows by canonical project ownership", async () => {
  const docs = {
    "dock-sms": {
      projectSlug: "docksteader",
      projectId: "docksteader",
      dateKey: "2026-07-09",
      source: "sms",
      createdAt: 1,
    },
    "dock-shortcut": {
      projectSlug: "docksteader",
      projectId: "docksteader",
      dateKey: "2026-07-09",
      source: "ios_shortcuts",
      shortcutEventType: "arrive_work",
      normalizedText: "DOCK_ONLY_MARKER",
      createdAt: 2,
    },
    "dock-legacy-id": {
      projectId: "docksteader",
      dateKey: "2026-07-09",
      source: "sms",
      normalizedText: "DOCK_LEGACY_MARKER",
      createdAt: 3,
    },
    "home-shortcut": {
      projectSlug: "home",
      projectId: "home",
      dateKey: "2026-07-09",
      source: "ios_shortcuts",
      shortcutEventType: "leave_work",
      normalizedText: "HOME_ONLY_MARKER",
      createdAt: 4,
    },
    "home-legacy-slug": {
      projectSlug: "home",
      dateKey: "2026-07-09",
      source: "sms",
      normalizedText: "HOME_LEGACY_MARKER",
      createdAt: 5,
    },
    unassigned: {
      projectSlug: null,
      projectId: null,
      dateKey: "2026-07-09",
      source: "ios_shortcuts",
      normalizedText: "UNASSIGNED_MARKER",
      createdAt: 6,
    },
    "unassigned-sentinel": {
      projectSlug: "_unassigned",
      projectId: "_unassigned",
      dateKey: "2026-07-09",
      source: "ios_shortcuts",
      normalizedText: "UNASSIGNED_SENTINEL_MARKER",
      createdAt: 6.5,
    },
    contradictory: {
      projectSlug: "docksteader",
      projectId: "home",
      dateKey: "2026-07-09",
      source: "ios_shortcuts",
      normalizedText: "CONTRADICTORY_MARKER",
      createdAt: 7,
    },
    malformed: {
      projectSlug: "!!!",
      projectId: null,
      dateKey: "2026-07-09",
      source: "ios_shortcuts",
      normalizedText: "MALFORMED_MARKER",
      createdAt: 8,
    },
  };
  const db = {
    collection(name) {
      assert.equal(name, "logEntries");
      const query = {
        filters: [],
        where(field, op, value) {
          this.filters.push({ field, op, value });
          return this;
        },
        orderBy() {
          return this;
        },
        limit() {
          return this;
        },
        async get() {
          const rows = Object.entries(docs)
            .filter(([, data]) =>
              this.filters.every((f) => {
                if (f.op === "==") return data[f.field] === f.value;
                if (f.op === ">=") return data[f.field] >= f.value;
                if (f.op === "<") return data[f.field] < f.value;
                return false;
              })
            )
            .map(([id, data]) => ({ id, data: () => data }));
          return { docs: rows };
        },
      };
      return query;
    },
  };

  const dockRows = await loadLogEntriesForProjectDay(db, "2026-07-09", "docksteader");
  const homeRows = await loadLogEntriesForProjectDay(db, "2026-07-09", "home");

  assert.deepEqual(
    dockRows.map((row) => row.id).sort(),
    ["dock-legacy-id", "dock-shortcut", "dock-sms"]
  );
  assert.deepEqual(
    homeRows.map((row) => row.id).sort(),
    ["home-legacy-slug", "home-shortcut"]
  );
  for (const rows of [dockRows, homeRows]) {
    const ids = new Set(rows.map((row) => row.id));
    assert.equal(ids.has("unassigned"), false);
    assert.equal(ids.has("unassigned-sentinel"), false);
    assert.equal(ids.has("contradictory"), false);
    assert.equal(ids.has("malformed"), false);
  }
});

test("lineText rewrites ios_shortcuts UTC Event time with shortcut timezone", () => {
  const line = lineText({
    source: "ios_shortcuts",
    shortcutEventAtIso: "2026-07-13T19:29:00.000Z",
    shortcutTimezone: "America/Toronto",
    normalizedText:
      "iOS Shortcuts tracking event - Event time: 2026-07-13T19:29:00.000Z. Left work.",
  });

  assert.match(line, /Event time:\s*3:29 PM/i);
  assert.ok(!/19:29/.test(line));
});

test("lineText rewrites ios_shortcuts Event time during EST with shortcut timezone", () => {
  const line = lineText({
    source: "ios_shortcuts",
    shortcutEventAtIso: "2026-01-15T19:29:00.000Z",
    shortcutTimezone: "America/Toronto",
    normalizedText:
      "iOS Shortcuts tracking event - Event time: 2026-01-15T19:29:00.000Z. Arrived home.",
  });

  assert.match(line, /Event time:\s*2:29 PM/i);
  assert.ok(!/19:29/.test(line));
});

test("lineText appends shortcut location when missing from message text", () => {
  const line = lineText({
    source: "ios_shortcuts",
    shortcutEventAtIso: "2026-07-13T19:29:00.000Z",
    shortcutTimezone: "America/Toronto",
    shortcutLocationLabel: "work bench",
    normalizedText: "iOS Shortcuts tracking event - Event time: 2026-07-13T19:29:00.000Z. Left work.",
  });

  assert.match(line, /Location:\s*work bench/i);
});

test("lineText ignores ios_shortcuts summaryText so deterministic local time is preserved", () => {
  const line = lineText({
    source: "ios_shortcuts",
    shortcutEventAtIso: "2026-07-13T19:29:00.000Z",
    shortcutTimezone: "America/Toronto",
    summaryText: "iOS Shortcuts tracking event - Event time: 7:29 PM EDT.",
    normalizedText:
      "iOS Shortcuts tracking event - Event time: 2026-07-13T19:29:00.000Z. Left work.",
  });

  assert.match(line, /Event time:\s*3:29 PM/i);
  assert.ok(!/7:29 PM EDT/i.test(line));
  assert.ok(!/19:29/.test(line));
});

test("lineText rewrites ios_shortcuts non-ISO Event time text to local time", () => {
  const line = lineText({
    source: "ios_shortcuts",
    shortcutEventAtIso: "2026-07-13T19:29:00.000Z",
    shortcutTimezone: "America/Toronto",
    normalizedText:
      "iOS Shortcuts tracking event - Event time: 7:29 PM EDT. Left work.",
  });

  assert.match(line, /Event time:\s*3:29 PM/i);
  assert.ok(!/7:29 PM EDT/i.test(line));
});

test("lineText rewrites ios_shortcuts Time label with UTC ISO text", () => {
  const line = lineText({
    source: "ios_shortcuts",
    shortcutEventAtIso: "2026-07-13T19:29:00.000Z",
    shortcutTimezone: "America/Toronto",
    normalizedText:
      "iOS Shortcuts tracking event - Time: 2026-07-13T19:29:00.000Z. Left work.",
  });

  assert.match(line, /Time:\s*3:29 PM/i);
  assert.ok(!/19:29/.test(line));
});

test("lineText rewrites ios_shortcuts non-ISO Time label to local time and keeps location", () => {
  const line = lineText({
    source: "ios_shortcuts",
    shortcutEventAtIso: "2026-07-13T19:29:00.000Z",
    shortcutTimezone: "America/Toronto",
    shortcutLocationLabel: "work bench",
    normalizedText:
      "iOS Shortcuts tracking event - Time: 7:29 PM EDT. Left work.",
  });

  assert.match(line, /Time:\s*3:29 PM/i);
  assert.match(line, /Location:\s*work bench/i);
  assert.ok(!/7:29 PM EDT/i.test(line));
});
