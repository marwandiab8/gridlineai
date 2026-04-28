const { test } = require("node:test");
const assert = require("node:assert/strict");
const { writeLogEntry, getLogEntryEffectiveDateKey } = require("./logEntryRepository");

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
