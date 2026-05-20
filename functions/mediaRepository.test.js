const { test } = require("node:test");
const assert = require("node:assert/strict");

test("saveOneInboundMedia stores backdated MMS under the intended report day", async (t) => {
  const mediaRepoPath = require.resolve("./mediaRepository");
  const twilioFetchPath = require.resolve("./twilioMediaFetch");

  delete require.cache[mediaRepoPath];
  delete require.cache[twilioFetchPath];

  const twilioFetch = require("./twilioMediaFetch");
  const originalFetch = twilioFetch.fetchTwilioMediaBuffer;
  twilioFetch.fetchTwilioMediaBuffer = async () => ({
    buffer: Buffer.from("fake-image-bytes"),
    contentType: "image/jpeg",
  });

  t.after(() => {
    twilioFetch.fetchTwilioMediaBuffer = originalFetch;
    delete require.cache[mediaRepoPath];
    delete require.cache[twilioFetchPath];
  });

  const { saveOneInboundMedia } = require("./mediaRepository");

  const added = [];
  let savedPath = null;
  const db = {
    collection(name) {
      assert.equal(name, "media");
      return {
        async add(doc) {
          added.push(doc);
          return { id: "media-1" };
        },
      };
    },
  };
  const bucket = {
    name: "test-bucket",
    file(storagePath) {
      savedPath = storagePath;
      return {
        async save() {},
      };
    },
  };
  const FieldValue = {
    serverTimestamp() {
      return { __type: "serverTimestamp" };
    },
  };
  const logger = {
    info() {},
    warn() {},
    error() {},
  };

  const result = await saveOneInboundMedia({
    db,
    bucket,
    FieldValue,
    accountSid: "AC12345678901234567890123456789012",
    authToken: "secret",
    mediaUrl: "https://example.test/image.jpg",
    contentType: "image/jpeg",
    mediaIndex: 0,
    messageSidTwilio: "MM1234567890",
    sourceMessageId: "msg-1",
    senderPhone: "+14370000000",
    projectSlug: "docksteader",
    reportDateKey: "2026-04-16",
    captionText: "log note (2026-04-16) photos - east side flooding",
    linkedLogEntryId: "log-1",
    uploadedByPhone: "+14370000000",
    logger,
    runId: "test-run",
  });

  assert.ok(result);
  assert.match(savedPath, /\/2026-04-16\//);
  assert.equal(added.length, 1);
  assert.equal(added[0].dateKey, "2026-04-16");
  assert.equal(added[0].reportDateKey, "2026-04-16");
  assert.equal(added[0].linkedLogEntryId, "log-1");
});

test("getMediaEffectiveDateKey prefers an inferred backdated caption date", () => {
  const { getMediaEffectiveDateKey } = require("./mediaRepository");

  const key = getMediaEffectiveDateKey({
    dateKey: "2026-04-17",
    captionText: "log note (2026-04-16): photos - east side flooding",
  });

  assert.equal(key, "2026-04-16");
});

test("loadRecentUncaptionedMediaGroups groups recent blank-caption media by source message", async () => {
  const { loadRecentUncaptionedMediaGroups } = require("./mediaRepository");
  const nowMs = Date.now();
  const docs = [
    {
      id: "m1",
      data: () => ({
        sourceMessageId: "msg-a",
        captionText: "",
        linkedLogEntryId: "log-1",
        createdAt: { toMillis: () => nowMs - 10_000 },
      }),
    },
    {
      id: "m2",
      data: () => ({
        sourceMessageId: "msg-a",
        captionText: "   ",
        linkedLogEntryId: "log-1",
        createdAt: { toMillis: () => nowMs - 12_000 },
      }),
    },
    {
      id: "m3",
      data: () => ({
        sourceMessageId: "msg-b",
        captionText: "already set",
        createdAt: { toMillis: () => nowMs - 8_000 },
      }),
    },
  ];
  const db = {
    collection(name) {
      assert.equal(name, "media");
      return {
        where(field, op, value) {
          assert.equal(field, "senderPhone");
          assert.equal(op, "==");
          assert.equal(value, "+14370000000");
          return {
            orderBy(sortField, sortDir) {
              assert.equal(sortField, "createdAt");
              assert.equal(sortDir, "desc");
              return {
                limit(count) {
                  assert.equal(count, 20);
                  return {
                    async get() {
                      return { empty: false, docs };
                    },
                  };
                },
              };
            },
          };
        },
      };
    },
  };

  const groups = await loadRecentUncaptionedMediaGroups({
    db,
    senderPhone: "+14370000000",
  });

  assert.deepEqual(groups, [
    {
      sourceMessageId: "msg-a",
      mediaIds: ["m1", "m2"],
      mediaCount: 2,
      linkedLogEntryIds: ["log-1"],
      latestCreatedAtMs: nowMs - 10_000,
    },
  ]);
});

test("applyCaptionToSourceMessageMedia updates all media in a batch", async () => {
  const { applyCaptionToSourceMessageMedia } = require("./mediaRepository");
  const updated = [];
  const docs = [
    {
      id: "m1",
      ref: {
        async update(patch) {
          updated.push(["m1", patch]);
        },
      },
    },
    {
      id: "m2",
      ref: {
        async update(patch) {
          updated.push(["m2", patch]);
        },
      },
    },
  ];
  const db = {
    collection(name) {
      assert.equal(name, "media");
      return {
        where(field, op, value) {
          assert.equal(field, "sourceMessageId");
          assert.equal(op, "==");
          assert.equal(value, "msg-a");
          return {
            async get() {
              return { empty: false, docs };
            },
          };
        },
      };
    },
  };
  const FieldValue = {
    serverTimestamp() {
      return { __type: "serverTimestamp" };
    },
  };

  const result = await applyCaptionToSourceMessageMedia({
    db,
    FieldValue,
    sourceMessageId: "msg-a",
    captionText: "East wall progress photo",
    captionSourceMessageId: "msg-caption",
  });

  assert.deepEqual(result, {
    updatedCount: 2,
    mediaIds: ["m1", "m2"],
  });
  assert.equal(updated.length, 2);
  assert.equal(updated[0][1].captionText, "East wall progress photo");
  assert.equal(updated[0][1].captionSourceMessageId, "msg-caption");
});
