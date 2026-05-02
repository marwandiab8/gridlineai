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
