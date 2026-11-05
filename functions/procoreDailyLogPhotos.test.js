const test = require("node:test");
const assert = require("node:assert/strict");
const sharp = require("sharp");
const {
  normalizeProcorePhotoSources,
  procorePhotoSourceHash,
  selectProcorePhotos,
  processProcorePhotoQueue,
  waitForProcorePhotos,
  loadProcorePhotoSet,
  photoTimeLabel,
  procorePhotoCaption,
  describeProcorePhotoSet,
} = require("./procoreDailyLogPhotos");

const FILES = "https://storage.procore.com/api/v5/files/ca-central-1/prostore";
// One photo as the export sends it (Procore's image fields).
const raw = (i, extra = {}) => ({
  id: 1000 + i,
  taken_at: new Date(Date.UTC(2026, 10, 3, 12 + Math.floor(i / 6), (i % 6) * 10)).toISOString(),
  uploader: i % 2 ? "Pat Lee" : "Sam Roy",
  category: "Site Photos",
  starred: false,
  description: "",
  thumbnail_url: `${FILES}/thumb-${i}?companyId=1&projectId=2&sig=abc`,
  url: `${FILES}/full-${i}?companyId=1&projectId=2&sig=abc`,
  ...extra,
});

test("only Procore's own file links are kept, and each photo's fields are read", () => {
  const list = normalizeProcorePhotoSources([
    raw(0),
    raw(1, { url: "http://storage.procore.com/x", thumbnail_url: "https://evil.example.com/x" }),
    raw(2, { url: "https://storage.procore.com.evil.example/x", thumbnail_url: "javascript:alert(1)" }),
    null,
    { id: "", url: `${FILES}/x` },
  ]);
  assert.equal(list.length, 1);
  assert.deepEqual(list[0], {
    id: "1000",
    takenAt: "2026-11-03T12:00:00.000Z",
    uploader: "Sam Roy",
    category: "Site Photos",
    starred: false,
    description: "",
    thumbUrl: `${FILES}/thumb-0?companyId=1&projectId=2&sig=abc`,
    fullUrl: `${FILES}/full-0?companyId=1&projectId=2&sig=abc`,
  });
});

test("4 large spread through the day and up to 24 small from the rest; starred ones are the large ones", () => {
  const sources = normalizeProcorePhotoSources(Array.from({ length: 40 }, (_, i) => raw(i)).reverse());
  const pick = selectProcorePhotos(sources);
  assert.equal(pick.large.length, 4);
  assert.equal(pick.thumbs.length, 24);
  assert.equal(pick.starred, false);
  assert.deepEqual(pick.large.map((p) => p.id), ["1005", "1015", "1025", "1035"]);
  const all = [...pick.large, ...pick.thumbs].map((p) => p.id);
  assert.equal(new Set(all).size, 28, "no photo shown twice");
  assert.ok(pick.thumbs.every((p, i) => i === 0 || p.takenAt >= pick.thumbs[i - 1].takenAt), "small ones in the order taken");

  const starred = normalizeProcorePhotoSources([raw(0), raw(1, { starred: true }), raw(2), raw(3, { starred: true }), raw(4), raw(5), raw(6)]);
  const starredPick = selectProcorePhotos(starred);
  assert.deepEqual(starredPick.large.map((p) => p.id), ["1001", "1003"]);
  assert.equal(starredPick.thumbs.length, 5);
  assert.equal(starredPick.starred, true);

  const few = selectProcorePhotos(normalizeProcorePhotoSources([raw(0), raw(1), raw(2)]));
  assert.equal(few.large.length, 3);
  assert.equal(few.thumbs.length, 0);
});

test("the photo list's hash ignores order and changes with a star or a new photo", () => {
  const a = normalizeProcorePhotoSources([raw(0), raw(1)]);
  assert.equal(procorePhotoSourceHash(a), procorePhotoSourceHash(a.slice().reverse()));
  assert.notEqual(procorePhotoSourceHash(a), procorePhotoSourceHash(normalizeProcorePhotoSources([raw(0), raw(1, { starred: true })])));
  assert.notEqual(procorePhotoSourceHash(a), procorePhotoSourceHash(normalizeProcorePhotoSources([raw(0), raw(1), raw(2)])));
});

function fakeStore() {
  const files = new Map([["procoreDailyLogs/docksteader/2026-11-03/small-999.jpg", Buffer.from("old")]]);
  const bucket = {
    file: (name) => ({
      name,
      async save(buf, opts) { files.set(name, { buf, opts }); },
      async delete() { files.delete(name); },
      async download() { return [files.get(name).buf]; },
    }),
    async getFiles({ prefix }) { return [[...files.keys()].filter((k) => k.startsWith(prefix)).map((name) => bucket.file(name))]; },
  };
  const docs = new Map();
  const db = {
    collection: (col) => ({
      doc: (id) => ({
        async set(data) { docs.set(`${col}/${id}`, { ...(docs.get(`${col}/${id}`) || {}), ...data }); },
        async get() { const d = docs.get(`${col}/${id}`); return { exists: Boolean(d), get: (k) => (d || {})[k] }; },
      }),
    }),
  };
  return { files, bucket, docs, db };
}

test("processProcorePhotoQueue keeps large and small copies, records them and drops the queued links", async () => {
  const { files, bucket, docs, db } = fakeStore();
  const photo = await sharp({ create: { width: 2400, height: 1800, channels: 3, background: { r: 90, g: 120, b: 150 } } }).jpeg().toBuffer();
  const fetched = [];
  const fetchImpl = async (url) => {
    fetched.push(url);
    if (url.includes("full-1?")) return { ok: false, status: 403 };
    return { ok: true, arrayBuffer: async () => photo };
  };
  let deleted = false;
  const queue = { projectSlug: "docksteader", dateKey: "2026-11-03", sourceHash: "h1", sources: Array.from({ length: 10 }, (_, i) => raw(i)) };
  await processProcorePhotoQueue({
    db, FieldValue: { serverTimestamp: () => "NOW" }, bucket, logger: { info() {}, warn() {}, error() {} },
    ref: { delete: async () => { deleted = true; } }, queue, fetchImpl, sharp,
  });
  const set = docs.get("procoreDailyLogs/docksteader__2026-11-03").photoSet;
  assert.equal(set.status, "done");
  assert.equal(set.total, 10);
  assert.equal(set.sourceHash, "h1");
  assert.equal(set.large.length, 3, "the one Procore refused is skipped");
  assert.equal(set.failed, 1);
  assert.equal(set.thumbs.length, 6);
  assert.ok(fetched.filter((u) => u.includes("/full-")).length === 4 && fetched.filter((u) => u.includes("/thumb-")).length === 6);
  const large = await sharp(files.get(set.large[0].storagePath).buf).metadata();
  const small = await sharp(files.get(set.thumbs[0].storagePath).buf).metadata();
  assert.equal(Math.max(large.width, large.height), 1600);
  assert.equal(Math.max(small.width, small.height), 360);
  assert.equal(files.has("procoreDailyLogs/docksteader/2026-11-03/small-999.jpg"), false, "an earlier pick's photo is removed");
  assert.equal(deleted, true);

  const loaded = await loadProcorePhotoSet(db, "docksteader", "2026-11-03");
  assert.equal(loaded.large.length, 3);
  assert.equal(loaded.thumbs.length, 6);
  assert.equal(await loadProcorePhotoSet(db, "docksteader", "2026-11-04"), null);
});

test("waitForProcorePhotos waits until the day's photo list has been dealt with, up to a limit", async () => {
  let exists = true;
  const db = {
    collection: (col) => ({
      doc: (id) => ({
        async get() {
          assert.equal(`${col}/${id}`, "procorePhotoQueue/docksteader__2026-11-03");
          return { exists };
        },
      }),
    }),
  };
  const slept = [];
  const sleep = async (ms) => {
    slept.push(ms);
    if (slept.length === 3) exists = false; // the photos are copied and the queue entry deleted
  };
  assert.equal(await waitForProcorePhotos(db, "docksteader", "2026-11-03", { sleep }), true);
  assert.deepEqual(slept, [3000, 3000, 3000]);

  exists = true;
  slept.length = 0;
  const never = async (ms) => { slept.push(ms); };
  assert.equal(await waitForProcorePhotos(db, "docksteader", "2026-11-03", { timeoutMs: 30000, intervalMs: 3000, sleep: never }), false);
  assert.equal(slept.length, 10);
});

test("the site log's wording for the photos", () => {
  assert.equal(photoTimeLabel("2026-11-03T21:34:56Z"), "4:34 PM");
  assert.equal(photoTimeLabel(""), "");
  assert.equal(procorePhotoCaption({ takenAt: "2026-11-03T21:34:56Z", uploader: "Pat Lee" }), "4:34 PM · Pat Lee");
  const four = Array(4).fill({});
  assert.equal(
    describeProcorePhotoSet({ total: 33, starred: false, large: four, thumbs: Array(24).fill({}) }),
    "33 photos in the Procore daily log: 4 shown large and 24 more small, picked through the day."
  );
  assert.equal(
    describeProcorePhotoSet({ total: 7, starred: true, large: [{}, {}], thumbs: Array(5).fill({}) }),
    "7 photos in the Procore daily log: 2 starred shown large and 5 more small."
  );
  assert.equal(describeProcorePhotoSet({ total: 1, starred: false, large: [{}], thumbs: [] }), "1 photo in the Procore daily log: 1 shown large.");
});
