const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  claimAdminLabourPdfDelivery,
  claimLabourSmsQuery,
  completeLabourSmsQuery,
  createAdminLabourPdfQueueOnce,
  markAdminLabourPdfSending,
  validateAdminLabourPdfQueue,
} = require("./labourAdminIdempotency");
const { buildLabourSmsRequestKey } = require("./labourAdminQuery");

const FieldValue = {
  serverTimestamp() {
    return { serverTimestamp: true };
  },
};

class FakeSnapshot {
  constructor(ref, value) {
    this.ref = ref;
    this.id = ref.id;
    this.exists = value !== undefined;
    this.value = value;
  }
  data() {
    return this.value;
  }
}

class FakeRef {
  constructor(db, collectionName, id) {
    this.db = db;
    this.collectionName = collectionName;
    this.id = id;
    this.path = `${collectionName}/${id}`;
  }
  async get() {
    return new FakeSnapshot(this, this.db.values.get(this.path));
  }
  async set(value, options = {}) {
    const before = this.db.values.get(this.path) || {};
    this.db.values.set(this.path, options.merge ? { ...before, ...value } : { ...value });
  }
}

class FakeDb {
  constructor() {
    this.values = new Map();
    this.transactionTail = Promise.resolve();
  }
  collection(name) {
    return {
      doc: (id) => new FakeRef(this, name, id),
    };
  }
  runTransaction(callback) {
    const run = this.transactionTail.then(async () => callback({
      get: (ref) => ref.get(),
      set: (ref, value, options) => ref.set(value, options),
    }));
    this.transactionTail = run.catch(() => {});
    return run;
  }
}

function requestKey() {
  return buildLabourSmsRequestKey("SM11111111111111111111111111111111");
}

test("duplicate webhook claims with the same MessageSid are consumed once", async () => {
  const db = new FakeDb();
  const key = requestKey();
  const [first, second] = await Promise.all([
    claimLabourSmsQuery({ db, FieldValue, requestKey: key, senderIdentity: "+15555550100" }),
    claimLabourSmsQuery({ db, FieldValue, requestKey: key, senderIdentity: "+15555550100" }),
  ]);
  assert.deepEqual([first.claimed, second.claimed], [true, false]);
  await completeLabourSmsQuery({
    ref: first.ref,
    FieldValue,
    command: "labour_admin_totals",
  });
  const third = await claimLabourSmsQuery({
    db,
    FieldValue,
    requestKey: key,
    senderIdentity: "+15555550100",
  });
  assert.equal(third.claimed, false);
  assert.equal(third.status, "completed");
});

test("an abandoned inbound processing lease can be reclaimed safely", async () => {
  const db = new FakeDb();
  const key = requestKey();
  const first = await claimLabourSmsQuery({
    db,
    FieldValue,
    requestKey: key,
    senderIdentity: "+15555550100",
    nowMs: 1000,
  });
  const duringLease = await claimLabourSmsQuery({
    db,
    FieldValue,
    requestKey: key,
    senderIdentity: "+15555550100",
    nowMs: 2000,
  });
  const afterLease = await claimLabourSmsQuery({
    db,
    FieldValue,
    requestKey: key,
    senderIdentity: "+15555550100",
    nowMs: 1000 + 5 * 60 * 1000 + 1,
  });
  assert.equal(first.claimed, true);
  assert.equal(duringLease.claimed, false);
  assert.equal(afterLease.claimed, true);
});

test("duplicate PDF requests create one deterministic queue document", async () => {
  const db = new FakeDb();
  const key = requestKey();
  const payload = {
    phoneE164: "+15555550100",
    query: {
      startKey: "2026-08-01",
      endKey: "2026-08-15",
      projectSlug: "docksteader",
    },
  };
  const [first, second] = await Promise.all([
    createAdminLabourPdfQueueOnce({ db, FieldValue, requestKey: key, payload }),
    createAdminLabourPdfQueueOnce({ db, FieldValue, requestKey: key, payload }),
  ]);
  assert.equal(first.created, true);
  assert.equal(second.created, false);
  assert.equal(first.ref.id, second.ref.id);
  assert.equal([...db.values.keys()].filter((path) => path.startsWith("labourPdfDeliveryQueue/")).length, 1);
});

test("queue identity rejects mismatched request keys and missing structured queries", () => {
  const key = requestKey();
  assert.throws(
    () => validateAdminLabourPdfQueue("admin-wrong", {
      adminQuery: true,
      requestKey: key,
      query: { startKey: "2026-08-01", endKey: "2026-08-15" },
    }),
    /Invalid administrator labour PDF queue identity/
  );
  assert.throws(
    () => validateAdminLabourPdfQueue(`admin-${key}`, {
      adminQuery: true,
      requestKey: key,
    }),
    /query is missing/
  );
});

test("delivery processing and sending transitions are transactional and single-use", async () => {
  const db = new FakeDb();
  const key = requestKey();
  const queued = await createAdminLabourPdfQueueOnce({
    db,
    FieldValue,
    requestKey: key,
    payload: {
      phoneE164: "+15555550100",
      query: { startKey: "2026-08-01", endKey: "2026-08-15" },
    },
  });
  const first = await claimAdminLabourPdfDelivery({
    db,
    queueRef: queued.ref,
    FieldValue,
    nowMs: 1_000,
  });
  const concurrent = await claimAdminLabourPdfDelivery({
    db,
    queueRef: queued.ref,
    FieldValue,
    nowMs: 1_001,
  });
  assert.equal(first.claimed, true);
  assert.equal(concurrent.claimed, false);
  assert.equal(concurrent.status, "processing");
  assert.equal(await markAdminLabourPdfSending({ db, queueRef: queued.ref, FieldValue }), true);
  assert.equal(await markAdminLabourPdfSending({ db, queueRef: queued.ref, FieldValue }), false);
  const afterSending = await claimAdminLabourPdfDelivery({
    db,
    queueRef: queued.ref,
    FieldValue,
    nowMs: 1_000_000,
  });
  assert.equal(afterSending.claimed, false);
  assert.equal(afterSending.status, "sending");
});
