const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  claimAdminLabourPdfDelivery,
  claimLabourSmsQuery,
  completeLabourSmsQuery,
  createAdminLabourPdfQueueOnce,
  markLabourSmsRequestOutcome,
  markAdminLabourPdfSending,
  recordAdminLabourPdfFailure,
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
    aggregation: { persisted: true },
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
  assert.throws(
    () => validateAdminLabourPdfQueue(`admin-${key}`, {
      adminQuery: true,
      requestKey: key,
      query: { startKey: "2026-08-01", endKey: "2026-08-15" },
    }),
    /persisted aggregation is missing/
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
      aggregation: { persisted: true },
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

test("PDF request is marked queued only after its deterministic job exists", async () => {
  const db = new FakeDb();
  const key = requestKey();
  const requestClaim = await claimLabourSmsQuery({
    db,
    FieldValue,
    requestKey: key,
    senderIdentity: "+15555550100",
  });
  const queued = await createAdminLabourPdfQueueOnce({
    db,
    FieldValue,
    requestKey: key,
    payload: {
      query: { startKey: "2026-08-01", endKey: "2026-08-15" },
      aggregation: { persisted: true },
    },
  });
  const requestRef = requestClaim.ref;
  await completeLabourSmsQuery({
    ref: requestRef,
    FieldValue,
    command: "labour_admin_report_pdf",
    queueDocId: queued.ref.id,
  });
  const request = (await requestRef.get()).data();
  assert.equal(request.status, "queued");
  assert.equal(request.queueDocId, queued.ref.id);
  const duplicate = await claimLabourSmsQuery({
    db,
    FieldValue,
    requestKey: key,
    senderIdentity: "+15555550100",
  });
  assert.equal(duplicate.claimed, false);
  assert.equal(duplicate.status, "queued");
});

test("transient delivery failures retry and the exhausted attempt claims one failure notification", async () => {
  const db = new FakeDb();
  const key = requestKey();
  const queued = await createAdminLabourPdfQueueOnce({
    db,
    FieldValue,
    requestKey: key,
    payload: {
      query: { startKey: "2026-08-01", endKey: "2026-08-15" },
      aggregation: { persisted: true },
    },
  });
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const claim = await claimAdminLabourPdfDelivery({
      db,
      queueRef: queued.ref,
      FieldValue,
      nowMs: attempt * 1_000_000,
    });
    assert.equal(claim.claimed, true);
    const failure = await recordAdminLabourPdfFailure({
      db,
      queueRef: queued.ref,
      FieldValue,
      error: new Error("temporary rendering failure"),
      maxAttempts: 3,
    });
    if (attempt < 3) {
      assert.deepEqual(
        { retry: failure.retry, notify: failure.notify, status: failure.status },
        { retry: true, notify: false, status: "failed" }
      );
    } else {
      assert.deepEqual(
        { retry: failure.retry, notify: failure.notify, status: failure.status },
        { retry: false, notify: true, status: "failure_sending" }
      );
    }
  }
  const duplicate = await claimAdminLabourPdfDelivery({
    db,
    queueRef: queued.ref,
    FieldValue,
    nowMs: 9_000_000,
  });
  assert.equal(duplicate.claimed, false);
  assert.equal(duplicate.status, "failure_sending");
});

test("a provider failure after sending begins becomes delivery-unknown and is never resent", async () => {
  const db = new FakeDb();
  const key = requestKey();
  const queued = await createAdminLabourPdfQueueOnce({
    db,
    FieldValue,
    requestKey: key,
    payload: {
      query: { startKey: "2026-08-01", endKey: "2026-08-15" },
      aggregation: { persisted: true },
    },
  });
  await claimAdminLabourPdfDelivery({ db, queueRef: queued.ref, FieldValue, nowMs: 1_000 });
  await markAdminLabourPdfSending({ db, queueRef: queued.ref, FieldValue });
  const failure = await recordAdminLabourPdfFailure({
    db,
    queueRef: queued.ref,
    FieldValue,
    error: new Error("provider timeout"),
    maxAttempts: 3,
  });
  assert.equal(failure.status, "delivery_unknown");
  assert.equal(failure.retry, false);
  const duplicate = await claimAdminLabourPdfDelivery({
    db,
    queueRef: queued.ref,
    FieldValue,
    nowMs: 9_000_000,
  });
  assert.equal(duplicate.claimed, false);
  assert.equal(duplicate.status, "delivery_unknown");
});

test("request delivery outcomes are durable and terminal for duplicate webhooks", async () => {
  const db = new FakeDb();
  const key = requestKey();
  await markLabourSmsRequestOutcome({
    db,
    FieldValue,
    requestKey: key,
    status: "delivered",
    messageSid: "SMaccepted11111111111111111111111111",
  });
  const duplicate = await claimLabourSmsQuery({
    db,
    FieldValue,
    requestKey: key,
    senderIdentity: "+15555550100",
  });
  assert.equal(duplicate.claimed, false);
  assert.equal(duplicate.status, "delivered");
});

test("late inbound completion cannot downgrade an already delivered request", async () => {
  const db = new FakeDb();
  const key = requestKey();
  await markLabourSmsRequestOutcome({
    db,
    FieldValue,
    requestKey: key,
    status: "delivered",
    messageSid: "SMaccepted11111111111111111111111111",
  });
  const ref = db.collection("labourSmsQueryRequests").doc(`labour-query-${key}`);
  await completeLabourSmsQuery({
    db,
    ref,
    FieldValue,
    command: "labour_admin_report_pdf",
    queueDocId: `admin-${key}`,
  });
  assert.equal((await ref.get()).data().status, "delivered");
});
