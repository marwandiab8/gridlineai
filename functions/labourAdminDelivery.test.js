const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  ADMIN_LABOUR_PDF_QUEUE_FAILURE_TEXT,
  dispatchAdministratorLabourPdfQueue,
  executeAdminLabourPdfFailureNotificationOnce,
  executeAdminLabourPdfDeliveryOnce,
  formatAdminLabourPdfAcknowledgement,
  formatAdminLabourPdfFailureMessage,
  formatAdminLabourPdfLinkMessage,
  persistAdminLabourPdfJob,
  shouldRetryAdminLabourPdfClaim,
  validatePersistedAdminLabourAggregation,
} = require("./labourAdminDelivery");
const {
  aggregateCanonicalLabour,
  buildLabourSmsRequestKey,
  normalizeProjectRegistry,
  normalizeWorkerRegistry,
  parseAdminLabourQuery,
  prepareAdminLabourQuery,
} = require("./labourAdminQuery");
const {
  claimAdminLabourPdfDelivery,
  completeLabourSmsQuery,
  createAdminLabourPdfQueueOnce,
  markAdminLabourPdfSending,
} = require("./labourAdminIdempotency");
const { generateAdminLabourReportPdf } = require("./labourAdminReportPdf");

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
    return { doc: (id) => new FakeRef(this, name, id) };
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

function fixture() {
  const query = {
    intent: "report",
    output: "pdf",
    projectSlug: "docksteader",
    projectName: "Docksteader",
    workerId: null,
    workerName: null,
    startKey: "2026-08-15",
    endKey: "2026-08-28",
    periodKind: "current_pay_period",
    periodLabel: "Current pay period",
  };
  const entries = [
    {
      id: "dock-one",
      reportDateKey: "2026-08-15",
      projectSlug: "docksteader",
      projectName: "Docksteader",
      workerId: "worker-one",
      workerName: "Worker One",
      minutesWorked: 510,
      valueSource: "minutesWorked",
    },
    {
      id: "dock-two",
      reportDateKey: "2026-08-16",
      projectSlug: "docksteader",
      projectName: "Docksteader",
      workerId: "worker-two",
      workerName: "Worker Two",
      minutesWorked: 480,
      valueSource: "minutesWorked",
    },
  ];
  const result = aggregateCanonicalLabour({
    canonical: { included: entries, excludedCount: 0, excludedReasons: {}, auditFlags: {} },
    request: query,
  });
  return { query, result };
}

test("failed acceptance command formats a clean readable acknowledgement and link", () => {
  const parsed = parseAdminLabourQuery(
    "Send me a PDF labour report for Docksteader for the current pay period."
  );
  const prepared = prepareAdminLabourQuery({
    parsed,
    projectRows: [{ id: "docksteader", name: "Docksteader", active: true }],
    workerRows: [],
    now: new Date("2026-08-18T16:59:14.000Z"),
  });
  assert.equal(prepared.status, "ready");
  assert.equal(
    formatAdminLabourPdfAcknowledgement(prepared.request),
    "Okay. Generating the administrator labour PDF for Docksteader, August 15–28, 2026. A protected link will follow shortly."
  );
  assert.equal(
    formatAdminLabourPdfLinkMessage(prepared.request, "https://example.test/protected"),
    "Docksteader labour report — August 15–28, 2026: https://example.test/protected"
  );
  assert.equal(
    formatAdminLabourPdfFailureMessage(prepared.request),
    "The Docksteader labour PDF could not be delivered. No labour records were changed. Please try again."
  );
  assert.doesNotMatch(formatAdminLabourPdfAcknowledgement(prepared.request), /adminsitrator|fo r|2026-08-/);
});

test("acknowledgement is unavailable until the durable queue write resolves", async () => {
  const { query, result } = fixture();
  let resolveQueue;
  let settled = false;
  const pending = persistAdminLabourPdfJob({
    queueWriter: () => new Promise((resolve) => { resolveQueue = resolve; }),
    requestKey: "a".repeat(40),
    payload: { query, aggregation: result },
  }).then((value) => {
    settled = true;
    return value;
  });
  await Promise.resolve();
  assert.equal(settled, false);
  resolveQueue({ created: true, ref: { id: "admin-job" } });
  const queued = await pending;
  assert.equal(settled, true);
  assert.match(queued.acknowledgement, /A protected link will follow shortly/);
});

test("failed queue writes reject and never return a future-link promise", async () => {
  const { query, result } = fixture();
  await assert.rejects(
    persistAdminLabourPdfJob({
      queueWriter: async () => { throw new Error("queue unavailable"); },
      requestKey: "b".repeat(40),
      payload: { query, aggregation: result },
    }),
    /queue unavailable/
  );
  assert.equal(
    ADMIN_LABOUR_PDF_QUEUE_FAILURE_TEXT,
    "I couldn’t queue the labour PDF. No report was sent. Please try again."
  );
});

test("administrator queue dispatch occurs before legacy delivery", async () => {
  const calls = [];
  const handled = await dispatchAdministratorLabourPdfQueue({
    snapshot: { data: () => ({ adminQuery: true }) },
    deliverAdministrator: async () => calls.push("administrator"),
  });
  assert.equal(handled, true);
  assert.deepEqual(calls, ["administrator"]);
  assert.equal(await dispatchAdministratorLabourPdfQueue({
    snapshot: { data: () => ({ adminQuery: false }) },
    deliverAdministrator: async () => calls.push("unexpected"),
  }), false);
  assert.deepEqual(calls, ["administrator"]);
});

test("an active processing lease remains retryable instead of silently completing", () => {
  assert.equal(shouldRetryAdminLabourPdfClaim({ claimed: false, status: "processing" }), true);
  assert.equal(shouldRetryAdminLabourPdfClaim({ claimed: false, status: "sent" }), false);
  assert.equal(shouldRetryAdminLabourPdfClaim({ claimed: true, status: "processing" }), false);
});

test("persisted aggregation integrity rejects Home crossover and changed totals", () => {
  const { query, result } = fixture();
  assert.equal(validatePersistedAdminLabourAggregation(query, result), result);
  const crossed = structuredClone(result);
  crossed.entries[0].projectSlug = "home";
  assert.throws(
    () => validatePersistedAdminLabourAggregation(query, crossed),
    /crossed the project boundary/
  );
  const changed = structuredClone(result);
  changed.totalMinutes += 60;
  assert.throws(
    () => validatePersistedAdminLabourAggregation(query, changed),
    /integrity validation/
  );
});

test("delivery is not marked sent until Twilio accepts the protected link", async () => {
  const { query, result } = fixture();
  let resolveProvider;
  let recorded = false;
  const pending = executeAdminLabourPdfDeliveryOnce({
    identity: {
      requestKey: "c".repeat(40),
      reportId: "admin-report",
      storagePath: `adminLabourReports/${"c".repeat(40)}.pdf`,
    },
    query,
    result,
    findReport: async () => ({
      type: "administratorLabourQuery",
      status: "ready",
      requestKey: "c".repeat(40),
      storagePath: `adminLabourReports/${"c".repeat(40)}.pdf`,
    }),
    beginReport: async () => assert.fail("ready report must not regenerate"),
    generateReport: async () => assert.fail("ready report must not regenerate"),
    finishReport: async () => assert.fail("ready report must not regenerate"),
    ensureAccessURL: async () => "https://example.test/protected",
    claimSend: async () => true,
    sendLinkMessage: () => new Promise((resolve) => { resolveProvider = resolve; }),
    recordSent: async () => { recorded = true; },
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(recorded, false);
  resolveProvider({ messageSid: "SMaccepted11111111111111111111111111" });
  await pending;
  assert.equal(recorded, true);
});

test("a retry reuses an existing private PDF after report finalization failed", async () => {
  const { query, result } = fixture();
  const identity = {
    requestKey: "d".repeat(40),
    reportId: "admin-report",
    storagePath: `adminLabourReports/${"d".repeat(40)}.pdf`,
  };
  let artifact = null;
  let report = null;
  let generatedCount = 0;
  let finishCount = 0;
  const run = () => executeAdminLabourPdfDeliveryOnce({
    identity,
    query,
    result,
    findReport: async () => report,
    findExistingArtifact: async () => artifact,
    beginReport: async () => {
      report = {
        type: "administratorLabourQuery",
        status: "generating",
        requestKey: identity.requestKey,
        storagePath: identity.storagePath,
      };
    },
    generateReport: async () => {
      generatedCount += 1;
      artifact = { storagePath: identity.storagePath, byteLength: 1234 };
      return artifact;
    },
    finishReport: async (generated) => {
      finishCount += 1;
      if (finishCount === 1) throw new Error("report finalize unavailable");
      report = { ...report, status: "ready", pdfByteLength: generated.byteLength };
      return report;
    },
    ensureAccessURL: async () => "https://example.test/protected",
    claimSend: async () => true,
    sendLinkMessage: async () => ({ messageSid: "SMaccepted22222222222222222222222222" }),
    recordSent: async () => {},
  });

  await assert.rejects(run(), /report finalize unavailable/);
  assert.equal(generatedCount, 1);
  await run();
  assert.equal(generatedCount, 1);
  assert.equal(finishCount, 2);
});

test("exhausted delivery produces one safe failure notification", async () => {
  const { query } = fixture();
  const sends = [];
  const records = [];
  const first = await executeAdminLabourPdfFailureNotificationOnce({
    query,
    sendFailureMessage: async ({ body }) => {
      sends.push(body);
      return { messageSid: "SMfailure11111111111111111111111111" };
    },
    recordFailureSent: async (record) => records.push(record),
  });
  assert.equal(first.status, "failed_notified");
  assert.deepEqual(sends, [
    "The Docksteader labour PDF could not be delivered. No labour records were changed. Please try again.",
  ]);
  assert.equal(records.length, 1);
});

test("end-to-end fake pipeline creates one private PDF, grant, and accepted link", async () => {
  const command = "Send me a PDF labour report for Docksteader for the current pay period.";
  const parsed = parseAdminLabourQuery(command);
  const prepared = prepareAdminLabourQuery({
    parsed,
    projectRows: [{ id: "docksteader", name: "Docksteader", active: true }],
    workerRows: [{ id: "+15555550100", name: "Worker One", active: true }],
    now: new Date("2026-08-18T16:59:14.000Z"),
  });
  const projects = normalizeProjectRegistry([{ id: "docksteader", name: "Docksteader", active: true }]);
  const workers = normalizeWorkerRegistry([{ id: "+15555550100", name: "Worker One", active: true }]);
  assert.equal(projects[0].projectSlug, "docksteader");
  assert.equal(workers[0].workerId, "+15555550100");
  const result = aggregateCanonicalLabour({
    canonical: {
      included: [{
        id: "entry-one",
        reportDateKey: "2026-08-15",
        projectSlug: "docksteader",
        projectName: "Docksteader",
        workerId: "+15555550100",
        workerName: "Worker One",
        minutesWorked: 510,
        valueSource: "minutesWorked",
      }],
      excludedCount: 0,
      excludedReasons: {},
      auditFlags: {},
    },
    request: prepared.request,
  });
  const db = new FakeDb();
  const requestKey = buildLabourSmsRequestKey("SM11111111111111111111111111111111");
  const queued = await persistAdminLabourPdfJob({
    queueWriter: ({ requestKey: key, payload }) => createAdminLabourPdfQueueOnce({
      db,
      FieldValue,
      requestKey: key,
      payload,
    }),
    requestKey,
    payload: { phoneE164: "+15555550100", query: prepared.request, aggregation: result },
  });
  const requestRef = db.collection("labourSmsQueryRequests").doc(`labour-query-${requestKey}`);
  await completeLabourSmsQuery({
    ref: requestRef,
    FieldValue,
    command: "labour_admin_report_pdf",
    queueDocId: queued.ref.id,
  });
  assert.equal((await requestRef.get()).data().status, "queued");

  const reports = new Map();
  const pdfWrites = [];
  const grants = new Map();
  const sentMessages = [];
  const auditMessages = [];
  const bucket = {
    file(path) {
      return {
        async save(bytes, options) {
          pdfWrites.push({ path, bytes, options });
        },
      };
    },
  };
  const deliver = async (snapshot) => {
    const claim = await claimAdminLabourPdfDelivery({
      db,
      queueRef: snapshot.ref,
      FieldValue,
      nowMs: 1000,
    });
    if (!claim.claimed) return;
    const data = (await snapshot.ref.get()).data();
    const identity = {
      requestKey,
      queueDocId: queued.ref.id,
      reportId: queued.ref.id,
      outboundMessageDocId: `labour-query-${requestKey}`,
      storagePath: `adminLabourReports/${requestKey}.pdf`,
    };
    await executeAdminLabourPdfDeliveryOnce({
      identity,
      query: data.query,
      result: data.aggregation,
      findReport: async () => reports.get(identity.reportId) || null,
      beginReport: async () => reports.set(identity.reportId, {
        type: "administratorLabourQuery",
        status: "generating",
        requestKey,
        storagePath: identity.storagePath,
      }),
      generateReport: (aggregation) => generateAdminLabourReportPdf({
        result: aggregation,
        generatedAt: new Date("2026-08-18T17:00:00.000Z"),
        storageBucket: bucket,
        storagePath: identity.storagePath,
      }),
      finishReport: async (generated) => {
        const ready = {
          type: "administratorLabourQuery",
          status: "ready",
          requestKey,
          storagePath: generated.storagePath,
        };
        reports.set(identity.reportId, ready);
        return ready;
      },
      ensureAccessURL: async () => {
        if (!grants.has(identity.reportId)) {
          grants.set(identity.reportId, "https://example.test/protected-link");
        }
        return grants.get(identity.reportId);
      },
      claimSend: () => markAdminLabourPdfSending({ db, queueRef: snapshot.ref, FieldValue }),
      sendLinkMessage: async ({ body }) => {
        sentMessages.push(body);
        return { messageSid: "SMaccepted11111111111111111111111111" };
      },
      recordSent: async ({ messageSid }) => snapshot.ref.set({ status: "sent", twilioMessageSid: messageSid }, { merge: true }),
      recordAuditMessage: async (message) => auditMessages.push(message),
    });
  };
  const snapshot = await queued.ref.get();
  assert.equal(await dispatchAdministratorLabourPdfQueue({ snapshot, deliverAdministrator: deliver }), true);
  assert.equal(await dispatchAdministratorLabourPdfQueue({ snapshot: await queued.ref.get(), deliverAdministrator: deliver }), true);
  assert.equal(pdfWrites.length, 1);
  assert.equal(pdfWrites[0].path, `adminLabourReports/${requestKey}.pdf`);
  assert.equal(grants.size, 1);
  assert.equal(sentMessages.length, 1);
  assert.equal(auditMessages.length, 1);
  assert.match(sentMessages[0], /^Docksteader labour report — August 15–28, 2026:/);
  assert.equal((await queued.ref.get()).data().status, "sent");
});

test("same MessageSid is idempotent while a new MessageSid creates a new job", async () => {
  const db = new FakeDb();
  const { query, result } = fixture();
  const create = (sid) => {
    const requestKey = buildLabourSmsRequestKey(sid);
    return persistAdminLabourPdfJob({
      queueWriter: ({ requestKey: key, payload }) => createAdminLabourPdfQueueOnce({
        db,
        FieldValue,
        requestKey: key,
        payload,
      }),
      requestKey,
      payload: { query, aggregation: result },
    });
  };
  const first = await create("SM11111111111111111111111111111111");
  const duplicate = await create("SM11111111111111111111111111111111");
  const newRequest = await create("SM22222222222222222222222222222222");
  assert.equal(first.ref.id, duplicate.ref.id);
  assert.equal(duplicate.created, false);
  assert.notEqual(first.ref.id, newRequest.ref.id);
  assert.equal([...db.values.keys()].filter((path) => path.startsWith("labourPdfDeliveryQueue/")).length, 2);
});
