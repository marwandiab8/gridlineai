const { buildAdminLabourArtifactIdentity, privacySafeReference } = require("./labourAdminQuery");

const COL_LABOUR_SMS_REQUESTS = "labourSmsQueryRequests";
const COL_LABOUR_PDF_QUEUE = "labourPdfDeliveryQueue";
const REQUEST_LEASE_MS = 5 * 60 * 1000;
const DELIVERY_LEASE_MS = 5 * 60 * 1000;

function assertRequestIdentity(requestKey) {
  const identity = buildAdminLabourArtifactIdentity(requestKey);
  if (!identity) throw new Error("Invalid labour query request key.");
  return identity;
}

async function claimLabourSmsQuery({ db, FieldValue, requestKey, senderIdentity, nowMs = Date.now() }) {
  const identity = assertRequestIdentity(requestKey);
  const ref = db.collection(COL_LABOUR_SMS_REQUESTS).doc(identity.requestDocId);
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (snap.exists) {
      const data = snap.data() || {};
      if (String(data.requestKey || "") !== requestKey) {
        throw new Error("Labour query idempotency identity mismatch.");
      }
      const activeLease =
        data.status === "processing" && Number(data.processingLeaseUntilMs || 0) > nowMs;
      if (
        [
          "completed",
          "queued",
          "sending",
          "delivered",
          "delivery_failed",
          "delivery_unknown",
        ].includes(data.status) ||
        activeLease
      ) {
        return { claimed: false, status: data.status || "unknown", identity, ref };
      }
    }
    tx.set(ref, {
      requestKey,
      senderRef: privacySafeReference(senderIdentity, "admin"),
      status: "processing",
      processingLeaseUntilMs: nowMs + REQUEST_LEASE_MS,
      attemptCount: snap.exists ? Number((snap.data() || {}).attemptCount || 0) + 1 : 1,
      createdAt: snap.exists ? (snap.data() || {}).createdAt || FieldValue.serverTimestamp() : FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    }, { merge: true });
    return { claimed: true, status: "processing", identity, ref };
  });
}

async function completeLabourSmsQuery({ db = null, ref, FieldValue, command, queueDocId = null }) {
  if (!ref) return;
  const queued = Boolean(queueDocId);
  const patch = {
    status: queued ? "queued" : "completed",
    command: String(command || "labour_admin_query").trim(),
    queueDocId: queueDocId || null,
    ...(queued
      ? { queuedAt: FieldValue.serverTimestamp() }
      : { completedAt: FieldValue.serverTimestamp() }),
    processingLeaseUntilMs: null,
    updatedAt: FieldValue.serverTimestamp(),
  };
  if (!queued || !db || typeof db.runTransaction !== "function") {
    await ref.set(patch, { merge: true });
    return;
  }
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const status = snap.exists ? String((snap.data() || {}).status || "") : "";
    if (["delivered", "delivery_failed", "delivery_unknown"].includes(status)) return;
    tx.set(ref, patch, { merge: true });
  });
}

async function failLabourSmsQuery({ db = null, ref, FieldValue, reason }) {
  if (!ref) return;
  const patch = {
    status: "failed",
    failureReason: String(reason || "query_failed").slice(0, 120),
    failedAt: FieldValue.serverTimestamp(),
    processingLeaseUntilMs: null,
    updatedAt: FieldValue.serverTimestamp(),
  };
  if (!db || typeof db.runTransaction !== "function") {
    await ref.set(patch, { merge: true });
    return;
  }
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const status = snap.exists ? String((snap.data() || {}).status || "") : "";
    if (["delivered", "delivery_failed", "delivery_unknown"].includes(status)) return;
    tx.set(ref, patch, { merge: true });
  });
}

async function createAdminLabourPdfQueueOnce({ db, FieldValue, requestKey, payload }) {
  const identity = assertRequestIdentity(requestKey);
  const ref = db.collection(COL_LABOUR_PDF_QUEUE).doc(identity.queueDocId);
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (snap.exists) {
      const data = snap.data() || {};
      if (String(data.requestKey || "") !== requestKey || data.adminQuery !== true) {
        throw new Error("Labour PDF queue identity mismatch.");
      }
      return { created: false, ref, identity, status: data.status || "unknown" };
    }
    tx.set(ref, {
      ...payload,
      adminQuery: true,
      requestKey,
      status: "queued",
      attemptCount: 0,
      lastError: null,
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    });
    return { created: true, ref, identity, status: "queued" };
  });
}

function validateAdminLabourPdfQueue(docId, data) {
  const record = data && typeof data === "object" ? data : {};
  const requestKey = String(record.requestKey || "").trim().toLowerCase();
  const identity = buildAdminLabourArtifactIdentity(requestKey);
  if (!identity || identity.queueDocId !== String(docId || "").trim() || record.adminQuery !== true) {
    throw new Error("Invalid administrator labour PDF queue identity.");
  }
  const query = record.query && typeof record.query === "object" ? record.query : null;
  if (!query) throw new Error("Administrator labour PDF query is missing.");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(query.startKey || "")) || !/^\d{4}-\d{2}-\d{2}$/.test(String(query.endKey || ""))) {
    throw new Error("Administrator labour PDF date range is invalid.");
  }
  const aggregation = record.aggregation && typeof record.aggregation === "object"
    ? record.aggregation
    : null;
  if (!aggregation) throw new Error("Administrator labour PDF persisted aggregation is missing.");
  return { identity, requestKey, query, aggregation };
}

async function claimAdminLabourPdfDelivery({ db, queueRef, nowMs = Date.now(), FieldValue }) {
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(queueRef);
    if (!snap.exists) return { claimed: false, status: "missing" };
    const data = snap.data() || {};
    validateAdminLabourPdfQueue(snap.id, data);
    const status = String(data.status || "queued");
    if ([
      "sent",
      "sending",
      "delivery_unknown",
      "failure_sending",
      "failed_notified",
      "failure_delivery_unknown",
    ].includes(status)) {
      return { claimed: false, status };
    }
    const leaseUntilMs = Number(data.processingLeaseUntilMs || 0);
    if (status === "processing" && leaseUntilMs > nowMs) {
      return { claimed: false, status: "processing" };
    }
    tx.set(queueRef, {
      status: "processing",
      attemptCount: Number(data.attemptCount || 0) + 1,
      processingLeaseUntilMs: nowMs + DELIVERY_LEASE_MS,
      processingAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
      lastError: null,
    }, { merge: true });
    return { claimed: true, status: "processing", data };
  });
}

async function recordAdminLabourPdfFailure({
  db,
  queueRef,
  FieldValue,
  error,
  maxAttempts,
}) {
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(queueRef);
    if (!snap.exists) return { retry: false, notify: false, status: "missing" };
    const data = snap.data() || {};
    const currentStatus = String(data.status || "");
    const terminalStatuses = new Set([
      "sent",
      "delivery_unknown",
      "failure_sending",
      "failed_notified",
      "failure_delivery_unknown",
    ]);
    if (terminalStatuses.has(currentStatus)) {
      return { retry: false, notify: false, status: currentStatus };
    }
    const attemptCount = Number(data.attemptCount || 0);
    const lastError = String(error && error.message || error || "delivery_failed").slice(0, 1000);
    if (currentStatus === "sending") {
      tx.set(queueRef, {
        status: "delivery_unknown",
        failedAt: FieldValue.serverTimestamp(),
        processingLeaseUntilMs: null,
        lastError,
        updatedAt: FieldValue.serverTimestamp(),
      }, { merge: true });
      return { retry: false, notify: false, status: "delivery_unknown", attemptCount };
    }
    if (attemptCount < Number(maxAttempts || 1)) {
      tx.set(queueRef, {
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        processingLeaseUntilMs: null,
        lastError,
        updatedAt: FieldValue.serverTimestamp(),
      }, { merge: true });
      return { retry: true, notify: false, status: "failed", attemptCount };
    }
    tx.set(queueRef, {
      status: "failure_sending",
      failedAt: FieldValue.serverTimestamp(),
      failureSendingAt: FieldValue.serverTimestamp(),
      processingLeaseUntilMs: null,
      lastError,
      updatedAt: FieldValue.serverTimestamp(),
    }, { merge: true });
    return { retry: false, notify: true, status: "failure_sending", attemptCount };
  });
}

async function markLabourSmsRequestOutcome({
  db,
  FieldValue,
  requestKey,
  status,
  queueDocId = null,
  messageSid = null,
  reason = null,
}) {
  const identity = assertRequestIdentity(requestKey);
  const allowed = new Set(["queued", "delivered", "delivery_failed", "delivery_unknown"]);
  if (!allowed.has(status)) throw new Error("Invalid labour query request outcome.");
  const ref = db.collection(COL_LABOUR_SMS_REQUESTS).doc(identity.requestDocId);
  await ref.set({
    requestKey,
    status,
    queueDocId: queueDocId || identity.queueDocId,
    ...(messageSid ? { deliveryMessageSid: messageSid } : {}),
    ...(reason ? { deliveryFailureReason: String(reason).slice(0, 120) } : {}),
    ...(status === "delivered" ? { deliveredAt: FieldValue.serverTimestamp() } : {}),
    ...(["delivery_failed", "delivery_unknown"].includes(status)
      ? { deliveryFailedAt: FieldValue.serverTimestamp() }
      : {}),
    updatedAt: FieldValue.serverTimestamp(),
  }, { merge: true });
  return ref;
}

async function markAdminLabourPdfSending({ db, queueRef, FieldValue }) {
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(queueRef);
    if (!snap.exists) return false;
    const data = snap.data() || {};
    if (data.status !== "processing") return false;
    tx.set(queueRef, {
      status: "sending",
      sendingAt: FieldValue.serverTimestamp(),
      processingLeaseUntilMs: null,
      updatedAt: FieldValue.serverTimestamp(),
    }, { merge: true });
    return true;
  });
}

module.exports = {
  COL_LABOUR_PDF_QUEUE,
  COL_LABOUR_SMS_REQUESTS,
  claimAdminLabourPdfDelivery,
  claimLabourSmsQuery,
  completeLabourSmsQuery,
  createAdminLabourPdfQueueOnce,
  failLabourSmsQuery,
  markLabourSmsRequestOutcome,
  markAdminLabourPdfSending,
  recordAdminLabourPdfFailure,
  validateAdminLabourPdfQueue,
};
