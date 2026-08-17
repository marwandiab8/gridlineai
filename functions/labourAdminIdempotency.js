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
      if (data.status === "completed" || data.status === "sending" || activeLease) {
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

async function completeLabourSmsQuery({ ref, FieldValue, command, queueDocId = null }) {
  if (!ref) return;
  await ref.set({
    status: "completed",
    command: String(command || "labour_admin_query").trim(),
    queueDocId: queueDocId || null,
    completedAt: FieldValue.serverTimestamp(),
    processingLeaseUntilMs: null,
    updatedAt: FieldValue.serverTimestamp(),
  }, { merge: true });
}

async function failLabourSmsQuery({ ref, FieldValue, reason }) {
  if (!ref) return;
  await ref.set({
    status: "failed",
    failureReason: String(reason || "query_failed").slice(0, 120),
    failedAt: FieldValue.serverTimestamp(),
    processingLeaseUntilMs: null,
    updatedAt: FieldValue.serverTimestamp(),
  }, { merge: true });
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
  return { identity, requestKey, query };
}

async function claimAdminLabourPdfDelivery({ db, queueRef, nowMs = Date.now(), FieldValue }) {
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(queueRef);
    if (!snap.exists) return { claimed: false, status: "missing" };
    const data = snap.data() || {};
    validateAdminLabourPdfQueue(snap.id, data);
    const status = String(data.status || "queued");
    if (["sent", "sending", "delivery_unknown"].includes(status)) {
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
  markAdminLabourPdfSending,
  validateAdminLabourPdfQueue,
};
