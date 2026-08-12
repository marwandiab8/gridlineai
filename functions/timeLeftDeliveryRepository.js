const COL_SHORTCUT_EVENTS = "iosShortcutEvents";

const DEFAULT_DELIVERY_STATUS = "unknown";

function isObjectLike(value) {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

function normalizeString(value) {
  const text = String(value == null ? "" : value).trim();
  return text || null;
}

function normalizeBoolean(value) {
  return Boolean(value);
}

function nowClock() {
  return new Date();
}

function toSafeAttemptCount(value) {
  const count = Number(value);
  if (!Number.isFinite(count) || count < 0) return 0;
  return Math.floor(count);
}

function deliveredStatus(status) {
  return status === "delivered" || status === "duplicate";
}

function sanitizeDeliveryState(input = {}) {
  return {
    status: normalizeString(input.status) || DEFAULT_DELIVERY_STATUS,
    mode: normalizeString(input.mode),
    targetProjectId: normalizeString(input.targetProjectId),
    lifeEventId: normalizeString(input.lifeEventId),
    idempotencyKey: normalizeString(input.idempotencyKey),
    lastErrorCode: normalizeString(input.lastErrorCode),
    lastErrorSummary: normalizeString(input.lastErrorSummary),
    retryable: normalizeBoolean(input.retryable),
  };
}

async function upsertTimeLeftDeliveryState(db, FieldValue, eventId, input, options = {}) {
  if (!isObjectLike(db) || !FieldValue || !eventId || !isObjectLike(input)) {
    return { ok: false, error: "invalid_input" };
  }

  const now = typeof options.clock === "function" ? options.clock() : nowClock();
  const timestamp = FieldValue.serverTimestamp ? FieldValue.serverTimestamp() : now;

  let existingAttemptCount = 0;
  try {
    const existing = await db.collection(COL_SHORTCUT_EVENTS).doc(eventId).get();
    if (existing && existing.exists) {
      const existingPayload = existing.data() || {};
      existingAttemptCount = toSafeAttemptCount(existingPayload.timeLeftDelivery?.attemptCount);
    }
  } catch (_) {}

  const attemptCount = existingAttemptCount > 0 ? existingAttemptCount + 1 : 1;
  const sanitized = sanitizeDeliveryState(input);

  const state = {
    status: sanitized.status,
    mode: sanitized.mode,
    targetProjectId: sanitized.targetProjectId,
    lifeEventId: sanitized.lifeEventId,
    idempotencyKey: sanitized.idempotencyKey,
    attemptCount,
    lastAttemptAt: timestamp,
    deliveredAt: deliveredStatus(sanitized.status) ? timestamp : null,
    lastErrorCode: sanitized.lastErrorCode,
    lastErrorSummary: sanitized.lastErrorSummary,
    retryable: sanitized.retryable,
    updatedAt: timestamp,
  };

  try {
    await db.collection(COL_SHORTCUT_EVENTS).doc(eventId).set(
      {
        timeLeftDelivery: state,
      },
      { merge: true }
    );
    return { ok: true, attemptCount, state };
  } catch (error) {
    return { ok: false, error: "storage_error", message: error && error.message };
  }
}

function createTimeLeftDeliveryRepository({ db, FieldValue }) {
  return {
    upsertTimeLeftDeliveryState: (eventId, input, options = {}) =>
      upsertTimeLeftDeliveryState(db, FieldValue, eventId, input, options),
  };
}

module.exports = {
  COL_SHORTCUT_EVENTS,
  createTimeLeftDeliveryRepository,
  upsertTimeLeftDeliveryState,
  sanitizeDeliveryState,
  deliveredStatus,
  nowClock,
};
