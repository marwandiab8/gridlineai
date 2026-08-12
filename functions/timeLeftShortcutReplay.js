const { SHORTCUT_EVENT_RULES } = require("./timeLeftLifeEventMapper");

const DEFAULT_LOOKBACK_DAYS = 7;
const DEFAULT_SCAN_LIMIT = 500;
const DEFAULT_ATTEMPT_LIMIT = 100;
const RETRYABLE_STATUSES = new Set([
  "retryable_failure",
  "delivery_error",
  "delivery_not_configured",
]);
const TERMINAL_STATUSES = new Set([
  "delivered",
  "duplicate",
  "conflict",
  "authentication_failure",
  "unsupported_event_type",
  "permanent_failure",
]);

function normalizeText(value) {
  return String(value == null ? "" : value).trim().toLowerCase();
}

function isSupportedShortcutEvent(event) {
  return Boolean(event && SHORTCUT_EVENT_RULES[normalizeText(event.eventType)]);
}

function shouldRetryShortcutEvent(event, currentMode = "production") {
  if (!isSupportedShortcutEvent(event)) return false;
  const delivery = event && event.timeLeftDelivery;
  if (!delivery || typeof delivery !== "object") return true;
  const status = normalizeText(delivery.status);
  if (!status) return true;
  if (TERMINAL_STATUSES.has(status)) return false;
  if (status === "off") {
    return normalizeText(currentMode) === "production" && normalizeText(delivery.mode) !== "production";
  }
  return RETRYABLE_STATUSES.has(status);
}

function boundedInteger(value, fallback, max) {
  const numeric = Math.floor(Number(value));
  if (!Number.isFinite(numeric) || numeric < 1) return fallback;
  return Math.min(numeric, max);
}

function tallyResult(summary, result) {
  const status = normalizeText(result && result.status) || "delivery_error";
  if (status === "delivered") summary.delivered += 1;
  else if (status === "duplicate") summary.duplicate += 1;
  else if (status === "conflict") summary.conflicts += 1;
  else if (status === "authentication_failure") summary.authenticationFailures += 1;
  else if (TERMINAL_STATUSES.has(status)) summary.permanentFailures += 1;
  else summary.retryableFailures += 1;
}

async function replayRecentShortcutEvents({
  db,
  deliver,
  clock = () => new Date(),
  currentMode = "production",
  lookbackDays = DEFAULT_LOOKBACK_DAYS,
  scanLimit = DEFAULT_SCAN_LIMIT,
  attemptLimit = DEFAULT_ATTEMPT_LIMIT,
}) {
  if (!db || typeof db.collection !== "function" || typeof deliver !== "function") {
    throw new Error("Replay requires Firestore and a delivery function.");
  }
  const safeLookbackDays = boundedInteger(lookbackDays, DEFAULT_LOOKBACK_DAYS, 30);
  const safeScanLimit = boundedInteger(scanLimit, DEFAULT_SCAN_LIMIT, DEFAULT_SCAN_LIMIT);
  const safeAttemptLimit = boundedInteger(attemptLimit, DEFAULT_ATTEMPT_LIMIT, DEFAULT_ATTEMPT_LIMIT);
  const cutoffMs = clock().getTime() - safeLookbackDays * 24 * 60 * 60 * 1000;
  const snapshot = await db
    .collection("iosShortcutEvents")
    .where("eventAtMs", ">=", cutoffMs)
    .orderBy("eventAtMs", "desc")
    .limit(safeScanLimit)
    .get();
  const summary = {
    scanned: snapshot.size,
    eligible: 0,
    attempted: 0,
    delivered: 0,
    duplicate: 0,
    skipped: 0,
    conflicts: 0,
    authenticationFailures: 0,
    permanentFailures: 0,
    retryableFailures: 0,
  };

  for (const doc of snapshot.docs) {
    const event = doc.data() || {};
    if (!shouldRetryShortcutEvent(event, currentMode)) {
      summary.skipped += 1;
      continue;
    }
    summary.eligible += 1;
    if (summary.attempted >= safeAttemptLimit) continue;
    summary.attempted += 1;
    try {
      tallyResult(summary, await deliver({ event: { ...event, id: doc.id }, eventId: doc.id }));
    } catch (_) {
      summary.retryableFailures += 1;
    }
  }
  return summary;
}

module.exports = {
  DEFAULT_ATTEMPT_LIMIT,
  DEFAULT_LOOKBACK_DAYS,
  DEFAULT_SCAN_LIMIT,
  RETRYABLE_STATUSES,
  TERMINAL_STATUSES,
  isSupportedShortcutEvent,
  replayRecentShortcutEvents,
  shouldRetryShortcutEvent,
};
