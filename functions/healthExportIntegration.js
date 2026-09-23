// Apple Health integration via the Health Auto Export app (https://www.healthyapps.dev), which
// runs entirely on the phone and exports on a schedule you configure - there is no way for a
// server to pull Health data directly, Apple only allows on-device access. See
// docs/health-auto-export-integration.md for the phone-side setup.
//
// Reuses the exact same integration token as the iOS Shortcuts / OwnTracks endpoints for auth,
// but does NOT reuse recordShortcutEvent's per-event pipeline: Health data arrives in batches
// (potentially dozens of records per export, and the app's own docs warn payloads can run to
// hundreds of MB) and each record already has a real start/end from Apple Health, so there is no
// arrive/leave pairing or per-event AI note-enrichment to do - records are mapped directly and
// delivered to TimeLeftToLive's batch ingestion endpoint.
const {
  extractShortcutToken,
  hashShortcutToken,
  checkShortcutRateLimit,
  findShortcutMemberByTokenHash,
} = require("./iosShortcutsIntegration");
const { parseHealthExportPayload } = require("./healthExportEventMapper");
const {
  readTimeLeftLifeEventConfigFromEnv,
  requireValidTimeLeftLifeEventConfig,
} = require("./timeLeftLifeEventConfig");
const { createTimeLeftLifeEventClient } = require("./timeLeftLifeEventClient");

const MAX_BATCH_SIZE = 100;

function jsonError(res, status, code, message) {
  res.status(status).json({ ok: false, error: code, message });
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function buildDeliveryClient({ env = process.env, logger } = {}) {
  try {
    const config = requireValidTimeLeftLifeEventConfig(readTimeLeftLifeEventConfigFromEnv(env));
    return createTimeLeftLifeEventClient(config, { logger });
  } catch (error) {
    if (logger && typeof logger.warn === "function") {
      logger.warn("healthExportEvents: TimeLeftToLive delivery disabled", {
        reason: error && error.message ? error.message : "invalid configuration",
      });
    }
    return null;
  }
}

async function handleHealthExportEventRequest({ db, req, res, logger, client } = {}) {
  const runId = `health-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  try {
    if (req.method !== "POST") {
      res.status(405).set("Allow", "POST").json({ ok: false, error: "method_not_allowed", message: "Use POST." });
      return;
    }

    const rawToken = extractShortcutToken(req);
    if (!rawToken) {
      jsonError(res, 401, "missing_token", "Missing integration token.");
      return;
    }
    const tokenHash = hashShortcutToken(rawToken);
    if (!checkShortcutRateLimit(tokenHash)) {
      jsonError(res, 429, "rate_limited", "Too many health-export requests. Try again shortly.");
      return;
    }
    const member = await findShortcutMemberByTokenHash(db, tokenHash);
    if (!member) {
      jsonError(res, 401, "invalid_token", "Invalid integration token.");
      return;
    }

    const { sleepEvents, workoutEvents, stepsEvents } = parseHealthExportPayload(req.body);
    const allEvents = [...sleepEvents, ...workoutEvents, ...stepsEvents];

    const counts = { sleep: sleepEvents.length, workouts: workoutEvents.length, steps: stepsEvents.length };
    if (allEvents.length === 0) {
      res.status(200).json({ ok: true, received: counts, delivered: 0, duplicates: 0, failed: 0 });
      return;
    }

    const deliveryClient = client || buildDeliveryClient({ logger });
    if (!deliveryClient) {
      // Configuration problem on our side, not the phone's - still acknowledge the export so
      // Health Auto Export doesn't treat it as a failure and keep retrying the same big payload.
      res.status(200).json({ ok: true, received: counts, delivered: 0, duplicates: 0, failed: 0, deliveryDisabled: true });
      return;
    }

    let delivered = 0;
    let duplicates = 0;
    let failed = 0;
    let deliveryDisabled = false;
    for (const batch of chunk(allEvents, MAX_BATCH_SIZE)) {
      const result = await deliveryClient.sendLifeEventsBatch(batch);
      if (result.status === "off") {
        // Delivery is deliberately turned off (e.g. local/staging), not broken - acknowledge
        // the export rather than reporting every record as a failure.
        deliveryDisabled = true;
        continue;
      }
      if (Array.isArray(result.results) && result.results.length) {
        for (const item of result.results) {
          if (item.status === "success" && !item.duplicate) delivered += 1;
          else if (item.status === "success" && item.duplicate) duplicates += 1;
          else failed += 1;
        }
      } else if (result.status !== "delivered") {
        failed += batch.length;
        if (logger && typeof logger.warn === "function") {
          logger.warn("healthExportEvents: batch delivery failed", { runId, status: result.status, summary: result.summary });
        }
      }
    }

    res.status(200).json({ ok: true, received: counts, delivered, duplicates, failed, ...(deliveryDisabled ? { deliveryDisabled: true } : {}) });
  } catch (err) {
    const status = Number(err && err.status) || 500;
    const code = String((err && err.code) || "health_export_failed");
    if (logger) {
      logger.error("healthExportEvents: failed", { runId, code, status, message: err && err.message, stack: err && err.stack });
    }
    jsonError(res, status >= 400 && status < 600 ? status : 500, code, status === 401 ? "Invalid integration token." : err.message || "Could not process this export.");
  }
}

module.exports = { handleHealthExportEventRequest, buildDeliveryClient };
