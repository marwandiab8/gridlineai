// OwnTracks integration: an alternative to the iOS Shortcuts location automations.
//
// OwnTracks (https://owntracks.org) is a dedicated location-tracking iOS app. Configured in
// "HTTP mode" with named circular Regions (Home/Work/Gym/etc.), it posts directly to this
// endpoint the moment it detects you crossing a region boundary - no Shortcuts automation,
// which iOS can silently delay or drop (especially "leaving" triggers), is involved.
//
// This module intentionally does not reimplement auth, parsing, dedupe, or TimeLeft delivery.
// It translates OwnTracks' own JSON shape into the same body shape the iOS Shortcuts endpoint
// already accepts, then hands off to that endpoint's own tested functions
// (parseShortcutEventPayload / recordShortcutEvent / findShortcutMemberByTokenHash), so both
// integrations share one auth model, one token, one dedupe path, and one TimeLeft delivery path.
// See docs/owntracks-integration.md for the phone-side setup.
const {
  extractShortcutToken,
  hashShortcutToken,
  checkShortcutRateLimit,
  findShortcutMemberByTokenHash,
  parseShortcutEventPayload,
  recordShortcutEvent,
} = require("./iosShortcutsIntegration");

/**
 * OwnTracks' iOS app has changed which auth fields it exposes across versions (custom HTTP
 * headers, HTTP Basic Auth username/password, or neither). Rather than betting the whole
 * integration on one of them, the token is accepted however it arrives:
 *   1. `Authorization: Bearer <token>` or `X-Gridline-Shortcut-Token: <token>` (same as Shortcuts).
 *   2. HTTP Basic Auth - the token is expected in the password field; the username can be
 *      anything (OwnTracks' Basic Auth username field cannot be left blank in every version).
 *   3. A `token` query-string parameter on the endpoint URL, for versions that expose neither.
 * This function never changes the Shortcuts endpoint's own `extractShortcutToken`, so that path
 * is untouched.
 */
function extractOwnTracksToken(req) {
  const shared = extractShortcutToken(req);
  if (shared) return shared;

  const authHeader = String((req.get && req.get("authorization")) || "").trim();
  const basicMatch = authHeader.match(/^Basic\s+(.+)$/i);
  if (basicMatch) {
    try {
      const decoded = Buffer.from(basicMatch[1].trim(), "base64").toString("utf8");
      const passwordPart = decoded.includes(":") ? decoded.slice(decoded.indexOf(":") + 1) : decoded;
      if (passwordPart.trim()) return passwordPart.trim();
    } catch (_) {
      // fall through to the query-string token
    }
  }

  const queryToken = req.query && req.query.token != null ? String(req.query.token).trim() : "";
  return queryToken;
}

// OwnTracks lets you name a Region anything. Only these canonical names map to a specific
// arrive_*/leave_* event that TimeLeftToLive's session pairing understands; everything else
// (e.g. a region named "Bells of Steel") becomes a generic arrive_location/leave_location event,
// exactly like an unrecognized Shortcuts location does.
const CANONICAL_REGION_EVENT_TYPES = {
  home: { enter: "arrive_home", leave: "leave_home" },
  work: { enter: "arrive_work", leave: "leave_work" },
  office: { enter: "arrive_work", leave: "leave_work" },
  gym: { enter: "arrive_gym", leave: "leave_gym" },
  fitness: { enter: "arrive_gym", leave: "leave_gym" },
};

function jsonError(res, status, code, message) {
  // OwnTracks only checks the HTTP status; the body is ignored on error, but keep it informative
  // for anyone debugging with curl.
  res.status(status).json({ ok: false, error: code, message });
}

/**
 * True for OwnTracks' regular location pings (`_type: "location"`), waypoint list dumps
 * (`_type: "waypoints"`), and anything else that is not a region-crossing event. These arrive
 * far more often than transitions and carry nothing this app tracks; they must be acknowledged
 * (200) without being treated as an error, or the app will keep retrying them.
 */
function isIgnorableOwnTracksPayload(payload) {
  return !payload || typeof payload !== "object" || payload._type !== "transition";
}

function normalizeRegionName(desc) {
  return String(desc || "")
    .trim()
    .toLowerCase();
}

/**
 * Maps one OwnTracks `_type: "transition"` message onto the body shape
 * `parseShortcutEventPayload` (from the iOS Shortcuts integration) already accepts, so every
 * downstream rule - supported event types, timezone/timestamp handling, coordinate validation,
 * project_slug resolution - is inherited rather than re-implemented.
 *
 * OwnTracks transition fields used: `desc` (the Region name you gave it), `event` ("enter" or
 * "leave"), `tst` (unix epoch seconds of the crossing), `lat`/`lon`, `tid` (2-char device/tracker
 * id). `project_slug` is intentionally omitted so the same active-project/allowed-project
 * fallback used by a plain Shortcuts call (with no project_slug) applies here too.
 */
function mapOwnTracksTransitionToShortcutBody(payload) {
  const region = normalizeRegionName(payload.desc);
  const direction = payload.event === "leave" ? "leave" : "enter";
  const canonical = CANONICAL_REGION_EVENT_TYPES[region];
  const eventType = canonical
    ? canonical[direction]
    : direction === "leave"
      ? "leave_location"
      : "arrive_location";

  const tst = Number(payload.tst);
  const timestamp = Number.isFinite(tst) && tst > 0 ? new Date(tst * 1000).toISOString() : undefined;

  return {
    event_type: eventType,
    timestamp,
    location_label: payload.desc || null,
    latitude: payload.lat,
    longitude: payload.lon,
    device_name: payload.tid || null,
    source: "owntracks",
    notes: payload.t ? `OwnTracks trigger: ${payload.t}` : "",
  };
}

async function handleOwnTracksEventRequest({
  db,
  FieldValue,
  req,
  res,
  logger,
  processAssistantMessage,
  openaiKey,
  timeLeftLifeEventDelivery,
}) {
  const runId = `owntracks-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  try {
    if (req.method !== "POST") {
      res.status(405).set("Allow", "POST").json({ ok: false, error: "method_not_allowed", message: "Use POST." });
      return;
    }

    const rawToken = extractOwnTracksToken(req);
    if (!rawToken) {
      jsonError(res, 401, "missing_token", "Missing integration token.");
      return;
    }
    const tokenHash = hashShortcutToken(rawToken);
    if (!checkShortcutRateLimit(tokenHash)) {
      jsonError(res, 429, "rate_limited", "Too many OwnTracks events. Try again shortly.");
      return;
    }
    const member = await findShortcutMemberByTokenHash(db, tokenHash);
    if (!member) {
      jsonError(res, 401, "invalid_token", "Invalid integration token.");
      return;
    }

    const payload = req.body && typeof req.body === "object" ? req.body : {};
    if (isIgnorableOwnTracksPayload(payload)) {
      // Regular location pings and waypoint dumps: acknowledge, nothing to record.
      res.status(200).json([]);
      return;
    }

    const translatedBody = mapOwnTracksTransitionToShortcutBody(payload);
    const parsed = parseShortcutEventPayload(translatedBody);
    if (!parsed.ok) {
      if (logger && typeof logger.warn === "function") {
        logger.warn("ownTracksEvents: rejected transition payload", {
          runId,
          code: parsed.code,
          status: parsed.status,
          desc: payload.desc,
          event: payload.event,
        });
      }
      jsonError(res, parsed.status, parsed.code, parsed.message);
      return;
    }

    if (logger && typeof logger.info === "function") {
      logger.info("ownTracksEvents: parsed transition", {
        runId,
        eventType: parsed.event.eventType,
        eventAtIso: parsed.event.eventAtIso,
        desc: payload.desc,
        tid: payload.tid,
      });
    }

    const result = await recordShortcutEvent({
      db,
      FieldValue,
      req,
      member,
      event: parsed.event,
      processAssistantMessage,
      openaiKey,
      runId,
    });

    if (!result.duplicate && typeof timeLeftLifeEventDelivery === "function") {
      try {
        await timeLeftLifeEventDelivery({
          event: { ...parsed.event, id: result.shortcutEventId },
          eventId: result.shortcutEventId,
        });
      } catch (err) {
        if (logger && typeof logger.warn === "function") {
          logger.warn("ownTracksEvents: TimeLeft delivery failed", {
            runId,
            shortcutEventId: result.shortcutEventId,
            message: err && err.message,
            code: err && err.code,
          });
        }
      }
    }

    // OwnTracks' HTTP protocol expects a JSON array in the response body (normally used to push
    // config/card/waypoint updates back to the device); an empty array is the correct "nothing to
    // send back" reply. Debug details go to the log instead of the response body.
    res.status(200).json([]);
  } catch (err) {
    const status = Number(err && err.status) || 500;
    const code = String((err && err.code) || "owntracks_event_failed");
    if (logger) {
      logger.error("ownTracksEvents: failed", {
        runId,
        code,
        status,
        message: err && err.message,
        stack: err && err.stack,
      });
    }
    jsonError(
      res,
      status >= 400 && status < 600 ? status : 500,
      code,
      status === 401 ? "Invalid integration token." : err.message || "Could not record OwnTracks event."
    );
  }
}

module.exports = {
  CANONICAL_REGION_EVENT_TYPES,
  isIgnorableOwnTracksPayload,
  mapOwnTracksTransitionToShortcutBody,
  extractOwnTracksToken,
  handleOwnTracksEventRequest,
};
