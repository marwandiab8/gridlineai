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
const { findStayToCloseOnRegionLeave, leaveKnownPlace } = require("./placeLearning");
const { closeStaysFromEvidence } = require("./stayClosing");

/**
 * The location evidence in an OwnTracks message (a ping or a crossing): where and when, and how
 * accurate the fix was. Null when it has no usable position.
 */
function ownTracksEvidence(payload) {
  const latitude = Number(payload.lat);
  const longitude = Number(payload.lon);
  const tst = Number(payload.tst);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude) || !Number.isFinite(tst) || tst <= 0) return null;
  const acc = Number(payload.acc);
  return { latitude, longitude, at: new Date(tst * 1000), accuracyMeters: Number.isFinite(acc) && acc > 0 ? acc : 0 };
}

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
    // OwnTracks is a native app, not a browser - it never needs a CORS preflight - but its own
    // networking layer appears to probe the endpoint with OPTIONS as part of its "connecting"
    // handshake before ever sending a real POST. Cloud Functions' automatic CORS handling was
    // answering that with an empty 204 (no body, not JSON), which fails NSJSONSerialization
    // immediately on the phone and stops the app from proceeding to publish anything at all.
    // Answering it the same way a real event would be acknowledged fixes that.
    if (req.method === "OPTIONS") {
      res.status(200).json([]);
      return;
    }
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
    const deps = { processAssistantMessage, openaiKey, runId, logger, timeLeftLifeEventDelivery };
    if (isIgnorableOwnTracksPayload(payload)) {
      // Regular location pings are not recorded as events, but each one is evidence of where you are:
      // it ends a "Log this place" stay you have driven away from, at about when you left
      // (stayEvidence.js). Waypoint dumps and the rest carry nothing to use.
      const evidence = payload && payload._type === "location" ? ownTracksEvidence(payload) : null;
      if (evidence) await closeStaysFromEvidence({ db, FieldValue, req, member, evidence, deviceName: payload.tid || null, deps });
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

    // Leaving a region also ends the matching "Log this place" stay (Known Places), at the exit time
    // OwnTracks reported. Those stays live in a separate collection that OwnTracks events never
    // touched, so a place learned by the Shortcut stayed "here now" forever. Never blocks the event.
    if (!result.duplicate && payload.event === "leave") {
      try {
        const region = normalizeRegionName(payload.desc);
        const place = await findStayToCloseOnRegionLeave(db, member.email, {
          name: payload.desc,
          latitude: Number(payload.lat),
          longitude: Number(payload.lon),
          leftAt: parsed.event.eventDate,
          allowProximity: !CANONICAL_REGION_EVENT_TYPES[region] || CANONICAL_REGION_EVENT_TYPES[region].leave === "leave_gym",
        });
        if (place) await leaveKnownPlace({ db, FieldValue, place, leftAt: parsed.event.eventDate });
      } catch (err) {
        if (logger && typeof logger.warn === "function") {
          logger.warn("ownTracksEvents: could not close the matching known place", { runId, message: err && err.message });
        }
      }
    }

    // The crossing's position is evidence for every other open stay too (arriving home ends a stay
    // across town that was never left).
    if (!result.duplicate) {
      const evidence = ownTracksEvidence(payload);
      if (evidence) {
        await closeStaysFromEvidence({
          db, FieldValue, req, member, deps,
          deviceName: payload.tid || null,
          evidence: { ...evidence, eventType: parsed.event.eventType, label: payload.desc || null },
        });
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
