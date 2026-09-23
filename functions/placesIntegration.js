// "Log this place": a Shortcut-triggered endpoint that only ever asks for a place's name once.
//
// The Shortcut (see docs/quick-log-shortcuts.md) gets the current location and POSTs just the
// coordinates here first. If they match somewhere already named, that visit is logged immediately
// and nothing further happens on the phone. If they do not, this endpoint records nothing and
// tells the Shortcut so - the Shortcut then asks locally (native "Ask for Text", no server round
// trip) and POSTs again with the name, which both saves the place for every future visit and logs
// this one.
//
// Like the OwnTracks integration, this module does not reimplement auth, event parsing, dedupe,
// or TimeLeft delivery - a resolved visit is recorded through iosShortcutsIntegration's own
// tested recordShortcutEvent, as an arrive_location event carrying the resolved name.
//
// The same endpoint also closes a stay: POST {"action":"leave"} (coordinates and/or name optional)
// records a leave_location event for the place being left and returns how long the stay lasted.
const {
  extractShortcutToken,
  hashShortcutToken,
  checkShortcutRateLimit,
  findShortcutMemberByTokenHash,
  parseShortcutEventPayload,
  recordShortcutEvent,
} = require("./iosShortcutsIntegration");
const {
  findNearbyKnownPlace,
  nameAndVisitPlace,
  visitKnownPlace,
  findPlaceToLeave,
  leaveKnownPlace,
} = require("./placeLearning");

function jsonError(res, status, code, message) {
  res.status(status).json({ ok: false, error: code, message });
}

function parseCoordinate(value) {
  if (value == null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : NaN; // NaN signals "present but not a number"
}

/** Builds the arrive_location-shaped body recordShortcutEvent's pipeline already understands. */
function buildVisitBody({ latitude, longitude, name, timestamp, timezone, deviceName, eventType = "arrive_location", notes }) {
  return {
    event_type: eventType,
    timestamp,
    timezone,
    location_label: name,
    latitude,
    longitude,
    device_name: deviceName || null,
    source: "quick_log",
    ...(notes ? { notes } : {}),
  };
}

function isLeaveRequest(payload) {
  const action = String(payload.action || payload.event || "").trim().toLowerCase();
  return ["leave", "left", "leaving", "depart", "departed", "exit"].includes(action);
}

/** "1 h 5 min", "42 min", "0 min". */
function formatStay(minutes) {
  const total = Math.max(0, Math.round(minutes));
  const hours = Math.floor(total / 60);
  const mins = total % 60;
  if (!hours) return `${mins} min`;
  return mins ? `${hours} h ${mins} min` : `${hours} h`;
}

/**
 * The instant this request's event happened (the payload's timestamp, or now), so a place's stay
 * is measured with the same clock as the event recorded for it.
 */
function requestEventDate(payload, eventType) {
  const parsed = parseShortcutEventPayload({ event_type: eventType, timestamp: payload.timestamp, timezone: payload.timezone });
  return parsed.ok ? parsed.event.eventDate : new Date();
}

/** Records a parsed event through the shared Shortcuts pipeline and forwards it to Time Left. */
async function recordAndDeliver({ db, FieldValue, req, member, event, processAssistantMessage, openaiKey, runId, logger, timeLeftLifeEventDelivery }) {
  const result = await recordShortcutEvent({ db, FieldValue, req, member, event, processAssistantMessage, openaiKey, runId });
  if (!result.duplicate && typeof timeLeftLifeEventDelivery === "function") {
    try {
      await timeLeftLifeEventDelivery({ event: { ...event, id: result.shortcutEventId }, eventId: result.shortcutEventId });
    } catch (err) {
      if (logger && typeof logger.warn === "function") {
        logger.warn("placesEvents: TimeLeft delivery failed", { runId, message: err && err.message });
      }
    }
  }
  return result;
}

async function handlePlaceLeave({ db, FieldValue, req, res, member, payload, deps }) {
  const latitude = parseCoordinate(payload.latitude);
  const longitude = parseCoordinate(payload.longitude);
  if (
    Number.isNaN(latitude) ||
    Number.isNaN(longitude) ||
    (latitude != null && (latitude < -90 || latitude > 90)) ||
    (longitude != null && (longitude < -180 || longitude > 180))
  ) {
    jsonError(res, 400, "invalid_coordinates", "latitude and longitude must be valid coordinates when given.");
    return;
  }
  const place = await findPlaceToLeave(db, member.email, {
    latitude: latitude == null ? undefined : latitude,
    longitude: longitude == null ? undefined : longitude,
    name: payload.name,
  });
  if (!place) {
    res.status(200).json({ ok: true, known: false, left: false });
    return;
  }

  const leftAt = requestEventDate(payload, "leave_location");
  const stay = await leaveKnownPlace({ db, FieldValue, place, leftAt });
  const parsed = parseShortcutEventPayload(
    buildVisitBody({
      eventType: "leave_location",
      latitude: latitude == null ? undefined : latitude,
      longitude: longitude == null ? undefined : longitude,
      name: stay.name,
      timestamp: leftAt.toISOString(),
      timezone: payload.timezone,
      deviceName: payload.device_name,
      notes: stay.durationMinutes != null ? `Stayed ${formatStay(stay.durationMinutes)} at ${stay.name}` : "",
    })
  );
  if (!parsed.ok) {
    jsonError(res, parsed.status, parsed.code, parsed.message);
    return;
  }
  await recordAndDeliver({ db, FieldValue, req, member, event: parsed.event, ...deps });
  res.status(200).json({
    ok: true,
    known: true,
    left: true,
    name: stay.name,
    arrivedAt: stay.arrivedAt,
    leftAt: leftAt.toISOString(),
    durationMinutes: stay.durationMinutes,
    duration: stay.durationMinutes != null ? formatStay(stay.durationMinutes) : null,
    averageMinutes: stay.averageMinutes,
    totalMinutesSpent: stay.totalMinutesSpent,
  });
}

async function handlePlaceLogRequest({
  db,
  FieldValue,
  req,
  res,
  logger,
  processAssistantMessage,
  openaiKey,
  timeLeftLifeEventDelivery,
}) {
  const runId = `places-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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
      jsonError(res, 429, "rate_limited", "Too many place-log requests. Try again shortly.");
      return;
    }
    const member = await findShortcutMemberByTokenHash(db, tokenHash);
    if (!member) {
      jsonError(res, 401, "invalid_token", "Invalid integration token.");
      return;
    }

    const payload = req.body && typeof req.body === "object" ? req.body : {};
    const deps = { processAssistantMessage, openaiKey, runId, logger, timeLeftLifeEventDelivery };
    if (isLeaveRequest(payload)) {
      await handlePlaceLeave({ db, FieldValue, req, res, member, payload, deps });
      return;
    }

    const latitude = parseCoordinate(payload.latitude);
    const longitude = parseCoordinate(payload.longitude);
    if (latitude == null || longitude == null || Number.isNaN(latitude) || Number.isNaN(longitude)) {
      jsonError(res, 400, "missing_coordinates", "latitude and longitude are required.");
      return;
    }
    if (latitude < -90 || latitude > 90) {
      jsonError(res, 400, "invalid_latitude", "latitude must be between -90 and 90.");
      return;
    }
    if (longitude < -180 || longitude > 180) {
      jsonError(res, 400, "invalid_longitude", "longitude must be between -180 and 180.");
      return;
    }

    const providedName = String(payload.name || "").trim();
    const arrivedAt = requestEventDate(payload, "arrive_location");

    if (!providedName) {
      const match = await findNearbyKnownPlace(db, member.email, latitude, longitude);
      if (!match) {
        // Nothing recorded yet - the Shortcut asks the user for a name and calls back with it.
        res.status(200).json({ ok: true, known: false });
        return;
      }
      const visit = await visitKnownPlace({ db, FieldValue, place: match, arrivedAt });
      const parsed = parseShortcutEventPayload(
        buildVisitBody({
          latitude,
          longitude,
          name: visit.name,
          timestamp: payload.timestamp,
          timezone: payload.timezone,
          deviceName: payload.device_name,
        })
      );
      if (parsed.ok) {
        await recordAndDeliver({ db, FieldValue, req, member, event: parsed.event, ...deps });
      } else if (logger && typeof logger.warn === "function") {
        logger.warn("placesEvents: known-place visit could not be recorded as an event", { runId, code: parsed.code });
      }
      res.status(200).json({
        ok: true,
        known: true,
        isNew: false,
        name: visit.name,
        visitCount: visit.visitCount,
        distanceMeters: match.distanceMeters,
        // Other known places also within range (e.g. a plaza's gas station AND convenience
        // store) - present when the guess might be the wrong one of several close together.
        alternatives: match.alternatives || [],
      });
      return;
    }

    // A name was supplied: save it (reusing a close-enough existing place instead of duplicating
    // it) and log this visit under that name.
    const saved = await nameAndVisitPlace({ db, FieldValue, memberEmail: member.email, latitude, longitude, name: providedName, arrivedAt });
    const parsed = parseShortcutEventPayload(
      buildVisitBody({
        latitude,
        longitude,
        name: saved.name,
        timestamp: payload.timestamp,
        timezone: payload.timezone,
        deviceName: payload.device_name,
      })
    );
    if (!parsed.ok) {
      jsonError(res, parsed.status, parsed.code, parsed.message);
      return;
    }
    await recordAndDeliver({ db, FieldValue, req, member, event: parsed.event, ...deps });
    res.status(200).json({ ok: true, known: true, isNew: saved.isNew, name: saved.name, visitCount: saved.visitCount });
  } catch (err) {
    const status = Number(err && err.status) || 500;
    const code = String((err && err.code) || "place_log_failed");
    if (logger) {
      logger.error("placesEvents: failed", { runId, code, status, message: err && err.message, stack: err && err.stack });
    }
    jsonError(res, status >= 400 && status < 600 ? status : 500, code, status === 401 ? "Invalid integration token." : err.message || "Could not log that place.");
  }
}

module.exports = { handlePlaceLogRequest };
