// Closes the "Log this place" stays a piece of location evidence shows you have left (stayEvidence.js)
// and records each departure through the shared Shortcuts pipeline - dashboard log note plus Time Left
// delivery - as a leave_location event at the time you actually left. Used by every endpoint that
// receives your coordinates: iOS Shortcuts events, OwnTracks crossings and pings, and "Log this place".
// It never fails the request that brought the evidence: problems are logged and the caller carries on.
const { parseShortcutEventPayload, recordShortcutEvent } = require("./iosShortcutsIntegration");
const { reconcileStaysWithEvidence } = require("./stayEvidence");

const DEFAULT_TIMEZONE = "America/Toronto";

/** "1 h 5 min", "42 min", "0 min". */
function formatStay(minutes) {
  const total = Math.max(0, Math.round(minutes));
  const hours = Math.floor(total / 60);
  const mins = total % 60;
  if (!hours) return `${mins} min`;
  return mins ? `${hours} h ${mins} min` : `${hours} h`;
}

/** "10:22 pm" in `timezone`. */
function formatClock(date, timezone) {
  try {
    return new Intl.DateTimeFormat("en-CA", { hour: "numeric", minute: "2-digit", hour12: true, timeZone: timezone || DEFAULT_TIMEZONE })
      .format(date)
      .replace(/\./g, "")
      .toLowerCase();
  } catch {
    return date.toISOString().slice(11, 16);
  }
}

function formatDistance(meters) {
  return meters >= 1000 ? `${Math.round(meters / 100) / 10} km` : `${Math.round(meters)} m`;
}

/** The arrive_location / leave_location body recordShortcutEvent's pipeline understands. */
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

/** The dashboard note for a stay closed by evidence, saying how its end was worked out. */
function closedStayNote({ stay, leftAt, basis, distanceMeters, evidenceLabel, timezone }) {
  const length = stay.durationMinutes != null ? `Stayed ${formatStay(stay.durationMinutes)} at ${stay.name}` : `Left ${stay.name}`;
  const clock = formatClock(leftAt, timezone);
  if (basis === "drive") return `${length} (left at ${clock}, when you started driving)`;
  const where = evidenceLabel ? `when you were at ${evidenceLabel}, ${formatDistance(distanceMeters)} away` : `${formatDistance(distanceMeters)} away`;
  return `${length} (left about ${clock}, estimated from your next location ${where})`;
}

async function recordAndDeliver({ db, FieldValue, req, member, event, processAssistantMessage, openaiKey, runId, logger, timeLeftLifeEventDelivery }) {
  const result = await recordShortcutEvent({ db, FieldValue, req, member, event, processAssistantMessage, openaiKey, runId });
  if (!result.duplicate && typeof timeLeftLifeEventDelivery === "function") {
    try {
      await timeLeftLifeEventDelivery({ event: { ...event, id: result.shortcutEventId }, eventId: result.shortcutEventId });
    } catch (err) {
      if (logger && typeof logger.warn === "function") logger.warn("stayClosing: TimeLeft delivery failed", { runId, message: err && err.message });
    }
  }
  return result;
}

/**
 * Applies `evidence` ({ latitude, longitude, at, eventType?, accuracyMeters?, exceptPlaceId?, label? })
 * to the member's open stays and records a leave_location event for each one it closes (unless the
 * departure was already recorded). Returns [{ name, durationMinutes, leftAt, estimated }].
 */
async function closeStaysFromEvidence({ db, FieldValue, req, member, evidence, timezone, deviceName, deps = {} }) {
  const out = [];
  try {
    const closed = await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: member.email, evidence });
    for (const c of closed) {
      out.push({ name: c.stay.name, durationMinutes: c.stay.durationMinutes, leftAt: c.leftAt.toISOString(), estimated: c.estimated });
      if (c.alreadyRecorded) continue; // the departure is already in the log; only the place needed closing
      const parsed = parseShortcutEventPayload(
        buildVisitBody({
          eventType: "leave_location",
          latitude: c.place.latitude,
          longitude: c.place.longitude,
          name: c.stay.name,
          timestamp: c.leftAt.toISOString(),
          timezone: timezone || DEFAULT_TIMEZONE,
          deviceName,
          notes: closedStayNote({ ...c, evidenceLabel: evidence.label, timezone }),
        })
      );
      if (parsed.ok) await recordAndDeliver({ db, FieldValue, req, member, event: parsed.event, ...deps });
    }
  } catch (err) {
    if (deps.logger && typeof deps.logger.warn === "function") {
      deps.logger.warn("stayClosing: could not apply location evidence", { runId: deps.runId, message: err && err.message });
    }
  }
  return out;
}

module.exports = { buildVisitBody, closeStaysFromEvidence, closedStayNote, formatClock, formatStay, recordAndDeliver };
