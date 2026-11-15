// A drive that ends at one of your known places is a visit there, logged the way "Log this place" would
// log it: an arrive_location carrying the place's activity when the drive finishes, so Time Left shows
// "Darts at Raging Bull" without anyone touching the phone. The departure is already handled: the next
// drive starting there (or any later sign of you elsewhere) closes the stay and records the leave with
// its length (stayEvidence.js / stayClosing.js).
//
// Arrivals OwnTracks reports itself - crossing into the Home, Work or Gym region - are left alone, so a
// gym saved as a known place inside the Gym region is counted once, by the crossing, not twice.
const { findNearbyKnownPlace, openVisitStartMs, visitKnownPlace } = require("./placeLearning");
const { buildVisitBody, recordAndDeliver } = require("./stayClosing");
const { parseShortcutEventPayload } = require("./iosShortcutsIntegration");

const COL_SHORTCUT_EVENTS = "iosShortcutEvents";
const SOURCE = "owntracks_motion";
// An OwnTracks region crossing this close to the moment you parked already recorded the arrival.
const REGION_ARRIVAL_WINDOW_MS = 10 * 60 * 1000;
const REGION_ARRIVAL_TYPES = ["arrive_home", "arrive_work", "arrive_gym"];
// Parking again at a place you checked into less than this long ago is the same visit.
const SAME_VISIT_WITHIN_MS = 12 * 60 * 60 * 1000;

/** A request stand-in carrying a stable idempotency key, so a retried finish records the visit once. */
function idempotentRequest(key) {
  return { get: (name) => (String(name).toLowerCase() === "idempotency-key" ? key : "") };
}

/** The id of a Home/Work/Gym arrival OwnTracks recorded within a few minutes of `atMs`, or null. */
async function regionArrivalNear(db, memberEmail, atMs) {
  for (const eventType of REGION_ARRIVAL_TYPES) {
    const snap = await db
      .collection(COL_SHORTCUT_EVENTS)
      .where("memberEmail", "==", memberEmail)
      .where("eventType", "==", eventType)
      .where("eventAtMs", ">=", atMs - REGION_ARRIVAL_WINDOW_MS)
      .where("eventAtMs", "<=", atMs + REGION_ARRIVAL_WINDOW_MS)
      .limit(1)
      .get()
      .catch(() => null);
    if (snap && !snap.empty) return snap.docs[0].id;
  }
  return null;
}

/**
 * Logs the visit a finished drive implies. `stop` is the finish emission from driveDetection.js
 * ({ driveId, latitude, longitude, atMs, label }). Returns { visited, place?, skipped? }. Never throws.
 */
async function visitPlaceAtDriveEnd({ db, FieldValue, member, stop, deviceName = null, timezone, deps = {} }) {
  const warn = (message, extra) => deps.logger && typeof deps.logger.warn === "function" && deps.logger.warn(message, { runId: deps.runId, ...extra });
  try {
    if (!stop || !Number.isFinite(stop.latitude) || !Number.isFinite(stop.longitude) || !Number.isFinite(stop.atMs)) return { visited: false, skipped: "no_position" };
    // The drive ended by crossing into Home/Work/Gym: OwnTracks recorded that arrival.
    if (stop.label) return { visited: false, skipped: "region_arrival" };
    const place = await findNearbyKnownPlace(db, member.email, stop.latitude, stop.longitude);
    if (!place) return { visited: false, skipped: "unknown_place" };
    const openMs = openVisitStartMs(place);
    if (openMs != null && stop.atMs >= openMs && stop.atMs - openMs < SAME_VISIT_WITHIN_MS) return { visited: false, skipped: "already_there", place };
    if (await regionArrivalNear(db, member.email, stop.atMs)) return { visited: false, skipped: "region_arrival", place };

    const arrivedAt = new Date(stop.atMs);
    const visit = await visitKnownPlace({ db, FieldValue, place, arrivedAt });
    const parsed = parseShortcutEventPayload({
      ...buildVisitBody({
        latitude: stop.latitude,
        longitude: stop.longitude,
        name: visit.name,
        timestamp: arrivedAt.toISOString(),
        timezone,
        deviceName,
        activity: visit.activity,
        notes: `Arrived when the drive ended here (OwnTracks), ${place.distanceMeters} m from the saved spot`,
      }),
      source: SOURCE,
    });
    if (!parsed.ok) {
      warn("autoVisits: visit payload rejected", { code: parsed.code, place: visit.name });
      return { visited: false, skipped: parsed.code, place };
    }
    const req = idempotentRequest(`visit-${stop.driveId || stop.atMs}`);
    const result = await recordAndDeliver({ db, FieldValue, req, member, event: parsed.event, ...deps });
    if (deps.logger && typeof deps.logger.info === "function") {
      deps.logger.info("autoVisits: drive ended at a known place", { runId: deps.runId, place: visit.name, activity: visit.activity, atMs: stop.atMs, duplicate: !!result.duplicate });
    }
    return { visited: !result.duplicate, place, shortcutEventId: result.shortcutEventId };
  } catch (err) {
    warn("autoVisits: could not log the visit", { message: err && err.message });
    return { visited: false, skipped: "error" };
  }
}

module.exports = { REGION_ARRIVAL_WINDOW_MS, SAME_VISIT_WITHIN_MS, regionArrivalNear, visitPlaceAtDriveEnd };
