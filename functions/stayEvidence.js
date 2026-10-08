// Ending "Log this place" stays from evidence of where you are.
//
// A place learned by "Log this place" has no leave automation of its own, so before this a stay stayed
// open until the next "Log this place" arrival - Raging Bull ran 107 hours, from a Saturday evening to
// the next "Log this place" four days later in Newmarket. But almost everything the phone sends carries
// its coordinates: OwnTracks Home/Work/Gym crossings and location pings, drive start/finish, Spotify.
// Each of those is evidence:
//   - at an open stay's place (within its radius plus a margin): you are still there. The latest such
//     moment is kept as `lastSeenAt` (written at most every few minutes), and a drive starting there as
//     `driveStartedAt` - the exact moment you left, if nothing later shows you still there.
//   - clearly elsewhere: the stay is over. It ends when your drive started there if that is known;
//     otherwise at the evidence time minus the time it takes to drive from the place (an estimate,
//     never before you were last seen there, and at most 12 hours after that).
// A departure already recorded some other way (a Shortcut leave, a manual fix) is reused, not doubled.
const {
  COL_KNOWN_PLACES,
  DEFAULT_RADIUS_METERS,
  haversineMeters,
  openVisitStartMs,
  leaveKnownPlace,
} = require("./placeLearning");

const COL_SHORTCUT_EVENTS = "iosShortcutEvents";

// GPS drift and leave triggers that fire at the edge of a parking lot: within this of a place's own
// radius you may still be there, so it never closes a stay.
const AWAY_MARGIN_METERS = 300;
// Door-to-door average for the drive from a place to where you were next seen (highway plus town).
const ASSUMED_TRAVEL_METERS_PER_SECOND = 50000 / 3600;
// An estimated stay never runs more than this past the last moment you were seen there.
const MAX_UNSEEN_STAY_MS = 12 * 60 * 60 * 1000;
// `lastSeenAt` is only rewritten when it moves on by this much, so frequent pings cost no writes.
const SEEN_WRITE_INTERVAL_MS = 5 * 60 * 1000;
// Events that mean you are setting off from where you are.
const DEPARTURE_EVENT_TYPES = new Set(["start_drive"]);

function toMillis(value) {
  if (value == null) return null;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (value instanceof Date) return Number.isFinite(value.getTime()) ? value.getTime() : null;
  if (typeof value === "string") {
    const ms = Date.parse(value);
    return Number.isFinite(ms) ? ms : null;
  }
  return Number.isFinite(value) ? value : null;
}

function radiusOf(place) {
  return Number.isFinite(place.radiusMeters) && place.radiusMeters > 0 ? place.radiusMeters : DEFAULT_RADIUS_METERS;
}

function placeNameKey(name) {
  return String(name || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

/**
 * When a stay ended, from what is known: it began at `startMs`, you were last seen there at
 * `lastSeenMs` (or null), a drive started there at `driveStartMs` (or null), and at `evidenceMs` you
 * were `distanceMeters` away. Returns { leftMs, estimated, basis } with basis "drive" or "estimate".
 */
function departureFromEvidence({ startMs, lastSeenMs = null, driveStartMs = null, evidenceMs, distanceMeters }) {
  const floor = Math.max(startMs, lastSeenMs != null && lastSeenMs <= evidenceMs ? lastSeenMs : startMs);
  if (driveStartMs != null && driveStartMs >= floor && driveStartMs <= evidenceMs) {
    return { leftMs: driveStartMs, estimated: false, basis: "drive" };
  }
  const travelMs = (Math.max(0, distanceMeters) / ASSUMED_TRAVEL_METERS_PER_SECOND) * 1000;
  const latest = evidenceMs - travelMs;
  const leftMs = Math.min(Math.max(latest, floor), floor + MAX_UNSEEN_STAY_MS, evidenceMs);
  // An estimate, so to the whole second - but never before you were last seen there.
  return { leftMs: Math.max(floor, Math.round(leftMs / 1000) * 1000), estimated: true, basis: "estimate" };
}

/**
 * The earliest departure from `place` already recorded since the stay began (a Shortcut leave, a
 * manual fix), or null. One indexed query: memberEmail + eventType + eventAtMs.
 */
async function findRecordedLeave(db, memberEmail, place, startMs) {
  const snap = await db
    .collection(COL_SHORTCUT_EVENTS)
    .where("memberEmail", "==", memberEmail)
    .where("eventType", "==", "leave_location")
    .where("eventAtMs", ">=", startMs)
    .get()
    .catch(() => null);
  if (!snap) return null;
  const key = placeNameKey(place.name);
  let best = null;
  snap.forEach((doc) => {
    const data = doc.data() || {};
    if (placeNameKey(data.locationLabel) !== key || !Number.isFinite(data.eventAtMs)) return;
    if (!best || data.eventAtMs < best) best = data.eventAtMs;
  });
  return best;
}

/**
 * Applies one piece of location evidence to the member's open stays. `evidence` is
 * { latitude, longitude, at, eventType?, accuracyMeters?, exceptPlaceId? }. Stays you are shown to be
 * at are refreshed; stays you have clearly left are closed (`leaveKnownPlace`), and returned as
 * [{ place, stay, leftAt, estimated, basis, alreadyRecorded, distanceMeters }] for the caller to record.
 * Costs one query when nothing is open, and never writes unless something changed.
 */
async function reconcileStaysWithEvidence({ db, FieldValue, memberEmail, evidence }) {
  const { latitude, longitude, eventType, exceptPlaceId } = evidence || {};
  const atMs = toMillis(evidence && evidence.at);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude) || atMs == null) return [];
  if (latitude === 0 && longitude === 0) return []; // a phone with no fix sends 0,0
  const accuracy = Number.isFinite(evidence.accuracyMeters) && evidence.accuracyMeters > 0 ? evidence.accuracyMeters : 0;

  const snap = await db.collection(COL_KNOWN_PLACES).where("memberEmail", "==", memberEmail).get();
  const open = [];
  snap.forEach((doc) => {
    const place = { id: doc.id, ...(doc.data() || {}) };
    if (place.id === exceptPlaceId) return;
    const startMs = openVisitStartMs(place);
    if (startMs == null || atMs < startMs) return;
    if (!Number.isFinite(place.latitude) || !Number.isFinite(place.longitude)) return;
    open.push({ place, startMs });
  });

  const closed = [];
  for (const { place, startMs } of open) {
    const distance = haversineMeters(latitude, longitude, place.latitude, place.longitude);
    const lastSeenMs = toMillis(place.lastSeenAt);
    const driveStartMs = toMillis(place.driveStartedAt);
    const seen = lastSeenMs != null && lastSeenMs >= startMs ? lastSeenMs : null;
    const drive = driveStartMs != null && driveStartMs >= startMs ? driveStartMs : null;

    if (distance <= radiusOf(place) + AWAY_MARGIN_METERS) {
      // Still there. A drive starting here is the likely departure; anything else moves "last seen".
      const update = {};
      if (DEPARTURE_EVENT_TYPES.has(eventType)) {
        if (drive == null || atMs > drive) update.driveStartedAt = new Date(atMs);
      } else if (seen == null || atMs - seen >= SEEN_WRITE_INTERVAL_MS) {
        update.lastSeenAt = new Date(atMs);
      }
      if (Object.keys(update).length) await db.collection(COL_KNOWN_PLACES).doc(place.id).set(update, { merge: true });
      continue;
    }
    if (distance <= radiusOf(place) + AWAY_MARGIN_METERS + accuracy) continue; // too vague to be sure

    const recordedMs = await findRecordedLeave(db, memberEmail, place, startMs);
    const departure =
      recordedMs != null && recordedMs <= atMs
        ? { leftMs: recordedMs, estimated: false, basis: "recorded" }
        : departureFromEvidence({ startMs, lastSeenMs: seen, driveStartMs: drive, evidenceMs: atMs, distanceMeters: distance });
    const leftAt = new Date(departure.leftMs);
    const stay = await leaveKnownPlace({ db, FieldValue, place, leftAt, estimated: departure.estimated });
    closed.push({
      place,
      stay,
      leftAt,
      estimated: departure.estimated,
      basis: departure.basis,
      alreadyRecorded: departure.basis === "recorded",
      distanceMeters: Math.round(distance),
    });
  }
  return closed;
}

module.exports = {
  AWAY_MARGIN_METERS,
  ASSUMED_TRAVEL_METERS_PER_SECOND,
  MAX_UNSEEN_STAY_MS,
  SEEN_WRITE_INTERVAL_MS,
  departureFromEvidence,
  findRecordedLeave,
  reconcileStaysWithEvidence,
};
