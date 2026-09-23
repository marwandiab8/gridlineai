// Location learning: "log this place" only ever asks for a name once per real-world spot.
//
// A member's known places are kept in the top-level `knownPlaces` collection, one document per
// place, matched by distance rather than by name (two different people's "Home" must never
// collide, and the same person visiting two different "Tim Hortons" locations are two places).
// Naming happens client-side, inside the Shortcut itself (an "Ask for Text" step), not through a
// server round-trip - see docs/quick-log-shortcuts.md - so a place is only ever written here once
// its name is already known; there is no separate "pending, unnamed" state to manage.
const COL_KNOWN_PLACES = "knownPlaces";

// A GPS fix drifts by tens of meters even standing still; 120m (roughly a block) is loose enough
// to recognize "the same parking lot" but tight enough not to merge two nearby, distinct places.
const DEFAULT_RADIUS_METERS = 120;
const EARTH_RADIUS_METERS = 6371000;

function toRadians(degrees) {
  return (degrees * Math.PI) / 180;
}

/** Great-circle distance between two coordinates, in meters. */
function haversineMeters(lat1, lon1, lat2, lon2) {
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) ** 2;
  return EARTH_RADIUS_METERS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Re-running "Log this place" while already there keeps the stay's original start; an open visit
// older than this is treated as a forgotten "leave" and replaced by the new arrival instead.
const STALE_OPEN_VISIT_MS = 12 * 60 * 60 * 1000;

/** Milliseconds for a Firestore Timestamp, Date, or epoch number; null when absent/invalid. */
function toMillis(value) {
  if (value == null) return null;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (value instanceof Date) return Number.isFinite(value.getTime()) ? value.getTime() : null;
  return Number.isFinite(value) ? value : null;
}

/**
 * When the member's current (not yet left) stay at `place` began, or null if they are not there.
 * Places saved before leave tracking existed have no currentVisitStartedAt, so an arrival with no
 * later lastLeftAt still counts as open - that lets the very first "leave" after this shipped
 * close out a visit that was logged before it.
 */
function openVisitStartMs(place) {
  const started = toMillis(place && place.currentVisitStartedAt);
  if (started != null) return started;
  const lastVisit = toMillis(place && place.lastVisitAt);
  const lastLeft = toMillis(place && place.lastLeftAt);
  if (lastVisit == null || (lastLeft != null && lastLeft >= lastVisit)) return null;
  return lastVisit;
}

/** The currentVisitStartedAt to write for an arrival at `arrivedAt`, given the place's prior state. */
function nextVisitStart(place, arrivedAt) {
  const arrivedMs = toMillis(arrivedAt);
  const openMs = place ? toMillis(place.currentVisitStartedAt) : null;
  if (openMs != null && arrivedMs != null && arrivedMs >= openMs && arrivedMs - openMs < STALE_OPEN_VISIT_MS) {
    return place.currentVisitStartedAt;
  }
  return arrivedAt;
}

function normalizePlaceName(name) {
  return String(name || "").trim().slice(0, 120);
}

/**
 * Every one of the member's known places within its own matching radius of (latitude,
 * longitude), nearest first. A plaza with a gas station and a convenience store 30m apart can
 * have both returned here - matching radius is about "is this the same physical spot", not
 * "is this the only business here".
 */
async function findNearbyKnownPlaces(db, memberEmail, latitude, longitude) {
  const snap = await db.collection(COL_KNOWN_PLACES).where("memberEmail", "==", memberEmail).get();
  const matches = [];
  snap.forEach((doc) => {
    const data = doc.data() || {};
    if (!Number.isFinite(data.latitude) || !Number.isFinite(data.longitude)) return;
    const distance = haversineMeters(latitude, longitude, data.latitude, data.longitude);
    const radius = Number.isFinite(data.radiusMeters) && data.radiusMeters > 0 ? data.radiusMeters : DEFAULT_RADIUS_METERS;
    if (distance <= radius) matches.push({ id: doc.id, ...data, distanceMeters: Math.round(distance) });
  });
  matches.sort((a, b) => a.distanceMeters - b.distanceMeters);
  return matches;
}

/**
 * The member's single closest known place to (latitude, longitude), if any is within range.
 * Used for coordinates-only recognition, where only one guess can be made; when a plaza has
 * several known places nearby, `alternatives` lists the others so a caller can offer a fix.
 */
async function findNearbyKnownPlace(db, memberEmail, latitude, longitude) {
  const matches = await findNearbyKnownPlaces(db, memberEmail, latitude, longitude);
  if (!matches.length) return null;
  const [best, ...alternatives] = matches;
  return { ...best, alternatives: alternatives.map((place) => ({ id: place.id, name: place.name })) };
}

/**
 * Records a visit at (latitude, longitude) under `name`. If an existing known place is close
 * enough to be the same real-world spot, that place is reused (and renamed, if the given name
 * differs - the most recent name a member gives a place wins) rather than creating a duplicate;
 * otherwise a new one is created. Either way, visit stats are updated and the resolved place is
 * returned.
 */
async function nameAndVisitPlace({ db, FieldValue, memberEmail, latitude, longitude, name, arrivedAt = new Date() }) {
  const cleanName = normalizePlaceName(name);
  if (!cleanName) {
    const err = new Error("A name is required to log a new place.");
    err.status = 400;
    err.code = "missing_name";
    throw err;
  }
  // Reuse a nearby place only when the name matches one already there (a revisit). A different
  // name nearby - the convenience store next to a gas station already saved - is a different
  // place, not a rename of the closest one, even though they're a few meters apart.
  const nearby = await findNearbyKnownPlaces(db, memberEmail, latitude, longitude);
  const existing = nearby.find((place) => normalizePlaceName(place.name).toLowerCase() === cleanName.toLowerCase());
  const now = FieldValue.serverTimestamp();
  if (existing) {
    const ref = db.collection(COL_KNOWN_PLACES).doc(existing.id);
    await ref.set(
      {
        name: cleanName,
        visitCount: (Number.isFinite(existing.visitCount) ? existing.visitCount : 0) + 1,
        lastVisitAt: now,
        currentVisitStartedAt: nextVisitStart(existing, arrivedAt),
        updatedAt: now,
      },
      { merge: true }
    );
    return { id: existing.id, name: cleanName, isNew: false, visitCount: (existing.visitCount || 0) + 1 };
  }
  const ref = db.collection(COL_KNOWN_PLACES).doc();
  await ref.set({
    memberEmail,
    name: cleanName,
    latitude,
    longitude,
    radiusMeters: DEFAULT_RADIUS_METERS,
    visitCount: 1,
    firstVisitAt: now,
    lastVisitAt: now,
    currentVisitStartedAt: arrivedAt,
    createdAt: now,
    updatedAt: now,
  });
  return { id: ref.id, name: cleanName, isNew: true, visitCount: 1 };
}

/** Records another visit to an already-known place (no naming involved). */
async function visitKnownPlace({ db, FieldValue, place, arrivedAt = new Date() }) {
  const now = FieldValue.serverTimestamp();
  const nextVisitCount = (Number.isFinite(place.visitCount) ? place.visitCount : 0) + 1;
  await db
    .collection(COL_KNOWN_PLACES)
    .doc(place.id)
    .set(
      {
        visitCount: nextVisitCount,
        lastVisitAt: now,
        currentVisitStartedAt: nextVisitStart(place, arrivedAt),
        updatedAt: now,
      },
      { merge: true }
    );
  return { id: place.id, name: place.name, isNew: false, visitCount: nextVisitCount };
}

/**
 * Which of the member's known places they are leaving. Leave automations often fire after the
 * phone is already past the place's radius, so coordinates alone are not enough:
 *   1. a `name` picks that place (nearest / currently-open one if several share it);
 *   2. otherwise a nearby place with an open stay, then any nearby place;
 *   3. otherwise the place with the most recently started open stay, wherever it is.
 * Returns null when nothing matches.
 */
async function findPlaceToLeave(db, memberEmail, { latitude, longitude, name } = {}) {
  const snap = await db.collection(COL_KNOWN_PLACES).where("memberEmail", "==", memberEmail).get();
  const places = [];
  snap.forEach((doc) => places.push({ id: doc.id, ...(doc.data() || {}) }));
  const hasCoords = Number.isFinite(latitude) && Number.isFinite(longitude);
  const withDistance = (place) =>
    hasCoords && Number.isFinite(place.latitude) && Number.isFinite(place.longitude)
      ? { ...place, distanceMeters: Math.round(haversineMeters(latitude, longitude, place.latitude, place.longitude)) }
      : { ...place, distanceMeters: null };
  const byOpenThenDistance = (a, b) => {
    const openA = openVisitStartMs(a);
    const openB = openVisitStartMs(b);
    if ((openA != null) !== (openB != null)) return openA != null ? -1 : 1;
    if (a.distanceMeters != null && b.distanceMeters != null) return a.distanceMeters - b.distanceMeters;
    return (openB || 0) - (openA || 0);
  };

  const cleanName = normalizePlaceName(name).toLowerCase();
  if (cleanName) {
    const named = places.filter((place) => normalizePlaceName(place.name).toLowerCase() === cleanName).map(withDistance);
    if (!named.length) return null;
    return named.sort(byOpenThenDistance)[0];
  }

  if (hasCoords) {
    const nearby = places.map(withDistance).filter((place) => {
      const radius = Number.isFinite(place.radiusMeters) && place.radiusMeters > 0 ? place.radiusMeters : DEFAULT_RADIUS_METERS;
      return place.distanceMeters != null && place.distanceMeters <= radius;
    });
    if (nearby.length) return nearby.sort(byOpenThenDistance)[0];
  }

  const open = places.filter((place) => openVisitStartMs(place) != null);
  if (!open.length) return null;
  open.sort((a, b) => openVisitStartMs(b) - openVisitStartMs(a));
  return withDistance(open[0]);
}

/**
 * Closes the member's open stay at `place` as of `leftAt` and folds its length into the place's
 * running totals. `durationMinutes` is null when there was no open stay to close (e.g. a second
 * "leave" in a row) - the departure is still recorded, it just has nothing to measure.
 */
async function leaveKnownPlace({ db, FieldValue, place, leftAt = new Date() }) {
  const startMs = openVisitStartMs(place);
  const leftMs = toMillis(leftAt);
  const durationMinutes = startMs != null && leftMs != null && leftMs >= startMs ? Math.round((leftMs - startMs) / 60000) : null;
  const totalMinutesSpent = (Number.isFinite(place.totalMinutesSpent) ? place.totalMinutesSpent : 0) + (durationMinutes || 0);
  const timedVisitCount = (Number.isFinite(place.timedVisitCount) ? place.timedVisitCount : 0) + (durationMinutes != null ? 1 : 0);
  const update = {
    lastLeftAt: leftAt,
    currentVisitStartedAt: FieldValue.delete(),
    updatedAt: FieldValue.serverTimestamp(),
  };
  if (durationMinutes != null) {
    Object.assign(update, { lastVisitDurationMinutes: durationMinutes, totalMinutesSpent, timedVisitCount });
  }
  await db.collection(COL_KNOWN_PLACES).doc(place.id).set(update, { merge: true });
  return {
    id: place.id,
    name: place.name,
    arrivedAt: startMs != null ? new Date(startMs).toISOString() : null,
    durationMinutes,
    totalMinutesSpent,
    timedVisitCount,
    averageMinutes: timedVisitCount ? Math.round(totalMinutesSpent / timedVisitCount) : null,
  };
}

module.exports = {
  COL_KNOWN_PLACES,
  DEFAULT_RADIUS_METERS,
  haversineMeters,
  normalizePlaceName,
  openVisitStartMs,
  findNearbyKnownPlace,
  findNearbyKnownPlaces,
  nameAndVisitPlace,
  visitKnownPlace,
  findPlaceToLeave,
  leaveKnownPlace,
};
