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
async function nameAndVisitPlace({ db, FieldValue, memberEmail, latitude, longitude, name }) {
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
    createdAt: now,
    updatedAt: now,
  });
  return { id: ref.id, name: cleanName, isNew: true, visitCount: 1 };
}

/** Records another visit to an already-known place (no naming involved). */
async function visitKnownPlace({ db, FieldValue, place }) {
  const now = FieldValue.serverTimestamp();
  const nextVisitCount = (Number.isFinite(place.visitCount) ? place.visitCount : 0) + 1;
  await db
    .collection(COL_KNOWN_PLACES)
    .doc(place.id)
    .set({ visitCount: nextVisitCount, lastVisitAt: now, updatedAt: now }, { merge: true });
  return { id: place.id, name: place.name, isNew: false, visitCount: nextVisitCount };
}

module.exports = {
  COL_KNOWN_PLACES,
  DEFAULT_RADIUS_METERS,
  haversineMeters,
  normalizePlaceName,
  findNearbyKnownPlace,
  findNearbyKnownPlaces,
  nameAndVisitPlace,
  visitKnownPlace,
};
