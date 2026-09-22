// Server-side logic behind the "Known Places" dashboard page (public/places.html).
//
// Mutations are callable-function-only, matching how every other dashboard-editable record in
// this app works (issues, notes, etc. all go through a callable rather than a direct client
// write) - see firestore.rules' `allow write: if false;` on knownPlaces. Every function here
// re-checks ownership itself: callables run under the Admin SDK, which bypasses Firestore rules
// entirely, so the rules' `memberEmail == operatorEmail()` check is not a safety net here on its
// own.
const { HttpsError } = require("firebase-functions/v2/https");
const { COL_KNOWN_PLACES, normalizePlaceName, DEFAULT_RADIUS_METERS } = require("./placeLearning");

const MIN_RADIUS_METERS = 20;
const MAX_RADIUS_METERS = 1000;

async function loadOwnedPlace(db, memberEmail, placeId) {
  const id = String(placeId || "").trim();
  if (!id) throw new HttpsError("invalid-argument", "placeId is required.");
  const ref = db.collection(COL_KNOWN_PLACES).doc(id);
  const snap = await ref.get();
  if (!snap.exists) throw new HttpsError("not-found", "That place could not be found.");
  const data = snap.data() || {};
  if (data.memberEmail !== memberEmail) {
    // Deliberately the same message as not-found: do not reveal that a place exists for someone else.
    throw new HttpsError("not-found", "That place could not be found.");
  }
  return { ref, data };
}

async function renameKnownPlace({ db, FieldValue, memberEmail, placeId, name }) {
  const cleanName = normalizePlaceName(name);
  if (!cleanName) throw new HttpsError("invalid-argument", "A name is required.");
  const { ref } = await loadOwnedPlace(db, memberEmail, placeId);
  await ref.set({ name: cleanName, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
  return { id: placeId, name: cleanName };
}

async function updateKnownPlaceRadius({ db, FieldValue, memberEmail, placeId, radiusMeters }) {
  const radius = Number(radiusMeters);
  if (!Number.isFinite(radius) || radius < MIN_RADIUS_METERS || radius > MAX_RADIUS_METERS) {
    throw new HttpsError("invalid-argument", `radiusMeters must be between ${MIN_RADIUS_METERS} and ${MAX_RADIUS_METERS}.`);
  }
  const { ref } = await loadOwnedPlace(db, memberEmail, placeId);
  await ref.set({ radiusMeters: Math.round(radius), updatedAt: FieldValue.serverTimestamp() }, { merge: true });
  return { id: placeId, radiusMeters: Math.round(radius) };
}

async function deleteKnownPlace({ db, memberEmail, placeId }) {
  const { ref } = await loadOwnedPlace(db, memberEmail, placeId);
  await ref.delete();
  return { id: placeId, deleted: true };
}

/**
 * Folds one or more places into a single survivor: the survivor's name and radius are kept
 * (unless a new name is explicitly given), its visit count becomes the sum of everyone folded in,
 * and the folded-in places are deleted. For the plaza case this fixes an over-eager split (two
 * entries that turned out to be the same business) rather than the usual case this app is built
 * for (two genuinely different businesses, which should stay separate).
 */
async function mergeKnownPlaces({ db, FieldValue, memberEmail, survivorId, mergeIds, name }) {
  const ids = Array.isArray(mergeIds) ? [...new Set(mergeIds.map((id) => String(id || "").trim()).filter(Boolean))] : [];
  const survivor = String(survivorId || "").trim();
  if (!survivor) throw new HttpsError("invalid-argument", "survivorId is required.");
  const toMerge = ids.filter((id) => id !== survivor);
  if (!toMerge.length) throw new HttpsError("invalid-argument", "At least one other place id is required to merge.");

  const survivorLoad = await loadOwnedPlace(db, memberEmail, survivor);
  const others = await Promise.all(toMerge.map((id) => loadOwnedPlace(db, memberEmail, id)));

  const combinedVisitCount = [survivorLoad, ...others].reduce(
    (sum, place) => sum + (Number.isFinite(place.data.visitCount) ? place.data.visitCount : 0),
    0
  );
  const cleanName = name != null ? normalizePlaceName(name) : "";
  const now = FieldValue.serverTimestamp();

  await survivorLoad.ref.set(
    {
      ...(cleanName ? { name: cleanName } : {}),
      visitCount: combinedVisitCount,
      updatedAt: now,
    },
    { merge: true }
  );
  await Promise.all(others.map((place) => place.ref.delete()));

  return { id: survivor, name: cleanName || survivorLoad.data.name, visitCount: combinedVisitCount, mergedIds: toMerge };
}

module.exports = {
  MIN_RADIUS_METERS,
  MAX_RADIUS_METERS,
  DEFAULT_RADIUS_METERS,
  renameKnownPlace,
  updateKnownPlaceRadius,
  deleteKnownPlace,
  mergeKnownPlaces,
};
