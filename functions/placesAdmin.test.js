const test = require("node:test");
const assert = require("node:assert/strict");
const { HttpsError } = require("firebase-functions/v2/https");
const {
  MIN_RADIUS_METERS,
  MAX_RADIUS_METERS,
  renameKnownPlace,
  updateKnownPlaceRadius,
  deleteKnownPlace,
  mergeKnownPlaces,
} = require("./placesAdmin");

const FieldValue = { serverTimestamp: () => new Date("2026-09-22T12:00:00.000Z") };

class FakeDb {
  constructor(seed = {}) {
    this.rows = new Map(Object.entries(seed).map(([id, data]) => [id, data]));
  }
  collection(col) {
    if (col !== "knownPlaces") throw new Error(`unexpected collection ${col}`);
    const rows = this.rows;
    return {
      doc(id) {
        return {
          async get() {
            const data = rows.get(id);
            return { id, exists: data != null, data: () => data };
          },
          async set(patch, options = {}) {
            const current = options.merge ? rows.get(id) || {} : {};
            rows.set(id, { ...current, ...patch });
          },
          async delete() {
            rows.delete(id);
          },
        };
      },
    };
  }
}

function seeded() {
  return new FakeDb({
    "place-1": { memberEmail: "user@example.com", name: "Gas Station", latitude: 43.76, longitude: -79.41, radiusMeters: 120, visitCount: 4 },
    "place-2": { memberEmail: "user@example.com", name: "Convenience Store", latitude: 43.7604, longitude: -79.41, radiusMeters: 120, visitCount: 2 },
    "someone-elses-place": { memberEmail: "other@example.com", name: "Their Gym", latitude: 43.8, longitude: -79.5, radiusMeters: 120, visitCount: 9 },
  });
}

test("renames an owned place", async () => {
  const db = seeded();
  const result = await renameKnownPlace({ db, FieldValue, memberEmail: "user@example.com", placeId: "place-1", name: "  Shell Gas Station  " });
  assert.equal(result.name, "Shell Gas Station");
  assert.equal((await db.collection("knownPlaces").doc("place-1").get()).data().name, "Shell Gas Station");
});

test("rejects an empty new name", async () => {
  const db = seeded();
  await assert.rejects(renameKnownPlace({ db, FieldValue, memberEmail: "user@example.com", placeId: "place-1", name: "   " }), HttpsError);
});

test("refuses to rename another member's place, and does not reveal it exists", async () => {
  const db = seeded();
  await assert.rejects(
    renameKnownPlace({ db, FieldValue, memberEmail: "user@example.com", placeId: "someone-elses-place", name: "Hijacked" }),
    (err) => err instanceof HttpsError && err.code === "not-found"
  );
  assert.equal((await db.collection("knownPlaces").doc("someone-elses-place").get()).data().name, "Their Gym");
});

test("updates the matching radius within bounds", async () => {
  const db = seeded();
  const result = await updateKnownPlaceRadius({ db, FieldValue, memberEmail: "user@example.com", placeId: "place-1", radiusMeters: 60 });
  assert.equal(result.radiusMeters, 60);
});

test("rejects a radius outside the allowed range", async () => {
  const db = seeded();
  await assert.rejects(updateKnownPlaceRadius({ db, FieldValue, memberEmail: "user@example.com", placeId: "place-1", radiusMeters: MIN_RADIUS_METERS - 1 }), HttpsError);
  await assert.rejects(updateKnownPlaceRadius({ db, FieldValue, memberEmail: "user@example.com", placeId: "place-1", radiusMeters: MAX_RADIUS_METERS + 1 }), HttpsError);
});

test("deletes an owned place", async () => {
  const db = seeded();
  const result = await deleteKnownPlace({ db, memberEmail: "user@example.com", placeId: "place-1" });
  assert.equal(result.deleted, true);
  assert.equal((await db.collection("knownPlaces").doc("place-1").get()).exists, false);
});

test("refuses to delete another member's place", async () => {
  const db = seeded();
  await assert.rejects(deleteKnownPlace({ db, memberEmail: "user@example.com", placeId: "someone-elses-place" }), (err) => err instanceof HttpsError && err.code === "not-found");
  assert.equal((await db.collection("knownPlaces").doc("someone-elses-place").get()).exists, true);
});

test("merging two places sums their visit counts and removes the merged-away one", async () => {
  const db = seeded();
  const result = await mergeKnownPlaces({ db, FieldValue, memberEmail: "user@example.com", survivorId: "place-1", mergeIds: ["place-2"] });
  assert.equal(result.visitCount, 6);
  assert.equal(result.name, "Gas Station");
  assert.equal((await db.collection("knownPlaces").doc("place-2").get()).exists, false, "the merged-away place must be deleted");
  assert.equal((await db.collection("knownPlaces").doc("place-1").get()).data().visitCount, 6);
});

test("merging can also rename the survivor in the same call", async () => {
  const db = seeded();
  const result = await mergeKnownPlaces({ db, FieldValue, memberEmail: "user@example.com", survivorId: "place-1", mergeIds: ["place-2"], name: "Corner Plaza" });
  assert.equal(result.name, "Corner Plaza");
  assert.equal((await db.collection("knownPlaces").doc("place-1").get()).data().name, "Corner Plaza");
});

test("cannot merge in a place you do not own", async () => {
  const db = seeded();
  await assert.rejects(
    mergeKnownPlaces({ db, FieldValue, memberEmail: "user@example.com", survivorId: "place-1", mergeIds: ["someone-elses-place"] }),
    (err) => err instanceof HttpsError && err.code === "not-found"
  );
  // nothing should have changed
  assert.equal((await db.collection("knownPlaces").doc("place-1").get()).data().visitCount, 4);
});

test("rejects a merge with no other place ids", async () => {
  const db = seeded();
  await assert.rejects(mergeKnownPlaces({ db, FieldValue, memberEmail: "user@example.com", survivorId: "place-1", mergeIds: [] }), HttpsError);
  await assert.rejects(mergeKnownPlaces({ db, FieldValue, memberEmail: "user@example.com", survivorId: "place-1", mergeIds: ["place-1"] }), HttpsError, "the survivor listed as its own merge target is not another place");
});
