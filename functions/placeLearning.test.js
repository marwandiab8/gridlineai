const test = require("node:test");
const assert = require("node:assert/strict");
const {
  DEFAULT_RADIUS_METERS,
  haversineMeters,
  findNearbyKnownPlace,
  findNearbyKnownPlaces,
  nameAndVisitPlace,
  visitKnownPlace,
} = require("./placeLearning");

const FieldValue = { serverTimestamp: () => new Date("2026-09-22T12:00:00.000Z") };

// Minimal fake Firestore: only what placeLearning.js actually needs (a single collection, an
// equality `where`, and merge-capable `set`).
class FakeDb {
  constructor() {
    this.rows = new Map();
    this.nextId = 0;
  }
  collection(col) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    const rows = this.rows.get(col);
    const db = this;
    return {
      where(field, op, value) {
        if (op !== "==") throw new Error(`unsupported op ${op}`);
        return {
          async get() {
            const docs = [...rows.entries()]
              .filter(([, data]) => data[field] === value)
              .map(([id, data]) => ({ id, data: () => data }));
            return { forEach: (fn) => docs.forEach(fn), docs, empty: docs.length === 0 };
          },
        };
      },
      doc(id) {
        const docId = id || `doc-${++db.nextId}`;
        return {
          id: docId,
          async set(data, options = {}) {
            const current = options.merge ? rows.get(docId) || {} : {};
            rows.set(docId, { ...current, ...data });
          },
          async get() {
            const data = rows.get(docId);
            return { id: docId, exists: data != null, data: () => data };
          },
        };
      },
    };
  }
}

// Toronto downtown-ish coordinates, ~50m apart, and a clearly distant point for contrast.
const HERE = { lat: 43.7615, lon: -79.4111 };
const NEARBY = { lat: 43.7619, lon: -79.4111 }; // ~44m north
const FAR_AWAY = { lat: 43.9, lon: -79.0 }; // tens of km away

test("haversineMeters is symmetric and ~0 for the same point", () => {
  assert.ok(haversineMeters(HERE.lat, HERE.lon, HERE.lat, HERE.lon) < 1);
  const a = haversineMeters(HERE.lat, HERE.lon, NEARBY.lat, NEARBY.lon);
  const b = haversineMeters(NEARBY.lat, NEARBY.lon, HERE.lat, HERE.lon);
  assert.equal(Math.round(a), Math.round(b));
  assert.ok(a > 30 && a < 60, `expected ~44m, got ${a}`);
});

test("finds no known place for a brand-new member", async () => {
  const db = new FakeDb();
  const match = await findNearbyKnownPlace(db, "user@example.com", HERE.lat, HERE.lon);
  assert.equal(match, null);
});

test("naming a new place creates it and logs the first visit", async () => {
  const db = new FakeDb();
  const saved = await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "  Bells of Steel  " });
  assert.equal(saved.isNew, true);
  assert.equal(saved.name, "Bells of Steel"); // trimmed
  assert.equal(saved.visitCount, 1);

  const match = await findNearbyKnownPlace(db, "user@example.com", HERE.lat, HERE.lon);
  assert.ok(match);
  assert.equal(match.name, "Bells of Steel");
  assert.equal(match.radiusMeters, DEFAULT_RADIUS_METERS);
});

test("a later visit within the radius is recognized as the same place, not a duplicate", async () => {
  const db = new FakeDb();
  await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "Bells of Steel" });
  const match = await findNearbyKnownPlace(db, "user@example.com", NEARBY.lat, NEARBY.lon);
  assert.ok(match, "a point ~44m away should still match a 120m-radius place");
  assert.equal(match.name, "Bells of Steel");

  const visit = await visitKnownPlace({ db, FieldValue, place: match });
  assert.equal(visit.isNew, false);
  assert.equal(visit.visitCount, 2);
});

test("a far-away point is not matched to an existing place", async () => {
  const db = new FakeDb();
  await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "Bells of Steel" });
  const match = await findNearbyKnownPlace(db, "user@example.com", FAR_AWAY.lat, FAR_AWAY.lon);
  assert.equal(match, null);
});

test("naming the same spot again with the same name is a revisit, not a duplicate", async () => {
  const db = new FakeDb();
  const first = await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "Bells of Steel" });
  const again = await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: NEARBY.lat, longitude: NEARBY.lon, name: "bells of steel" }); // case-insensitive
  assert.equal(again.isNew, false);
  assert.equal(again.id, first.id);
  assert.equal(again.visitCount, 2);
  assert.equal((db.rows.get("knownPlaces") || new Map()).size, 1);
});

test("a plaza: naming a second, differently-named business nearby creates a separate place instead of overwriting the first", async () => {
  const db = new FakeDb();
  const gasStation = await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "Gas Station" });
  const store = await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: NEARBY.lat, longitude: NEARBY.lon, name: "Convenience Store" });
  assert.equal(store.isNew, true);
  assert.notEqual(store.id, gasStation.id, "a different business a few meters away must be its own place");
  assert.equal((db.rows.get("knownPlaces") || new Map()).size, 2);

  // Both are still independently recognized afterward, by name, at either point in the plaza.
  const backAtGas = await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "Gas Station" });
  assert.equal(backAtGas.id, gasStation.id);
  assert.equal(backAtGas.visitCount, 2);
});

test("findNearbyKnownPlaces returns every plaza business within range, nearest first, and findNearbyKnownPlace lists the rest as alternatives", async () => {
  const db = new FakeDb();
  await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "Gas Station" });
  await nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: NEARBY.lat, longitude: NEARBY.lon, name: "Convenience Store" });

  const all = await findNearbyKnownPlaces(db, "user@example.com", HERE.lat, HERE.lon);
  assert.equal(all.length, 2);
  assert.equal(all[0].name, "Gas Station", "the exact point should list itself as nearest");

  const best = await findNearbyKnownPlace(db, "user@example.com", HERE.lat, HERE.lon);
  assert.equal(best.name, "Gas Station");
  assert.deepEqual(best.alternatives.map((a) => a.name), ["Convenience Store"]);
});

test("known places are scoped per member", async () => {
  const db = new FakeDb();
  await nameAndVisitPlace({ db, FieldValue, memberEmail: "alice@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "Alice's spot" });
  const match = await findNearbyKnownPlace(db, "bob@example.com", HERE.lat, HERE.lon);
  assert.equal(match, null, "bob must not see alice's known place even at the exact same coordinates");
});

test("rejects naming a place with an empty name", async () => {
  const db = new FakeDb();
  await assert.rejects(
    nameAndVisitPlace({ db, FieldValue, memberEmail: "user@example.com", latitude: HERE.lat, longitude: HERE.lon, name: "   " }),
    /name is required/
  );
});
