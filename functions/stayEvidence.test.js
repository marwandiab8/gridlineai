const test = require("node:test");
const assert = require("node:assert/strict");
const { departureFromEvidence, reconcileStaysWithEvidence, MAX_UNSEEN_STAY_MS } = require("./stayEvidence");

const FieldValue = { serverTimestamp: () => new Date("2026-10-08T12:00:00.000Z"), delete: () => ({ __delete: true }) };

// A small fake Firestore: equality and range where(), merge set() with delete sentinels.
class FakeDb {
  constructor(seed = {}) {
    this.rows = new Map(Object.entries(seed).map(([col, docs]) => [col, new Map(Object.entries(docs))]));
    this.writes = 0;
  }
  collection(col) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    const rows = this.rows.get(col);
    const db = this;
    const query = (filters) => ({
      where: (field, op, value) => query([...filters, { field, op, value }]),
      async get() {
        const docs = [...rows.entries()]
          .filter(([, d]) => filters.every((f) => (f.op === "==" ? d[f.field] === f.value : f.op === ">=" ? d[f.field] >= f.value : d[f.field] <= f.value)))
          .map(([id, d]) => ({ id, data: () => d }));
        return { docs, empty: !docs.length, forEach: (fn) => docs.forEach(fn) };
      },
    });
    return {
      ...query([]),
      doc: (id) => ({
        async set(data, opts = {}) {
          db.writes += 1;
          const out = { ...(opts.merge ? rows.get(id) || {} : {}) };
          for (const [k, v] of Object.entries(data)) {
            if (v && v.__delete) delete out[k];
            else out[k] = v;
          }
          rows.set(id, out);
        },
      }),
    };
  }
}

const ME = "marwan@example.com";
// The real places and times from the bug report (Toronto is UTC-4 in October).
const RAGING_BULL = { latitude: 43.5471, longitude: -80.2968 };
const HOME = { latitude: 43.7064, longitude: -80.3934 }; // ~19.5 km from Raging Bull
const ragingBullStay = (extra = {}) => ({
  memberEmail: ME,
  name: "Raging Bull",
  ...RAGING_BULL,
  radiusMeters: 120,
  currentVisitStartedAt: new Date("2026-10-03T23:02:57Z"),
  lastVisitAt: new Date("2026-10-03T23:02:57Z"),
  ...extra,
});

test("a drive starting at the place is the departure", () => {
  const d = departureFromEvidence({ startMs: 0, lastSeenMs: 60_000, driveStartMs: 120_000, evidenceMs: 3_600_000, distanceMeters: 20_000 });
  assert.deepEqual(d, { leftMs: 120_000, estimated: false, basis: "drive" });
});

test("without a drive, the departure is the evidence time minus the drive back, never before you were last seen", () => {
  // 25 km at 50 km/h is 30 min.
  assert.equal(departureFromEvidence({ startMs: 0, evidenceMs: 2 * 3_600_000, distanceMeters: 25_000 }).leftMs, 1.5 * 3_600_000);
  assert.equal(departureFromEvidence({ startMs: 0, lastSeenMs: 1.9 * 3_600_000, evidenceMs: 2 * 3_600_000, distanceMeters: 25_000 }).leftMs, 1.9 * 3_600_000);
  // A drive that started before you were last seen there again doesn't count.
  assert.equal(departureFromEvidence({ startMs: 0, lastSeenMs: 600_000, driveStartMs: 300_000, evidenceMs: 3_600_000, distanceMeters: 0 }).basis, "estimate");
});

test("an estimated stay never runs more than 12 hours past when you were last seen", () => {
  const d = departureFromEvidence({ startMs: 0, evidenceMs: 4 * 24 * 3_600_000, distanceMeters: 70_000 });
  assert.equal(d.leftMs, MAX_UNSEEN_STAY_MS);
  assert.equal(d.estimated, true);
});

test("Raging Bull: arriving home that night ends the stay at about 10:21 pm, not four days later in Newmarket", async () => {
  const db = new FakeDb({ knownPlaces: { rb: ragingBullStay() } });
  const closed = await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...HOME, at: new Date("2026-10-04T02:45:48Z"), eventType: "arrive_home" } });
  assert.equal(closed.length, 1);
  assert.equal(closed[0].estimated, true);
  const left = closed[0].leftAt.toISOString();
  assert.ok(left > "2026-10-04T02:20:00Z" && left < "2026-10-04T02:23:00Z", left);
  const place = db.rows.get("knownPlaces").get("rb");
  assert.equal(place.currentVisitStartedAt, undefined, "closed");
  assert.equal(place.lastLeaveEstimated, true);
  assert.ok(place.lastVisitDurationMinutes >= 197 && place.lastVisitDurationMinutes <= 200, String(place.lastVisitDurationMinutes));
});

test("events at the place keep it open and remember when you were last there; a drive start there is the departure", async () => {
  const db = new FakeDb({ knownPlaces: { rb: ragingBullStay() } });
  const near = { latitude: RAGING_BULL.latitude + 0.001, longitude: RAGING_BULL.longitude }; // ~110 m: the parking lot
  const at = (iso) => new Date(iso);
  assert.deepEqual(await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...near, at: at("2026-10-04T00:00:00Z"), eventType: "start_spotify" } }), []);
  assert.equal(db.rows.get("knownPlaces").get("rb").lastSeenAt.toISOString(), "2026-10-04T00:00:00.000Z");
  const writes = db.writes;
  await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...near, at: at("2026-10-04T00:02:00Z") } });
  assert.equal(db.writes, writes, "a ping two minutes later costs no write");
  await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...near, at: at("2026-10-04T02:21:30Z"), eventType: "start_drive" } });
  const closed = await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...HOME, at: at("2026-10-04T02:45:48Z"), eventType: "finish_drive" } });
  assert.equal(closed[0].basis, "drive");
  assert.equal(closed[0].estimated, false);
  assert.equal(closed[0].leftAt.toISOString(), "2026-10-04T02:21:30.000Z");
  const place = db.rows.get("knownPlaces").get("rb");
  assert.equal(place.lastSeenAt, undefined, "evidence ends with the stay");
  assert.equal(place.driveStartedAt, undefined);
});

test("Quick Oil Change: a departure already recorded (a manual fix) is reused, not logged a second time", async () => {
  const start = new Date("2026-09-23T17:52:21Z");
  const db = new FakeDb({
    knownPlaces: { qoc: { memberEmail: ME, name: "Quick Oil Change", latitude: 43.7936, longitude: -79.7626, currentVisitStartedAt: start, lastVisitAt: start } },
    iosShortcutEvents: {
      e1: { memberEmail: ME, eventType: "leave_location", locationLabel: "Quick Oil Change", eventAtMs: Date.parse("2026-09-23T18:38:00Z") },
      e2: { memberEmail: ME, eventType: "leave_location", locationLabel: "Somewhere else", eventAtMs: Date.parse("2026-09-23T18:00:00Z") },
    },
  });
  const closed = await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...HOME, at: new Date("2026-09-23T22:38:16Z") } });
  assert.equal(closed[0].alreadyRecorded, true);
  assert.equal(closed[0].leftAt.toISOString(), "2026-09-23T18:38:00.000Z");
  assert.equal(db.rows.get("knownPlaces").get("qoc").lastVisitDurationMinutes, 46);
});

test("vague, missing or out-of-order evidence closes nothing", async () => {
  const db = new FakeDb({ knownPlaces: { rb: ragingBullStay() } });
  const half = { latitude: RAGING_BULL.latitude + 0.004, longitude: RAGING_BULL.longitude }; // ~445 m
  assert.deepEqual(await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...half, at: new Date("2026-10-04T01:00:00Z"), accuracyMeters: 500 } }), [], "a fix only good to 500 m");
  assert.deepEqual(await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { latitude: 0, longitude: 0, at: new Date("2026-10-04T01:00:00Z") } }), [], "no GPS fix");
  assert.deepEqual(await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...HOME, at: new Date("2026-10-03T22:00:00Z") } }), [], "before the stay began");
  assert.deepEqual(await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: ME, evidence: { ...HOME, at: new Date("2026-10-04T02:00:00Z"), exceptPlaceId: "rb" } }), [], "the place being arrived at");
  assert.deepEqual(await reconcileStaysWithEvidence({ db, FieldValue, memberEmail: "someone@else.com", evidence: { ...HOME, at: new Date("2026-10-04T02:00:00Z") } }), [], "another member's places");
  assert.ok(db.rows.get("knownPlaces").get("rb").currentVisitStartedAt, "still open");
});
