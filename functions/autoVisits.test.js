const test = require("node:test");
const assert = require("node:assert/strict");
const { visitPlaceAtDriveEnd } = require("./autoVisits");
const { recordDriveEmission } = require("./driveDetection");

const FieldValue = {
  serverTimestamp: () => new Date("2026-11-14T00:40:00.000Z"),
  delete: () => ({ __delete: true }),
  arrayUnion: (...values) => ({ __arrayUnion: values }),
};
class FakeDocSnap {
  constructor(id, data) { this.id = id; this._data = data; this.exists = data != null; }
  data() { return this._data; }
}
class FakeDocRef {
  constructor(db, col, id) { Object.assign(this, { db, col, id }); }
  async get() { return new FakeDocSnap(this.id, this.db._get(this.col, this.id)); }
  async set(data, options = {}) {
    const out = { ...(options.merge ? this.db._get(this.col, this.id) || {} : {}) };
    for (const [k, v] of Object.entries(data || {})) {
      if (v && v.__delete) delete out[k];
      else if (v && v.__arrayUnion) out[k] = [...new Set([...(out[k] || []), ...v.__arrayUnion])];
      else out[k] = v;
    }
    this.db._set(this.col, this.id, out);
  }
}
class FakeQuery {
  constructor(db, col) { Object.assign(this, { db, col, filters: [], _limit: Infinity }); }
  where(field, op, value) { this.filters.push({ field, op, value }); return this; }
  limit(n) { this._limit = n; return this; }
  async get() {
    const docs = [...(this.db.rows.get(this.col) || new Map()).entries()]
      .filter(([, d]) => this.filters.every((f) => {
        const v = String(f.field).split(".").reduce((acc, k) => (acc == null ? undefined : acc[k]), d);
        return f.op === "==" ? v === f.value : f.op === ">=" ? v >= f.value : v <= f.value;
      }))
      .slice(0, this._limit)
      .map(([id, d]) => new FakeDocSnap(id, d));
    return { empty: !docs.length, docs, forEach: (fn) => docs.forEach(fn) };
  }
}
class FakeCollection extends FakeQuery {
  doc(id) { return new FakeDocRef(this.db, this.col, id || `doc-${++this.db.nextId}`); }
}
class FakeDb {
  constructor(seed = {}) { this.rows = new Map(Object.entries(seed).map(([c, docs]) => [c, new Map(Object.entries(docs))])); this.nextId = 0; }
  collection(col) { if (!this.rows.has(col)) this.rows.set(col, new Map()); return new FakeCollection(this, col); }
  _get(col, id) { return this.rows.get(col)?.get(id) || null; }
  _set(col, id, data) { if (!this.rows.has(col)) this.rows.set(col, new Map()); this.rows.get(col).set(id, data); }
  async runTransaction(fn) { return fn({ get: (ref) => ref.get(), create: (ref, data) => ref.set(data), set: (ref, data, options) => ref.set(data, options) }); }
}

// The real Friday Nov 13, 2026 evening: the drive from Fergus finished 18 m from the Raging Bull at
// 7:34:19 pm, and the next drive started 700 m from it at 9:57:10 pm (where the previous ping had been).
const ME = "marwan@example.com";
const RAGING_BULL = { latitude: 43.5470973273051, longitude: -80.29679976158542 };
const DRIVE_END = { latitude: 43.546936, longitude: -80.296852 };
const ARRIVED_MS = Date.parse("2026-11-14T00:34:19Z");
const LEFT_MS = Date.parse("2026-11-14T02:57:10Z");
const ragingBull = (extra = {}) => ({
  memberEmail: ME, name: "Raging Bull", ...RAGING_BULL, radiusMeters: 120, activity: "Darts", activitySource: "set",
  visitCount: 1, timedVisitCount: 1, totalMinutesSpent: 200,
  lastVisitAt: new Date("2026-10-03T23:02:57Z"), lastLeftAt: new Date("2026-10-04T02:22:35Z"), ...extra,
});
function seededDb(extra = {}, place = ragingBull()) {
  return new FakeDb({
    appMembers: { [ME]: { active: true, approvedPhoneE164: "+14375550123", shortcutIntegration: { enabled: true } } },
    knownPlaces: { rb: place },
    ...extra,
  });
}
const member = (db) => ({ email: ME, data: db._get("appMembers", ME) });
const assistant = async () => ({ inboundRef: { id: "in" }, outboundRef: { id: "out" }, outboundMeta: { logEntryId: null } });
const deps = (delivered = []) => ({ processAssistantMessage: assistant, openaiKey: null, runId: "t", logger: { warn() {}, info() {} }, timeLeftLifeEventDelivery: async ({ event }) => delivered.push(event) });
const events = (db, type) => [...(db.rows.get("iosShortcutEvents") || new Map()).values()].filter((e) => !type || e.eventType === type).sort((a, b) => a.eventAtMs - b.eventAtMs);
const finish = (extra = {}) => ({ kind: "finish", driveId: "d1", atMs: ARRIVED_MS, ...DRIVE_END, startedAtMs: ARRIVED_MS - 35 * 60000, distanceMeters: 27000, maxSpeedKmh: 90, label: null, ...extra });

test("a drive that ends 18 m from the Raging Bull logs 'Darts at Raging Bull' and checks you in", async () => {
  const db = seededDb();
  const delivered = [];
  const out = await visitPlaceAtDriveEnd({ db, FieldValue, member: member(db), stop: finish(), deviceName: "47", timezone: "America/Toronto", deps: deps(delivered) });
  assert.equal(out.visited, true);
  assert.equal(out.place.name, "Raging Bull");
  const [visit] = events(db, "arrive_location");
  assert.equal(visit.locationLabel, "Raging Bull");
  assert.equal(visit.placeActivity, "Darts");
  assert.equal(visit.source, "owntracks_motion");
  assert.equal(visit.eventAtMs, ARRIVED_MS);
  assert.equal(visit.status, "recorded");
  assert.match(visit.notes, /drive ended here .* 18 m from the saved spot/);
  assert.deepEqual(delivered.map((e) => [e.eventType, e.placeActivity]), [["arrive_location", "Darts"]]);
  const place = db._get("knownPlaces", "rb");
  assert.equal(place.visitCount, 2);
  assert.equal(place.currentVisitStartedAt.getTime(), ARRIVED_MS);
});

test("through recordDriveEmission: the finish logs the visit, and the next drive away ends it with its length", async () => {
  const db = seededDb();
  const delivered = [];
  const d = deps(delivered);
  await recordDriveEmission({ db, FieldValue, member: member(db), emission: finish(), deviceName: "47", deps: d });
  // A retried finish (the sweep and a late ping can both see it) records nothing twice.
  await recordDriveEmission({ db, FieldValue, member: member(db), emission: finish(), deviceName: "47", deps: d });
  assert.equal(events(db, "arrive_location").length, 1);
  await recordDriveEmission({ db, FieldValue, member: member(db), emission: { kind: "start", driveId: "d2", atMs: LEFT_MS, latitude: 43.543156, longitude: -80.2892 }, deviceName: "47", deps: d });
  const [leave] = events(db, "leave_location");
  assert.ok(leave, "the departure was recorded");
  assert.equal(leave.locationLabel, "Raging Bull");
  assert.equal(leave.placeActivity, "Darts");
  assert.equal(leave.stayStartedAtIso, new Date(ARRIVED_MS).toISOString());
  assert.match(leave.notes, /Stayed 2 h 2\d min at Raging Bull/);
  assert.ok(Math.abs(leave.eventAtMs - LEFT_MS) <= 2 * 60000, "left within two minutes of the drive starting 700 m away");
  assert.deepEqual(delivered.map((e) => e.eventType).sort(), ["arrive_location", "finish_drive", "leave_location", "start_drive"]);
  const place = db._get("knownPlaces", "rb");
  assert.equal(place.visitCount, 2);
  assert.equal(place.timedVisitCount, 2);
  assert.ok(place.lastVisitDurationMinutes >= 141 && place.lastVisitDurationMinutes <= 143, String(place.lastVisitDurationMinutes));
});

test("an arrival OwnTracks already reported as a region crossing is not logged a second time", async () => {
  const gym = ragingBull({ name: "GoodLife Guelph", activity: "Gym" });
  const db = seededDb({ iosShortcutEvents: { crossing: { memberEmail: ME, eventType: "arrive_gym", eventAtMs: ARRIVED_MS - 60000, source: "owntracks" } } }, gym);
  const out = await visitPlaceAtDriveEnd({ db, FieldValue, member: member(db), stop: finish(), timezone: "America/Toronto", deps: deps() });
  assert.deepEqual([out.visited, out.skipped], [false, "region_arrival"]);
  assert.equal(events(db, "arrive_location").length, 0);
  const labelled = await visitPlaceAtDriveEnd({ db, FieldValue, member: member(db), stop: finish({ label: "Gym" }), timezone: "America/Toronto", deps: deps() });
  assert.equal(labelled.skipped, "region_arrival");
});

test("a stop away from every known place logs nothing, and parking again during an open visit is the same visit", async () => {
  const db = seededDb();
  const elsewhere = await visitPlaceAtDriveEnd({ db, FieldValue, member: member(db), stop: finish({ latitude: 43.6, longitude: -80.3 }), timezone: "America/Toronto", deps: deps() });
  assert.deepEqual([elsewhere.visited, elsewhere.skipped], [false, "unknown_place"]);
  const open = seededDb({}, ragingBull({ currentVisitStartedAt: new Date(ARRIVED_MS - 3600000), lastVisitAt: new Date(ARRIVED_MS - 3600000), lastLeftAt: new Date("2026-10-04T02:22:35Z") }));
  const again = await visitPlaceAtDriveEnd({ db: open, FieldValue, member: member(open), stop: finish(), timezone: "America/Toronto", deps: deps() });
  assert.deepEqual([again.visited, again.skipped], [false, "already_there"]);
  assert.equal(open._get("knownPlaces", "rb").visitCount, 1);
  assert.equal(events(open, "arrive_location").length, 0);
});
