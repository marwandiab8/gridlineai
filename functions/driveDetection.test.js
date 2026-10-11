const test = require("node:test");
const assert = require("node:assert/strict");
const { hashShortcutToken } = require("./iosShortcutsIntegration");
const { handleOwnTracksEventRequest } = require("./ownTracksIntegration");
const {
  advanceDriveState,
  checkDriveTimeout,
  closeStaleDrives,
  idleState,
  pointFromOwnTracks,
  SILENCE_MS,
} = require("./driveDetection");

// Toronto is UTC-4 in October. HOME is the same point the stay tests use.
const HOME = { lat: 43.7064, lon: -80.3934 };
const KM_LAT = 1 / 111.2; // ~1 km of latitude
const tst = (iso) => Math.floor(new Date(iso).getTime() / 1000);
const ping = (iso, { lat = HOME.lat, lon = HOME.lon, acc = 10, vel, ...extra } = {}) => ({
  _type: "location",
  lat,
  lon,
  acc,
  tst: tst(iso),
  tid: "MD",
  ...(vel != null ? { vel } : {}),
  ...extra,
});
const north = (km) => HOME.lat + km * KM_LAT;

function run(pings, state = idleState()) {
  const emissions = [];
  for (const p of pings) {
    const next = advanceDriveState(state, pointFromOwnTracks(p));
    state = next.state;
    emissions.push(...next.emissions);
  }
  return { state, emissions };
}

// --- the detector itself ---

test("asleep at home all morning: drifting, zero-speed pings never start a drive (the 2026-10-10 case)", () => {
  const pings = [];
  // 7:00-10:35 am EDT, a ping every ~15 min, wandering up to ~60 m with GPS drift.
  for (let i = 0; i < 15; i++) {
    const minutes = 11 * 60 + i * 15;
    const iso = new Date(Date.UTC(2026, 9, 10, 0, minutes)).toISOString();
    const drift = ((i % 3) - 1) * 0.0005; // about +/- 55 m
    pings.push(ping(iso, { lat: HOME.lat + drift, lon: HOME.lon - drift, acc: 35, vel: 0 }));
  }
  // One wild fix (a 600 m jump with 300 m accuracy) in the middle of it.
  pings.splice(7, 0, ping("2026-10-10T13:52:00Z", { lat: north(0.6), acc: 300 }));
  const { state, emissions } = run(pings);
  assert.deepEqual(emissions, []);
  assert.equal(state.status, "idle");
});

test("a single GPS jump with no speed of its own does not start a drive", () => {
  const { emissions } = run([
    ping("2026-10-10T13:00:00Z", { acc: 20 }),
    ping("2026-10-10T13:01:00Z", { lat: north(0.4), acc: 20 }), // 400 m in a minute: looks fast...
    ping("2026-10-10T13:02:00Z", { acc: 20 }), // ...but straight back
  ]);
  assert.deepEqual(emissions, []);
});

test("a morning drive starts when you set off and finishes when you parked", () => {
  const { state, emissions } = run([
    ping("2026-10-09T08:20:00Z", { vel: 0 }), // 4:20 am, still home
    ping("2026-10-09T08:28:00Z", { lat: north(1.5), vel: 55 }),
    ping("2026-10-09T08:33:00Z", { lat: north(6), vel: 70 }),
    ping("2026-10-09T08:45:00Z", { lat: north(20), vel: 90 }),
    ping("2026-10-09T09:01:00Z", { lat: north(38), vel: 3 }), // parked at site
    ping("2026-10-09T09:09:00Z", { lat: north(38.02), vel: 0 }), // still parked 8 min later
  ]);
  assert.equal(emissions.length, 2, JSON.stringify(emissions));
  const [start, finish] = emissions;
  assert.equal(start.kind, "start");
  // 1.5 km at 55 km/h is ~1.6 min, so you left home about 4:26 am - after the 4:20 ping.
  assert.ok(start.atMs > Date.parse("2026-10-09T08:26:00Z") && start.atMs < Date.parse("2026-10-09T08:27:00Z"), new Date(start.atMs).toISOString());
  assert.equal(start.latitude, HOME.lat, "the drive starts where you were");
  assert.equal(finish.kind, "finish");
  assert.equal(finish.driveId, start.driveId);
  // Arrived between the 4:45 moving ping and the 5:01 stopped one.
  assert.ok(finish.atMs > Date.parse("2026-10-09T08:45:00Z") && finish.atMs <= Date.parse("2026-10-09T09:01:00Z"));
  assert.ok(finish.distanceMeters > 36000 && finish.distanceMeters < 40000, String(finish.distanceMeters));
  assert.equal(state.status, "idle");
});

test("without OwnTracks' own speed, distance between pings is enough (significant-change mode)", () => {
  const { emissions } = run([
    ping("2026-10-09T19:20:00Z"),
    ping("2026-10-09T19:25:00Z", { lat: north(4) }), // 48 km/h
    ping("2026-10-09T19:30:00Z", { lat: north(9) }), // 60 km/h
  ]);
  assert.equal(emissions.length, 1);
  assert.equal(emissions[0].kind, "start");
  assert.ok(emissions[0].atMs >= Date.parse("2026-10-09T19:20:00Z"));
});

test("a red light or a drive-thru does not end the drive", () => {
  const { state, emissions } = run([
    ping("2026-10-09T12:00:00Z", { vel: 0 }),
    ping("2026-10-09T12:03:00Z", { lat: north(2), vel: 50 }),
    ping("2026-10-09T12:06:00Z", { lat: north(4), vel: 50 }),
    ping("2026-10-09T12:07:00Z", { lat: north(4.05), vel: 0 }), // stopped...
    ping("2026-10-09T12:11:00Z", { lat: north(4.06), vel: 2 }), // ...4 minutes at the window
    ping("2026-10-09T12:14:00Z", { lat: north(6), vel: 50 }), // and off again
  ]);
  assert.deepEqual(emissions.map((e) => e.kind), ["start"]);
  assert.equal(state.status, "driving");
  assert.equal(state.drive.stop, null);
});

test("when the pings stop after parking, the sweep finishes the drive at the last ping, not before 20 min", () => {
  const { state } = run([
    ping("2026-10-09T20:57:00Z", { vel: 0 }),
    ping("2026-10-09T21:00:00Z", { lat: north(2), vel: 60 }),
    ping("2026-10-09T21:04:00Z", { lat: north(6), vel: 60 }),
  ]);
  const last = Date.parse("2026-10-09T21:04:00Z");
  assert.deepEqual(checkDriveTimeout(state, last + SILENCE_MS - 1000).emissions, []);
  const done = checkDriveTimeout(state, last + SILENCE_MS);
  assert.equal(done.emissions.length, 1);
  assert.equal(done.emissions[0].kind, "finish");
  assert.equal(done.emissions[0].atMs, last);
  assert.equal(done.emissions[0].reason, "silence");
  assert.equal(done.state.status, "idle");
});

test("crossing into the Home region marks the arrival time and place", () => {
  const { state } = run([
    ping("2026-10-09T21:00:00Z", { lat: north(10), vel: 0 }),
    ping("2026-10-09T21:03:00Z", { lat: north(7), vel: 60 }),
    ping("2026-10-09T21:06:00Z", { lat: north(3), vel: 60 }),
    { _type: "transition", event: "enter", desc: "Home", lat: HOME.lat, lon: HOME.lon, acc: 15, tst: tst("2026-10-09T21:09:30Z"), t: "c" },
  ]);
  assert.equal(state.drive.stop.label, "Home");
  const done = checkDriveTimeout(state, Date.parse("2026-10-09T21:17:00Z"));
  assert.equal(done.emissions[0].label, "Home");
  assert.ok(done.emissions[0].atMs <= Date.parse("2026-10-09T21:09:30Z"));
});

test("old pings delivered late, duplicates and inaccurate fixes leave the state alone", () => {
  const start = run([ping("2026-10-09T12:00:00Z")]).state;
  assert.equal(advanceDriveState(start, pointFromOwnTracks(ping("2026-10-09T11:59:00Z"))).ignored, "out_of_order");
  assert.equal(advanceDriveState(start, pointFromOwnTracks(ping("2026-10-09T12:00:00Z"))).ignored, "out_of_order");
  assert.equal(advanceDriveState(start, pointFromOwnTracks(ping("2026-10-09T12:05:00Z", { acc: 500 }))).ignored, "inaccurate");
  assert.equal(pointFromOwnTracks({ _type: "waypoints" }), null);
  assert.equal(pointFromOwnTracks(ping("2026-10-09T12:05:00Z", { lat: 0, lon: 0 })), null);
});

// --- through the real OwnTracks endpoint and the shared recording pipeline ---

const FieldValue = {
  serverTimestamp: () => new Date("2026-10-10T12:00:00.000Z"),
  delete: () => ({ __delete: true }),
  arrayUnion: (...values) => ({ __arrayUnion: values }),
};

class FakeDocSnap {
  constructor(id, data) {
    this.id = id;
    this._data = data;
    this.exists = data != null;
  }
  data() {
    return this._data;
  }
}
class FakeDocRef {
  constructor(db, col, id) {
    Object.assign(this, { db, col, id });
  }
  async get() {
    return new FakeDocSnap(this.id, this.db._get(this.col, this.id));
  }
  async set(data, options = {}) {
    const current = options.merge ? this.db._get(this.col, this.id) || {} : {};
    const out = { ...current };
    for (const [k, v] of Object.entries(data || {})) {
      if (v && v.__delete) delete out[k];
      else if (v && v.__arrayUnion) out[k] = [...new Set([...(out[k] || []), ...v.__arrayUnion])];
      else out[k] = v;
    }
    this.db._set(this.col, this.id, out);
  }
}
class FakeQuery {
  constructor(db, col) {
    Object.assign(this, { db, col, filters: [], _limit: Infinity });
  }
  where(field, op, value) {
    this.filters.push({ field, op, value });
    return this;
  }
  limit(n) {
    this._limit = n;
    return this;
  }
  async get() {
    const docs = [...(this.db.rows.get(this.col) || new Map()).entries()]
      .filter(([, d]) =>
        this.filters.every((f) => {
          const v = String(f.field).split(".").reduce((acc, k) => (acc == null ? undefined : acc[k]), d);
          return f.op === "==" ? v === f.value : f.op === ">=" ? v >= f.value : v <= f.value;
        })
      )
      .slice(0, this._limit)
      .map(([id, d]) => new FakeDocSnap(id, d));
    return { empty: !docs.length, docs, forEach: (fn) => docs.forEach(fn) };
  }
}
class FakeCollection extends FakeQuery {
  doc(id) {
    return new FakeDocRef(this.db, this.col, id || `doc-${++this.db.nextId}`);
  }
}
class FakeDb {
  constructor(seed = {}) {
    this.rows = new Map(Object.entries(seed).map(([c, docs]) => [c, new Map(Object.entries(docs))]));
    this.nextId = 0;
  }
  collection(col) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    return new FakeCollection(this, col);
  }
  _get(col, id) {
    return this.rows.get(col)?.get(id) || null;
  }
  _set(col, id, data) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    this.rows.get(col).set(id, data);
  }
  async runTransaction(fn) {
    return fn({
      get: (ref) => ref.get(),
      create: (ref, data) => ref.set(data),
      set: (ref, data, options) => ref.set(data, options),
    });
  }
}

const ME = "marwan@example.com";
const TOKEN = "drive-token";
function seededDb(member = {}, extra = {}) {
  return new FakeDb({
    appMembers: {
      [ME]: { active: true, approvedPhoneE164: "+14375550123", shortcutIntegration: { enabled: true, tokenHash: hashShortcutToken(TOKEN) }, ...member },
    },
    ...extra,
  });
}
const assistant = async () => ({ inboundRef: { id: "in" }, outboundRef: { id: "out" }, outboundMeta: { logEntryId: null } });

async function post(db, body, delivered = []) {
  const response = {
    statusCode: 200,
    body: null,
    status(c) {
      this.statusCode = c;
      return this;
    },
    set() {
      return this;
    },
    json(b) {
      this.body = b;
      return this;
    },
  };
  await handleOwnTracksEventRequest({
    db,
    FieldValue,
    req: { method: "POST", body, query: {}, get: (n) => (String(n).toLowerCase() === "authorization" ? `Bearer ${TOKEN}` : "") },
    res: response,
    logger: { error() {}, warn() {}, info() {} },
    openaiKey: null,
    processAssistantMessage: assistant,
    timeLeftLifeEventDelivery: async ({ event }) => delivered.push(event.eventType),
  });
  return response;
}

const driveEvents = (db) =>
  [...(db.rows.get("iosShortcutEvents") || new Map()).values()]
    .filter((e) => e.eventType === "start_drive" || e.eventType === "finish_drive")
    .sort((a, b) => a.eventAtMs - b.eventAtMs);

const commute = [
  ping("2026-10-09T08:20:00Z", { vel: 0 }),
  ping("2026-10-09T08:28:00Z", { lat: north(1.5), vel: 55 }),
  ping("2026-10-09T08:33:00Z", { lat: north(6), vel: 70 }),
  ping("2026-10-09T08:45:00Z", { lat: north(20), vel: 90 }),
  ping("2026-10-09T09:01:00Z", { lat: north(38), vel: 3 }),
  ping("2026-10-09T09:09:00Z", { lat: north(38.02), vel: 0 }),
];

test("OwnTracks pings record start_drive and finish_drive through the Shortcuts pipeline and TimeLeft", async () => {
  const db = seededDb();
  const delivered = [];
  for (const p of commute) {
    const response = await post(db, p, delivered);
    assert.deepEqual(response.body, [], "every ping is still acknowledged the OwnTracks way");
  }
  const events = driveEvents(db);
  assert.deepEqual(events.map((e) => e.eventType), ["start_drive", "finish_drive"]);
  assert.ok(events.every((e) => e.source === "owntracks_motion" && e.status === "recorded"));
  assert.match(events[1].notes, /drove 3\d(\.\d)? km in \d+ min/);
  assert.equal(events[0].reportDateKey, "2026-10-09");
  assert.deepEqual(delivered, ["start_drive", "finish_drive"]);
});

test("a drive the CarPlay Shortcut already logged is not recorded twice", async () => {
  const carplayStartMs = Date.parse("2026-10-09T08:25:00Z");
  const db = seededDb({}, {
    iosShortcutEvents: { carplay: { memberEmail: ME, eventType: "start_drive", eventAtMs: carplayStartMs, source: "ios_shortcuts" } },
  });
  for (const p of commute) await post(db, p);
  const events = driveEvents(db);
  assert.deepEqual(events.map((e) => [e.eventType, e.source]), [
    ["start_drive", "ios_shortcuts"],
    ["finish_drive", "owntracks_motion"],
  ]);
});

test("the scheduled sweep finishes a drive whose pings stopped, and only once", async () => {
  const db = seededDb();
  for (const p of commute.slice(0, 4)) await post(db, p); // driving, then OwnTracks went quiet
  assert.deepEqual(driveEvents(db).map((e) => e.eventType), ["start_drive"]);
  const deps = { processAssistantMessage: assistant, openaiKey: null, runId: "t", logger: { warn() {}, info() {} } };
  const quietSince = Date.parse("2026-10-09T08:45:00Z");
  assert.equal(await closeStaleDrives({ db, FieldValue, nowMs: quietSince + 5 * 60 * 1000, deps }), 0);
  assert.equal(await closeStaleDrives({ db, FieldValue, nowMs: quietSince + SILENCE_MS, deps }), 1);
  assert.equal(await closeStaleDrives({ db, FieldValue, nowMs: quietSince + 2 * SILENCE_MS, deps }), 0);
  const events = driveEvents(db);
  assert.deepEqual(events.map((e) => e.eventType), ["start_drive", "finish_drive"]);
  assert.equal(events[1].eventAtMs, quietSince);
});

test("a member can switch drive detection off", async () => {
  const db = seededDb({ driveDetection: { enabled: false } });
  for (const p of commute) await post(db, p);
  assert.deepEqual(driveEvents(db), []);
});
