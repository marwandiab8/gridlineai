const test = require("node:test");
const assert = require("node:assert/strict");
const { openPendingStop, applyEvidenceToPendingStop, adoptPendingStopAt, handlePlaceReply, parsePlaceReply, MAX_TEXTS_PER_DAY } = require("./unknownStops");

const FieldValue = { serverTimestamp: () => new Date("2026-11-14T03:00:00.000Z"), delete: () => ({ __delete: true }), arrayUnion: (...values) => ({ __arrayUnion: values }) };
class FakeDocSnap { constructor(id, data) { this.id = id; this._data = data; this.exists = data != null; } data() { return this._data; } }
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
  async delete() { this.db.rows.get(this.col)?.delete(this.id); }
}
class FakeQuery {
  constructor(db, col) { Object.assign(this, { db, col, filters: [], _limit: Infinity }); }
  where(field, op, value) { this.filters.push({ field, op, value }); return this; }
  limit(n) { this._limit = n; return this; }
  async get() {
    const docs = [...(this.db.rows.get(this.col) || new Map()).entries()]
      .filter(([, d]) => this.filters.every((f) => { const v = String(f.field).split(".").reduce((acc, k) => (acc == null ? undefined : acc[k]), d); return f.op === "==" ? v === f.value : f.op === ">=" ? v >= f.value : v <= f.value; }))
      .slice(0, this._limit).map(([id, d]) => new FakeDocSnap(id, d));
    return { empty: !docs.length, docs, forEach: (fn) => docs.forEach(fn) };
  }
}
class FakeCollection extends FakeQuery { doc(id) { return new FakeDocRef(this.db, this.col, id || `doc-${++this.db.nextId}`); } }
class FakeDb {
  constructor(seed = {}) { this.rows = new Map(Object.entries(seed).map(([c, docs]) => [c, new Map(Object.entries(docs))])); this.nextId = 0; }
  collection(col) { if (!this.rows.has(col)) this.rows.set(col, new Map()); return new FakeCollection(this, col); }
  _get(col, id) { return this.rows.get(col)?.get(id) || null; }
  _set(col, id, data) { if (!this.rows.has(col)) this.rows.set(col, new Map()); this.rows.get(col).set(id, data); }
  async runTransaction(fn) { return fn({ get: (ref) => ref.get(), create: (ref, data) => ref.set(data), set: (ref, data, options) => ref.set(data, options) }); }
}

// Friday Nov 13, 2026: the drive ended at the Raging Bull at 7:34:19 pm, nobody had saved the place, and the
// next sign of life was 20 km away (home) at 10:17 pm. Toronto is UTC-5.
const ME = "marwan@example.com";
const PHONE = "+14375550123";
const RB = { latitude: 43.546936, longitude: -80.296852 };
const HOME = { latitude: 43.70636, longitude: -80.393547 };
const ARRIVED = Date.parse("2026-11-14T00:34:19Z");
const LEFT = Date.parse("2026-11-14T02:57:10Z");
const pubAnswer = { osm_type: "way", osm_id: 1, category: "amenity", type: "pub", name: "Raging Bull Sports Bar", address: { house_number: "25", road: "Macdonell Street", city: "Guelph" } };
const fetchPub = async () => ({ ok: true, status: 200, json: async () => pubAnswer });
const assistant = async () => ({ inboundRef: { id: "in" }, outboundRef: { id: "out" }, outboundMeta: { logEntryId: "log-1" } });
function seededDb(extra = {}) {
  return new FakeDb({ appMembers: { [ME]: { active: true, approvedPhoneE164: PHONE, shortcutIntegration: { enabled: true } } }, logEntries: { "log-1": { rawText: "note about Raging Bull Sports Bar" } }, ...extra });
}
const member = (db) => ({ email: ME, data: db._get("appMembers", ME) });
function deps(overrides = {}) {
  const texts = []; const delivered = [];
  return { texts, delivered, processAssistantMessage: assistant, openaiKey: null, runId: "t", logger: { warn() {}, info() {} }, fetchImpl: fetchPub, sendSms: async (to, body) => texts.push({ to, body }), timeLeftLifeEventDelivery: async ({ event }) => delivered.push(event), now: () => LEFT + 60000, ...overrides };
}
const stop = (extra = {}) => ({ kind: "finish", driveId: "d1", atMs: ARRIVED, ...RB, ...extra });
const events = (db, type) => [...(db.rows.get("iosShortcutEvents") || new Map()).entries()].filter(([, e]) => !type || e.eventType === type).map(([id, e]) => ({ id, ...e }));
const places = (db) => [...(db.rows.get("knownPlaces") || new Map()).values()];
const pending = (db) => [...(db.rows.get("pendingStops") || new Map()).values()];

test("a stop at an unknown spot is remembered, and leaving 2 h 23 min later names it, saves it, logs the visit and texts once", async () => {
  const db = seededDb();
  const d = deps();
  const opened = await openPendingStop({ db, FieldValue, member: member(db), stop: stop(), deps: d });
  assert.equal(opened.status, "open");
  assert.equal(pending(db).length, 1);

  // A ping while still parked only refreshes "last seen".
  const still = await applyEvidenceToPendingStop({ db, FieldValue, member: member(db), deps: d, evidence: { ...RB, atMs: ARRIVED + 3600000, eventType: "ping", accuracyMeters: 10 } });
  assert.equal(still.status, "open");
  assert.equal(places(db).length, 0);

  const settled = await applyEvidenceToPendingStop({ db, FieldValue, member: member(db), deps: d, evidence: { ...HOME, atMs: Date.parse("2026-11-14T03:17:49Z"), eventType: "arrive_home", label: "Home" } });
  assert.equal(settled.status, "logged");
  assert.equal(settled.placeName, "Raging Bull Sports Bar");
  assert.equal(settled.activity, "Eating out");
  assert.equal(settled.texted, true);

  const [place] = places(db);
  assert.equal(place.name, "Raging Bull Sports Bar");
  assert.equal(place.activity, "Eating out");
  assert.equal(place.activitySource, "suggested");
  assert.equal(place.confirmation, "pending");
  assert.equal(place.visitCount, 1);
  assert.ok(place.lastVisitDurationMinutes >= 140 && place.lastVisitDurationMinutes <= 165, String(place.lastVisitDurationMinutes));

  const arrive = events(db, "arrive_location")[0];
  const leave = events(db, "leave_location")[0];
  assert.equal(arrive.locationLabel, "Raging Bull Sports Bar");
  assert.equal(arrive.placeActivity, "Eating out");
  assert.equal(arrive.eventAtMs, ARRIVED);
  assert.match(arrive.notes, /named from OpenStreetMap/);
  assert.equal(leave.stayStartedAtIso, new Date(ARRIVED).toISOString());
  assert.match(leave.notes, /Stayed 2 h \d+ min at Raging Bull Sports Bar/);
  assert.deepEqual(d.delivered.map((e) => e.eventType), ["arrive_location", "leave_location"]);

  assert.equal(d.texts.length, 1);
  assert.equal(d.texts[0].to, PHONE);
  assert.match(d.texts[0].body, /^New place logged: Raging Bull Sports Bar \(Guelph\), Fri 7:34 pm to 9:\d\d pm \(2 h \d+ min\), as "Eating out"\. Reply OK to keep it, or PLACE name, activity to rename/);
  const confirmation = db._get("placeConfirmations", PHONE);
  assert.equal(confirmation.placeName, "Raging Bull Sports Bar");
  assert.equal(confirmation.eventIds.length, 2);
  assert.equal(pending(db)[0].status, "logged");
});

test("replying PLACE renames the place, sets the activity, fixes the events and re-sends them; OK keeps it", async () => {
  const db = seededDb();
  const d = deps();
  await openPendingStop({ db, FieldValue, member: member(db), stop: stop(), deps: d });
  await applyEvidenceToPendingStop({ db, FieldValue, member: member(db), deps: d, evidence: { ...HOME, atMs: LEFT + 20 * 60000, eventType: "ping" } });
  const unrelated = await handlePlaceReply({ db, FieldValue, from: PHONE, body: "log note: the bulkhead at grid H is done", deps: d });
  assert.equal(unrelated.handled, false, "ordinary texts still go to the assistant");

  const renamed = await handlePlaceReply({ db, FieldValue, from: PHONE, body: "PLACE Raging Bull, Darts", deps: d });
  assert.equal(renamed.handled, true);
  assert.match(renamed.reply, /^Saved: Raging Bull \(Darts\)/);
  const [place] = places(db);
  assert.equal(place.name, "Raging Bull");
  assert.equal(place.activity, "Darts");
  assert.equal(place.activitySource, "set");
  assert.equal(place.confirmation, "renamed");
  for (const event of events(db)) {
    assert.equal(event.locationLabel, "Raging Bull");
    assert.equal(event.placeActivity, "Darts");
    assert.equal(event.timeLeftDelivery.status, "retryable_failure");
  }
  assert.equal(db._get("logEntries", "log-1").rawText, "note about Raging Bull");
  assert.equal(db._get("placeConfirmations", PHONE), null);
  assert.equal((await handlePlaceReply({ db, FieldValue, from: PHONE, body: "ok", deps: d })).handled, false, "nothing pending any more");

  const again = seededDb();
  const d2 = deps();
  await openPendingStop({ db: again, FieldValue, member: member(again), stop: stop(), deps: d2 });
  await applyEvidenceToPendingStop({ db: again, FieldValue, member: member(again), deps: d2, evidence: { ...HOME, atMs: LEFT + 20 * 60000, eventType: "ping" } });
  const kept = await handlePlaceReply({ db: again, FieldValue, from: PHONE, body: "OK", deps: d2 });
  assert.match(kept.reply, /^Kept: Raging Bull Sports Bar \(Eating out\)/);
  assert.equal(places(again)[0].confirmation, "confirmed");
});

test("a short stop, a stop OwnTracks covered as a region, and a stop you logged by hand are not named", async () => {
  const d = deps();
  const short = seededDb();
  await openPendingStop({ db: short, FieldValue, member: member(short), stop: stop(), deps: d });
  const r1 = await applyEvidenceToPendingStop({ db: short, FieldValue, member: member(short), deps: d, evidence: { ...HOME, atMs: ARRIVED + 10 * 60000, eventType: "ping" } });
  assert.equal(r1.status, "short");
  assert.equal(places(short).length, 0);

  const region = seededDb({ iosShortcutEvents: { crossing: { memberEmail: ME, eventType: "arrive_home", eventAtMs: ARRIVED + 2 * 60000 } } });
  await openPendingStop({ db: region, FieldValue, member: member(region), stop: stop(), deps: d });
  const r2 = await applyEvidenceToPendingStop({ db: region, FieldValue, member: member(region), deps: d, evidence: { latitude: 43.9, longitude: -80.1, atMs: LEFT, eventType: "ping" } });
  assert.equal(r2.status, "region");
  assert.equal(places(region).length, 0);

  const manual = seededDb();
  await openPendingStop({ db: manual, FieldValue, member: member(manual), stop: stop(), deps: d });
  assert.equal(await adoptPendingStopAt({ db: manual, FieldValue, member: member(manual), ...RB, deps: d }), true);
  assert.equal(pending(manual)[0].status, "named_manually");
  assert.equal(await applyEvidenceToPendingStop({ db: manual, FieldValue, member: member(manual), deps: d, evidence: { ...HOME, atMs: LEFT, eventType: "ping" } }), null);
  assert.equal(d.texts.length, 0);
});

test("a drive starting at the stop is the departure, and no more than three new-place texts go out a day", async () => {
  const db = seededDb({ unknownStopState: { [ME]: { textsDateKey: "2026-11-13", textsToday: MAX_TEXTS_PER_DAY } } });
  const d = deps();
  await openPendingStop({ db, FieldValue, member: member(db), stop: stop(), deps: d });
  await applyEvidenceToPendingStop({ db, FieldValue, member: member(db), deps: d, evidence: { ...RB, atMs: LEFT, eventType: "start_drive" } });
  assert.equal(pending(db)[0].driveStartedAtMs, LEFT);
  const settled = await applyEvidenceToPendingStop({ db, FieldValue, member: member(db), deps: d, evidence: { ...HOME, atMs: LEFT + 21 * 60000, eventType: "ping" } });
  assert.equal(settled.status, "logged");
  assert.equal(pending(db)[0].basis, "drive");
  assert.match(events(db, "leave_location")[0].notes, /left at 9:57 pm, when you started driving/);
  assert.equal(settled.texted, false, "the day's three texts were already used");
  assert.equal(places(db).length, 1, "the place is still saved and the visit logged");
});

test("parsePlaceReply", () => {
  assert.deepEqual(parsePlaceReply("ok"), { kind: "keep" });
  assert.deepEqual(parsePlaceReply("Yes!"), { kind: "keep" });
  assert.deepEqual(parsePlaceReply("PLACE Raging Bull, Darts"), { kind: "rename", name: "Raging Bull", activity: "Darts" });
  assert.deepEqual(parsePlaceReply("place: Layal's house"), { kind: "rename", name: "Layal's house", activity: "" });
  assert.equal(parsePlaceReply("Okay so the drywall is late"), null);
  assert.equal(parsePlaceReply("log note: done"), null);
});
