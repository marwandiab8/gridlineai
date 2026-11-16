const test = require("node:test");
const assert = require("node:assert/strict");
const { isHeartbeat, stampHeartbeat, recordHeartbeat, recordHealthHeartbeat } = require("./heartbeat");
const { assessTrackingHealth, alertIssueOnce } = require("./trackingHealth");
const { hashShortcutToken } = require("./iosShortcutsIntegration");
const { handleOwnTracksEventRequest } = require("./ownTracksIntegration");

const FieldValue = { serverTimestamp: () => new Date("2026-11-15T12:00:00.000Z"), delete: () => ({ __delete: true }), arrayUnion: (...values) => ({ __arrayUnion: values }) };
class FakeDocSnap { constructor(id, data) { this.id = id; this._data = data; this.exists = data != null; } data() { return this._data; } }
class FakeDocRef {
  constructor(db, col, id) { Object.assign(this, { db, col, id }); }
  async get() { return new FakeDocSnap(this.id, this.db._get(this.col, this.id)); }
  async set(data, options = {}) { const out = { ...(options.merge ? this.db._get(this.col, this.id) || {} : {}) }; for (const [k, v] of Object.entries(data || {})) { if (v && v.__delete) delete out[k]; else out[k] = v; } this.db._set(this.col, this.id, out); }
  async delete() { this.db.rows.get(this.col)?.delete(this.id); }
}
class FakeQuery {
  constructor(db, col) { Object.assign(this, { db, col, filters: [], _limit: Infinity }); }
  where(field, op, value) { this.filters.push({ field, op, value }); return this; }
  limit(n) { this._limit = n; return this; }
  async get() { const docs = [...(this.db.rows.get(this.col) || new Map()).entries()].filter(([, d]) => this.filters.every((f) => { const v = String(f.field).split(".").reduce((acc, k) => (acc == null ? undefined : acc[k]), d); return f.op === "==" ? v === f.value : f.op === ">=" ? v >= f.value : v <= f.value; })).slice(0, this._limit).map(([id, d]) => new FakeDocSnap(id, d)); return { empty: !docs.length, docs, forEach: (fn) => docs.forEach(fn) }; }
}
class FakeCollection extends FakeQuery { doc(id) { return new FakeDocRef(this.db, this.col, id || `doc-${++this.db.nextId}`); } }
class FakeDb {
  constructor(seed = {}) { this.rows = new Map(Object.entries(seed).map(([c, docs]) => [c, new Map(Object.entries(docs))])); this.nextId = 0; }
  collection(col) { if (!this.rows.has(col)) this.rows.set(col, new Map()); return new FakeCollection(this, col); }
  _get(col, id) { return this.rows.get(col)?.get(id) || null; }
  _set(col, id, data) { if (!this.rows.has(col)) this.rows.set(col, new Map()); this.rows.get(col).set(id, data); }
  async runTransaction(fn) { return fn({ get: (ref) => ref.get(), create: (ref, data) => ref.set(data), set: (ref, data, options) => ref.set(data, options) }); }
}

const ME = "marwan@example.com";
const PHONE = "+14375550123";
const TOKEN = "hb-token";
const HOME = { latitude: 43.70636, longitude: -80.393547 };
const WORK = { latitude: 43.6, longitude: -80.3 }; // ~14 km from home
const H = 3600000;
const NOW = Date.parse("2026-11-16T12:00:00Z");
const memberDoc = { active: true, approvedPhoneE164: PHONE, shortcutIntegration: { enabled: true, tokenHash: hashShortcutToken(TOKEN) } };
const member = { email: ME, data: memberDoc };
const driveAtHome = (lastMs) => ({ status: "idle", lastPointAtMs: lastMs, state: { status: "idle", last: { ...HOME, atMs: lastMs, accuracyMeters: 5 } } });
const beat = (coords, tst) => ({ _type: "location", t: "h", tid: "HB", lat: coords.latitude, lon: coords.longitude, ...(tst ? { tst } : {}) });

test("a heartbeat is told apart from an OwnTracks ping and stamped with the time when the Shortcut sends none", () => {
  assert.equal(isHeartbeat(beat(HOME)), true);
  assert.equal(isHeartbeat({ _type: "location", lat: 1, lon: 2, t: "p" }), false);
  assert.equal(isHeartbeat({ _type: "transition", t: "h" }), false);
  const stamped = stampHeartbeat({ _type: "location", t: "h", lat: 1, lon: 2 }, NOW);
  assert.equal(stamped.tst, Math.floor(NOW / 1000));
  assert.equal(stampHeartbeat({ _type: "location", t: "h", lat: 1, lon: 2, tst: 1700000000 }, NOW).tst, 1700000000);
});

test("a heartbeat far from where OwnTracks last saw the phone, with OwnTracks quiet for hours, texts once", async () => {
  const db = new FakeDb({ appMembers: { [ME]: memberDoc }, driveDetection: { [ME]: driveAtHome(NOW - 5 * H) } });
  const texts = [];
  const sendSms = async (to, body) => texts.push({ to, body });
  const r = await recordHeartbeat({ db, FieldValue, member, payload: beat(WORK, NOW / 1000), nowMs: NOW, sendSms });
  assert.equal(r.ownTracksStopped, true);
  assert.equal(r.alerted, true);
  assert.equal(texts.length, 1);
  assert.match(texts[0].body, /^Tracking check: Your phone has moved 1\d(\.\d)? km since OwnTracks last reported \(Mon 2:00 am\), so OwnTracks seems to have stopped/);
  assert.equal(db._get("heartbeats", ME).count, 1);
  const again = await recordHeartbeat({ db, FieldValue, member, payload: beat(WORK, (NOW + H) / 1000), nowMs: NOW + H, sendSms });
  assert.equal(again.alerted, false, "the same problem is not texted twice in a day");
  assert.equal(db._get("heartbeats", ME).count, 2);
  // At home, OwnTracks quiet is expected: no text.
  const quiet = new FakeDb({ appMembers: { [ME]: memberDoc }, driveDetection: { [ME]: driveAtHome(NOW - 40 * H) } });
  const r2 = await recordHeartbeat({ db: quiet, FieldValue, member, payload: beat({ latitude: HOME.latitude + 0.0005, longitude: HOME.longitude }, NOW / 1000), nowMs: NOW, sendSms });
  assert.equal(r2.ownTracksStopped, false);
  assert.equal(texts.length, 1);
});

test("the scheduled check reads the heartbeat: silence at home is fine, a silent phone or a stopped OwnTracks is not", async () => {
  const quietAtHome = new FakeDb({ driveDetection: { [ME]: driveAtHome(NOW - 41 * H) }, heartbeats: { [ME]: { lastAtMs: NOW - 2 * H, ...HOME } } });
  assert.equal((await assessTrackingHealth({ db: quietAtHome, memberEmail: ME, nowMs: NOW })).ok, true, "OwnTracks quiet for 41 h while the heartbeat shows the phone at the same spot");
  const phoneSilent = new FakeDb({ driveDetection: { [ME]: driveAtHome(NOW - 2 * H) }, heartbeats: { [ME]: { lastAtMs: NOW - 15 * H, ...HOME } } });
  assert.deepEqual((await assessTrackingHealth({ db: phoneSilent, memberEmail: ME, nowMs: NOW })).issues.map((i) => i.code), ["heartbeat_silent"]);
  const stopped = new FakeDb({ driveDetection: { [ME]: driveAtHome(NOW - 5 * H) }, heartbeats: { [ME]: { lastAtMs: NOW - H, ...WORK } } });
  assert.deepEqual((await assessTrackingHealth({ db: stopped, memberEmail: ME, nowMs: NOW })).issues.map((i) => i.code), ["owntracks_dead"]);
});

test("through the OwnTracks endpoint: a heartbeat is recorded and kept out of the drive detector", async () => {
  const db = new FakeDb({ appMembers: { [ME]: memberDoc }, driveDetection: { [ME]: driveAtHome(NOW - 2 * H) } });
  const response = { statusCode: 200, body: null, status(c) { this.statusCode = c; return this; }, set() { return this; }, json(b) { this.body = b; return this; } };
  await handleOwnTracksEventRequest({
    db, FieldValue,
    req: { method: "POST", body: beat(HOME), query: {}, get: (n) => (String(n).toLowerCase() === "authorization" ? `Bearer ${TOKEN}` : "") },
    res: response, logger: { error() {}, warn() {}, info() {} }, openaiKey: null, processAssistantMessage: async () => ({}), timeLeftLifeEventDelivery: async () => {},
  });
  assert.deepEqual(response.body, []);
  const hb = db._get("heartbeats", ME);
  assert.ok(hb && Math.abs(hb.lastAtMs - Date.now()) < 60000, "stamped with now");
  assert.equal(db._get("driveDetection", ME).lastPointAtMs, NOW - 2 * H, "the drive detector did not see the heartbeat");
});

test("alertIssueOnce records the issue so the scheduled check does not repeat it", async () => {
  const db = new FakeDb({});
  const texts = [];
  const sendSms = async (to, body) => texts.push(body);
  assert.equal(await alertIssueOnce({ db, FieldValue, member, issue: { code: "x", text: "X happened." }, sendSms, nowMs: NOW }), true);
  assert.equal(await alertIssueOnce({ db, FieldValue, member, issue: { code: "x", text: "X happened." }, sendSms, nowMs: NOW + H }), false);
  assert.equal(await alertIssueOnce({ db, FieldValue, member, issue: { code: "y", text: "Y happened." }, sendSms, nowMs: NOW + H }), true);
  assert.equal(db._get("trackingHealth", ME).lastAlertCodes, "x,y");
  assert.equal(texts.length, 2);
});

test("a Health Auto Export export is a heartbeat too: thousands of steps while OwnTracks is quiet mean the phone went somewhere", async () => {
  const db = new FakeDb({ appMembers: { [ME]: memberDoc }, driveDetection: { [ME]: driveAtHome(NOW - 9 * H) } });
  const texts = [];
  const sendSms = async (to, body) => texts.push(body);
  const first = await recordHealthHeartbeat({ db, FieldValue, member, stepsByDate: { "2026-11-16": 1200 }, nowMs: NOW - 3 * H, sendSms });
  assert.deepEqual([first.delta, first.stepsSinceOwnTracks, first.ownTracksStopped], [1200, 1200, false]);
  const second = await recordHealthHeartbeat({ db, FieldValue, member, stepsByDate: { "2026-11-15": 8000, "2026-11-16": 4500 }, nowMs: NOW, sendSms });
  assert.deepEqual([second.delta, second.stepsSinceOwnTracks, second.ownTracksStopped, second.alerted], [3300, 4500, true, true]);
  assert.match(texts[0], /^Tracking check: You have taken 4,500 steps since OwnTracks last reported \(Sun 10:00 pm, 9 h ago\), so OwnTracks seems to have stopped/);
  const hb = db._get("heartbeats", ME);
  assert.equal(hb.healthCount, 2);
  assert.equal(hb.healthStepsTotal, 4500);
  assert.equal(hb.healthLastAtMs, NOW);
  // OwnTracks reports again: the count starts over.
  db._set("driveDetection", ME, driveAtHome(NOW + H));
  const third = await recordHealthHeartbeat({ db, FieldValue, member, stepsByDate: { "2026-11-16": 4600 }, nowMs: NOW + 2 * H, sendSms });
  assert.deepEqual([third.delta, third.stepsSinceOwnTracks, third.ownTracksStopped], [100, 100, false]);
  assert.equal(texts.length, 1);
});

test("the scheduled check reads the Health Auto Export heartbeat: few steps at home is fine, many steps with OwnTracks quiet is not", async () => {
  const quiet = new FakeDb({ driveDetection: { [ME]: driveAtHome(NOW - 41 * H) }, heartbeats: { [ME]: { healthLastAtMs: NOW - 2 * H, healthStepsSinceOwnTracks: 800 } } });
  assert.equal((await assessTrackingHealth({ db: quiet, memberEmail: ME, nowMs: NOW })).ok, true, "41 h of OwnTracks silence with 800 steps is a weekend at home");
  const walked = new FakeDb({ driveDetection: { [ME]: driveAtHome(NOW - 9 * H) }, heartbeats: { [ME]: { healthLastAtMs: NOW - 2 * H, healthStepsSinceOwnTracks: 3500 } } });
  assert.deepEqual((await assessTrackingHealth({ db: walked, memberEmail: ME, nowMs: NOW })).issues.map((i) => i.code), ["owntracks_dead"]);
  const noExport = new FakeDb({ driveDetection: { [ME]: driveAtHome(NOW - 2 * H) }, heartbeats: { [ME]: { healthLastAtMs: NOW - 30 * H, healthStepsSinceOwnTracks: 0 } } });
  assert.deepEqual((await assessTrackingHealth({ db: noExport, memberEmail: ME, nowMs: NOW })).issues.map((i) => i.code), ["health_export_silent"]);
});
