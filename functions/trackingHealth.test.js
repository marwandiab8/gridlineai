const test = require("node:test");
const assert = require("node:assert/strict");
const { assessTrackingHealth, runTrackingHealthCheck, SILENT_AFTER_MS, REPEAT_ALERT_AFTER_MS } = require("./trackingHealth");

const FieldValue = { serverTimestamp: () => new Date("2026-11-15T14:00:00.000Z") };
class FakeDocSnap {
  constructor(id, data) { this.id = id; this._data = data; this.exists = data != null; }
  data() { return this._data; }
}
class FakeDocRef {
  constructor(db, col, id) { Object.assign(this, { db, col, id }); }
  async get() { return new FakeDocSnap(this.id, this.db._get(this.col, this.id)); }
  async set(data, options = {}) { this.db._set(this.col, this.id, { ...(options.merge ? this.db._get(this.col, this.id) || {} : {}), ...data }); }
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
class FakeCollection extends FakeQuery { doc(id) { return new FakeDocRef(this.db, this.col, id); } }
class FakeDb {
  constructor(seed = {}) { this.rows = new Map(Object.entries(seed).map(([c, docs]) => [c, new Map(Object.entries(docs))])); }
  collection(col) { if (!this.rows.has(col)) this.rows.set(col, new Map()); return new FakeCollection(this, col); }
  _get(col, id) { return this.rows.get(col)?.get(id) || null; }
  _set(col, id, data) { if (!this.rows.has(col)) this.rows.set(col, new Map()); this.rows.get(col).set(id, data); }
}

const ME = "marwan@example.com";
const NOW = Date.parse("2026-11-15T20:30:00Z"); // Sunday 3:30 pm Toronto
const H = 3600000;
const member = { active: true, approvedPhoneE164: "+14375550123", shortcutIntegration: { enabled: true } };
const db = (extra = {}) => new FakeDb({ appMembers: { [ME]: member }, ...extra });
const ev = (eventType, atMs, extra = {}) => ({ memberEmail: ME, eventType, eventAtMs: atMs, source: "owntracks", timeLeftDelivery: { status: "delivered" }, ...extra });

test("a phone that last reported two hours ago is healthy, and nothing is texted", async () => {
  const d = db({ driveDetection: { [ME]: { status: "idle", lastPointAtMs: NOW - 2 * H } } });
  const texts = [];
  const summary = await runTrackingHealthCheck({ db: d, FieldValue, nowMs: NOW, sendSms: async (to, body) => texts.push({ to, body }) });
  assert.deepEqual(summary, { checked: 1, withIssues: 0, alerted: 0 });
  assert.deepEqual(texts, []);
  assert.equal(d._get("trackingHealth", ME).ok, true);
});

test("OwnTracks silent since Friday night is reported once, then again only after a day", async () => {
  const friday = Date.parse("2026-11-14T03:19:17Z"); // the real last ping
  const d = db({ driveDetection: { [ME]: { status: "idle", lastPointAtMs: friday } } });
  const texts = [];
  const send = async (to, body) => texts.push({ to, body });
  await runTrackingHealthCheck({ db: d, FieldValue, nowMs: NOW, sendSms: send });
  assert.equal(texts.length, 1);
  assert.equal(texts[0].to, "+14375550123");
  assert.match(texts[0].body, /^Tracking check: No location data since Fri 10:19 pm \(41 h\)\. Open OwnTracks once/);
  await runTrackingHealthCheck({ db: d, FieldValue, nowMs: NOW + 9 * H, sendSms: send });
  assert.equal(texts.length, 1, "the same problem is not texted twice in a day");
  await runTrackingHealthCheck({ db: d, FieldValue, nowMs: NOW + REPEAT_ALERT_AFTER_MS + H, sendSms: send });
  assert.equal(texts.length, 2);
  assert.ok(NOW - friday > SILENT_AFTER_MS);
});

test("an arrival at work with no departure for 20 hours is an issue; a later departure clears it", async () => {
  const open = db({ driveDetection: { [ME]: { lastPointAtMs: NOW - H } }, iosShortcutEvents: { a: ev("arrive_work", NOW - 20 * H) } });
  const a = await assessTrackingHealth({ db: open, memberEmail: ME, nowMs: NOW });
  assert.deepEqual(a.issues.map((i) => i.code), ["open_arrive_work"]);
  assert.match(a.issues[0].text, /Arrived at work Sat 7:30 pm and no departure/);
  const closed = db({ driveDetection: { [ME]: { lastPointAtMs: NOW - H } }, iosShortcutEvents: { a: ev("arrive_work", NOW - 20 * H), l: ev("leave_work", NOW - 12 * H) } });
  assert.equal((await assessTrackingHealth({ db: closed, memberEmail: ME, nowMs: NOW })).ok, true);
});

test("events that did not reach Time Left in the last day are counted by status", async () => {
  const d = db({
    driveDetection: { [ME]: { lastPointAtMs: NOW - H } },
    iosShortcutEvents: {
      ok: ev("start_drive", NOW - 5 * H),
      dup: ev("finish_drive", NOW - 4 * H, { timeLeftDelivery: { status: "duplicate" } }),
      conflict: ev("arrive_location", NOW - 3 * H, { timeLeftDelivery: { status: "conflict" } }),
      unsent: ev("leave_location", NOW - 2 * H, { timeLeftDelivery: null }),
      fresh: ev("arrive_home", NOW - 10 * 60000, { timeLeftDelivery: null }),
      old: ev("arrive_location", NOW - 30 * H, { timeLeftDelivery: { status: "conflict" } }),
      someoneElse: { memberEmail: "other@example.com", eventType: "arrive_home", eventAtMs: NOW - H, timeLeftDelivery: { status: "conflict" } },
    },
  });
  const a = await assessTrackingHealth({ db: d, memberEmail: ME, nowMs: NOW });
  assert.deepEqual(a.issues.map((i) => i.code), ["delivery"]);
  assert.equal(a.issues[0].text, "2 events from the last day did not reach Time Left (conflict 1, not sent 1).");
});

test("a drive left open for hours with no pings is flagged", async () => {
  const d = db({ driveDetection: { [ME]: { status: "driving", lastPointAtMs: NOW - 4 * H } } });
  const a = await assessTrackingHealth({ db: d, memberEmail: ME, nowMs: NOW });
  assert.deepEqual(a.issues.map((i) => i.code), ["stuck_drive"]);
});
