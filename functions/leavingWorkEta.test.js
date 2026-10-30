const test = require("node:test");
const assert = require("node:assert/strict");
const { hashShortcutToken } = require("./iosShortcutsIntegration");
const {
  buildEtaMessage,
  buildSpokenReply,
  parseRouteDuration,
  fetchDrivingEta,
  estimateDrive,
  resolveHome,
  resolveOrigin,
  resolveEtaConfig,
  decideFallback,
  handleLeavingWorkEtaRequest,
  noteWorkExitForEtaFallback,
  evaluateEtaFallbackOnPing,
  relayEtaContactReply,
  siriSentRecently,
  localHour,
} = require("./leavingWorkEta");

// ---- minimal fake Firestore (dotted-path where(), ranges, limit, merge set, subcollections, transactions)

const FieldValue = {
  serverTimestamp: () => new Date("2026-10-30T20:05:00.000Z"),
  arrayUnion: (...values) => ({ __arrayUnion: values }),
};

function getPath(obj, path) {
  return String(path).split(".").reduce((o, k) => (o == null ? undefined : o[k]), obj);
}

let autoId = 0;

class FakeDocSnap {
  constructor(id, data) {
    this.id = id;
    this._data = data;
    this.exists = data != null;
  }
  data() {
    return this._data == null ? undefined : { ...this._data };
  }
}

class FakeQuery {
  constructor(db, col) {
    this.db = db;
    this.col = col;
    this.filters = [];
    this._limit = Infinity;
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
    const docs = [];
    for (const [id, data] of this.db._col(this.col).entries()) {
      const ok = this.filters.every(({ field, op, value }) => {
        const actual = getPath(data, field);
        if (op === "==") return actual === value;
        if (op === ">=") return actual >= value;
        if (op === "<=") return actual <= value;
        throw new Error(`unsupported op ${op}`);
      });
      if (ok) docs.push(new FakeDocSnap(id, data));
      if (docs.length >= this._limit) break;
    }
    return { empty: docs.length === 0, docs, forEach: (fn) => docs.forEach(fn) };
  }
}

class FakeCollection extends FakeQuery {
  doc(id) {
    return new FakeDocRef(this.db, this.col, id || `auto-${++autoId}`);
  }
  async add(data) {
    const ref = this.doc();
    await ref.set(data);
    return ref;
  }
}

class FakeDocRef {
  constructor(db, col, id) {
    this.db = db;
    this.col = col;
    this.id = id;
  }
  collection(sub) {
    return new FakeCollection(this.db, `${this.col}/${this.id}/${sub}`);
  }
  async get() {
    return new FakeDocSnap(this.id, this.db._get(this.col, this.id));
  }
  _setSync(data, options = {}) {
    const current = options.merge ? this.db._get(this.col, this.id) || {} : {};
    this.db._set(this.col, this.id, { ...current, ...data });
  }
  async set(data, options) {
    this._setSync(data, options);
  }
  async update(data) {
    this._setSync(data, { merge: true });
  }
}

class FakeDb {
  constructor() {
    this.rows = new Map();
  }
  _col(col) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    return this.rows.get(col);
  }
  _get(col, id) {
    const data = this._col(col).get(id);
    return data === undefined ? null : data;
  }
  _set(col, id, data) {
    this._col(col).set(id, data);
  }
  collection(col) {
    return new FakeCollection(this, col);
  }
  async runTransaction(fn) {
    const tx = {
      get: async (ref) => ref.get(),
      set: (ref, data, options) => ref._setSync(data, options),
      create: (ref, data) => ref._setSync(data),
    };
    return fn(tx);
  }
}

// ---- fixtures

const TOKEN = "glsc_test_token";
const EMAIL = "mdiab@mathesonconstructors.com";
const SITE = { latitude: 43.6532, longitude: -79.7331 }; // a Peel-ish site
const HOME = { latitude: 43.7055, longitude: -80.3774 }; // Fergus
const CONTACT = "+15195550199";
const MEMBER_PHONE = "+15195550100";
const NOW = new Date("2026-10-30T20:09:00.000Z"); // 4:09 PM Toronto (EDT)

function seedMember(db, overrides = {}) {
  db._set("appMembers", EMAIL, {
    displayName: "Marwan Diab",
    approvedPhoneE164: MEMBER_PHONE,
    shortcutIntegration: { tokenHash: hashShortcutToken(TOKEN), enabled: true },
    etaNotify: { contactName: "Ashley", contactPhoneE164: CONTACT, home: HOME, ...overrides.etaNotify },
    ...overrides.member,
  });
  return { email: EMAIL, data: db._get("appMembers", EMAIL) };
}

function fakeReq({ body = {}, headers = {}, method = "POST" } = {}) {
  const lower = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]));
  return { method, body, get: (name) => lower[String(name).toLowerCase()] || "" };
}

function fakeRes() {
  const res = { statusCode: null, body: null, headers: {} };
  res.status = (code) => {
    res.statusCode = code;
    return res;
  };
  res.set = (k, v) => {
    res.headers[k] = v;
    return res;
  };
  res.json = (payload) => {
    res.body = payload;
    return res;
  };
  res.send = (payload) => {
    res.body = payload;
    return res;
  };
  return res;
}

function fakeSms() {
  const sent = [];
  return {
    sent,
    client: { messages: { create: async (msg) => (sent.push(msg), { sid: `SM${sent.length}`, status: "queued" }) } },
  };
}

function routesFetch(seconds, { distanceMeters = 61000, status = 200, staticSeconds = seconds - 300 } = {}) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url, init: { ...init, body: JSON.parse(init.body) } });
    const ok = status >= 200 && status < 300;
    return {
      ok,
      status,
      json: async () =>
        ok
          ? { routes: [{ duration: `${seconds}s`, staticDuration: `${staticSeconds}s`, distanceMeters }] }
          : { error: { status: "PERMISSION_DENIED", message: "nope" } },
    };
  };
  return { fetchImpl, calls };
}

// ---- formatting

test("buildEtaMessage reads exactly as asked, with the local arrival clock", () => {
  const at = new Date("2026-10-30T20:51:00.000Z");
  assert.equal(buildEtaMessage({ firstName: "Marwan", minutes: 42, arriveAt: at, timezone: "America/Toronto" }), "Marwan leaving work, will be home in 42 minutes, at 4:51 PM.");
  assert.equal(buildEtaMessage({ firstName: "Marwan", minutes: 1, arriveAt: at, timezone: "America/Toronto" }), "Marwan leaving work, will be home in 1 minute, at 4:51 PM.");
  assert.equal(buildEtaMessage({ firstName: "Marwan", minutes: 68, arriveAt: at, timezone: "America/Toronto", estimated: true }), "Marwan leaving work, will be home in about 68 minutes, at 4:51 PM.");
  assert.equal(buildSpokenReply({ contactName: "Ashley", minutes: 42, arriveAt: at, timezone: "America/Toronto" }), "Told Ashley you'll be home in 42 minutes, around 4:51 PM.");
});

test("localHour respects the member's timezone", () => {
  assert.equal(localHour(new Date("2026-10-30T20:09:00.000Z"), "America/Toronto"), 16);
  assert.equal(localHour(new Date("2026-10-30T18:30:00.000Z"), "America/Toronto"), 14);
});

// ---- route lookup

test("parseRouteDuration handles the Routes API's string and object forms", () => {
  assert.equal(parseRouteDuration("1832s"), 1832);
  assert.equal(parseRouteDuration("1832.5s"), 1832.5);
  assert.equal(parseRouteDuration({ seconds: 90 }), 90);
  assert.equal(parseRouteDuration(null), null);
});

test("fetchDrivingEta calls computeRoutes with traffic and returns its duration", async () => {
  const { fetchImpl, calls } = routesFetch(2520, { distanceMeters: 63400 });
  const out = await fetchDrivingEta({ origin: SITE, destination: HOME, apiKey: "key", fetchImpl });
  assert.deepEqual(out, { seconds: 2520, distanceMeters: 63400, staticSeconds: 2220, source: "google_routes" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://routes.googleapis.com/directions/v2:computeRoutes");
  assert.equal(calls[0].init.headers["X-Goog-Api-Key"], "key");
  assert.match(calls[0].init.headers["X-Goog-FieldMask"], /routes\.duration/);
  assert.equal(calls[0].init.body.routingPreference, "TRAFFIC_AWARE");
  assert.equal(calls[0].init.body.travelMode, "DRIVE");
  assert.deepEqual(calls[0].init.body.destination.location.latLng, HOME);
});

test("fetchDrivingEta falls back to a straight-line estimate without a key or on an API error", async () => {
  const noKey = await fetchDrivingEta({ origin: SITE, destination: HOME, apiKey: null, fetchImpl: async () => { throw new Error("must not be called"); } });
  assert.equal(noKey.source, "estimate");
  assert.deepEqual(noKey, estimateDrive(SITE, HOME));
  const warned = [];
  const { fetchImpl } = routesFetch(0, { status: 403 });
  const failed = await fetchDrivingEta({ origin: SITE, destination: HOME, apiKey: "key", fetchImpl, logger: { warn: (m, d) => warned.push([m, d]) } });
  assert.equal(failed.source, "estimate");
  assert.ok(failed.seconds > 0);
  assert.equal(warned.length, 1);
  assert.match(warned[0][1].message, /PERMISSION_DENIED/);
});

// ---- home and origin resolution

test("resolveHome prefers etaNotify.home, then homePlaceId, then a known place named Home", async () => {
  const db = new FakeDb();
  db._set("knownPlaces", "p-home", { memberEmail: EMAIL, name: "Home", latitude: 1, longitude: 2 });
  db._set("knownPlaces", "p-tagged", { memberEmail: EMAIL, name: "The house", activity: "Home", latitude: 3, longitude: 4 });
  db._set("knownPlaces", "p-other", { memberEmail: EMAIL, name: "Costco", latitude: 5, longitude: 6 });

  const explicit = await resolveHome({ db, member: { email: EMAIL }, config: resolveEtaConfig({ email: EMAIL, data: { etaNotify: { contactPhoneE164: CONTACT, home: HOME } } }) });
  assert.equal(explicit.source, "etaNotify.home");
  assert.equal(explicit.latitude, HOME.latitude);

  const byId = await resolveHome({ db, member: { email: EMAIL }, config: resolveEtaConfig({ email: EMAIL, data: { etaNotify: { contactPhoneE164: CONTACT, homePlaceId: "p-tagged" } } }) });
  assert.equal(byId.source, "etaNotify.homePlaceId");
  assert.equal(byId.latitude, 3);

  const byName = await resolveHome({ db, member: { email: EMAIL }, config: resolveEtaConfig({ email: EMAIL, data: { etaNotify: { contactPhoneE164: CONTACT } } }) });
  assert.equal(byName.source, "knownPlaces.name");
  assert.equal(byName.latitude, 1);

  db._col("knownPlaces").delete("p-home");
  const byActivity = await resolveHome({ db, member: { email: EMAIL }, config: resolveEtaConfig({ email: EMAIL, data: { etaNotify: { contactPhoneE164: CONTACT } } }) });
  assert.equal(byActivity.source, "knownPlaces.activity");

  db._col("knownPlaces").delete("p-tagged");
  assert.equal(await resolveHome({ db, member: { email: EMAIL }, config: resolveEtaConfig({ email: EMAIL, data: { etaNotify: { contactPhoneE164: CONTACT } } }) }), null);
});

test("resolveOrigin prefers the Shortcut's coordinates, then a recent OwnTracks fix, then the Work place", async () => {
  const db = new FakeDb();
  const member = { email: EMAIL, data: {} };
  const nowMs = NOW.getTime();

  const fromShortcut = await resolveOrigin({ db, member, event: { latitude: 10, longitude: 20 }, nowMs });
  assert.deepEqual(fromShortcut, { latitude: 10, longitude: 20, ageMs: 0, source: "shortcut" });

  assert.equal(await resolveOrigin({ db, member, event: { latitude: null, longitude: null }, nowMs }), null);

  db._set("knownPlaces", "p-work", { memberEmail: EMAIL, name: "Work", latitude: SITE.latitude, longitude: SITE.longitude });
  const fromWork = await resolveOrigin({ db, member, event: {}, nowMs });
  assert.equal(fromWork.source, "knownPlaces.work");

  db._set("driveDetection", EMAIL, { state: { last: { latitude: 30, longitude: 40, atMs: nowMs - 2 * 60 * 1000, accuracyMeters: 20 } } });
  const fromFix = await resolveOrigin({ db, member, event: {}, nowMs });
  assert.equal(fromFix.source, "owntracks.last");
  assert.equal(fromFix.latitude, 30);

  db._set("driveDetection", EMAIL, { state: { last: { latitude: 30, longitude: 40, atMs: nowMs - 30 * 60 * 1000, accuracyMeters: 20 } } });
  const staleIgnored = await resolveOrigin({ db, member, event: {}, nowMs });
  assert.equal(staleIgnored.source, "knownPlaces.work");
});

// ---- the Siri endpoint

async function runSiri({ db, body, headers = {}, fetchImpl, sms, recordDeparture, now = NOW, mapsApiKey = "key" }) {
  const req = fakeReq({ body, headers: { "X-Gridline-Shortcut-Token": TOKEN, ...headers } });
  const res = fakeRes();
  await handleLeavingWorkEtaRequest({
    db,
    FieldValue,
    req,
    res,
    logger: { info() {}, warn() {}, error() {} },
    getTwilio: () => ({ smsClient: sms.client, fromPhone: "+15550001111" }),
    mapsApiKey,
    fetchImpl,
    recordDeparture,
    now,
  });
  return res;
}

test("Siri endpoint: texts the contact with the traffic ETA, logs it, records the departure, and answers Siri", async () => {
  const db = new FakeDb();
  seedMember(db);
  const sms = fakeSms();
  const { fetchImpl, calls } = routesFetch(2520, { distanceMeters: 63400 }); // 42 min
  const recorded = [];
  const res = await runSiri({
    db,
    body: { latitude: SITE.latitude, longitude: SITE.longitude, timezone: "America/Toronto" },
    headers: { "Idempotency-Key": "siri-1" },
    fetchImpl,
    sms,
    recordDeparture: async ({ member, event }) => (recorded.push({ member, event }), { duplicate: false, shortcutEventId: "ev-1" }),
  });

  assert.equal(res.statusCode, 200, JSON.stringify(res.body));
  assert.equal(res.body.sent, true);
  assert.equal(res.body.eta_minutes, 42);
  assert.equal(res.body.arrive_at, "4:51 PM");
  assert.equal(res.body.distance_km, 63.4);
  assert.equal(res.body.route_source, "google_routes");
  assert.equal(res.body.origin_source, "shortcut");
  assert.equal(res.body.message, "Marwan leaving work, will be home in 42 minutes, at 4:51 PM.");
  assert.equal(res.body.spoken, "Told Ashley you'll be home in 42 minutes, around 4:51 PM.");
  assert.equal(res.body.departure_logged, true);

  assert.equal(sms.sent.length, 1);
  assert.deepEqual(sms.sent[0], { to: CONTACT, from: "+15550001111", body: res.body.message });
  assert.deepEqual(calls[0].init.body.origin.location.latLng, SITE);

  const logged = [...db._col("messages").values()];
  assert.equal(logged.length, 1);
  assert.equal(logged[0].command, "leaving_work_eta");
  assert.equal(logged[0].to, CONTACT);
  assert.equal(logged[0].direction, "outbound");

  const state = db._get("etaNotifications", EMAIL);
  assert.equal(state.lastSentAtMs, NOW.getTime());
  assert.equal(state.lastIdempotencyKey, "siri-1");
  assert.equal(state.lastSent.trigger, "siri");
  assert.equal(state.lastSent.minutes, 42);
  assert.equal(state.pendingFallback, null);
  assert.equal([...db._col(`etaNotifications/${EMAIL}/sends`).values()].length, 1);

  assert.equal(recorded.length, 1);
  assert.equal(recorded[0].member.email, EMAIL);
  assert.equal(recorded[0].event.eventType, "leave_work");
  assert.equal(recorded[0].event.latitude, SITE.latitude);
  assert.match(recorded[0].event.notes, /Told Ashley: home in 42 min \(4:51 PM\)/);
});

test("Siri endpoint: a second ask within the gap (or the same Idempotency-Key) does not text again", async () => {
  const db = new FakeDb();
  seedMember(db);
  const sms = fakeSms();
  const { fetchImpl } = routesFetch(2520);
  const body = { latitude: SITE.latitude, longitude: SITE.longitude };

  const first = await runSiri({ db, body, headers: { "Idempotency-Key": "k1" }, fetchImpl, sms, recordDeparture: null });
  assert.equal(first.body.sent, true);

  const replay = await runSiri({ db, body, headers: { "Idempotency-Key": "k1" }, fetchImpl, sms, recordDeparture: null, now: new Date(NOW.getTime() + 5000) });
  assert.equal(replay.statusCode, 200);
  assert.equal(replay.body.sent, false);
  assert.equal(replay.body.duplicate, true);
  assert.equal(replay.body.reason, "idempotent_replay");

  const again = await runSiri({ db, body, headers: { "Idempotency-Key": "k2" }, fetchImpl, sms, recordDeparture: null, now: new Date(NOW.getTime() + 10 * 60 * 1000) });
  assert.equal(again.body.sent, false);
  assert.equal(again.body.reason, "sent_recently");
  assert.equal(again.body.arrive_at, "4:51 PM");
  assert.match(again.body.spoken, /^Already told Ashley 10 minutes ago: home around 4:51 PM\.$/);

  const later = await runSiri({ db, body, headers: { "Idempotency-Key": "k3" }, fetchImpl, sms, recordDeparture: null, now: new Date(NOW.getTime() + 50 * 60 * 1000) });
  assert.equal(later.body.sent, true);
  assert.equal(sms.sent.length, 2);
});

test("Siri endpoint: without the Routes key the text still goes out, marked as an estimate", async () => {
  const db = new FakeDb();
  seedMember(db);
  const sms = fakeSms();
  const res = await runSiri({ db, body: { latitude: SITE.latitude, longitude: SITE.longitude }, fetchImpl: null, sms, recordDeparture: null, mapsApiKey: null });
  assert.equal(res.body.sent, true);
  assert.equal(res.body.route_source, "estimate");
  assert.match(res.body.message, /^Marwan leaving work, will be home in about \d+ minutes, at \d{1,2}:\d{2} [AP]M\.$/);
});

test("Siri endpoint: Twilio failure releases the claim and reports sms_failed-style error", async () => {
  const db = new FakeDb();
  seedMember(db);
  const { fetchImpl } = routesFetch(2520);
  const failing = { client: { messages: { create: async () => { const e = new Error("twilio down"); e.status = 502; e.code = "sms_failed"; throw e; } } } };
  const res = await runSiri({ db, body: { latitude: SITE.latitude, longitude: SITE.longitude }, fetchImpl, sms: failing, recordDeparture: null });
  assert.equal(res.statusCode, 502);
  assert.equal(res.body.ok, false);
  assert.equal(res.body.error, "sms_failed");
  assert.equal(db._get("etaNotifications", EMAIL).claimedAtMs, null);
  assert.equal(db._get("etaNotifications", EMAIL).lastSentAtMs, undefined);
});

test("Siri endpoint: rejects bad tokens and unconfigured members with a sentence Siri can say", async () => {
  const db = new FakeDb();
  seedMember(db, { etaNotify: { contactPhoneE164: "" } });
  const sms = fakeSms();
  const res = await runSiri({ db, body: {}, fetchImpl: null, sms, recordDeparture: null });
  assert.equal(res.statusCode, 409);
  assert.equal(res.body.error, "eta_not_configured");
  assert.ok(res.body.spoken);

  const badToken = fakeRes();
  await handleLeavingWorkEtaRequest({ db, FieldValue, req: fakeReq({ headers: { Authorization: "Bearer glsc_wrong" } }), res: badToken, logger: { error() {} }, getTwilio: () => ({}) });
  assert.equal(badToken.statusCode, 401);
  assert.equal(sms.sent.length, 0);
});

test("Siri endpoint: when home is unknown it says so instead of guessing", async () => {
  const db = new FakeDb();
  seedMember(db, { etaNotify: { home: null } });
  const sms = fakeSms();
  const res = await runSiri({ db, body: { latitude: SITE.latitude, longitude: SITE.longitude }, fetchImpl: null, sms, recordDeparture: null });
  assert.equal(res.statusCode, 409);
  assert.equal(res.body.error, "home_not_configured");
  assert.equal(sms.sent.length, 0);
});

// ---- the OwnTracks fallback

const CONFIG = resolveEtaConfig({ email: EMAIL, data: { etaNotify: { contactPhoneE164: CONTACT, home: HOME } } });

function pending(overrides = {}) {
  const armedAtMs = NOW.getTime();
  return {
    armedAtMs,
    exitLatitude: SITE.latitude,
    exitLongitude: SITE.longitude,
    exitDistanceToHomeMeters: 52000,
    home: HOME,
    expiresAtMs: armedAtMs + 60 * 60 * 1000,
    ...overrides,
  };
}

test("decideFallback: waits, then sends only on real progress toward home", () => {
  const later = NOW.getTime() + 9 * 60 * 1000;
  // too soon, even with progress
  assert.equal(decideFallback({ pending: pending(), point: { latitude: 43.66, longitude: -79.90 }, nowMs: NOW.getTime() + 2 * 60 * 1000, config: CONFIG }).action, "waiting");
  // 9 minutes later, 10 km closer -> send
  const send = decideFallback({ pending: pending(), point: { latitude: 43.66, longitude: -79.90, accuracyMeters: 30 }, nowMs: later, config: CONFIG });
  assert.equal(send.action, "send");
  assert.ok(send.progress > 4000);
  // 9 minutes later, still at the exit point -> returned
  assert.equal(decideFallback({ pending: pending(), point: { latitude: SITE.latitude + 0.001, longitude: SITE.longitude, accuracyMeters: 30 }, nowMs: later, config: CONFIG }).action, "returned");
  // moved 1 km the wrong way only -> waiting (not yet "away")
  assert.equal(decideFallback({ pending: pending(), point: { latitude: 43.6532, longitude: -79.72, accuracyMeters: 30 }, nowMs: later, config: CONFIG }).action, "waiting");
  // 5 km farther from home than the site -> away
  assert.equal(decideFallback({ pending: pending(), point: { latitude: 43.6532, longitude: -79.66, accuracyMeters: 30 }, nowMs: later, config: CONFIG }).action, "away");
  // at home already -> arrived
  assert.equal(decideFallback({ pending: pending(), point: { latitude: HOME.latitude, longitude: HOME.longitude, accuracyMeters: 30 }, nowMs: later, config: CONFIG }).action, "arrived");
  // bad fix -> inaccurate (keeps waiting)
  assert.equal(decideFallback({ pending: pending(), point: { latitude: 43.66, longitude: -79.90, accuracyMeters: 900 }, nowMs: later, config: CONFIG }).action, "inaccurate");
  // past expiry -> expired
  assert.equal(decideFallback({ pending: pending(), point: { latitude: 43.66, longitude: -79.90, accuracyMeters: 30 }, nowMs: NOW.getTime() + 61 * 60 * 1000, config: CONFIG }).action, "expired");
});

test("noteWorkExitForEtaFallback arms only after the cutoff hour and not when a text just went out", async () => {
  const db = new FakeDb();
  const member = seedMember(db);
  const logger = { info() {}, warn() {} };

  const early = await noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt: new Date("2026-10-30T17:30:00.000Z"), latitude: SITE.latitude, longitude: SITE.longitude, logger }); // 1:30 PM
  assert.equal(early.armed, false);
  assert.equal(early.reason, "too_early");
  assert.equal(db._get("etaNotifications", EMAIL), null);

  const armed = await noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt: NOW, latitude: SITE.latitude, longitude: SITE.longitude, logger });
  assert.equal(armed.armed, true);
  const state = db._get("etaNotifications", EMAIL);
  assert.equal(state.pendingFallback.armedAtMs, NOW.getTime());
  assert.ok(state.pendingFallback.exitDistanceToHomeMeters > 50000);
  assert.deepEqual(state.pendingFallback.home, { ...HOME, source: "etaNotify.home" });

  db._set("etaNotifications", EMAIL, { lastSentAtMs: NOW.getTime() - 5 * 60 * 1000, lastSent: { trigger: "siri" } });
  const recent = await noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt: NOW, latitude: SITE.latitude, longitude: SITE.longitude, logger });
  assert.equal(recent.armed, false);
  assert.equal(recent.reason, "sent_recently");

  const disabled = await noteWorkExitForEtaFallback({ db: new FakeDb(), FieldValue, member: seedMember(new FakeDb(), { etaNotify: { fallback: { enabled: false } } }), exitAt: NOW, latitude: SITE.latitude, longitude: SITE.longitude, logger });
  assert.equal(disabled.reason, "disabled");
});

test("evaluateEtaFallbackOnPing: a later ping with progress sends the text from where you are and logs the exit-time departure", async () => {
  const db = new FakeDb();
  const member = seedMember(db);
  const logger = { info() {}, warn() {} };
  const sms = fakeSms();
  const { fetchImpl, calls } = routesFetch(2100); // 35 min
  const recorded = [];
  const deps = { db, FieldValue, member, getTwilio: () => ({ smsClient: sms.client, fromPhone: "+15550001111" }), mapsApiKey: "key", fetchImpl, logger, recordDeparture: async ({ event }) => (recorded.push(event), { duplicate: false, shortcutEventId: "ev-fb" }) };

  await noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt: NOW, latitude: SITE.latitude, longitude: SITE.longitude, logger });

  const none = await evaluateEtaFallbackOnPing({ ...deps, point: { latitude: 43.66, longitude: -79.90, atMs: NOW.getTime() + 60 * 1000, accuracyMeters: 20 }, now: new Date(NOW.getTime() + 60 * 1000) });
  assert.equal(none.action, "waiting");
  assert.equal(sms.sent.length, 0);

  const pingAt = new Date(NOW.getTime() + 9 * 60 * 1000);
  const sent = await evaluateEtaFallbackOnPing({ ...deps, point: { latitude: 43.66, longitude: -79.9, atMs: pingAt.getTime(), accuracyMeters: 20 }, now: pingAt });
  assert.equal(sent.action, "sent");
  assert.equal(sent.minutes, 35);
  assert.equal(sms.sent.length, 1);
  assert.equal(sms.sent[0].to, CONTACT);
  assert.equal(sms.sent[0].body, `Marwan leaving work, will be home in 35 minutes, at ${sent.arriveAtClock}.`);
  assert.deepEqual(calls[0].init.body.origin.location.latLng, { latitude: 43.66, longitude: -79.9 });

  const state = db._get("etaNotifications", EMAIL);
  assert.equal(state.pendingFallback, null);
  assert.equal(state.lastSent.trigger, "owntracks_fallback");
  assert.equal(state.lastSent.origin.source, "owntracks_fallback");

  assert.equal(recorded.length, 1);
  assert.equal(recorded[0].eventType, "leave_work");
  assert.equal(recorded[0].eventAtMs, NOW.getTime()); // the exit OwnTracks reported, not the ping
  assert.equal(recorded[0].source, "owntracks");

  const after = await evaluateEtaFallbackOnPing({ ...deps, point: { latitude: 43.67, longitude: -80.0, atMs: pingAt.getTime() + 60000, accuracyMeters: 20 }, now: new Date(pingAt.getTime() + 60000) });
  assert.equal(after.action, "none");
  assert.equal(sms.sent.length, 1);
});

test("evaluateEtaFallbackOnPing: coming back to the site or driving away disarms without texting", async () => {
  const db = new FakeDb();
  const member = seedMember(db);
  const logger = { info() {}, warn() {} };
  const sms = fakeSms();
  const deps = { db, FieldValue, member, getTwilio: () => ({ smsClient: sms.client, fromPhone: "+15550001111" }), mapsApiKey: null, logger };
  const later = new Date(NOW.getTime() + 9 * 60 * 1000);

  await noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt: NOW, latitude: SITE.latitude, longitude: SITE.longitude, logger });
  const returned = await evaluateEtaFallbackOnPing({ ...deps, point: { latitude: SITE.latitude, longitude: SITE.longitude, atMs: later.getTime(), accuracyMeters: 15 }, now: later });
  assert.equal(returned.action, "returned");
  assert.equal(db._get("etaNotifications", EMAIL).pendingFallback, null);
  assert.equal(db._get("etaNotifications", EMAIL).lastFallbackOutcome.reason, "returned");

  await noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt: NOW, latitude: SITE.latitude, longitude: SITE.longitude, logger });
  const away = await evaluateEtaFallbackOnPing({ ...deps, point: { latitude: 43.6532, longitude: -79.66, atMs: later.getTime(), accuracyMeters: 15 }, now: later });
  assert.equal(away.action, "away");
  assert.equal(sms.sent.length, 0);
});

test("siriSentRecently is true only for a Siri send inside the departure window", async () => {
  const db = new FakeDb();
  const member = { email: EMAIL };
  assert.equal(await siriSentRecently({ db, member, nowMs: NOW.getTime() }), false);
  db._set("etaNotifications", EMAIL, { lastSentAtMs: NOW.getTime() - 3 * 60 * 1000, lastSent: { trigger: "siri" } });
  assert.equal(await siriSentRecently({ db, member, nowMs: NOW.getTime() }), true);
  db._set("etaNotifications", EMAIL, { lastSentAtMs: NOW.getTime() - 3 * 60 * 1000, lastSent: { trigger: "owntracks_fallback" } });
  assert.equal(await siriSentRecently({ db, member, nowMs: NOW.getTime() }), false);
  db._set("etaNotifications", EMAIL, { lastSentAtMs: NOW.getTime() - 40 * 60 * 1000, lastSent: { trigger: "siri" } });
  assert.equal(await siriSentRecently({ db, member, nowMs: NOW.getTime() }), false);
});

// ---- contact replies

test("relayEtaContactReply forwards the contact's text to the member and swallows it from the assistant", async () => {
  const db = new FakeDb();
  seedMember(db);
  const sms = fakeSms();
  const out = await relayEtaContactReply({ db, FieldValue, from: CONTACT, body: "ok drive safe", smsClient: sms.client, fromPhone: "+15550001111", logger: { warn() {} }, runId: "r" });
  assert.deepEqual(out, { handled: true, relayed: true, memberEmail: EMAIL });
  assert.equal(sms.sent.length, 1);
  assert.deepEqual(sms.sent[0], { to: MEMBER_PHONE, from: "+15550001111", body: "Ashley: ok drive safe" });
  const logged = [...db._col("messages").values()];
  assert.equal(logged[0].etaNotify.trigger, "contact_reply_relay");

  const stranger = await relayEtaContactReply({ db, FieldValue, from: "+14165550000", body: "hi", smsClient: sms.client, fromPhone: "+15550001111", logger: { warn() {} }, runId: "r" });
  assert.deepEqual(stranger, { handled: false });
  assert.equal(sms.sent.length, 1);

  const media = await relayEtaContactReply({ db, FieldValue, from: CONTACT, body: "", mediaCount: 1, smsClient: sms.client, fromPhone: "+15550001111", logger: { warn() {} }, runId: "r" });
  assert.equal(media.relayed, true);
  assert.equal(sms.sent[1].body, "Ashley: [sent an attachment]");
});

test("evaluateEtaFallbackOnPing: a ping delivered late from OwnTracks' queue never triggers a text", async () => {
  const db = new FakeDb();
  const member = seedMember(db);
  const logger = { info() {}, warn() {} };
  const sms = fakeSms();
  await noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt: NOW, latitude: SITE.latitude, longitude: SITE.longitude, logger });
  const pingAt = NOW.getTime() + 9 * 60 * 1000;
  const out = await evaluateEtaFallbackOnPing({
    db, FieldValue, member, logger, mapsApiKey: null,
    getTwilio: () => ({ smsClient: sms.client, fromPhone: "+15550001111" }),
    point: { latitude: 43.66, longitude: -79.9, atMs: pingAt, accuracyMeters: 20 },
    now: new Date(pingAt + 12 * 60 * 1000),
  });
  assert.equal(out.action, "stale_ping");
  assert.equal(sms.sent.length, 0);
  assert.ok(db._get("etaNotifications", EMAIL).pendingFallback, "still armed for a fresh ping");
});
