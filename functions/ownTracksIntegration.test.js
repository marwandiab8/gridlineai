const test = require("node:test");
const assert = require("node:assert/strict");
const { hashShortcutToken } = require("./iosShortcutsIntegration");
const {
  isIgnorableOwnTracksPayload,
  mapOwnTracksTransitionToShortcutBody,
  extractOwnTracksToken,
  handleOwnTracksEventRequest,
} = require("./ownTracksIntegration");

const FieldValue = {
  serverTimestamp: () => new Date("2026-09-22T12:00:00.000Z"),
  delete: () => ({ __delete: true }),
  arrayUnion: (...values) => ({ __arrayUnion: values }),
};

// Same minimal fake Firestore as iosShortcutsIntegration.test.js, since this endpoint
// deliberately shares that module's auth/record/dedupe path rather than reimplementing it.
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
    this.db = db;
    this.col = col;
    this.id = id;
  }
  async get() {
    return new FakeDocSnap(this.id, this.db._get(this.col, this.id));
  }
  async set(data, options = {}) {
    const current = options.merge ? this.db._get(this.col, this.id) || {} : {};
    this.db._set(this.col, this.id, mergeSentinels(current, data));
  }
  async update(data) {
    await this.set(data, { merge: true });
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
  limit(value) {
    this._limit = value;
    return this;
  }
  async get() {
    const rows = [...(this.db.rows.get(this.col) || new Map()).entries()]
      .filter(([, data]) => this.filters.every((f) => matchesFilter(data, f)))
      .slice(0, this._limit)
      .map(([id, data]) => new FakeDocSnap(id, data));
    return { empty: rows.length === 0, docs: rows };
  }
}

class FakeCollection extends FakeQuery {
  doc(id) {
    return new FakeDocRef(this.db, this.col, id || `doc-${++this.db.nextId}`);
  }
}

class FakeDb {
  constructor(seed = {}) {
    this.rows = new Map();
    this.nextId = 0;
    for (const [col, docs] of Object.entries(seed)) {
      this.rows.set(col, new Map(Object.entries(docs)));
    }
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
    const tx = {
      get: (ref) => ref.get(),
      create: (ref, data) => ref.set(data),
      set: (ref, data, options) => ref.set(data, options),
      update: (ref, data) => ref.update(data),
    };
    return fn(tx);
  }
}

function getPath(obj, path) {
  return String(path)
    .split(".")
    .reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

function matchesFilter(data, filter) {
  const actual = getPath(data, filter.field);
  if (filter.op === "==") return actual === filter.value;
  if (filter.op === ">=") return actual >= filter.value;
  if (filter.op === "<=") return actual <= filter.value;
  throw new Error(`Unsupported op ${filter.op}`);
}

function mergeSentinels(target, patch) {
  const out = { ...target };
  for (const [key, value] of Object.entries(patch || {})) {
    if (value && value.__delete) delete out[key];
    else if (value && value.__arrayUnion) {
      const current = Array.isArray(out[key]) ? out[key] : [];
      out[key] = [...new Set([...current, ...value.__arrayUnion])];
    } else if (value && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date)) {
      out[key] = mergeSentinels(out[key] && typeof out[key] === "object" ? out[key] : {}, value);
    } else out[key] = value;
  }
  return out;
}

function req({ token = "", body = {}, headers: extraHeaders = {}, query = {}, method = "POST" } = {}) {
  const headers = { ...extraHeaders };
  if (token) headers.authorization = `Bearer ${token}`;
  return {
    method,
    body,
    query,
    get(name) {
      return headers[String(name).toLowerCase()] || "";
    },
  };
}

function res() {
  return {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    set() {
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
  };
}

function seededDb(token = "secret-token") {
  return new FakeDb({
    appMembers: {
      "user@example.com": {
        active: true,
        approvedPhoneE164: "+14375550123",
        shortcutIntegration: { enabled: true, tokenHash: hashShortcutToken(token) },
      },
    },
  });
}

async function callHandler({ db = seededDb(), request = req({ token: "secret-token" }) } = {}) {
  const response = res();
  const calls = [];
  await handleOwnTracksEventRequest({
    db,
    FieldValue,
    req: request,
    res: response,
    logger: { error() {}, warn() {}, info() {} },
    openaiKey: null,
    processAssistantMessage: async (input) => {
      calls.push(input);
      return {
        inboundRef: { id: "msg-in" },
        outboundRef: { id: "msg-out" },
        outboundMeta: { logEntryId: "log-1", projectSlug: "home", command: "log_note" },
      };
    },
  });
  return { response, calls, db };
}

// --- method handling ---

test("OPTIONS is acknowledged the same way a real event would be, not left to an empty CORS 204", async () => {
  // Regression test: OwnTracks' own networking layer probes with OPTIONS before ever sending its
  // real POST, and an empty non-JSON response there (Cloud Functions' default CORS behavior)
  // makes the phone fail to parse it and never proceed to publish anything at all.
  const { response } = await callHandler({ request: req({ method: "OPTIONS" }) });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body, []);
});

test("rejects other non-POST methods with a real JSON body", async () => {
  const { response } = await callHandler({ request: req({ method: "GET" }) });
  assert.equal(response.statusCode, 405);
  assert.equal(response.body.error, "method_not_allowed");
});

// --- translation ---

test("maps a Home enter/leave transition onto the canonical Shortcuts event types", () => {
  const enter = mapOwnTracksTransitionToShortcutBody({ _type: "transition", event: "enter", desc: "Home", tst: 1758000000, lat: 43.7, lon: -79.7, tid: "MD" });
  assert.equal(enter.event_type, "arrive_home");
  assert.equal(enter.timestamp, new Date(1758000000 * 1000).toISOString());
  assert.equal(enter.latitude, 43.7);
  assert.equal(enter.longitude, -79.7);
  assert.equal(enter.device_name, "MD");
  assert.equal(enter.source, "owntracks");

  const leave = mapOwnTracksTransitionToShortcutBody({ _type: "transition", event: "leave", desc: "Home", tst: 1758000600 });
  assert.equal(leave.event_type, "leave_home");
});

test("matches region names case-insensitively and with surrounding space", () => {
  const enter = mapOwnTracksTransitionToShortcutBody({ _type: "transition", event: "enter", desc: "  WORK  ", tst: 1758000000 });
  assert.equal(enter.event_type, "arrive_work");
  const office = mapOwnTracksTransitionToShortcutBody({ _type: "transition", event: "enter", desc: "Office", tst: 1758000000 });
  assert.equal(office.event_type, "arrive_work");
  const gym = mapOwnTracksTransitionToShortcutBody({ _type: "transition", event: "leave", desc: "Fitness", tst: 1758000000 });
  assert.equal(gym.event_type, "leave_gym");
});

test("falls back to generic arrive/leave_location for an unrecognized region name", () => {
  const enter = mapOwnTracksTransitionToShortcutBody({ _type: "transition", event: "enter", desc: "Bells of Steel", tst: 1758000000 });
  assert.equal(enter.event_type, "arrive_location");
  assert.equal(enter.location_label, "Bells of Steel");
  const leave = mapOwnTracksTransitionToShortcutBody({ _type: "transition", event: "leave", desc: "Bells of Steel", tst: 1758000000 });
  assert.equal(leave.event_type, "leave_location");
});

test("ignores regular location pings and waypoint dumps, not just transitions", () => {
  assert.equal(isIgnorableOwnTracksPayload({ _type: "location", lat: 43.7, lon: -79.7 }), true);
  assert.equal(isIgnorableOwnTracksPayload({ _type: "waypoints", waypoints: [] }), true);
  assert.equal(isIgnorableOwnTracksPayload({}), true);
  assert.equal(isIgnorableOwnTracksPayload(null), true);
  assert.equal(isIgnorableOwnTracksPayload({ _type: "transition", event: "enter", desc: "Home" }), false);
});

// --- end-to-end handler ---

test("records a Home transition through the same pipeline as an iOS Shortcut event", async () => {
  const { response, calls, db } = await callHandler({
    request: req({ token: "secret-token", body: { _type: "transition", event: "enter", desc: "Home", tst: 1758000000, lat: 43.7, lon: -79.7, tid: "MD" } }),
  });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body, []);
  assert.equal(calls.length, 1);
  assert.match(calls[0].body, /Arrived home/);
  assert.equal(calls[0].channel, "ios_shortcuts");
  const stored = [...db.rows.get("iosShortcutEvents").values()][0];
  assert.equal(stored.eventType, "arrive_home");
  assert.equal(stored.source, "owntracks");
});

test("acknowledges a plain location ping without recording anything", async () => {
  const { response, calls, db } = await callHandler({
    request: req({ token: "secret-token", body: { _type: "location", lat: 43.7, lon: -79.7, tst: 1758000000 } }),
  });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body, []);
  assert.equal(calls.length, 0);
  assert.equal((db.rows.get("iosShortcutEvents") || new Map()).size, 0);
});

test("rejects a missing or invalid token the same way the Shortcuts endpoint does", async () => {
  const missing = await callHandler({ request: req({ body: { _type: "transition", event: "enter", desc: "Home", tst: 1758000000 } }) });
  assert.equal(missing.response.statusCode, 401);
  assert.equal(missing.response.body.error, "missing_token");

  const invalid = await callHandler({ request: req({ token: "wrong-token", body: { _type: "transition", event: "enter", desc: "Home", tst: 1758000000 } }) });
  assert.equal(invalid.response.statusCode, 401);
  assert.equal(invalid.response.body.error, "invalid_token");
});

test("accepts the token via HTTP Basic Auth password field, for OwnTracks versions with no custom-header option", () => {
  const basic = Buffer.from("owntracks:secret-token", "utf8").toString("base64");
  const withUsername = extractOwnTracksToken(req({ headers: { authorization: `Basic ${basic}` } }));
  assert.equal(withUsername, "secret-token");

  const noUsername = Buffer.from(":secret-token", "utf8").toString("base64");
  assert.equal(extractOwnTracksToken(req({ headers: { authorization: `Basic ${noUsername}` } })), "secret-token");
});

test("accepts the token as a ?token= query parameter as a last resort", () => {
  assert.equal(extractOwnTracksToken(req({ query: { token: "secret-token" } })), "secret-token");
});

test("prefers a Bearer/custom header token over Basic Auth or the query string when more than one is present", () => {
  const basic = Buffer.from(":other-token", "utf8").toString("base64");
  const value = extractOwnTracksToken(req({ token: "secret-token", headers: { authorization: `Basic ${basic}` }, query: { token: "yet-another" } }));
  assert.equal(value, "secret-token");
});

test("records a Home transition authenticated only via Basic Auth", async () => {
  const basic = Buffer.from("owntracks:secret-token", "utf8").toString("base64");
  const { response } = await callHandler({
    request: req({ headers: { authorization: `Basic ${basic}` }, body: { _type: "transition", event: "enter", desc: "Home", tst: 1758000000 } }),
  });
  assert.equal(response.statusCode, 200);
});

test("an unrecognized region still records as a generic Places visit", async () => {
  const { response, db } = await callHandler({
    request: req({ token: "secret-token", body: { _type: "transition", event: "enter", desc: "Bells of Steel", tst: 1758000000 } }),
  });
  assert.equal(response.statusCode, 200);
  const stored = [...db.rows.get("iosShortcutEvents").values()][0];
  assert.equal(stored.eventType, "arrive_location");
  assert.equal(stored.locationLabel, "Bells of Steel");
});

test("a repeated transition with the same idempotency key is not recorded twice", async () => {
  const db = seededDb();
  const body = { _type: "transition", event: "enter", desc: "Home", tst: 1758000000 };
  const request = () => {
    const r = req({ token: "secret-token", body });
    const original = r.get;
    r.get = (name) => (String(name).toLowerCase() === "idempotency-key" ? "run-1" : original(name));
    return r;
  };
  const first = res();
  const calls = [];
  const pam = async (input) => {
    calls.push(input);
    return { inboundRef: { id: "in" }, outboundRef: { id: "out" }, outboundMeta: { logEntryId: "log-1", projectSlug: "home" } };
  };
  await handleOwnTracksEventRequest({ db, FieldValue, req: request(), res: first, logger: { warn() {}, info() {}, error() {} }, openaiKey: null, processAssistantMessage: pam });
  const second = res();
  await handleOwnTracksEventRequest({ db, FieldValue, req: request(), res: second, logger: { warn() {}, info() {}, error() {} }, openaiKey: null, processAssistantMessage: pam });
  assert.equal(calls.length, 1, "assistant processing must run only once for the same idempotency key");
});
