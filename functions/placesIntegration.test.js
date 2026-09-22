const test = require("node:test");
const assert = require("node:assert/strict");
const { hashShortcutToken } = require("./iosShortcutsIntegration");
const { handlePlaceLogRequest } = require("./placesIntegration");

const FieldValue = {
  serverTimestamp: () => new Date("2026-09-22T12:00:00.000Z"),
  delete: () => ({ __delete: true }),
  arrayUnion: (...values) => ({ __arrayUnion: values }),
};

// Same minimal fake Firestore as ownTracksIntegration.test.js (dotted-path where() matching
// included), extended with nothing new - placesIntegration.js writes through the same
// recordShortcutEvent path plus placeLearning's own simple knownPlaces collection.
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
    return { empty: rows.length === 0, docs: rows, forEach: (fn) => rows.forEach(fn) };
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

function req({ token = "", body = {} } = {}) {
  const headers = token ? { authorization: `Bearer ${token}` } : {};
  return {
    method: "POST",
    body,
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

const HERE = { latitude: 43.7615, longitude: -79.4111 };

async function call({ db = seededDb(), request } = {}) {
  const response = res();
  const calls = [];
  await handlePlaceLogRequest({
    db,
    FieldValue,
    req: request,
    res: response,
    logger: { warn() {}, info() {}, error() {} },
    openaiKey: null,
    processAssistantMessage: async (input) => {
      calls.push(input);
      return {
        inboundRef: { id: "in" },
        outboundRef: { id: "out" },
        outboundMeta: { logEntryId: "log-1", projectSlug: "home", command: "log_note" },
      };
    },
  });
  return { response, calls, db };
}

test("a brand-new location is not logged and is reported as unknown", async () => {
  const { response, calls, db } = await call({ request: req({ token: "secret-token", body: HERE }) });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body, { ok: true, known: false });
  assert.equal(calls.length, 0, "nothing should be logged until a name is supplied");
  assert.equal((db.rows.get("knownPlaces") || new Map()).size, 0);
});

test("naming a new location saves it and logs the visit", async () => {
  const { response, calls, db } = await call({ request: req({ token: "secret-token", body: { ...HERE, name: "Bells of Steel" } }) });
  assert.equal(response.statusCode, 200);
  assert.equal(response.body.known, true);
  assert.equal(response.body.isNew, true);
  assert.equal(response.body.name, "Bells of Steel");
  assert.equal(response.body.visitCount, 1);
  assert.equal(calls.length, 1);
  assert.match(calls[0].body, /Arrived at location/);
  const place = [...db.rows.get("knownPlaces").values()][0];
  assert.equal(place.name, "Bells of Steel");
  const event = [...db.rows.get("iosShortcutEvents").values()][0];
  assert.equal(event.eventType, "arrive_location");
  assert.equal(event.locationLabel, "Bells of Steel");
  assert.equal(event.source, "quick_log");
});

test("visiting a known location again does not ask for a name and logs it automatically", async () => {
  const db = seededDb();
  await call({ db, request: req({ token: "secret-token", body: { ...HERE, name: "Bells of Steel" } }) });
  const { response, calls } = await call({ db, request: req({ token: "secret-token", body: HERE }) });
  assert.equal(response.statusCode, 200);
  assert.deepEqual({ known: response.body.known, isNew: response.body.isNew, name: response.body.name, visitCount: response.body.visitCount }, {
    known: true,
    isNew: false,
    name: "Bells of Steel",
    visitCount: 2,
  });
  assert.equal(calls.length, 1, "the second call should also log a visit event");
  assert.equal((db.rows.get("knownPlaces") || new Map()).size, 1, "must not create a duplicate place");
});

test("visiting near (not exactly at) a known location still recognizes it", async () => {
  const db = seededDb();
  await call({ db, request: req({ token: "secret-token", body: { ...HERE, name: "Bells of Steel" } }) });
  const nearby = { latitude: HERE.latitude + 0.0004, longitude: HERE.longitude }; // ~44m away
  const { response } = await call({ db, request: req({ token: "secret-token", body: nearby }) });
  assert.equal(response.body.known, true);
  assert.equal(response.body.name, "Bells of Steel");
});

test("a plaza with two named businesses: naming the second does not overwrite the first, and both are logged independently", async () => {
  const db = seededDb();
  const gas = await call({ db, request: req({ token: "secret-token", body: { ...HERE, name: "Gas Station" } }) });
  assert.equal(gas.response.body.isNew, true);
  const nearby = { latitude: HERE.latitude + 0.0004, longitude: HERE.longitude }; // ~44m away, same plaza
  const store = await call({ db, request: req({ token: "secret-token", body: { ...nearby, name: "Convenience Store" } }) });
  assert.equal(store.response.body.isNew, true, "a different business nearby must not be folded into the gas station entry");
  assert.equal((db.rows.get("knownPlaces") || new Map()).size, 2);

  // Revisiting the gas station's exact spot still resolves to the gas station, not the store,
  // and the store is surfaced as an alternative since it's also in range.
  const backAtGas = await call({ db, request: req({ token: "secret-token", body: HERE }) });
  assert.equal(backAtGas.response.body.name, "Gas Station");
  assert.equal(backAtGas.response.body.alternatives.length, 1);
  assert.equal(backAtGas.response.body.alternatives[0].name, "Convenience Store");
  assert.ok(backAtGas.response.body.alternatives[0].id);
});

test("rejects missing or invalid coordinates", async () => {
  const missing = await call({ request: req({ token: "secret-token", body: {} }) });
  assert.equal(missing.response.statusCode, 400);
  assert.equal(missing.response.body.error, "missing_coordinates");

  const invalid = await call({ request: req({ token: "secret-token", body: { latitude: 999, longitude: -79.4 } }) });
  assert.equal(invalid.response.statusCode, 400);
  assert.equal(invalid.response.body.error, "invalid_latitude");
});

test("rejects a missing or invalid token", async () => {
  const missing = await call({ request: req({ body: HERE }) });
  assert.equal(missing.response.statusCode, 401);
  assert.equal(missing.response.body.error, "missing_token");

  const invalid = await call({ request: req({ token: "wrong", body: HERE }) });
  assert.equal(invalid.response.statusCode, 401);
  assert.equal(invalid.response.body.error, "invalid_token");
});

test("known places are scoped per member/token", async () => {
  const db = new FakeDb({
    appMembers: {
      "alice@example.com": {
        active: true,
        approvedPhoneE164: "+14375550111",
        shortcutIntegration: { enabled: true, tokenHash: hashShortcutToken("alice-token") },
      },
      "bob@example.com": {
        active: true,
        approvedPhoneE164: "+14375550122",
        shortcutIntegration: { enabled: true, tokenHash: hashShortcutToken("bob-token") },
      },
    },
  });
  await call({ db, request: req({ token: "alice-token", body: { ...HERE, name: "Alice's place" } }) });
  const { response } = await call({ db, request: req({ token: "bob-token", body: HERE }) });
  assert.equal(response.body.known, false, "bob must be asked to name it even though alice already named the same coordinates");
});
