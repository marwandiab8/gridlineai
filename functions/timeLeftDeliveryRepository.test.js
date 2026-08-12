const assert = require("node:assert/strict");
const test = require("node:test");

const {
  createTimeLeftDeliveryRepository,
  upsertTimeLeftDeliveryState,
} = require("./timeLeftDeliveryRepository");

const FieldValue = {
  serverTimestamp: () => ({ __serverTimestamp: true }),
};

class FakeDocSnapshot {
  constructor(exists, data) {
    this.exists = exists;
    this._data = data || null;
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
    return new FakeDocSnapshot(this.db._has(this.col, this.id), this.db._get(this.col, this.id));
  }
  async set(data, options = {}) {
    if (options.merge) {
      const current = this.db._get(this.col, this.id) || {};
      const payload = { ...current };
      const merged = mergeSentinels(payload, data);
      this.db._set(this.col, this.id, merged);
      return;
    }
    this.db._set(this.col, this.id, data);
  }
}

class FakeCollection {
  constructor(db, col) {
    this.db = db;
    this.col = col;
  }
  doc(id) {
    return new FakeDocRef(this.db, this.col, id);
  }
}

class FakeDb {
  constructor(seed = {}) {
    this.rows = new Map();
    for (const [col, docs] of Object.entries(seed)) {
      this.rows.set(col, new Map(Object.entries(docs)));
    }
  }
  collection(col) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    return new FakeCollection(this, col);
  }
  _has(col, id) {
    const group = this.rows.get(col);
    return !!group && group.has(id);
  }
  _get(col, id) {
    return this.rows.get(col)?.get(id) || null;
  }
  _set(col, id, data) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    this.rows.get(col).set(id, data);
  }
}

function mergeSentinels(target, patch) {
  const out = { ...target };
  for (const [key, value] of Object.entries(patch || {})) {
    if (value && value.__delete) {
      delete out[key];
      continue;
    }
    if (value && Array.isArray(value)) {
      out[key] = value.slice();
      continue;
    }
    if (value && value.__arrayUnion) {
      const current = Array.isArray(out[key]) ? out[key] : [];
      out[key] = [...new Set([...current, ...value.__arrayUnion])];
      continue;
    }
    if (value && typeof value === "object" && !Array.isArray(value)) {
      out[key] = mergeSentinels(isObjectLike(out[key]) ? out[key] : {}, value);
      continue;
    }
    out[key] = value;
  }
  return out;
}

function isObjectLike(value) {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

test("creates a first delivery state document with attempt count", async () => {
  const db = new FakeDb({
    iosShortcutEvents: {
      evt_1: { status: "processing" },
    },
  });
  const repository = createTimeLeftDeliveryRepository({ db, FieldValue });

  const result = await repository.upsertTimeLeftDeliveryState("evt_1", {
    status: "off",
    mode: "off",
    targetProjectId: "gridlineai",
    retryable: false,
  });

  const event = db._get("iosShortcutEvents", "evt_1");
  assert.equal(result.ok, true);
  assert.equal(result.attemptCount, 1);
  assert.equal(event.timeLeftDelivery.status, "off");
  assert.equal(event.timeLeftDelivery.mode, "off");
  assert.equal(event.timeLeftDelivery.targetProjectId, "gridlineai");
  assert.equal(event.timeLeftDelivery.attemptCount, 1);
  assert.equal(event.timeLeftDelivery.lastAttemptAt.__serverTimestamp, true);
  assert.equal(event.timeLeftDelivery.deliveredAt, null);
});

test("re-uses delivery state and increments attempt count", async () => {
  const db = new FakeDb({
    iosShortcutEvents: {
      evt_1: {
        status: "recorded",
        timeLeftDelivery: {
          status: "retryable_failure",
          attemptCount: 2,
          lastErrorCode: "timeout",
        },
      },
    },
  });
  const repository = createTimeLeftDeliveryRepository({ db, FieldValue });

  const result = await repository.upsertTimeLeftDeliveryState("evt_1", {
    status: "retryable_failure",
    mode: "staging",
    targetProjectId: "gridlineai",
    retryable: true,
    lastErrorCode: "network_error",
  });

  const event = db._get("iosShortcutEvents", "evt_1");
  assert.equal(result.ok, true);
  assert.equal(result.attemptCount, 3);
  assert.equal(event.timeLeftDelivery.attemptCount, 3);
  assert.equal(event.timeLeftDelivery.retryable, true);
  assert.equal(event.timeLeftDelivery.lastErrorCode, "network_error");
});

test("returns invalid_input when eventId is missing", async () => {
  const db = new FakeDb();
  const repository = createTimeLeftDeliveryRepository({ db, FieldValue });
  const result = await repository.upsertTimeLeftDeliveryState("", { status: "off" });
  assert.equal(result.ok, false);
  assert.equal(result.error, "invalid_input");
});

test("upsertTimeLeftDeliveryState can be used directly", async () => {
  const db = new FakeDb({
    iosShortcutEvents: {
      evt_2: { status: "processing" },
    },
  });

  const result = await upsertTimeLeftDeliveryState(db, FieldValue, "evt_2", {
    status: "delivered",
    mode: "off",
    targetProjectId: "gridlineai",
  });
  const event = db._get("iosShortcutEvents", "evt_2");

  assert.equal(result.ok, true);
  assert.equal(result.state.status, "delivered");
  assert.equal(event.timeLeftDelivery.status, "delivered");
  assert.equal(event.timeLeftDelivery.deliveredAt.__serverTimestamp, true);
});
