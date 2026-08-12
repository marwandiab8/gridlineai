const assert = require("node:assert/strict");
const test = require("node:test");

const { createTimeLeftLifeEventDelivery } = require("./timeLeftLifeEventDelivery");
const {
  createTimeLeftDeliveryRepository,
} = require("./timeLeftDeliveryRepository");
const {
  mapShortcutEventToTimeLeftLifeEvent,
  parseOccurredAt,
} = require("./timeLeftLifeEventMapper");

const FieldValue = {
  serverTimestamp: () => ({ __serverTimestamp: true }),
};

class FakeDb {
  constructor() {
    this.rows = new Map([
      [
        "iosShortcutEvents",
        new Map([
          [
            "evt_1",
            {
              status: "processing",
            },
          ],
        ]),
      ],
    ]);
  }
  collection(name) {
    if (!this.rows.has(name)) this.rows.set(name, new Map());
    return {
      doc: (id) => new FakeDocRef(this, name, id),
    };
  }
  _get(col, id) {
    return this.rows.get(col)?.get(id) || null;
  }
  _set(col, id, data) {
    if (!this.rows.has(col)) this.rows.set(col, new Map());
    this.rows.get(col).set(id, data);
  }
}

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
    this.db._set(this.col, this.id, merge(current, data));
  }
}

function merge(current = {}, patch = {}) {
  const out = { ...current };
  for (const [key, value] of Object.entries(patch)) {
    if (key === "timeLeftDelivery" && isObjectLike(value)) {
      out[key] = isObjectLike(out[key]) ? { ...out[key], ...value } : value;
      continue;
    }
    out[key] = value;
  }
  return out;
}

function isObjectLike(value) {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

function baseEvent(overrides = {}) {
  return {
    id: "evt_1",
    eventType: "arrive_work",
    eventAtIso: parseOccurredAt({ eventAtIso: "2026-07-09T12:30:00.000Z" }),
    timezone: "America/Toronto",
    ...overrides,
  };
}

test("writes delivery state on successful client response", async () => {
  const db = new FakeDb();
  const repository = createTimeLeftDeliveryRepository({ db, FieldValue });
  let sent;
  const delivery = createTimeLeftLifeEventDelivery({
    mapper: mapShortcutEventToTimeLeftLifeEvent,
    client: {
      config: {
        mode: "off",
        targetProjectId: "gridlineai",
      },
      sendLifeEvent: async (event) => {
        sent = event;
        return {
          status: "off",
          retryable: false,
          lifeEventId: null,
          idempotencyKey: null,
        };
      },
    },
    repository,
    logger: { debug() {} },
  });

  const result = await delivery({ event: baseEvent(), eventId: "evt_1" });
  const eventDoc = db.collection("iosShortcutEvents").doc("evt_1");
  const stored = (await eventDoc.get()).data();

  assert.equal(result.ok, true);
  assert.equal(result.status, "off");
  assert.equal(result.lastErrorCode, null);
  assert.equal(stored.timeLeftDelivery.status, "off");
  assert.equal(stored.timeLeftDelivery.mode, "off");
  assert.equal(stored.timeLeftDelivery.targetProjectId, "gridlineai");
  assert.ok(!!sent);
});

test("records unsupported shortcut event as a skipped delivery", async () => {
  const db = new FakeDb();
  const repository = createTimeLeftDeliveryRepository({ db, FieldValue });
  let called = false;

  const delivery = createTimeLeftLifeEventDelivery({
    mapper: mapShortcutEventToTimeLeftLifeEvent,
    client: {
      config: {
        mode: "off",
        targetProjectId: "gridlineai",
      },
      sendLifeEvent: async () => {
        called = true;
        return { status: "off", retryable: false };
      },
    },
    repository,
  });

  const result = await delivery({
    event: baseEvent({ eventType: "unknown_event" }),
    eventId: "evt_1",
  });
  const stored = (await db.collection("iosShortcutEvents").doc("evt_1").get()).data();

  assert.equal(result.ok, false);
  assert.equal(result.status, "unsupported_event_type");
  assert.equal(called, false);
  assert.equal(stored.timeLeftDelivery.status, "unsupported_event_type");
  assert.equal(stored.timeLeftDelivery.retryable, false);
  assert.equal(stored.timeLeftDelivery.lastErrorCode, "unsupported_event");
});

test("returns retryable state when client result is retryable", async () => {
  const db = new FakeDb();
  const repository = createTimeLeftDeliveryRepository({ db, FieldValue });
  const delivery = createTimeLeftLifeEventDelivery({
    mapper: mapShortcutEventToTimeLeftLifeEvent,
    client: {
      config: {
        mode: "off",
        targetProjectId: "gridlineai",
      },
      sendLifeEvent: async () => ({
        status: "retryable_failure",
        retryable: true,
        errorCode: "timeout",
        summary: "request timed out",
      }),
    },
    repository,
  });

  const result = await delivery({ event: baseEvent(), eventId: "evt_1" });
  const stored = (await db.collection("iosShortcutEvents").doc("evt_1").get()).data();

  assert.equal(result.ok, false);
  assert.equal(result.status, "retryable_failure");
  assert.equal(stored.timeLeftDelivery.retryable, true);
  assert.equal(stored.timeLeftDelivery.lastErrorCode, "timeout");
  assert.equal(stored.timeLeftDelivery.lastErrorSummary, "request timed out");
});
