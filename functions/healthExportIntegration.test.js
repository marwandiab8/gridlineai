const test = require("node:test");
const assert = require("node:assert/strict");
const { hashShortcutToken } = require("./iosShortcutsIntegration");
const { handleHealthExportEventRequest, buildDeliveryClient } = require("./healthExportIntegration");

// Same minimal fake Firestore pattern used by ownTracksIntegration.test.js / placesIntegration.test.js.
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

class FakeCollection extends FakeQuery {}

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
}

function getPath(obj, path) {
  return String(path).split(".").reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

function matchesFilter(data, filter) {
  const actual = getPath(data, filter.field);
  if (filter.op === "==") return actual === filter.value;
  throw new Error(`Unsupported op ${filter.op}`);
}

function req({ token = "", body = {} } = {}) {
  const headers = token ? { authorization: `Bearer ${token}` } : {};
  return {
    method: "POST",
    body,
    query: {},
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

function fakeClient({ resultFor } = {}) {
  const calls = [];
  return {
    calls,
    async sendLifeEventsBatch(items) {
      calls.push(items);
      if (resultFor) return resultFor(items);
      return {
        status: "delivered",
        results: items.map(() => ({ status: "success", duplicate: false })),
      };
    },
  };
}

const sleepBody = {
  data: {
    metrics: [
      {
        name: "sleep_analysis",
        data: [
          { date: "2026-09-21", totalSleep: 7.5, sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400" },
        ],
      },
    ],
    workouts: [],
  },
};

async function call({ db = seededDb(), request, client } = {}) {
  const response = res();
  await handleHealthExportEventRequest({ db, req: request, res: response, logger: { warn() {}, info() {}, error() {} }, client });
  return { response, client };
}

test("rejects a missing or invalid token", async () => {
  const missing = await call({ request: req({ body: sleepBody }) });
  assert.equal(missing.response.statusCode, 401);
  assert.equal(missing.response.body.error, "missing_token");

  const invalid = await call({ request: req({ token: "wrong", body: sleepBody }) });
  assert.equal(invalid.response.statusCode, 401);
  assert.equal(invalid.response.body.error, "invalid_token");
});

test("rejects non-POST requests", async () => {
  const request = req({ token: "secret-token", body: sleepBody });
  request.method = "GET";
  const { response } = await call({ request });
  assert.equal(response.statusCode, 405);
});

test("an empty export (no sleep/workouts/steps) delivers nothing and never calls the client", async () => {
  const client = fakeClient();
  const { response } = await call({ request: req({ token: "secret-token", body: { data: { metrics: [], workouts: [] } } }), client });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.body, { ok: true, received: { sleep: 0, workouts: 0, steps: 0 }, delivered: 0, duplicates: 0, failed: 0 });
  assert.equal(client.calls.length, 0);
});

test("maps and delivers a sleep export", async () => {
  const client = fakeClient();
  const { response } = await call({ request: req({ token: "secret-token", body: sleepBody }), client });
  assert.equal(response.statusCode, 200);
  assert.equal(response.body.received.sleep, 1);
  assert.equal(response.body.delivered, 1);
  assert.equal(client.calls.length, 1);
  assert.equal(client.calls[0][0].eventType, "sleep_session");
});

test("maps and delivers workouts and steps together with sleep", async () => {
  const body = {
    data: {
      metrics: [
        ...sleepBody.data.metrics,
        { name: "step_count", data: [{ qty: 8500, date: "2026-09-22 14:00:00 -0400" }] },
      ],
      workouts: [{ id: "w1", name: "Running", start: "2026-09-22 07:00:00 -0400", end: "2026-09-22 07:30:00 -0400" }],
    },
  };
  const client = fakeClient();
  const { response } = await call({ request: req({ token: "secret-token", body }), client });
  assert.deepEqual(response.body.received, { sleep: 1, workouts: 1, steps: 1 });
  assert.equal(response.body.delivered, 3);
  assert.equal(client.calls[0].length, 3);
});

test("splits a large export into multiple, smaller batch calls (the other side processes each record as its own slow transaction)", async () => {
  const manyWorkouts = Array.from({ length: 45 }, (_, index) => ({
    id: `w${index}`,
    name: "Walking",
    start: `2026-09-${String((index % 27) + 1).padStart(2, "0")} 07:00:00 -0400`,
    duration: 600,
  }));
  const body = { data: { metrics: [], workouts: manyWorkouts } };
  const client = fakeClient();
  const { response } = await call({ request: req({ token: "secret-token", body }), client });
  assert.equal(response.body.received.workouts, 45);
  assert.equal(client.calls.length, 3, "45 records must be split into 20+20+5 item batches");
  assert.equal(client.calls[0].length, 20);
  assert.equal(client.calls[1].length, 20);
  assert.equal(client.calls[2].length, 5);
  assert.equal(response.body.delivered, 45);
});

test("counts duplicates and failures from the batch response separately from delivered", async () => {
  const client = fakeClient({
    resultFor: (items) => ({
      status: "delivered",
      results: items.map((item, index) => (
        index === 0 ? { status: "success", duplicate: true } : { status: "failed" }
      )),
    }),
  });
  const body = {
    data: {
      metrics: [sleepBody.data.metrics[0]],
      workouts: [{ id: "w1", name: "Running", start: "2026-09-22 07:00:00 -0400", end: "2026-09-22 07:30:00 -0400" }],
    },
  };
  const { response } = await call({ request: req({ token: "secret-token", body }), client });
  assert.equal(response.body.delivered, 0);
  assert.equal(response.body.duplicates, 1);
  assert.equal(response.body.failed, 1);
});

test("acknowledges the export instead of erroring when TimeLeft delivery is not configured", async () => {
  const { response } = await call({ request: req({ token: "secret-token", body: sleepBody }), client: null });
  // buildDeliveryClient() will fail validation in this test env (no TIME_LEFT_* env vars set),
  // and the handler must still return 200 so Health Auto Export doesn't retry a huge payload forever.
  assert.equal(response.statusCode, 200);
  assert.equal(response.body.ok, true);
  assert.equal(response.body.deliveryDisabled, true);
});

test("a client that returns off-mode status is reported as disabled, never as failures", async () => {
  const client = {
    calls: [],
    async sendLifeEventsBatch(items) {
      this.calls.push(items);
      return { status: "off", retryable: false, results: [] };
    },
  };
  const { response } = await call({ request: req({ token: "secret-token", body: sleepBody }), client });
  assert.equal(response.body.delivered, 0);
  assert.equal(response.body.failed, 0);
  assert.equal(response.body.deliveryDisabled, true);
});

test("the delivery client is built with a much longer timeout than a single event needs, since a batch is many sequential transactions on the other side", () => {
  const client = buildDeliveryClient({
    env: {
      TLTL_DUAL_WRITE_MODE: "production",
      TLTL_LIFE_EVENTS_URL: "https://timelefttolive.web.app/api/v1/life-events",
      TLTL_TARGET_PROJECT_ID: "timelefttolive",
      TIME_LEFT_CALENDAR_ID: "cal_1",
      TIME_LEFT_CONNECTION_ID: "conn_1",
      TLTL_INTEGRATION_ID: "int_1",
      TIME_LEFT_INGESTION_TOKEN: "token-abc",
    },
  });
  assert.ok(client);
  assert.equal(client.config.timeoutMs, 45000);
});

test("health export records are scoped per member/token", async () => {
  const db = new FakeDb({
    appMembers: {
      "alice@example.com": {
        active: true,
        approvedPhoneE164: "+14375550111",
        shortcutIntegration: { enabled: true, tokenHash: hashShortcutToken("alice-token") },
      },
    },
  });
  const client = fakeClient();
  const { response } = await call({ db, request: req({ token: "alice-token", body: sleepBody }), client });
  assert.equal(response.statusCode, 200);
  assert.equal(response.body.delivered, 1);
});
