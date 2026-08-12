const test = require("node:test");
const assert = require("node:assert/strict");
const {
  createTimeLeftDeliveryRepository,
} = require("./timeLeftDeliveryRepository");
const { createTimeLeftLifeEventDelivery } = require("./timeLeftLifeEventDelivery");
const {
  hashShortcutToken,
  handleShortcutEventRequest,
  parseShortcutEventPayload,
} = require("./iosShortcutsIntegration");
const { mapShortcutEventToTimeLeftLifeEvent } = require("./timeLeftLifeEventMapper");

const FieldValue = {
  serverTimestamp: () => new Date("2026-07-09T12:00:00.000Z"),
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
  async update(data) { await this.set(data, { merge: true }); }
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
  async add(data) {
    const ref = this.doc();
    await ref.set(data);
    return ref;
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
    if (value && value.__delete) {
      delete out[key];
    } else if (value && value.__arrayUnion) {
      const current = Array.isArray(out[key]) ? out[key] : [];
      out[key] = [...new Set([...current, ...value.__arrayUnion])];
    } else if (value && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date)) {
      out[key] = mergeSentinels(out[key] && typeof out[key] === "object" ? out[key] : {}, value);
    } else {
      out[key] = value;
    }
  }
  return out;
}

function req({ token = "", body = {}, headers = {}, method = "POST" } = {}) {
  const allHeaders = { ...headers };
  if (token) allHeaders.authorization = `Bearer ${token}`;
  return {
    method,
    body,
    get(name) {
      return allHeaders[String(name).toLowerCase()] || "";
    },
  };
}

function res() {
  return {
    statusCode: 200,
    body: null,
    headers: {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    set(key, value) {
      this.headers[key] = value;
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
        shortcutIntegration: {
          enabled: true,
          tokenHash: hashShortcutToken(token),
        },
      },
    },
  });
}

async function callHandler({
  db = seededDb(),
  request = req({ token: "secret-token" }),
  processAssistantMessage,
  timeLeftLifeEventDelivery,
} = {}) {
  const response = res();
  const calls = [];
  await handleShortcutEventRequest({
    db,
    FieldValue,
    req: request,
    res: response,
    logger: { error() {} },
    openaiKey: null,
    processAssistantMessage:
      processAssistantMessage ||
      (async (input) => {
        calls.push(input);
        return {
          inboundRef: { id: "msg-in" },
          outboundRef: { id: "msg-out" },
          outboundMeta: { logEntryId: "log-1", projectSlug: "home", command: "log_note" },
        };
      }),
    timeLeftLifeEventDelivery,
  });
  return { response, calls, db };
}

test("creates event with a valid token and uses assistant processing path", async () => {
  const { response, calls, db } = await callHandler({
    request: req({
      token: "secret-token",
      body: {
        event_type: "arrive_work",
        timestamp: "2026-07-09T08:30:00-04:00",
        timezone: "America/Toronto",
        location_label: "work",
      },
    }),
  });
  assert.equal(response.statusCode, 200);
  assert.equal(response.body.ok, true);
  assert.equal(response.body.event_id, "log-1");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].channel, "ios_shortcuts");
  assert.match(calls[0].body, /log note \(2026-07-09\).*Arrived at work/);
  assert.match(calls[0].body, /Event time:\s*8:30 AM/);
  assert.equal(db._get("logEntries", "log-1").shortcutEventType, "arrive_work");
});

test("accepts location aliases from Shortcut payload", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "arrive_work",
    timestamp: "2026-07-09T08:30:00-04:00",
    timezone: "America/Toronto",
    location: { name: "Downtown office", city: "Toronto" },
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.locationLabel, "Downtown office");
});

test("accepts location aliases from map payload object", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "arrive_home",
    timestamp: "2026-07-09T08:30:00-04:00",
    timeZone: "America/Toronto",
    place: { formattedAddress: "123 Pine Street, Toronto, ON" },
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.locationLabel, "123 Pine Street, Toronto, ON");
});

test("parses latitude/longitude from a location object", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "start_spotify",
    timestamp: "2026-07-09T08:30:00-04:00",
    timezone: "America/Toronto",
    location: { latitude: 43.76079578096695, longitude: -79.758 },
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.latitude, 43.76079578096695);
  assert.equal(parsed.event.longitude, -79.758);
});

test("treats empty coordinate strings as missing coordinates", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "leave_home",
    timestamp: "2026-07-09T07:15:00-04:00",
    timezone: "America/Toronto",
    latitude: " ",
    longitude: "",
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.latitude, null);
  assert.equal(parsed.event.longitude, null);
});

test("rejects missing token", async () => {
  const { response } = await callHandler({ request: req({ body: { event_type: "arrive_work" } }) });
  assert.equal(response.statusCode, 401);
  assert.equal(response.body.error, "missing_token");
});

test("rejects invalid token", async () => {
  const { response } = await callHandler({ request: req({ token: "wrong", body: { event_type: "arrive_work" } }) });
  assert.equal(response.statusCode, 401);
  assert.equal(response.body.error, "invalid_token");
});

test("rejects invalid event_type", async () => {
  const { response } = await callHandler({ request: req({ token: "secret-token", body: { event_type: "lunch" } }) });
  assert.equal(response.statusCode, 400);
  assert.equal(response.body.error, "invalid_event_type");
});

test("preserves provided timestamp", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "leave_work",
    timestamp: "2026-07-09T17:45:00-04:00",
    timezone: "America/Toronto",
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.eventAtIso, "2026-07-09T21:45:00.000Z");
  assert.equal(parsed.event.reportDateKey, "2026-07-09");
});

test("preserves 15:29 with America/Toronto offset and stores 19:29Z", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "leave_work",
    timestamp: "2026-07-13T15:29:00-04:00",
    timezone: "America/Toronto",
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.eventAtIso, "2026-07-13T19:29:00.000Z");
});

test("interprets timezone-less timestamps in the supplied timezone", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "arrive_work",
    timestamp: "2026-07-09T08:30:00",
    timezone: "America/Toronto",
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.eventAtIso, "2026-07-09T12:30:00.000Z");
  assert.equal(parsed.event.reportDateKey, "2026-07-09");
});

test("normalizes optional project_slug from Shortcut payload", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "leave_work",
    timestamp: "2026-07-09T17:45:00-04:00",
    timezone: "America/Toronto",
    project_slug: " Dock Steader ",
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.projectSlug, "dock-steader");
});

test("accepts camelCase timeZone field", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "arrive_home",
    timestamp: "2026-07-09T08:30:00-04:00",
    timeZone: "America/Toronto",
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.timezone, "America/Toronto");
});

test("accepts common Apple Shortcuts event type formatting", () => {
  const parsed = parseShortcutEventPayload({
    event_type: "Arrive Work",
    timestamp: "2026-07-09T08:30:00-04:00",
    timezone: "America/Toronto",
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.eventType, "arrive_work");
});

test("accepts gym, workout, and Spotify Shortcut events", () => {
  const cases = [
    ["Arrived at the GYM", "arrive_gym"],
    ["left the gym", "leave_gym"],
    ["started workout", "start_workout"],
    ["finished workout", "finish_workout"],
    ["started listening to spotify", "start_spotify"],
  ];
  for (const [input, expected] of cases) {
    const parsed = parseShortcutEventPayload({
      event_type: input,
      timestamp: "2026-07-09T08:30:00-04:00",
      timezone: "America/Toronto",
    });
    assert.equal(parsed.ok, true, input);
    assert.equal(parsed.event.eventType, expected);
  }
});

test("falls back to server receive time when timestamp is missing", () => {
  const parsed = parseShortcutEventPayload(
    { event_type: "arrive_home", timezone: "America/Toronto" },
    new Date("2026-07-10T01:30:00.000Z")
  );
  assert.equal(parsed.ok, true);
  assert.equal(parsed.event.eventAtIso, "2026-07-10T01:30:00.000Z");
  assert.equal(parsed.event.reportDateKey, "2026-07-09");
});

test("idempotency key returns existing event without reprocessing", async () => {
  const db = seededDb();
  const first = await callHandler({
    db,
    request: req({
      token: "secret-token",
      headers: { "idempotency-key": "run-1" },
      body: { event_type: "arrive_work", timestamp: "2026-07-09T08:30:00-04:00" },
    }),
  });
  const second = await callHandler({
    db,
    request: req({
      token: "secret-token",
      headers: { "idempotency-key": "run-1" },
      body: { event_type: "arrive_work", timestamp: "2026-07-09T08:30:00-04:00" },
    }),
  });
  assert.equal(first.response.body.duplicate, false);
  assert.equal(second.response.body.duplicate, true);
  assert.equal(second.calls.length, 0);
});

test("dedupes obvious duplicate events without idempotency key", async () => {
  const db = seededDb();
  await callHandler({
    db,
    request: req({
      token: "secret-token",
      body: { event_type: "leave_home", timestamp: "2026-07-09T07:00:00-04:00" },
    }),
  });
  const duplicate = await callHandler({
    db,
    request: req({
      token: "secret-token",
      body: { event_type: "leave_home", timestamp: "2026-07-09T07:00:30-04:00" },
    }),
  });
  assert.equal(duplicate.response.body.duplicate, true);
  assert.equal(duplicate.calls.length, 0);
});

test("writes explicit allowed project_slug onto Shortcut log entry", async () => {
  const db = seededDb();
  db._set("appMembers", "user@example.com", {
    ...db._get("appMembers", "user@example.com"),
    projectSlugs: ["site-a", "site-b"],
  });
  const { response, calls } = await callHandler({
    db,
    request: req({
      token: "secret-token",
      body: {
        event_type: "leave_work",
        timestamp: "2026-07-09T17:00:00-04:00",
        project_slug: "Site A",
      },
    }),
  });

  assert.equal(response.statusCode, 200);
  assert.match(calls[0].body, /Project: site-a\./);
  assert.equal(db._get("iosShortcutEvents", response.body.shortcut_event_id).projectSlug, "site-a");
  assert.equal(db._get("logEntries", "log-1").projectSlug, "site-a");
  assert.equal(db._get("logEntries", "log-1").projectId, "site-a");
});

test("uses active project fallback for Shortcut report inclusion", async () => {
  const db = seededDb();
  db._set("smsUsers", "+14375550123", {
    activeProjectSlug: "site-b",
    projectSlugs: ["site-a", "site-b"],
  });
  const { response } = await callHandler({
    db,
    request: req({
      token: "secret-token",
      body: {
        event_type: "arrive_work",
        timestamp: "2026-07-09T08:00:00-04:00",
      },
    }),
  });

  assert.equal(response.statusCode, 200);
  assert.equal(db._get("iosShortcutEvents", response.body.shortcut_event_id).projectSlug, "site-b");
  assert.equal(db._get("logEntries", "log-1").projectSlug, "site-b");
});

test("rejects project_slug outside token owner projects", async () => {
  const db = seededDb();
  db._set("appMembers", "user@example.com", {
    ...db._get("appMembers", "user@example.com"),
    projectSlugs: ["site-a"],
  });
  const { response } = await callHandler({
    db,
    request: req({
      token: "secret-token",
      body: {
        event_type: "leave_work",
        timestamp: "2026-07-09T17:00:00-04:00",
        project_slug: "site-b",
      },
    }),
  });

  assert.equal(response.statusCode, 403);
  assert.equal(response.body.error, "project_not_allowed");
});

test("records TimeLeftToLive state after successful Shortcut ingest", async () => {
  const db = seededDb();
  const repository = createTimeLeftDeliveryRepository({ db, FieldValue });
  const delivery = createTimeLeftLifeEventDelivery({
    mapper: mapShortcutEventToTimeLeftLifeEvent,
    client: {
      config: {
        mode: "off",
        targetProjectId: "gridlineai",
      },
      sendLifeEvent: async () => ({ status: "off", retryable: false }),
    },
    repository,
    clock: () => new Date("2026-07-09T12:00:00.000Z"),
    logger: { debug() {}, warn() {} },
  });

  const { response } = await callHandler({
    db,
    request: req({
      token: "secret-token",
      body: { event_type: "arrive_work", timestamp: "2026-07-09T08:30:00-04:00" },
    }),
    timeLeftLifeEventDelivery: delivery,
  });

  const eventDoc = db._get("iosShortcutEvents", response.body.shortcut_event_id);
  assert.equal(response.statusCode, 200);
  assert.equal(eventDoc.timeLeftDelivery.status, "off");
  assert.equal(eventDoc.timeLeftDelivery.mode, "off");
  assert.equal(eventDoc.timeLeftDelivery.targetProjectId, "gridlineai");
  assert.equal(eventDoc.timeLeftDelivery.attemptCount, 1);
});

test("preserves successful Shortcut response when TimeLeft delivery throws", async () => {
  let deliveryCalled = false;
  const db = seededDb();
  const { response, calls } = await callHandler({
    db,
    request: req({
      token: "secret-token",
      body: { event_type: "arrive_work", timestamp: "2026-07-09T08:30:00-04:00" },
    }),
    timeLeftLifeEventDelivery: async () => {
      deliveryCalled = true;
      throw new Error("delivery outage");
    },
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.body.ok, true);
  assert.equal(deliveryCalled, true);
  assert.equal(calls.length, 1);
  assert.equal(db._get("iosShortcutEvents", response.body.shortcut_event_id).status, "recorded");
});
