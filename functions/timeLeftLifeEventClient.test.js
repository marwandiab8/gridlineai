const assert = require("node:assert/strict");
const test = require("node:test");

const {
  createTimeLeftLifeEventClient,
} = require("./timeLeftLifeEventClient");
const {
  validateTimeLeftLifeEventConfig,
} = require("./timeLeftLifeEventConfig");

function mockFetchWithResponse(resolver) {
  const calls = [];
  const fn = async (url, init) => {
    calls.push({ url, init });
    return resolver({ url, init, calls });
  };
  return { fn, calls };
}

function responseFrom(bodyText, status = 200) {
  return {
    status,
    async text() {
      return bodyText;
    },
  };
}

function sampleEvent() {
  return {
    schemaVersion: 1,
    sourceApp: "gridlineai",
    sourceFirebaseProjectId: "gridlineai",
    sourceRecordId: "evt_001",
    eventType: "arrive_work",
    eventClass: "activity_boundary",
    activityFamily: "work",
    categoryId: "work",
    title: "Arrived at work",
    occurredAt: "2026-07-09T12:30:00.000Z",
    timezone: "America/Toronto",
    privacyLevel: "ownerOnly",
  };
}

function baseConfig() {
  return {
    endpointUrl: "https://timelefttolive-stg-go.web.app/api/v1/life-events",
    calendarId: "cal_123",
    connectionId: "conn_123",
    integrationId: "int_123",
    bearerToken: "token-abc",
    targetProjectId: "gridlineai-stage",
    timeoutMs: 2000,
    mode: "staging",
  };
}

test("off mode performs zero HTTP requests", async () => {
  const calls = [];
  const client = createTimeLeftLifeEventClient({ ...baseConfig(), mode: "off" }, {
    fetch: async () => {
      calls.push(1);
      return responseFrom("{}");
    },
  });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "off");
  assert.equal(result.retryable, false);
  assert.equal(calls.length, 0);
});

test("staging target accepted", () => {
  const result = validateTimeLeftLifeEventConfig(baseConfig());
  assert.equal(result.valid, true);
  assert.equal(result.config.endpointUrl, baseConfig().endpointUrl);
});

test("production target rejected", () => {
  const result = validateTimeLeftLifeEventConfig({ ...baseConfig(), mode: "production" });
  assert.equal(result.valid, false);
  assert.equal(result.errors.join("|").includes("production is disabled"), true);
});

test("obsolete staging target rejected", () => {
  const result = validateTimeLeftLifeEventConfig({
    ...baseConfig(),
    targetProjectId: "timelefttolive-stg-marwan",
  });
  assert.equal(result.valid, false);
  assert.equal(result.errors.join("|").includes("not allowed"), true);
});

test("incorrect staging host rejected", () => {
  const result = validateTimeLeftLifeEventConfig({
    ...baseConfig(),
    endpointUrl: "https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItem",
  });
  assert.equal(result.valid, false);
  assert.equal(result.errors.join("|").includes("/api/v1/life-events"), true);
});

test("delivers created response", async () => {
  const { fn, calls } = mockFetchWithResponse(async () =>
    responseFrom(
      JSON.stringify({
        duplicate: false,
        lifeEventId: "le_1",
        idempotencyKey: "idem-001",
      })
    )
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "delivered");
  assert.equal(result.retryable, false);
  assert.equal(result.lifeEventId, "le_1");
  assert.equal(result.idempotencyKey, "idem-001");
  assert.equal(calls.length, 1);
});

test("returns duplicate status", async () => {
  const { fn, calls } = mockFetchWithResponse(async () =>
    responseFrom(
      JSON.stringify({
        duplicate: true,
        lifeEventId: "le_1",
        idempotencyKey: "idem-001",
      })
    )
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "duplicate");
  assert.equal(result.retryable, false);
  assert.equal(result.lifeEventId, "le_1");
  assert.equal(result.idempotencyKey, "idem-001");
  assert.equal(calls.length, 1);
});

test("maps 400 response to permanent failure", async () => {
  const { fn } = mockFetchWithResponse(async () =>
    responseFrom(JSON.stringify({ error: "bad request" }), 400)
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "permanent_failure");
  assert.equal(result.retryable, false);
  assert.equal(result.errorCode, "http_400");
});

test("maps 401/403 to authentication failure", async () => {
  const first = mockFetchWithResponse(async () =>
    responseFrom(JSON.stringify({ error: "no token" }), 401)
  );
  const second = mockFetchWithResponse(async () =>
    responseFrom(JSON.stringify({ error: "forbidden" }), 403)
  );
  const client401 = createTimeLeftLifeEventClient(baseConfig(), { fetch: first.fn });
  const client403 = createTimeLeftLifeEventClient(baseConfig(), { fetch: second.fn });
  const r401 = await client401.sendLifeEvent(sampleEvent());
  const r403 = await client403.sendLifeEvent(sampleEvent());
  assert.equal(r401.status, "authentication_failure");
  assert.equal(r401.retryable, false);
  assert.equal(r403.status, "authentication_failure");
  assert.equal(r403.retryable, false);
});

test("maps conflict response and keeps existingLifeEventId", async () => {
  const { fn } = mockFetchWithResponse(async () =>
    responseFrom(
      JSON.stringify({
        existingLifeEventId: "existing_1",
        lifeEventId: "le_1",
        idempotencyKey: "idem-1",
      }),
      409
    )
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "conflict");
  assert.equal(result.retryable, false);
  assert.equal(result.existingLifeEventId, "existing_1");
  assert.equal(result.lifeEventId, "le_1");
});

test("maps 429 to retryable failure", async () => {
  const { fn } = mockFetchWithResponse(async () =>
    responseFrom(JSON.stringify({ error: "rate limited" }), 429)
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "retryable_failure");
  assert.equal(result.retryable, true);
});

test("maps 5xx to retryable failure", async () => {
  const { fn } = mockFetchWithResponse(async () =>
    responseFrom(JSON.stringify({ error: "server down" }), 502)
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "retryable_failure");
  assert.equal(result.retryable, true);
});

test("maps timeout to retryable failure", async () => {
  const timeoutFetch = async (_url, init) => {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        resolve(responseFrom(JSON.stringify({}), 200));
      }, 1000);
      if (init && init.signal) {
        init.signal.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            const error = new Error("request timeout");
            error.name = "AbortError";
            reject(error);
          },
          { once: true }
        );
      }
    });
  };
  const client = createTimeLeftLifeEventClient(
    { ...baseConfig(), timeoutMs: 1 },
    { fetch: timeoutFetch }
  );
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "retryable_failure");
  assert.equal(result.retryable, true);
  assert.equal(result.errorCode, "timeout");
});

test("maps network failures to retryable failure", async () => {
  const networkFetch = async () => {
    throw new Error("Network unreachable");
  };
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: networkFetch });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "retryable_failure");
  assert.equal(result.retryable, true);
  assert.equal(result.errorCode, "network_error");
});

test("handles invalid JSON response safely", async () => {
  const { fn } = mockFetchWithResponse(async () =>
    responseFrom("{not-json", 200)
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const result = await client.sendLifeEvent(sampleEvent());
  assert.equal(result.status, "retryable_failure");
  assert.equal(result.retryable, true);
  assert.equal(result.errorCode, "invalid_json");
});

test("does not return bearer token in logs or result", async () => {
  const logs = [];
  const observed = [];
  const bearerToken = "super-secret-token-value";
  const logger = {
    debug(message, fields) {
      logs.push({ level: "debug", message, fields });
    },
    error(message, fields) {
      logs.push({ level: "error", message, fields });
    },
  };
  const { fn } = mockFetchWithResponse(async () => {
    observed.push("called");
    throw new Error("Network failed");
  });
  const client = createTimeLeftLifeEventClient(
    { ...baseConfig(), bearerToken },
    { fetch: fn, logger }
  );
  const result = await client.sendLifeEvent(sampleEvent());
  const logText = JSON.stringify(logs);
  const resultText = JSON.stringify(result);
  assert.equal(logText.includes(bearerToken), false);
  assert.equal(resultText.includes(bearerToken), false);
  assert.equal(observed.length, 1);
});

test("sets request headers and payload correctly", async () => {
  const { fn, calls } = mockFetchWithResponse(async () =>
    responseFrom(JSON.stringify({ duplicate: false, lifeEventId: "le_1" }), 200)
  );
  const client = createTimeLeftLifeEventClient(baseConfig(), { fetch: fn });
  const event = sampleEvent();
  const result = await client.sendLifeEvent(event);
  const call = calls[0];
  assert.equal(result.status, "delivered");
  assert.equal(call.init.method, "POST");
  assert.equal(call.init.headers.Authorization, "Bearer token-abc");
  assert.equal(call.init.headers["Content-Type"], "application/json");
  const body = JSON.parse(call.init.body);
  assert.equal(body.calendarId, "cal_123");
  assert.equal(body.connectionId, "conn_123");
  assert.equal(body.integrationId, "int_123");
  assert.equal(body.targetProjectId, "gridlineai-stage");
  assert.equal(body.lifeEvent.sourceRecordId, "evt_001");
});
