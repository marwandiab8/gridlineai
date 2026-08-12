const assert = require("node:assert/strict");
const test = require("node:test");

const {
  replayRecentShortcutEvents,
  shouldRetryShortcutEvent,
} = require("./timeLeftShortcutReplay");

function event(delivery) {
  return { eventType: "arrive_work", timeLeftDelivery: delivery };
}

test("retries missing, former-off, and recoverable delivery states", () => {
  assert.equal(shouldRetryShortcutEvent(event(undefined)), true);
  assert.equal(shouldRetryShortcutEvent(event({ status: "off", mode: "off" })), true);
  assert.equal(shouldRetryShortcutEvent(event({ status: "retryable_failure" })), true);
  assert.equal(shouldRetryShortcutEvent(event({ status: "delivery_error" })), true);
  assert.equal(shouldRetryShortcutEvent(event({ status: "delivery_not_configured" })), true);
});

test("does not retry terminal or unsupported delivery states", () => {
  for (const status of [
    "delivered",
    "duplicate",
    "conflict",
    "authentication_failure",
    "unsupported_event_type",
    "permanent_failure",
  ]) {
    assert.equal(shouldRetryShortcutEvent(event({ status })), false, status);
  }
  assert.equal(shouldRetryShortcutEvent({ eventType: "not_supported" }), false);
  assert.equal(shouldRetryShortcutEvent(event({ status: "off", mode: "production" })), false);
});

function fakeDb(rows) {
  const state = { cutoff: null, order: null, limit: null };
  const query = {
    where(field, operator, value) {
      state.cutoff = { field, operator, value };
      return this;
    },
    orderBy(field, direction) {
      state.order = { field, direction };
      return this;
    },
    limit(value) {
      state.limit = value;
      return this;
    },
    async get() {
      return {
        size: rows.length,
        docs: rows.map((row) => ({ id: row.id, data: () => row.data })),
      };
    },
  };
  return { db: { collection: () => query }, state };
}

test("replay is bounded, skips terminal states, and reports outcomes", async () => {
  const rows = [
    { id: "missing", data: event(undefined) },
    { id: "off", data: event({ status: "off", mode: "off" }) },
    { id: "done", data: event({ status: "delivered", mode: "production" }) },
  ];
  const { db, state } = fakeDb(rows);
  const attempted = [];
  const summary = await replayRecentShortcutEvents({
    db,
    clock: () => new Date("2026-08-12T12:00:00.000Z"),
    scanLimit: 500,
    attemptLimit: 1,
    deliver: async ({ eventId }) => {
      attempted.push(eventId);
      return { status: "delivered" };
    },
  });

  assert.deepEqual(attempted, ["missing"]);
  assert.equal(summary.scanned, 3);
  assert.equal(summary.eligible, 2);
  assert.equal(summary.attempted, 1);
  assert.equal(summary.delivered, 1);
  assert.equal(summary.skipped, 1);
  assert.equal(state.limit, 500);
  assert.deepEqual(state.order, { field: "eventAtMs", direction: "desc" });
  assert.equal(state.cutoff.field, "eventAtMs");
});

test("replay reports duplicate, conflict, authentication, and retryable results", async () => {
  const statuses = ["duplicate", "conflict", "authentication_failure", "retryable_failure"];
  const { db } = fakeDb(statuses.map((status, index) => ({ id: String(index), data: event(undefined) })));
  let index = 0;
  const summary = await replayRecentShortcutEvents({
    db,
    deliver: async () => ({ status: statuses[index++] }),
  });
  assert.equal(summary.duplicate, 1);
  assert.equal(summary.conflicts, 1);
  assert.equal(summary.authenticationFailures, 1);
  assert.equal(summary.retryableFailures, 1);
});
