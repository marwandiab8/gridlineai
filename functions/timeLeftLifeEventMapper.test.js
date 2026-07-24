const assert = require("node:assert/strict");
const test = require("node:test");

const {
  mapShortcutEventToTimeLeftLifeEvent,
  DEFAULT_TIMEZONE,
} = require("./timeLeftLifeEventMapper");

function baseShortcutEvent(overrides = {}) {
  return {
    id: "evt_001",
    eventType: "arrive_work",
    eventAtIso: "2026-07-09T12:30:00.000Z",
    eventAtMs: 175, // present but ignored because eventAtIso is preferred
    timezone: "America/Toronto",
    reportDateKey: "2026-07-09",
    projectSlug: "site-a",
    memberEmail: "user@example.com",
    ...overrides,
  };
}

function baseAssertion(event) {
  assert.equal(event.ok, true);
  assert.equal(event.event.schemaVersion, 1);
  assert.equal(event.event.sourceApp, "gridlineai");
  assert.equal(event.event.sourceFirebaseProjectId, "gridlineai");
  assert.equal(event.event.sourceRecordId, "evt_001");
  assert.equal(event.event.privacyLevel, "ownerOnly");
  assert.equal(event.event.occurredAt, "2026-07-09T12:30:00.000Z");
  return event.event;
}

test("maps arrive_work", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "arrive_work" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "arrive_work");
  assert.equal(body.eventClass, "activity_boundary");
  assert.equal(body.activityFamily, "work");
  assert.equal(body.categoryId, "work");
  assert.equal(body.title, "Arrived at work");
});

test("maps leave_work", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "leave_work" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "leave_work");
  assert.equal(body.eventClass, "activity_boundary");
  assert.equal(body.activityFamily, "work");
});

test("maps arrive_home", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "arrive_home" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "arrive_home");
  assert.equal(body.eventClass, "activity_boundary");
  assert.equal(body.activityFamily, "home");
  assert.equal(body.categoryId, "home");
});

test("maps leave_home", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "leave_home" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "leave_home");
  assert.equal(body.activityFamily, "home");
  assert.equal(body.categoryId, "home");
});

test("maps arrive_gym", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "arrive_gym" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "arrive_gym");
  assert.equal(body.activityFamily, "gym");
  assert.equal(body.categoryId, "gym");
});

test("maps leave_gym", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "leave_gym" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "leave_gym");
  assert.equal(body.activityFamily, "gym");
  assert.equal(body.categoryId, "gym");
});

test("maps start_workout", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "start_workout" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "start_workout");
  assert.equal(body.activityFamily, "workout");
  assert.equal(body.categoryId, "workout");
  assert.equal(body.title, "Started workout");
});

test("maps finish_workout", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "finish_workout" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "finish_workout");
  assert.equal(body.activityFamily, "workout");
  assert.equal(body.categoryId, "workout");
  assert.equal(body.title, "Finished workout");
});

test("maps arrive_location", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({ eventType: "arrive_location", locationLabel: "Riverside Park" })
  );
  const body = baseAssertion(event);
  assert.equal(body.eventType, "arrive_location");
  assert.equal(body.eventClass, "location");
  assert.equal(body.activityFamily, "location");
  assert.equal(body.categoryId, "other_location");
  assert.equal(body.location.label, "Riverside Park");
});

test("maps leave_location", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({ eventType: "leave_location", locationLabel: "Riverside Park" })
  );
  const body = baseAssertion(event);
  assert.equal(body.eventType, "leave_location");
  assert.equal(body.eventClass, "location");
  assert.equal(body.activityFamily, "location");
  assert.equal(body.categoryId, "other_location");
  assert.equal(body.location.label, "Riverside Park");
});

test("maps start_spotify", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "start_spotify" }));
  const body = baseAssertion(event);
  assert.equal(body.eventType, "start_spotify");
  assert.equal(body.activityFamily, "spotify");
  assert.equal(body.categoryId, "spotify");
  assert.equal(body.eventClass, "activity_boundary");
});

test("uses stable sourceRecordId", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ id: "stable-id-77" }));
  assert.equal(event.event.sourceRecordId, "stable-id-77");
});

test("uses stable occurredAt", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      eventAtIso: "2026-07-10T01:30:00.000Z",
      eventAtMs: 1880000000000,
      eventType: "arrive_home",
    })
  );
  assert.equal(event.event.occurredAt, "2026-07-10T01:30:00.000Z");
});

test("mapper output is deterministic", () => {
  const first = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "arrive_home" }));
  const second = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent({ eventType: "arrive_home" }));
  assert.deepEqual(first, second);
});

test("defaults missing timezone to America/Toronto", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      timezone: "",
      eventType: "arrive_work",
    })
  );
  assert.equal(event.event.timezone, DEFAULT_TIMEZONE);
});

test("preserves source timezone when provided", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      timezone: "Europe/London",
      eventType: "arrive_work",
    })
  );
  assert.equal(event.event.timezone, "Europe/London");
});

test("preserves valid coordinates", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      locationLabel: "Toronto",
      latitude: 43.7,
      longitude: -79.4,
      eventType: "arrive_home",
    })
  );
  assert.equal(event.event.location.label, "Toronto");
  assert.equal(event.event.location.latitude, 43.7);
  assert.equal(event.event.location.longitude, -79.4);
});

test("omits incomplete coordinate pair safely", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      locationLabel: "Toronto",
      latitude: 43.7,
      eventType: "arrive_home",
    })
  );
  assert.equal(event.event.location.label, "Toronto");
  assert.equal(event.event.location.latitude, undefined);
});

test("rejects invalid latitude", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      locationLabel: "Toronto",
      latitude: 95,
      longitude: -79.4,
      eventType: "arrive_home",
    })
  );
  assert.equal(event.event.location.latitude, undefined);
  assert.equal(event.event.location.longitude, undefined);
});

test("rejects invalid longitude", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      locationLabel: "Toronto",
      latitude: 43.7,
      longitude: 181,
      eventType: "arrive_home",
    })
  );
  assert.equal(event.event.location.latitude, undefined);
  assert.equal(event.event.location.longitude, undefined);
});

test("does not include memberEmail", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(baseShortcutEvent());
  assert.equal(event.event.memberEmail, undefined);
  assert.equal(event.event.metadata && event.event.metadata.memberEmail, undefined);
});

test("removes secret-like metadata", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({
      metadata: {
        projectSlug: "site-a",
        secretToken: "shhh",
        apiKey: "very-secret",
        webhookUrl: "https://example.com",
      },
      eventType: "arrive_work",
    })
  );
  assert.equal(event.event.metadata.apiKey, undefined);
  assert.equal(event.event.metadata.webhookUrl, undefined);
  assert.equal(event.event.metadata.secretToken, undefined);
  assert.equal(event.event.metadata.projectSlug, "site-a");
  assert.equal(event.event.metadata.reportDateKey, "2026-07-09");
});

test("rejects unsupported event types", () => {
  const event = mapShortcutEventToTimeLeftLifeEvent(
    baseShortcutEvent({ eventType: "unknown_event" })
  );
  assert.equal(event.ok, false);
  assert.equal(event.status, "unsupported_event_type");
});
