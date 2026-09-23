const assert = require("node:assert/strict");
const test = require("node:test");

const {
  parseHealthAutoExportDate,
  dedupeSleepDataPoints,
  buildSleepLifeEvent,
  buildWorkoutLifeEvent,
  buildStepsLifeEvent,
  parseHealthExportPayload,
} = require("./healthExportEventMapper");

test("parses Health Auto Export's space-separated date format with a UTC offset", () => {
  const date = parseHealthAutoExportDate("2026-09-21 23:00:00 -0400");
  assert.ok(date instanceof Date);
  assert.equal(date.toISOString(), "2026-09-22T03:00:00.000Z");
});

test("returns null for an empty or unparseable date", () => {
  assert.equal(parseHealthAutoExportDate(""), null);
  assert.equal(parseHealthAutoExportDate(null), null);
  assert.equal(parseHealthAutoExportDate("not a date"), null);
});

test("dedupes aggregated sleep rows (one per night) by sleepStart/sleepEnd", () => {
  const points = [
    { date: "2026-09-21", sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400", totalSleep: 7.5 },
    { date: "2026-09-20", sleepStart: "2026-09-20 22:45:00 -0400", sleepEnd: "2026-09-21 06:15:00 -0400", totalSleep: 7.0 },
  ];
  const deduped = dedupeSleepDataPoints(points);
  assert.equal(deduped.length, 2);
});

test("dedupes unaggregated sleep rows (many stage segments per night) down to one per night", () => {
  const points = [
    { startDate: "...", endDate: "...", value: "Core", sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400" },
    { startDate: "...", endDate: "...", value: "Deep", sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400" },
    { startDate: "...", endDate: "...", value: "REM", sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400" },
  ];
  const deduped = dedupeSleepDataPoints(points);
  assert.equal(deduped.length, 1);
});

test("prefers an aggregated (totalSleep-bearing) row over a bare stage segment for the same night", () => {
  const points = [
    { value: "Core", sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400" },
    { totalSleep: 7.5, sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400" },
  ];
  const deduped = dedupeSleepDataPoints(points);
  assert.equal(deduped.length, 1);
  assert.equal(deduped[0].totalSleep, 7.5);
});

test("builds a sleep life event with startAt/endAt and rounded metrics", () => {
  const event = buildSleepLifeEvent({
    sleepStart: "2026-09-21 23:00:00 -0400",
    sleepEnd: "2026-09-22 06:30:00 -0400",
    totalSleep: 7.512345,
    core: 3.5,
    deep: 1.5,
    rem: 2.0,
    inBed: 8.0,
  });
  assert.equal(event.eventType, "sleep_session");
  assert.equal(event.eventClass, "activity_boundary");
  assert.equal(event.activityFamily, "sleep");
  assert.equal(event.categoryId, "sleep");
  assert.equal(event.startAt, "2026-09-22T03:00:00.000Z");
  assert.equal(event.endAt, "2026-09-22T10:30:00.000Z");
  assert.equal(event.occurredAt, event.startAt);
  assert.equal(event.sourceApp, "gridlineai");
  assert.equal(event.metrics.totalSleepHours, 7.51);
  assert.equal(event.metrics.deepHours, 1.5);
  assert.ok(event.sourceRecordId.startsWith("sleep:"));
});

test("sleep event has a stable sourceRecordId across re-exports of the same night", () => {
  const point = { sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400", totalSleep: 7.5 };
  const first = buildSleepLifeEvent(point);
  const second = buildSleepLifeEvent({ ...point, totalSleep: 7.6 }); // a re-export can revise the total
  assert.equal(first.sourceRecordId, second.sourceRecordId);
});

test("returns null for a sleep row missing sleepStart or sleepEnd", () => {
  assert.equal(buildSleepLifeEvent({ sleepStart: "2026-09-21 23:00:00 -0400" }), null);
});

test("builds a completed_workout life event from start/end", () => {
  const event = buildWorkoutLifeEvent({
    id: "550e8400-e29b-41d4-a716-446655440000",
    name: "Running",
    start: "2026-09-22 07:00:00 -0400",
    end: "2026-09-22 07:30:00 -0400",
    duration: 1800,
    distance: { qty: 3.5, units: "mi" },
    activeEnergyBurned: { qty: 350, units: "kcal" },
    avgHeartRate: { qty: 150, units: "bpm" },
  });
  assert.equal(event.eventType, "completed_workout");
  assert.equal(event.eventClass, "completed_activity");
  assert.equal(event.activityFamily, "workout");
  assert.equal(event.title, "Running");
  assert.equal(event.startAt, "2026-09-22T11:00:00.000Z");
  assert.equal(event.endAt, "2026-09-22T11:30:00.000Z");
  assert.equal(event.durationSeconds, undefined, "endAt is present, so the server derives duration itself");
  assert.equal(event.metrics.distance, 3.5);
  assert.equal(event.metrics.activeEnergyKcal, 350);
  assert.equal(event.metrics.avgHeartRateBpm, 150);
  assert.equal(event.sourceRecordId, "workout:550e8400-e29b-41d4-a716-446655440000");
});

test("builds a workout life event from duration alone when no end time is supplied", () => {
  const event = buildWorkoutLifeEvent({
    id: "abc123",
    name: "Cycling",
    start: "2026-09-22 07:00:00 -0400",
    duration: 900,
  });
  assert.equal(event.startAt, "2026-09-22T11:00:00.000Z");
  assert.equal(event.endAt, undefined);
  assert.equal(event.durationSeconds, 900);
});

test("falls back to a generic 'Workout' title when name is blank", () => {
  const event = buildWorkoutLifeEvent({ id: "x", name: "  ", start: "2026-09-22 07:00:00 -0400", duration: 60 });
  assert.equal(event.title, "Workout");
});

test("returns null for a workout with neither an end time nor a duration", () => {
  assert.equal(buildWorkoutLifeEvent({ id: "x", name: "Running", start: "2026-09-22 07:00:00 -0400" }), null);
});

test("builds a daily_steps point event, never a session", () => {
  const event = buildStepsLifeEvent({ date: "2026-09-22 12:00:00 +0000", qty: 8500 });
  assert.equal(event.eventType, "daily_steps");
  assert.equal(event.eventClass, "system");
  assert.equal(event.startAt, undefined);
  assert.equal(event.endAt, undefined);
  assert.equal(event.metrics.steps, 8500);
  assert.equal(event.title, "8,500 steps");
});

test("parseHealthExportPayload splits a full Health Auto Export body into sleep/workout/steps events", () => {
  const body = {
    data: {
      metrics: [
        {
          name: "sleep_analysis",
          units: "hr",
          data: [
            { date: "2026-09-21", totalSleep: 7.5, sleepStart: "2026-09-21 23:00:00 -0400", sleepEnd: "2026-09-22 06:30:00 -0400" },
          ],
        },
        {
          name: "step_count",
          units: "count",
          data: [
            { qty: 3000, date: "2026-09-22 08:00:00 -0400" },
            { qty: 5500, date: "2026-09-22 14:00:00 -0400" },
          ],
        },
      ],
      workouts: [
        { id: "w1", name: "Running", start: "2026-09-22 07:00:00 -0400", end: "2026-09-22 07:30:00 -0400" },
      ],
    },
  };
  const { sleepEvents, workoutEvents, stepsEvents } = parseHealthExportPayload(body);
  assert.equal(sleepEvents.length, 1);
  assert.equal(workoutEvents.length, 1);
  assert.equal(stepsEvents.length, 1);
  assert.equal(stepsEvents[0].metrics.steps, 8500, "same-day step samples are summed into one daily total");
});

test("groups steps by the local calendar date Health Auto Export sent, not by its UTC-shifted equivalent", () => {
  // 11pm Eastern is still September 21st locally, even though it is September 22nd in UTC -
  // grouping by a UTC-converted date would wrongly split this one real day into two.
  const body = {
    data: {
      metrics: [
        {
          name: "step_count",
          data: [
            { qty: 6000, date: "2026-09-21 09:00:00 -0400" },
            { qty: 1500, date: "2026-09-21 23:30:00 -0400" },
          ],
        },
      ],
      workouts: [],
    },
  };
  const { stepsEvents } = parseHealthExportPayload(body);
  assert.equal(stepsEvents.length, 1, "both samples belong to the same local day");
  assert.equal(stepsEvents[0].metrics.steps, 7500);
  assert.equal(stepsEvents[0].sourceRecordId, "steps:2026-09-21");
});

test("parseHealthExportPayload tolerates a payload with no data section", () => {
  const result = parseHealthExportPayload({});
  assert.deepEqual(result, { sleepEvents: [], workoutEvents: [], stepsEvents: [] });
});
