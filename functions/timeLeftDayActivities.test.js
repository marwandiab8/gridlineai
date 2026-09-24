const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  glanceItemFromLifeEvent,
  composeDayAtAGlance,
  loadTimeLeftActivitiesForStorylines,
} = require("./timeLeftDayActivities");

const ts = (iso) => ({ toDate: () => new Date(iso) });
const night = {
  sourceApp: "gridlineai",
  eventType: "sleep_session",
  title: "Slept",
  occurredAt: ts("2026-09-23T02:42:07Z"),
  startAt: ts("2026-09-23T02:42:07Z"),
  endAt: ts("2026-09-23T07:56:46Z"),
  metrics: { totalSleepHours: 5.11, deepHours: 0.87, remHours: 1.25, coreHours: 2.98 },
};

test("sleep reads as bedtime, hours slept, wake time and stages", () => {
  const item = glanceItemFromLifeEvent(night);
  assert.equal(item.kind, "sleep");
  assert.equal(
    item.text,
    "Went to bed at 10:42 PM, slept for 5 h 7 min, woke up at 3:56 AM (deep 52 min · REM 1 h 15 min · core 2 h 59 min)."
  );
});

test("events the journal already shows from Gridline are skipped", () => {
  for (const eventType of ["arrive_work", "journal", "image", "start_spotify", "projectReport"]) {
    assert.equal(glanceItemFromLifeEvent({ sourceApp: "gridlineai", eventType, title: "x", occurredAt: ts("2026-09-23T12:00:00Z") }), null, eventType);
  }
});

test("other sources become readable lines", () => {
  const steps = glanceItemFromLifeEvent({ sourceApp: "gridlineai", eventType: "daily_steps", metrics: { steps: 6137 }, occurredAt: ts("2026-09-23T12:00:00Z") });
  assert.equal(steps.kind, "steps");
  assert.equal(steps.text, "Walked 6,137 steps so far today.");

  const bot = glanceItemFromLifeEvent({
    sourceApp: "DartstRacker2026", eventType: "dartsRecord", title: "Bot practice: 201 vs Club (won)",
    startAt: ts("2026-09-24T00:00:00Z"),
    metadata: { practiceType: "bot", startScore: 201, rank: "club", result: "won", playerAvg: 60.3, playerDarts: 10 },
  });
  assert.equal(bot.text, "Won a 201 bot game vs Club at 8:00 PM (avg 60.3, 10 darts).");

  const walk = glanceItemFromLifeEvent({
    sourceApp: "gridlineai", eventType: "completed_workout", title: "Outdoor Walk",
    startAt: ts("2026-09-23T22:00:00Z"), endAt: ts("2026-09-23T22:32:00Z"),
    metrics: { distance: 2.4, distanceUnits: "km", activeEnergyKcal: 140.4 },
  });
  assert.equal(walk.text, "Outdoor Walk at 6:00 PM (32 min, 2.4 km, 140 kcal).");

  const gym = { sourceApp: "GYM-K2", eventType: "workout", title: "Chest", startAt: ts("2026-09-23T10:11:00Z"), metadata: { routineName: "Chest", exerciseCount: 6, allSetCount: 15 } };
  assert.equal(glanceItemFromLifeEvent(gym).text, "Chest workout (6 exercises, 15 sets), logged at 6:11 AM.");
  assert.equal(glanceItemFromLifeEvent(gym, { skipGymWorkouts: true }), null);
});

test("the glance starts with sleep, keeps activities in time order, and ends with steps", () => {
  const glance = composeDayAtAGlance(
    [
      { time: "7:06 AM EDT", atMs: Date.parse("2026-09-23T11:06:00Z"), text: "Was at work from 7:06 AM to 1:13 PM." },
      { time: "4:28 AM EDT", atMs: Date.parse("2026-09-23T08:28:00Z"), text: "Left home at 4:28 AM." },
    ],
    [
      { kind: "steps", atMs: Date.parse("2026-09-23T12:00:00Z"), text: "Walked 6,137 steps so far today." },
      { kind: "activity", atMs: Date.parse("2026-09-24T00:00:00Z"), text: "Won a 201 bot game vs Club at 8:00 PM." },
      { kind: "sleep", atMs: Date.parse("2026-09-23T02:42:07Z"), text: "Went to bed at 10:42 PM, slept for 5 h 7 min." },
    ]
  );
  assert.deepEqual(glance.map((item) => item.text), [
    "Went to bed at 10:42 PM, slept for 5 h 7 min.",
    "Left home at 4:28 AM.",
    "Was at work from 7:06 AM to 1:13 PM.",
    "Won a 201 bot game vs Club at 8:00 PM.",
    "Walked 6,137 steps so far today.",
  ]);
});

function fakeDbs(events, { member = { timeLeftCalendarId: "cal-1" }, fail = false } = {}) {
  const db = {
    collection: () => ({ doc: () => ({ get: async () => ({ exists: Boolean(member), data: () => member }) }) }),
  };
  const timeLeftDb = {
    collection: () => ({
      doc: () => ({
        collection: () => ({
          where: () => ({
            where: () => ({
              get: async () => {
                if (fail) throw Object.assign(new Error("PERMISSION_DENIED"), { code: 7 });
                return { docs: events.map((event) => ({ data: () => event })) };
              },
            }),
          }),
        }),
      }),
    }),
  };
  return { db, timeLeftDb };
}

test("loading keeps the night that ended on the report day and drops events from other days", async () => {
  const lastNight = { ...night, occurredAt: ts("2026-09-24T02:17:44Z"), startAt: ts("2026-09-24T02:17:44Z"), endAt: ts("2026-09-24T05:58:08Z") };
  const yesterdayDarts = { sourceApp: "MyDoubleProgress", eventType: "darts_practice", occurredAt: ts("2026-09-23T01:10:08Z"), metrics: { darts: 74 } };
  const { db, timeLeftDb } = fakeDbs([night, lastNight, yesterdayDarts]);
  const result = await loadTimeLeftActivitiesForStorylines({
    db,
    timeLeftDb,
    storylines: [{ identity: "email:marwan@example.com", author: "Marwan Diab", workouts: [] }],
    dayStart: new Date("2026-09-23T04:00:00Z"),
    nextDayStart: new Date("2026-09-24T04:00:00Z"),
  });
  const items = result.get("email:marwan@example.com");
  assert.equal(items.length, 1);
  assert.match(items[0].text, /^Went to bed at 10:42 PM/);
});

test("loading leaves the glance to Gridline alone when TimeLeftToLive cannot be read", async () => {
  const warnings = [];
  const { db, timeLeftDb } = fakeDbs([night], { fail: true });
  const result = await loadTimeLeftActivitiesForStorylines({
    db,
    timeLeftDb,
    storylines: [{ identity: "email:marwan@example.com", author: "Marwan Diab" }],
    dayStart: new Date("2026-09-23T04:00:00Z"),
    nextDayStart: new Date("2026-09-24T04:00:00Z"),
    logger: { warn: (msg, meta) => warnings.push(meta) },
  });
  assert.equal(result.size, 0);
  assert.equal(warnings.length, 1);
});
