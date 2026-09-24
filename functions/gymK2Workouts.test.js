const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  normalizeGymWorkout,
  formatSet,
  formatTimeRange,
  formatMinutes,
  formatWorkoutForAi,
  loadGymWorkoutsForStorylines,
} = require("./gymK2Workouts");

const chest = {
  status: "final",
  routineName: "Chest",
  focus: ["Chest"],
  unit: "lb",
  dateKey: "2026-09-23",
  startedAt: "2026-09-23T09:17:03.747Z",
  finishedAt: "2026-09-23T10:11:38.470Z",
  exercises: [
    {
      name: "Dips",
      firstEditTime: 1790157070786,
      lastEditTime: 1790157660009,
      sets: [{ weight: "0", reps: "9" }, { weight: "0", reps: "6 wide handle" }],
    },
    {
      name: "MTS Chest Press",
      firstEditTime: 1790155208501,
      lastEditTime: 1790155647568,
      sets: [{ weight: "40", reps: "20" }, { weight: "", reps: "" }],
    },
    { name: "Empty", sets: [{ weight: "", reps: "" }] },
  ],
};

test("normalizeGymWorkout keeps finished workouts in exercise order and drops empty sets", () => {
  const workout = normalizeGymWorkout("w1", chest);
  assert.equal(workout.routineName, "Chest");
  assert.deepEqual(workout.exercises.map((e) => e.name), ["MTS Chest Press", "Dips"]);
  assert.equal(workout.exercises[0].sets.length, 1);
  assert.equal(formatTimeRange(workout.exercises[0].startMs, workout.exercises[0].endMs), "5:20 AM – 5:27 AM");
  assert.equal(formatMinutes(workout.startMs, workout.endMs), "55 min");
});

test("normalizeGymWorkout skips drafts, archived, and empty workouts", () => {
  assert.equal(normalizeGymWorkout("d", { ...chest, status: "draft" }), null);
  assert.equal(normalizeGymWorkout("a", { ...chest, archivedAtMs: 1 }), null);
  assert.equal(normalizeGymWorkout("e", { status: "final", exercises: [] }), null);
});

test("formatSet reads like the gym app", () => {
  assert.equal(formatSet({ weight: "40", reps: "20" }), "40 lb × 20 reps");
  assert.equal(formatSet({ weight: "0", reps: "9" }), "Bodyweight × 9 reps");
  assert.equal(formatSet({ weight: "0", reps: "6 wide handle" }), "Bodyweight × 6 wide handle");
  assert.equal(formatSet({ weight: "135", reps: "1" }), "135 lb × 1 rep");
  assert.equal(formatSet({ weight: "20", reps: "8" }, "kg"), "20 kg × 8 reps");
});

test("formatWorkoutForAi attributes the workout to its author", () => {
  const line = formatWorkoutForAi(normalizeGymWorkout("w1", chest), "Marwan Diab");
  assert.match(line, /^\[workout\] 5:17 AM – 6:11 AM \[author=Marwan Diab\] Chest workout; focus Chest: MTS Chest Press \(40 lb × 20 reps\)/);
});

function fakeDb(docs) {
  return {
    collection: (name) => ({
      doc: (id) => ({
        get: async () => {
          const data = (docs[name] || {})[id];
          return { exists: Boolean(data), data: () => data };
        },
        collection: (sub) => ({
          where: (field, op, value) => ({
            get: async () => {
              if (docs.throwOnQuery) throw Object.assign(new Error("PERMISSION_DENIED"), { code: 7 });
              const rows = ((docs[`${name}/${id}/${sub}`]) || []).filter((row) => row.data[field] === value);
              return { docs: rows.map((row) => ({ id: row.id, data: () => row.data })) };
            },
          }),
        }),
      }),
    }),
  };
}

test("loadGymWorkoutsForStorylines maps linked contributors to their finished workouts", async () => {
  const db = fakeDb({ appMembers: { "marwan@example.com": { gymK2Uid: "gym-uid" } } });
  const gymDb = fakeDb({
    "users/gym-uid/workouts": [
      { id: "w1", data: chest },
      { id: "w2", data: { ...chest, dateKey: "2026-09-22" } },
      { id: "w3", data: { ...chest, status: "draft" } },
    ],
  });
  const result = await loadGymWorkoutsForStorylines({
    db,
    gymDb,
    dateKey: "2026-09-23",
    storylines: [
      { identity: "email:marwan@example.com", author: "Marwan Diab" },
      { identity: "email:ashley@example.com", author: "Ashley Trower" },
      { identity: "phone:15195550202", author: "Someone" },
    ],
  });
  assert.deepEqual([...result.keys()], ["email:marwan@example.com"]);
  assert.deepEqual(result.get("email:marwan@example.com").map((w) => w.id), ["w1"]);
});

test("loadGymWorkoutsForStorylines leaves out workouts when gym-k2 cannot be read", async () => {
  const warnings = [];
  const db = fakeDb({ appMembers: { "marwan@example.com": { gymK2Uid: "gym-uid" } } });
  const result = await loadGymWorkoutsForStorylines({
    db,
    gymDb: fakeDb({ throwOnQuery: true }),
    dateKey: "2026-09-23",
    storylines: [{ identity: "email:marwan@example.com", author: "Marwan Diab" }],
    logger: { warn: (msg, meta) => warnings.push(meta) },
  });
  assert.equal(result.size, 0);
  assert.equal(warnings.length, 1);
});
