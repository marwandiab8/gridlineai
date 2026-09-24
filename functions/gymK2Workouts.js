// K2 Gym Tracker workouts for the journal PDF.
//
// The gym app (Firebase project "gym-k2") stores each workout at users/{uid}/workouts/{id}
// with its exercises, every set, and when each exercise was worked on. A journal contributor
// is linked to their gym-app user by `gymK2Uid` on their appMembers record; their finished
// workouts for the report day are then shown in their storyline the way the app's
// "Workout Details" screen shows them. Reading gym-k2 needs the Cloud Functions service
// account to have read access (roles/datastore.viewer) on that project; without it the journal
// is simply rendered without workouts.
const admin = require("firebase-admin");

const GYM_K2_PROJECT_ID = "gym-k2";
const GYM_APP_NAME = "gym-k2-reader";
const TIME_ZONE = "America/Toronto";

function getGymDb() {
  const existing = admin.apps.find((app) => app && app.name === GYM_APP_NAME);
  const app = existing || admin.initializeApp({ projectId: GYM_K2_PROJECT_ID }, GYM_APP_NAME);
  return app.firestore();
}

function toMs(value) {
  if (value == null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) && value > 0 ? value : null;
  if (typeof value.toDate === "function") return value.toDate().getTime();
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) && ms > 0 ? ms : null;
}

function clean(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function normalizeSet(set) {
  const weight = clean(set && set.weight);
  const reps = clean(set && set.reps);
  if (!weight && !reps) return null;
  return { weight, reps, rpe: clean(set && set.rpe) };
}

/** A finished gym-app workout reduced to what the journal shows; null for drafts/archived/empty. */
function normalizeGymWorkout(id, data) {
  const doc = data || {};
  if (doc.status && doc.status !== "final") return null;
  if (doc.archivedAtMs) return null;
  const exercises = (Array.isArray(doc.exercises) ? doc.exercises : [])
    .map((exercise) => {
      const sets = (Array.isArray(exercise && exercise.sets) ? exercise.sets : []).map(normalizeSet).filter(Boolean);
      const startMs = toMs(exercise && exercise.firstEditTime);
      const endMs = toMs(exercise && exercise.lastEditTime);
      return {
        name: clean(exercise && exercise.name) || "Exercise",
        note: clean(exercise && exercise.exerciseNote),
        sets,
        startMs,
        endMs: endMs && startMs && endMs < startMs ? null : endMs,
      };
    })
    .filter((exercise) => exercise.sets.length)
    .sort((a, b) => (a.startMs || Infinity) - (b.startMs || Infinity));
  if (!exercises.length) return null;
  return {
    id,
    routineName: clean(doc.routineName) || "Workout",
    focus: (Array.isArray(doc.focus) ? doc.focus : []).map(clean).filter(Boolean),
    unit: clean(doc.unit) || "lb",
    notes: clean(doc.notes),
    startMs: toMs(doc.startedAt),
    endMs: toMs(doc.finishedAt),
    exercises,
  };
}

function formatClock(ms) {
  if (!ms) return "";
  return new Intl.DateTimeFormat("en-US", {
    timeZone: TIME_ZONE,
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  }).format(new Date(ms));
}

function formatTimeRange(startMs, endMs) {
  const start = formatClock(startMs);
  const end = formatClock(endMs);
  if (start && end && start !== end) return `${start} – ${end}`;
  return start || end;
}

function formatMinutes(startMs, endMs) {
  if (!startMs || !endMs || endMs <= startMs) return "";
  const minutes = Math.round((endMs - startMs) / 60000);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? `${hours} h ${rest} min` : `${hours} h`;
}

/** { weight: "40 lb" | "Bodyweight", reps: "20 reps" | "6 wide handle" | "" } */
function formatSetParts(set, unit = "lb") {
  const rawWeight = clean(set && set.weight);
  const weightNumber = Number(rawWeight);
  const weight = !rawWeight || weightNumber === 0
    ? "Bodyweight"
    : Number.isFinite(weightNumber)
      ? `${weightNumber} ${unit}`
      : rawWeight;
  const rawReps = clean(set && set.reps);
  const reps = !rawReps ? "" : /^\d+$/.test(rawReps) ? `${rawReps} rep${rawReps === "1" ? "" : "s"}` : rawReps;
  return { weight, reps };
}

/** "40 lb × 20 reps", "Bodyweight × 9 reps", "Bodyweight × 6 wide handle". */
function formatSet(set, unit = "lb") {
  const { weight, reps } = formatSetParts(set, unit);
  return reps ? `${weight} × ${reps}` : weight;
}

/** One line per workout for the AI input, so the story can mention it without listing every set. */
function formatWorkoutForAi(workout, author) {
  const range = formatTimeRange(workout.startMs, workout.endMs);
  const exercises = workout.exercises.map((exercise) => {
    const sets = exercise.sets.map((set) => formatSet(set, workout.unit)).join(", ");
    return `${exercise.name} (${sets})`;
  });
  const focus = workout.focus.length ? `; focus ${workout.focus.join(", ")}` : "";
  const notes = workout.notes ? `; notes: ${workout.notes}` : "";
  return `[workout] ${range ? `${range} ` : ""}[author=${author}] ${workout.routineName} workout${focus}: ${exercises.join("; ")}${notes}`.slice(0, 1200);
}

/**
 * Finished workouts for each storyline whose author is linked to a gym-app user.
 * Returns Map(storyline identity -> workouts[]). Any failure (no link, no permission on
 * gym-k2, network) leaves that storyline without workouts rather than failing the PDF.
 */
async function loadGymWorkoutsForStorylines({ db, gymDb, storylines, dateKey, logger, runId }) {
  const result = new Map();
  const lines = Array.isArray(storylines) ? storylines : [];
  for (const line of lines) {
    const email = String(line && line.identity || "").startsWith("email:") ? line.identity.slice(6) : "";
    if (!email) continue;
    try {
      const memberSnap = await db.collection("appMembers").doc(email).get();
      const gymUid = memberSnap.exists ? clean((memberSnap.data() || {}).gymK2Uid) : "";
      if (!gymUid) continue;
      const targetDb = gymDb || getGymDb();
      const snap = await targetDb
        .collection("users")
        .doc(gymUid)
        .collection("workouts")
        .where("dateKey", "==", dateKey)
        .get();
      const workouts = snap.docs
        .map((doc) => normalizeGymWorkout(doc.id, doc.data()))
        .filter(Boolean)
        .sort((a, b) => (a.startMs || 0) - (b.startMs || 0));
      if (workouts.length) result.set(line.identity, workouts);
    } catch (err) {
      if (logger && typeof logger.warn === "function") {
        logger.warn("gymK2Workouts: could not load workouts for the journal", {
          runId,
          dateKey,
          code: err && err.code,
          errorMessage: err && err.message,
        });
      }
    }
  }
  return result;
}

module.exports = {
  GYM_K2_PROJECT_ID,
  normalizeGymWorkout,
  formatSet,
  formatSetParts,
  formatTimeRange,
  formatMinutes,
  formatWorkoutForAi,
  loadGymWorkoutsForStorylines,
};
