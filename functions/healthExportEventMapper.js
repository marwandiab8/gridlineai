// Turns a Health Auto Export payload (see docs/health-auto-export-integration.md for the
// app's own JSON shape) into life events shaped exactly like TimeLeftToLive's ingestion API
// expects (schemaVersion, sourceApp, eventType, eventClass, startAt/endAt, etc. - see
// functions/src/ingestion/lifeEventFoundation.js in the timelefttolive repo for the receiving
// side's validation, which this intentionally mirrors field-for-field).
//
// Unlike the iOS Shortcuts events, these already carry a real start AND end time from Apple
// Health, so there is no arrive/leave pairing to do here - each record becomes exactly one
// life event with startAt/endAt set directly.

const DEFAULT_SOURCE_APP = "gridlineai";
const DEFAULT_SOURCE_FIREBASE_PROJECT_ID = "gridlineai";
const DEFAULT_TIMEZONE = "America/Toronto";
const SCHEMA_VERSION = 1;

/** Health Auto Export sends "yyyy-MM-dd HH:mm:ss Z" (e.g. "2026-09-21 23:00:00 -0400"). */
function parseHealthAutoExportDate(value) {
  const text = String(value == null ? "" : value).trim();
  if (!text) return null;
  const match = /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) ([+-]\d{2})(\d{2})$/.exec(text);
  const iso = match ? `${match[1]}T${match[2]}${match[3]}:${match[4]}` : text;
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? null : date;
}

function toIso(value) {
  const date = value instanceof Date ? value : parseHealthAutoExportDate(value);
  return date ? date.toISOString() : null;
}

function roundNumber(value, decimals = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) return undefined;
  const factor = 10 ** decimals;
  return Math.round(num * factor) / factor;
}

/**
 * Sleep can arrive "aggregated" (one row per night, with sleepStart/sleepEnd/totalSleep etc.)
 * or "unaggregated" (many rows per night, one per sleep stage segment, each still carrying the
 * same overall sleepStart/sleepEnd). Either way, every row belonging to the same night shares
 * the same sleepStart+sleepEnd pair, so deduping on that pair yields exactly one session per
 * night regardless of which export mode is configured.
 */
function dedupeSleepDataPoints(dataPoints) {
  const byNight = new Map();
  for (const point of Array.isArray(dataPoints) ? dataPoints : []) {
    const start = point && point.sleepStart;
    const end = point && point.sleepEnd;
    if (!start || !end) continue;
    const key = `${start}|${end}`;
    const existing = byNight.get(key);
    // Prefer an aggregated row (has totalSleep) over a bare stage segment, if both appear.
    if (!existing || (point.totalSleep !== undefined && existing.totalSleep === undefined)) {
      byNight.set(key, point);
    }
  }
  return [...byNight.values()];
}

function buildSleepLifeEvent(point) {
  const startAt = toIso(point.sleepStart);
  const endAt = toIso(point.sleepEnd);
  if (!startAt || !endAt) return null;
  const sourceRecordId = `sleep:${point.sleepStart}|${point.sleepEnd}`;
  const metrics = {};
  if (Number.isFinite(Number(point.totalSleep))) metrics.totalSleepHours = roundNumber(point.totalSleep);
  if (Number.isFinite(Number(point.core))) metrics.coreHours = roundNumber(point.core);
  if (Number.isFinite(Number(point.deep))) metrics.deepHours = roundNumber(point.deep);
  if (Number.isFinite(Number(point.rem))) metrics.remHours = roundNumber(point.rem);
  if (Number.isFinite(Number(point.inBed))) metrics.inBedHours = roundNumber(point.inBed);

  return {
    schemaVersion: SCHEMA_VERSION,
    sourceApp: DEFAULT_SOURCE_APP,
    sourceFirebaseProjectId: DEFAULT_SOURCE_FIREBASE_PROJECT_ID,
    sourceRecordId,
    eventType: "sleep_session",
    eventClass: "activity_boundary",
    activityFamily: "sleep",
    categoryId: "sleep",
    title: "Slept",
    occurredAt: startAt,
    startAt,
    endAt,
    timezone: DEFAULT_TIMEZONE,
    privacyLevel: "ownerOnly",
    metrics: Object.keys(metrics).length ? metrics : undefined,
  };
}

function buildWorkoutLifeEvent(workout) {
  const startAt = toIso(workout.start);
  const endAt = toIso(workout.end);
  if (!startAt) return null;
  const durationSeconds = Number.isFinite(Number(workout.duration))
    ? Math.max(0, Math.round(Number(workout.duration)))
    : undefined;
  if (!endAt && durationSeconds === undefined) return null;
  const sourceRecordId = workout.id ? `workout:${workout.id}` : `workout:${workout.start}`;
  const metrics = {};
  if (Number.isFinite(Number(workout.distance?.qty))) {
    metrics.distance = roundNumber(workout.distance.qty);
    metrics.distanceUnits = workout.distance.units || undefined;
  }
  if (Number.isFinite(Number(workout.activeEnergyBurned?.qty))) {
    metrics.activeEnergyKcal = roundNumber(workout.activeEnergyBurned.qty, 0);
  }
  if (Number.isFinite(Number(workout.totalEnergy?.qty))) {
    metrics.totalEnergyKcal = roundNumber(workout.totalEnergy.qty, 0);
  }
  if (Number.isFinite(Number(workout.avgHeartRate?.qty))) {
    metrics.avgHeartRateBpm = roundNumber(workout.avgHeartRate.qty, 0);
  }

  return {
    schemaVersion: SCHEMA_VERSION,
    sourceApp: DEFAULT_SOURCE_APP,
    sourceFirebaseProjectId: DEFAULT_SOURCE_FIREBASE_PROJECT_ID,
    sourceRecordId,
    eventType: "completed_workout",
    eventClass: "completed_activity",
    activityFamily: "workout",
    categoryId: "workout",
    title: String(workout.name || "Workout").trim() || "Workout",
    occurredAt: startAt,
    startAt,
    endAt: endAt || undefined,
    durationSeconds: endAt ? undefined : durationSeconds, // let the server compute it from startAt/endAt when both are known
    timezone: DEFAULT_TIMEZONE,
    privacyLevel: "ownerOnly",
    metrics: Object.keys(metrics).length ? metrics : undefined,
  };
}

/**
 * Steps have no natural start/end - they're a running daily total, not a session. Recorded as a
 * system-class point event (shows in the Moments list, never invents ring/duration time), one per
 * calendar day, keyed so a re-export of the same day updates the same record instead of piling up
 * duplicates as the day's count keeps climbing.
 */
function buildStepsLifeEvent(point) {
  const at = toIso(point.date);
  if (!at) return null;
  const qty = Number(point.qty);
  if (!Number.isFinite(qty)) return null;
  const dateKey = at.slice(0, 10);
  return {
    schemaVersion: SCHEMA_VERSION,
    sourceApp: DEFAULT_SOURCE_APP,
    sourceFirebaseProjectId: DEFAULT_SOURCE_FIREBASE_PROJECT_ID,
    sourceRecordId: `steps:${dateKey}`,
    eventType: "daily_steps",
    eventClass: "system",
    activityFamily: "steps",
    categoryId: "steps",
    title: `${Math.round(qty).toLocaleString("en-US")} steps`,
    occurredAt: at,
    timezone: DEFAULT_TIMEZONE,
    privacyLevel: "ownerOnly",
    metrics: { steps: Math.round(qty) },
  };
}

/** Splits a raw Health Auto Export payload into the three record kinds this integration understands. */
function parseHealthExportPayload(body) {
  const data = body && typeof body === "object" ? body.data : null;
  const metrics = Array.isArray(data?.metrics) ? data.metrics : [];
  const workouts = Array.isArray(data?.workouts) ? data.workouts : [];

  const sleepMetric = metrics.find((metric) => metric && metric.name === "sleep_analysis");
  const stepsMetric = metrics.find((metric) => metric && metric.name === "step_count");

  const sleepEvents = dedupeSleepDataPoints(sleepMetric?.data)
    .map((point) => buildSleepLifeEvent(point))
    .filter(Boolean);
  const workoutEvents = workouts.map((workout) => buildWorkoutLifeEvent(workout)).filter(Boolean);

  // step_count arrives as one row per sample interval (often hourly); sum same-day rows into one
  // running daily total per date rather than sending dozens of tiny point events per day.
  const stepsByDate = new Map();
  for (const point of Array.isArray(stepsMetric?.data) ? stepsMetric.data : []) {
    const at = toIso(point?.date);
    const qty = Number(point?.qty);
    if (!at || !Number.isFinite(qty)) continue;
    const dateKey = at.slice(0, 10);
    stepsByDate.set(dateKey, (stepsByDate.get(dateKey) || 0) + qty);
  }
  const stepsEvents = [...stepsByDate.entries()]
    .map(([dateKey, qty]) => buildStepsLifeEvent({ date: `${dateKey} 12:00:00 +0000`, qty }))
    .filter(Boolean);

  return { sleepEvents, workoutEvents, stepsEvents };
}

module.exports = {
  parseHealthAutoExportDate,
  dedupeSleepDataPoints,
  buildSleepLifeEvent,
  buildWorkoutLifeEvent,
  buildStepsLifeEvent,
  parseHealthExportPayload,
};
