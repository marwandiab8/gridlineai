// TimeLeftToLive activities for the journal's "Day at a glance".
//
// TimeLeftToLive (Firebase project "timelefttolive") collects a person's day from every source:
// sleep and steps from Apple Health, K2 gym workouts, darts practice, and Gridline's own tracking.
// A journal contributor is linked to their TimeLeftToLive calendar by `timeLeftCalendarId` on
// their appMembers record. For the report day we read that calendar's life events and turn the
// ones the journal does not already show into short lines: the night's sleep first, then the
// day's activities in time order, then the day's step total. Reading timelefttolive needs the
// Cloud Functions service account to have roles/datastore.viewer on that project; without it
// the glance keeps only Gridline's own tracked activities.
const admin = require("firebase-admin");

const TIME_LEFT_PROJECT_ID = "timelefttolive";
const TIME_LEFT_APP_NAME = "timelefttolive-reader";
const TIME_ZONE = "America/Toronto";
// A night that ends on the report day can start the evening before.
const SLEEP_LOOKBACK_MS = 18 * 60 * 60 * 1000;

// Gridline events the journal already shows from its own data (notes, photos, tracking).
const GRIDLINE_SOURCE_APP = "gridlineai";
const GRIDLINE_EVENTS_FROM_TIMELEFT = new Set(["sleep_session", "daily_steps", "completed_workout"]);

function getTimeLeftDb() {
  const existing = admin.apps.find((app) => app && app.name === TIME_LEFT_APP_NAME);
  const app = existing || admin.initializeApp({ projectId: TIME_LEFT_PROJECT_ID }, TIME_LEFT_APP_NAME);
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

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clock(ms) {
  if (!ms) return "";
  return new Intl.DateTimeFormat("en-US", {
    timeZone: TIME_ZONE,
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  }).format(new Date(ms));
}

function minutesLabel(totalMinutes) {
  const minutes = Math.round(totalMinutes);
  if (!Number.isFinite(minutes) || minutes <= 0) return "";
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? `${hours} h ${rest} min` : `${hours} h`;
}

const hoursLabel = (hours) => (num(hours) && hours > 0 ? minutesLabel(hours * 60) : "");

function eventStartMs(event) {
  return toMs(event.startAt) || toMs(event.occurredAt);
}

function eventEndMs(event) {
  const end = toMs(event.endAt);
  if (end) return end;
  const start = eventStartMs(event);
  const duration = num(event.durationSeconds);
  return start && duration && duration > 0 ? start + duration * 1000 : null;
}

function sleepLine(event) {
  const start = eventStartMs(event);
  const end = eventEndMs(event);
  const metrics = event.metrics || {};
  const asleepHours = num(metrics.totalSleepHours);
  const slept = asleepHours && asleepHours > 0
    ? hoursLabel(asleepHours)
    : start && end ? minutesLabel((end - start) / 60000) : "";
  const stages = [
    ["deep", metrics.deepHours],
    ["REM", metrics.remHours],
    ["core", metrics.coreHours],
  ]
    .map(([label, hours]) => (hoursLabel(hours) ? `${label} ${hoursLabel(hours)}` : ""))
    .filter(Boolean);
  const parts = [];
  if (start) parts.push(`Went to bed at ${clock(start)}`);
  if (slept) parts.push(`slept for ${slept}`);
  if (end) parts.push(`woke up at ${clock(end)}`);
  if (!parts.length) return "";
  const sentence = parts.join(", ");
  return `${sentence.charAt(0).toUpperCase()}${sentence.slice(1)}${stages.length ? ` (${stages.join(" · ")})` : ""}.`;
}

function healthWorkoutLine(event) {
  const start = eventStartMs(event);
  const end = eventEndMs(event);
  const metrics = event.metrics || {};
  const details = [];
  const duration = start && end ? minutesLabel((end - start) / 60000) : "";
  if (duration) details.push(duration);
  if (num(metrics.distance)) details.push(`${metrics.distance} ${clean(metrics.distanceUnits) || "km"}`);
  if (num(metrics.activeEnergyKcal)) details.push(`${Math.round(metrics.activeEnergyKcal)} kcal`);
  if (num(metrics.avgHeartRateBpm)) details.push(`avg ${Math.round(metrics.avgHeartRateBpm)} bpm`);
  const when = start ? ` at ${clock(start)}` : "";
  return `${clean(event.title) || "Workout"}${when}${details.length ? ` (${details.join(", ")})` : ""}.`;
}

function gymWorkoutLine(event) {
  const md = event.metadata || {};
  const start = eventStartMs(event);
  const counts = [];
  if (num(md.exerciseCount)) counts.push(`${md.exerciseCount} exercise${md.exerciseCount === 1 ? "" : "s"}`);
  if (num(md.allSetCount)) counts.push(`${md.allSetCount} sets`);
  const name = clean(md.routineName || event.title) || "Gym";
  return `${name} workout${counts.length ? ` (${counts.join(", ")})` : ""}${start ? `, logged at ${clock(start)}` : ""}.`;
}

function dartsLine(event) {
  const md = event.metadata || {};
  const metrics = event.metrics || {};
  const start = eventStartMs(event);
  const when = start ? ` at ${clock(start)}` : "";
  if (event.eventType === "darts_practice") {
    const bits = [];
    if (num(metrics.doublesCompleted)) bits.push(`${metrics.doublesCompleted} doubles`);
    if (num(metrics.darts)) bits.push(`${metrics.darts} darts`);
    if (num(metrics.dartsPerDouble)) bits.push(`${metrics.dartsPerDouble} darts per double`);
    return `Doubles practice${when}${bits.length ? ` (${bits.join(", ")})` : ""}.`;
  }
  if (event.eventType === "dartsRecord" && clean(md.practiceType) === "bot") {
    const outcome = clean(md.result) === "won" ? "Won" : clean(md.result) === "lost" ? "Lost" : "Played";
    const game = [num(md.startScore) ? md.startScore : "", "bot game"].filter(Boolean).join(" ");
    const opponent = clean(md.rank) ? ` vs ${clean(md.rank).replace(/^./, (c) => c.toUpperCase())}` : "";
    const bits = [];
    if (num(md.playerAvg)) bits.push(`avg ${md.playerAvg}`);
    if (num(md.playerDarts)) bits.push(`${md.playerDarts} darts`);
    return `${outcome} a ${game}${opponent}${when}${bits.length ? ` (${bits.join(", ")})` : ""}.`;
  }
  if (event.eventType === "dartsRecord") {
    const bits = [];
    if (num(md.total)) bits.push(`scored ${md.total}`);
    if (clean(md.mode)) bits.push(`${clean(md.mode)} mode`);
    return `Darts practice${when}${bits.length ? ` (${bits.join(", ")})` : ""}.`;
  }
  return "";
}

/**
 * One life event as a glance item, or null when the journal already covers it (Gridline's own
 * notes, photos and tracking) or it has nothing to say.
 * kind: "sleep" (shown first), "steps" (shown last), or "activity" (in time order).
 */
function glanceItemFromLifeEvent(event, { skipGymWorkouts = false } = {}) {
  if (!event || typeof event !== "object") return null;
  const type = clean(event.eventType);
  const sourceApp = clean(event.sourceApp);
  if (sourceApp === GRIDLINE_SOURCE_APP && !GRIDLINE_EVENTS_FROM_TIMELEFT.has(type)) return null;

  const atMs = eventStartMs(event);
  if (type === "sleep_session") {
    const text = sleepLine(event);
    return text ? { kind: "sleep", atMs, text } : null;
  }
  if (type === "daily_steps") {
    const steps = num(event.metrics && event.metrics.steps);
    return steps && steps > 0
      ? { kind: "steps", atMs, text: `Walked ${Math.round(steps).toLocaleString("en-US")} steps so far today.` }
      : null;
  }
  if (type === "completed_workout") return { kind: "activity", atMs, text: healthWorkoutLine(event) };
  if (sourceApp === "GYM-K2" && type === "workout") {
    return skipGymWorkouts ? null : { kind: "activity", atMs, text: gymWorkoutLine(event) };
  }
  const darts = dartsLine(event);
  if (darts) return { kind: "activity", atMs, text: darts };

  const title = clean(event.title);
  if (!title) return null;
  return { kind: "activity", atMs, text: `${title}${atMs ? ` at ${clock(atMs)}` : ""}.` };
}

/**
 * The glance for one person: the night's sleep, then Gridline's tracked activities and the
 * TimeLeftToLive activities together in time order, then the step total.
 */
function composeDayAtAGlance(gridlineActivities, timeLeftItems) {
  const items = Array.isArray(timeLeftItems) ? timeLeftItems : [];
  const sleep = items.filter((item) => item.kind === "sleep").sort((a, b) => (a.atMs || 0) - (b.atMs || 0));
  const steps = items.filter((item) => item.kind === "steps").slice(-1);
  const middle = [
    ...(Array.isArray(gridlineActivities) ? gridlineActivities : []),
    ...items.filter((item) => item.kind === "activity"),
  ].sort((a, b) => (a.atMs || Infinity) - (b.atMs || Infinity));
  const seen = new Set();
  return [...sleep, ...middle, ...steps]
    .map((item) => ({ time: item.time || "", text: clean(item.text), atMs: item.atMs || null }))
    .filter((item) => {
      if (!item.text) return false;
      const key = item.text.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

/**
 * Glance items for each storyline whose author is linked to a TimeLeftToLive calendar.
 * Returns Map(storyline identity -> items[]). Failures (no link, no read access) leave that
 * storyline with Gridline's own activities only.
 */
async function loadTimeLeftActivitiesForStorylines({
  db,
  timeLeftDb,
  storylines,
  dayStart,
  nextDayStart,
  logger,
  runId,
}) {
  const result = new Map();
  const startMs = toMs(dayStart);
  const endMs = toMs(nextDayStart);
  if (!startMs || !endMs) return result;
  for (const line of Array.isArray(storylines) ? storylines : []) {
    const email = String(line && line.identity || "").startsWith("email:") ? line.identity.slice(6) : "";
    if (!email) continue;
    try {
      const memberSnap = await db.collection("appMembers").doc(email).get();
      const calendarId = memberSnap.exists ? clean((memberSnap.data() || {}).timeLeftCalendarId) : "";
      if (!calendarId) continue;
      const targetDb = timeLeftDb || getTimeLeftDb();
      const snap = await targetDb
        .collection("lifeCalendars")
        .doc(calendarId)
        .collection("lifeEvents")
        .where("occurredAt", ">=", new Date(startMs - SLEEP_LOOKBACK_MS))
        .where("occurredAt", "<", new Date(endMs))
        .get();
      const skipGymWorkouts = Array.isArray(line.workouts) && line.workouts.length > 0;
      const items = [];
      for (const doc of snap.docs) {
        const event = doc.data() || {};
        if (event.ingestionStatus === "invalid" || event.deletedAt) continue;
        const item = glanceItemFromLifeEvent(event, { skipGymWorkouts });
        if (!item) continue;
        // Sleep belongs to the day it ends on; everything else to the day it happened.
        const anchor = item.kind === "sleep" ? eventEndMs(event) : toMs(event.occurredAt) || item.atMs;
        if (!anchor || anchor < startMs || anchor >= endMs) continue;
        items.push(item);
      }
      if (items.length) result.set(line.identity, items);
    } catch (err) {
      if (logger && typeof logger.warn === "function") {
        logger.warn("timeLeftDayActivities: could not load TimeLeftToLive activities", {
          runId,
          code: err && err.code,
          message: err && err.message,
        });
      }
    }
  }
  return result;
}

module.exports = {
  TIME_LEFT_PROJECT_ID,
  glanceItemFromLifeEvent,
  composeDayAtAGlance,
  loadTimeLeftActivitiesForStorylines,
};
