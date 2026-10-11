// Drive detection from OwnTracks movement - no CarPlay, no Shortcuts automation.
//
// The CarPlay "connects"/"disconnects" automations fired on their own: on 2026-10-10 the phone logged
// six "Finished driving" events between 9:03 and 10:25 am while it sat at home and nobody was awake,
// with no matching "connects". A drive is movement, so this module works it out from the location
// pings OwnTracks already posts to `ownTracksEvents`, and records the same start_drive / finish_drive
// events the CarPlay Shortcuts sent - same pipeline, same dashboard notes, same TimeLeft delivery.
//
// How it decides, per member (state in `driveDetection/{memberEmail}`):
//   - A ping's speed is the larger of OwnTracks' own `vel` (km/h) and the speed worked out from the
//     distance to the previous ping - the latter only for a real jump (beyond GPS drift and both fixes'
//     accuracy) between pings close enough in time to mean something. Fixes worse than 100 m are ignored.
//   - Start: two moving pings in a row (>= 20 km/h) that have taken you >= 300 m from where you were,
//     or one that has already taken you >= 1 km. The drive is backdated to when you must have set off
//     to cover that distance at that speed - never before the previous ping - and starts where that
//     previous ping put you.
//   - Finish: no driving speed for 7 minutes (a red light or a drive-thru does not end it), or no
//     pings at all for 20 minutes (OwnTracks' significant-change mode goes quiet once you park). The
//     drive ends when you stopped, not when that was noticed. Crossing into an OwnTracks region
//     (Home/Work/Gym) marks the arrival moment and place. A scheduled sweep closes drives whose pings
//     have stopped (`closeStaleDrives`).
// Lying in bed, the phone's pings show no speed and drift less than 150 m, so nothing starts.
const { haversineMeters } = require("./placeLearning");
const { parseShortcutEventPayload } = require("./iosShortcutsIntegration");
const { closeStaysFromEvidence, recordAndDeliver, formatStay } = require("./stayClosing");

const COL_DRIVE_DETECTION = "driveDetection";
const COL_SHORTCUT_EVENTS = "iosShortcutEvents";
const DEFAULT_TIMEZONE = "America/Toronto";
const SOURCE = "owntracks_motion";

const DRIVING_SPEED_KMH = 20;
const MAX_PLAUSIBLE_SPEED_KMH = 250; // anything faster is a bad fix
const MAX_ACCURACY_METERS = 100;
const MIN_JUMP_METERS = 150; // GPS drift while standing still stays under this
const MIN_SPEED_GAP_MS = 5 * 1000;
const MAX_SPEED_GAP_MS = 20 * 60 * 1000; // a longer gap says nothing about how fast you went
const START_MIN_DISPLACEMENT_METERS = 300;
const START_SINGLE_PING_METERS = 1000;
const STOP_CONFIRM_MS = 7 * 60 * 1000;
const STOP_RESET_METERS = 1000; // crawling traffic that covers this much is still the drive
const SILENCE_MS = 20 * 60 * 1000;
const ARRIVAL_SPEED_KMH = 40; // to estimate when you pulled in between a moving and a stopped ping
// A CarPlay Shortcut (or anything else) that already recorded the same start/finish this close wins.
const EXISTING_EVENT_WINDOW_MS = 5 * 60 * 1000;

function finiteNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/**
 * The motion evidence in an OwnTracks location ping or region crossing, or null when it has no
 * usable position: { latitude, longitude, atMs, accuracyMeters, velKmh, automotive, enteredRegion }.
 */
function pointFromOwnTracks(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (payload._type !== "location" && payload._type !== "transition") return null;
  const latitude = finiteNumber(payload.lat);
  const longitude = finiteNumber(payload.lon);
  const tst = finiteNumber(payload.tst);
  if (latitude == null || longitude == null || tst == null || tst <= 0) return null;
  if (latitude === 0 && longitude === 0) return null; // no fix
  const acc = finiteNumber(payload.acc);
  const vel = finiteNumber(payload.vel);
  const activities = Array.isArray(payload.motionactivities) ? payload.motionactivities.map((a) => String(a).toLowerCase()) : [];
  return {
    latitude,
    longitude,
    atMs: Math.round(tst * 1000),
    accuracyMeters: acc != null && acc > 0 ? acc : 0,
    velKmh: vel != null && vel >= 0 ? vel : null,
    automotive: activities.includes("automotive"),
    enteredRegion: payload._type === "transition" && payload.event === "enter" ? String(payload.desc || "").trim() || null : null,
  };
}

function idleState() {
  return { status: "idle", last: null, candidate: null, drive: null };
}

function distanceBetween(a, b) {
  return haversineMeters(a.latitude, a.longitude, b.latitude, b.longitude);
}

/**
 * How fast you are going at `point`, in km/h (null when the pings cannot tell), plus the distance from
 * `prev`. OwnTracks' own `vel` is the speed at that instant, so it wins: a ping that says 3 km/h in the
 * site lot means you have parked, even though the average since the last ping was highway speed.
 * Without it, the average between the pings stands in.
 */
function speedBetween(prev, point) {
  const distance = prev ? distanceBetween(prev, point) : 0;
  if (point.velKmh != null) return { distance, speedKmh: point.velKmh };
  if (!prev) return { distance, speedKmh: null };
  const gap = point.atMs - prev.atMs;
  if (gap < MIN_SPEED_GAP_MS || gap > MAX_SPEED_GAP_MS) return { distance, speedKmh: null };
  const jump = Math.max(MIN_JUMP_METERS, prev.accuracyMeters + point.accuracyMeters);
  if (distance < jump) return { distance, speedKmh: 0 }; // still within drift of the last fix
  const kmh = (distance / (gap / 1000)) * 3.6;
  return { distance, speedKmh: kmh <= MAX_PLAUSIBLE_SPEED_KMH ? kmh : null };
}

function isMoving(point, speedKmh) {
  if (speedKmh != null) return speedKmh >= DRIVING_SPEED_KMH;
  return point.automotive === true;
}

/** When you must have set off from `prev` to be `distance` away at `point` going `speedKmh`. */
function estimateDeparture(prev, point, distance, speedKmh) {
  if (!prev) return point.atMs;
  const metersPerSecond = Math.max(speedKmh || 0, DRIVING_SPEED_KMH) / 3.6;
  const travelMs = (distance / metersPerSecond) * 1000;
  return Math.round(Math.max(prev.atMs, point.atMs - travelMs));
}

/** When you pulled in, somewhere between a moving `prev` and a stopped `point`. */
function estimateArrival(prev, point, distance) {
  if (!prev) return point.atMs;
  const travelMs = (distance / (ARRIVAL_SPEED_KMH / 3.6)) * 1000;
  return Math.round(Math.min(point.atMs, prev.atMs + travelMs));
}

function finishDrive(state, stop, reason) {
  const drive = state.drive;
  const emission = {
    kind: "finish",
    driveId: drive.id,
    atMs: stop.atMs,
    latitude: stop.latitude,
    longitude: stop.longitude,
    label: stop.label || null,
    startedAtMs: drive.startedAtMs,
    distanceMeters: Math.round(drive.distanceMeters),
    maxSpeedKmh: Math.round(drive.maxSpeedKmh),
    reason,
  };
  return { state: { ...state, status: "idle", candidate: null, drive: null }, emission };
}

/**
 * Moves the detector on by one ping. Pure: returns { state, emissions, ignored } where each emission
 * is a { kind: "start" | "finish", ... } to record. Out-of-order, duplicate and inaccurate pings are
 * ignored without touching the state.
 */
function advanceDriveState(previousState, point) {
  const state = previousState && previousState.status ? JSON.parse(JSON.stringify(previousState)) : idleState();
  if (!point) return { state, emissions: [], ignored: "no_position" };
  if (point.accuracyMeters > MAX_ACCURACY_METERS) return { state, emissions: [], ignored: "inaccurate" };
  const prev = state.last;
  if (prev && point.atMs <= prev.atMs) return { state, emissions: [], ignored: "out_of_order" };

  const emissions = [];
  const { distance, speedKmh } = speedBetween(prev, point);
  const moving = isMoving(point, speedKmh);
  const fix = { latitude: point.latitude, longitude: point.longitude, atMs: point.atMs, accuracyMeters: point.accuracyMeters };

  if (state.status === "driving" && state.drive) {
    const drive = state.drive;
    // The pings stopped long enough for the drive to be over before this one arrived, and this one
    // finds you where the last one did: you parked there. (Far away instead, you drove in the gap.)
    const parkedInGap = prev && distance < Math.max(MIN_JUMP_METERS, prev.accuracyMeters + point.accuracyMeters);
    if (prev && point.atMs - prev.atMs >= SILENCE_MS && !moving && parkedInGap) {
      const stop = drive.stop || { latitude: prev.latitude, longitude: prev.longitude, atMs: prev.atMs };
      const done = finishDrive(state, stop, "silence");
      emissions.push(done.emission);
      return { state: { ...done.state, last: fix }, emissions };
    }
    drive.distanceMeters += distance;
    if (speedKmh != null) drive.maxSpeedKmh = Math.max(drive.maxSpeedKmh, speedKmh);
    if (moving) {
      drive.lastMovingAtMs = point.atMs;
      drive.stop = null;
    } else if (drive.stop && distanceBetween(drive.stop, point) > STOP_RESET_METERS) {
      drive.stop = { latitude: point.latitude, longitude: point.longitude, atMs: point.atMs };
    } else if (!drive.stop) {
      drive.stop = { latitude: point.latitude, longitude: point.longitude, atMs: estimateArrival(prev, point, distance) };
    }
    if (point.enteredRegion) {
      // Crossing into Home/Work/Gym is the arrival moment and place - confirmed once you stay put
      // (a later moving ping, driving on through the region, clears it again).
      drive.stop = { latitude: point.latitude, longitude: point.longitude, atMs: point.atMs, label: point.enteredRegion };
    }
    state.last = fix;
    if (drive.stop && point.atMs - drive.stop.atMs >= STOP_CONFIRM_MS) {
      const done = finishDrive(state, drive.stop, "stopped");
      emissions.push(done.emission);
      return { state: done.state, emissions };
    }
    return { state, emissions };
  }

  // Idle: is this the start of a drive?
  if (prev && point.atMs - prev.atMs > MAX_SPEED_GAP_MS) state.candidate = null;
  if (!moving) {
    state.candidate = null;
    state.last = fix;
    return { state, emissions };
  }
  if (!state.candidate) {
    const origin = prev || fix;
    state.candidate = {
      count: 0,
      origin: { latitude: origin.latitude, longitude: origin.longitude, atMs: origin.atMs },
      startAtMs: estimateDeparture(prev, point, distance, speedKmh),
      distanceMeters: 0,
      maxSpeedKmh: 0,
    };
  }
  const candidate = state.candidate;
  candidate.count += 1;
  candidate.distanceMeters += distance;
  candidate.maxSpeedKmh = Math.max(candidate.maxSpeedKmh, speedKmh || 0);
  const displacement = distanceBetween(candidate.origin, point);
  state.last = fix;

  const confirmed =
    (candidate.count >= 2 && displacement >= START_MIN_DISPLACEMENT_METERS) ||
    (speedKmh != null && displacement >= START_SINGLE_PING_METERS);
  if (!confirmed) return { state, emissions };

  const driveId = String(candidate.startAtMs);
  state.status = "driving";
  state.candidate = null;
  state.drive = {
    id: driveId,
    startedAtMs: candidate.startAtMs,
    startLatitude: candidate.origin.latitude,
    startLongitude: candidate.origin.longitude,
    distanceMeters: candidate.distanceMeters,
    maxSpeedKmh: candidate.maxSpeedKmh,
    lastMovingAtMs: point.atMs,
    stop: null,
  };
  emissions.push({
    kind: "start",
    driveId,
    atMs: candidate.startAtMs,
    latitude: candidate.origin.latitude,
    longitude: candidate.origin.longitude,
  });
  return { state, emissions };
}

/**
 * Closes a drive whose pings have stopped, as of `nowMs`: a confirmed stop ends it when you stopped,
 * and 20 minutes of silence ends it at the last ping. Pure, like advanceDriveState.
 */
function checkDriveTimeout(previousState, nowMs) {
  if (!previousState || previousState.status !== "driving" || !previousState.drive) {
    return { state: previousState || idleState(), emissions: [] };
  }
  const state = JSON.parse(JSON.stringify(previousState));
  const { drive, last } = state;
  if (drive.stop && nowMs - drive.stop.atMs >= STOP_CONFIRM_MS) {
    const done = finishDrive(state, drive.stop, "stopped");
    return { state: done.state, emissions: [done.emission] };
  }
  if (last && nowMs - last.atMs >= SILENCE_MS) {
    const done = finishDrive(state, { latitude: last.latitude, longitude: last.longitude, atMs: last.atMs }, "silence");
    return { state: done.state, emissions: [done.emission] };
  }
  return { state, emissions: [] };
}

function formatKm(meters) {
  return `${Math.round(meters / 100) / 10} km`;
}

function emissionNotes(emission) {
  if (emission.kind === "start") return "Detected from movement (OwnTracks)";
  const minutes = (emission.atMs - emission.startedAtMs) / 60000;
  return `Detected from movement (OwnTracks): drove ${formatKm(emission.distanceMeters)} in ${formatStay(minutes)}`;
}

function memberTimezone(member) {
  const tz = member && member.data && (member.data.timezone || member.data.timeZone);
  return typeof tz === "string" && tz.trim() ? tz.trim() : DEFAULT_TIMEZONE;
}

/** A start/finish already recorded near this moment, by a Shortcut or by an earlier sweep. */
async function findNearbyDriveEvent(db, memberEmail, eventType, atMs) {
  const snap = await db
    .collection(COL_SHORTCUT_EVENTS)
    .where("memberEmail", "==", memberEmail)
    .where("eventType", "==", eventType)
    .where("eventAtMs", ">=", atMs - EXISTING_EVENT_WINDOW_MS)
    .where("eventAtMs", "<=", atMs + EXISTING_EVENT_WINDOW_MS)
    .limit(1)
    .get()
    .catch(() => null);
  return snap && !snap.empty ? snap.docs[0].id : null;
}

/** A request stand-in carrying a stable idempotency key, so a retried emission is recorded once. */
function idempotentRequest(key) {
  return { get: (name) => (String(name).toLowerCase() === "idempotency-key" ? key : "") };
}

/**
 * Records one start/finish emission as a start_drive/finish_drive event through the shared Shortcuts
 * pipeline. Returns { recorded, skipped?, shortcutEventId? }. Never throws.
 */
async function recordDriveEmission({ db, FieldValue, member, emission, deviceName, deps = {} }) {
  const eventType = emission.kind === "start" ? "start_drive" : "finish_drive";
  try {
    const existing = await findNearbyDriveEvent(db, member.email, eventType, emission.atMs);
    if (existing) return { recorded: false, skipped: "already_recorded", shortcutEventId: existing };

    const timezone = memberTimezone(member);
    const parsed = parseShortcutEventPayload({
      event_type: eventType,
      timestamp: new Date(emission.atMs).toISOString(),
      timezone,
      latitude: emission.latitude,
      longitude: emission.longitude,
      ...(emission.label ? { location_label: emission.label } : {}),
      device_name: deviceName || null,
      source: SOURCE,
      notes: emissionNotes(emission),
    });
    if (!parsed.ok) return { recorded: false, skipped: parsed.code };

    const req = idempotentRequest(`drive-${emission.kind}-${emission.driveId}`);
    const result = await recordAndDeliver({ db, FieldValue, req, member, event: parsed.event, ...deps });

    // Setting off ends any "Log this place" stay you drove away from, at the moment you left.
    if (emission.kind === "start" && !result.duplicate) {
      await closeStaysFromEvidence({
        db,
        FieldValue,
        req,
        member,
        timezone,
        deviceName,
        deps,
        evidence: { latitude: emission.latitude, longitude: emission.longitude, at: new Date(emission.atMs), eventType: "start_drive" },
      });
    }
    return { recorded: !result.duplicate, shortcutEventId: result.shortcutEventId };
  } catch (err) {
    if (deps.logger && typeof deps.logger.warn === "function") {
      deps.logger.warn("driveDetection: could not record drive event", { runId: deps.runId, eventType, message: err && err.message });
    }
    return { recorded: false, skipped: "error" };
  }
}

async function updateDriveState(db, memberEmail, step) {
  const ref = db.collection(COL_DRIVE_DETECTION).doc(memberEmail);
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const current = snap.exists ? (snap.data() || {}).state || null : null;
    const next = step(current);
    if (next.ignored) return next;
    tx.set(ref, {
      memberEmail,
      status: next.state.status,
      lastPointAtMs: next.state.last ? next.state.last.atMs : null,
      state: next.state,
      updatedAtMs: Date.now(),
    });
    return next;
  });
}

function driveDetectionEnabled(member) {
  return !(member && member.data && member.data.driveDetection && member.data.driveDetection.enabled === false);
}

/**
 * Feeds one OwnTracks message (ping or crossing) to the member's drive detector and records any drive
 * it starts or finishes. Never throws and never blocks the OwnTracks acknowledgement on failure.
 */
async function applyOwnTracksToDrive({ db, FieldValue, member, payload, deps = {} }) {
  if (!driveDetectionEnabled(member)) return [];
  const point = pointFromOwnTracks(payload);
  if (!point) return [];
  try {
    const next = await updateDriveState(db, member.email, (current) => advanceDriveState(current, point));
    const out = [];
    for (const emission of next.emissions || []) {
      const result = await recordDriveEmission({ db, FieldValue, member, emission, deviceName: payload.tid || null, deps });
      out.push({ ...emission, ...result });
    }
    return out;
  } catch (err) {
    if (deps.logger && typeof deps.logger.warn === "function") {
      deps.logger.warn("driveDetection: could not apply ping", { runId: deps.runId, message: err && err.message });
    }
    return [];
  }
}

/**
 * Scheduled sweep: finishes every drive whose pings have stopped (see checkDriveTimeout). Members are
 * loaded from `appMembers` for the shared recording pipeline. Returns the number of drives finished.
 */
async function closeStaleDrives({ db, FieldValue, nowMs = Date.now(), deps = {} }) {
  const snap = await db.collection(COL_DRIVE_DETECTION).where("status", "==", "driving").get();
  let finished = 0;
  for (const doc of snap.docs) {
    const memberEmail = doc.id;
    try {
      const next = await updateDriveState(db, memberEmail, (current) => {
        const result = checkDriveTimeout(current, nowMs);
        return result.emissions.length ? result : { ...result, ignored: "still_driving" };
      });
      if (!next.emissions || !next.emissions.length) continue;
      const memberSnap = await db.collection("appMembers").doc(memberEmail).get();
      if (!memberSnap.exists) continue;
      const member = { email: memberEmail, data: memberSnap.data() || {} };
      for (const emission of next.emissions) {
        await recordDriveEmission({ db, FieldValue, member, emission, deps });
        finished += 1;
      }
    } catch (err) {
      if (deps.logger && typeof deps.logger.warn === "function") {
        deps.logger.warn("driveDetection: sweep failed for member", { runId: deps.runId, memberEmail, message: err && err.message });
      }
    }
  }
  return finished;
}

module.exports = {
  COL_DRIVE_DETECTION,
  DRIVING_SPEED_KMH,
  STOP_CONFIRM_MS,
  SILENCE_MS,
  advanceDriveState,
  applyOwnTracksToDrive,
  checkDriveTimeout,
  closeStaleDrives,
  idleState,
  pointFromOwnTracks,
  recordDriveEmission,
};
