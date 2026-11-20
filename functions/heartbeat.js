// The heartbeat: three times a day a Shortcuts automation posts the phone's position to the OwnTracks
// endpoint as a location ping tagged `t: "h"` (docs/heartbeat-shortcut.md). It is not a drive-detector
// point (the detector stays OwnTracks-only, so the comparison below means something), but it is evidence
// for open stays, and above all it says two things the health check could not know before: the phone is
// alive, and where it is. A heartbeat far from where OwnTracks last saw the phone, with OwnTracks quiet
// for hours, means OwnTracks has stopped reporting: the member is told at once, not at the next check.
const COL_HEARTBEATS = "heartbeats";
const COL_DRIVE_DETECTION = "driveDetection";
const MOVED_METERS = 500;
const OWNTRACKS_QUIET_MS = 3 * 3600 * 1000;
// Health Auto Export already posts several times a day; each export is a heartbeat of the second kind: no
// position, but the step count says whether the phone went anywhere. Steps at home stay small; a walk or
// an outing does not.
const STEPS_MOVED = 3000;
const STEPS_STAYED_PUT = 1500;
const HEALTH_OWNTRACKS_QUIET_MS = 6 * 3600 * 1000;

function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function haversineMeters(lat1, lon1, lat2, lon2) {
  const r = (d) => (d * Math.PI) / 180;
  const a = Math.sin(r(lat2 - lat1) / 2) ** 2 + Math.cos(r(lat1)) * Math.cos(r(lat2)) * Math.sin(r(lon2 - lon1) / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/** True for the Shortcut's heartbeat ping: `t: "h"`, `heartbeat: true`, or tracker id HB. */
function isHeartbeat(payload) {
  if (!payload || typeof payload !== "object" || payload._type !== "location") return false;
  return payload.t === "h" || payload.heartbeat === true || String(payload.tid || "").trim().toUpperCase() === "HB";
}

/**
 * The simplest Shortcut (docs/heartbeat-shortcut.md): a copy of "Log this place" with nothing but its URL
 * changed, so it still posts { latitude, longitude } - to this endpoint with `?heartbeat=1` on the URL.
 * Returns that request as the OwnTracks location ping tagged `t: "h"` the handler expects, or null when the
 * URL does not say heartbeat (OwnTracks itself never sends a query string).
 */
function heartbeatFromRequest(req, body) {
  const query = req && req.query && typeof req.query === "object" ? req.query : {};
  if (!("heartbeat" in query)) return null;
  const flag = String(query.heartbeat == null ? "" : query.heartbeat).trim().toLowerCase();
  if (flag === "0" || flag === "false" || flag === "no") return null;
  const src = body && typeof body === "object" ? body : {};
  const pick = (...keys) => {
    for (const key of keys) if (src[key] != null && src[key] !== "") return src[key];
    return null;
  };
  return { _type: "location", t: "h", tid: "HB", lat: pick("lat", "latitude"), lon: pick("lon", "longitude"), acc: pick("acc", "accuracy", "horizontalAccuracy"), tst: pick("tst") };
}

/** Shortcuts cannot easily send a unix time, so a heartbeat without one is stamped now. */
function stampHeartbeat(payload, nowMs = Date.now()) {
  const tst = finite(payload.tst);
  if (tst == null || tst <= 0) payload.tst = Math.floor(nowMs / 1000);
  return payload;
}

function formatWhen(ms, timezone) {
  try {
    return new Intl.DateTimeFormat("en-CA", { weekday: "short", hour: "numeric", minute: "2-digit", hour12: true, timeZone: timezone || "America/Toronto" })
      .format(new Date(ms)).replace(/\./g, "").replace(/,/g, "").toLowerCase().replace(/^./, (c) => c.toUpperCase());
  } catch {
    return new Date(ms).toISOString().slice(0, 16);
  }
}

/**
 * Records the heartbeat in heartbeats/{member} and compares it with where OwnTracks last saw the phone.
 * Returns { movedMeters, ownTracksQuietMs, ownTracksStopped } and, when OwnTracks looks stopped and
 * `sendSms` is given, texts the member once a day (through trackingHealth's alert record).
 */
async function recordHeartbeat({ db, FieldValue, member, payload, nowMs = Date.now(), sendSms = null, timezone = "America/Toronto", logger = null }) {
  const latitude = finite(payload.lat);
  const longitude = finite(payload.lon);
  if (latitude == null || longitude == null || (latitude === 0 && longitude === 0)) return null;
  const atMs = Math.round((finite(payload.tst) || nowMs / 1000) * 1000);
  const accuracy = finite(payload.acc);

  const driveSnap = await db.collection(COL_DRIVE_DETECTION).doc(member.email).get().catch(() => null);
  const drive = driveSnap && driveSnap.exists ? driveSnap.data() || {} : {};
  const last = drive.state && drive.state.last ? drive.state.last : null;
  const ownTracksLastMs = Number.isFinite(drive.lastPointAtMs) ? drive.lastPointAtMs : null;
  const movedMeters = last && Number.isFinite(last.latitude) ? Math.round(haversineMeters(latitude, longitude, last.latitude, last.longitude)) : null;
  const ownTracksQuietMs = ownTracksLastMs != null ? atMs - ownTracksLastMs : null;
  const ownTracksStopped = movedMeters != null && movedMeters > MOVED_METERS + (accuracy || 0) && ownTracksQuietMs != null && ownTracksQuietMs > OWNTRACKS_QUIET_MS;

  const ref = db.collection(COL_HEARTBEATS).doc(member.email);
  const prev = await ref.get().catch(() => null);
  const count = prev && prev.exists ? Number((prev.data() || {}).count) || 0 : 0;
  await ref.set(
    {
      memberEmail: member.email,
      lastAtMs: atMs,
      lastAt: new Date(atMs),
      latitude,
      longitude,
      accuracyMeters: accuracy != null && accuracy > 0 ? accuracy : 0,
      count: count + 1,
      ownTracksLastAtMs: ownTracksLastMs,
      movedSinceOwnTracksMeters: movedMeters,
      updatedAt: FieldValue && typeof FieldValue.serverTimestamp === "function" ? FieldValue.serverTimestamp() : new Date(nowMs),
    },
    { merge: true }
  );

  const result = { atMs, movedMeters, ownTracksQuietMs, ownTracksStopped, alerted: false };
  if (ownTracksStopped && typeof sendSms === "function") {
    const { alertIssueOnce } = require("./trackingHealth");
    const km = Math.round(movedMeters / 100) / 10;
    const issue = {
      code: "owntracks_dead",
      text: `Your phone has moved ${km} km since OwnTracks last reported (${formatWhen(ownTracksLastMs, timezone)}), so OwnTracks seems to have stopped. Open it once; location access should be Always.`,
    };
    result.alerted = await alertIssueOnce({ db, FieldValue, member, issue, sendSms, nowMs, logger });
  }
  if (logger && typeof logger.info === "function") logger.info("heartbeat: recorded", { memberEmail: member.email, movedMeters, ownTracksQuietMs, ownTracksStopped, alerted: result.alerted });
  return result;
}

/**
 * Records a Health Auto Export export as a heartbeat (fields health* in heartbeats/{member}) and keeps a
 * running count of steps taken since OwnTracks last reported. `stepsByDate` is { "YYYY-MM-DD": total } from
 * the export (the day's running total). Thousands of steps while OwnTracks has been quiet for hours means
 * the phone went somewhere OwnTracks did not report: the member is texted once a day, like the GPS heartbeat.
 */
async function recordHealthHeartbeat({ db, FieldValue, member, stepsByDate, nowMs = Date.now(), sendSms = null, timezone = "America/Toronto", logger = null }) {
  const entries = Object.entries(stepsByDate || {})
    .filter(([key, value]) => /^\d{4}-\d{2}-\d{2}$/.test(key) && Number.isFinite(Number(value)))
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const latest = entries.length ? { dateKey: entries[entries.length - 1][0], total: Math.round(Number(entries[entries.length - 1][1])) } : null;

  const driveSnap = await db.collection(COL_DRIVE_DETECTION).doc(member.email).get().catch(() => null);
  const drive = driveSnap && driveSnap.exists ? driveSnap.data() || {} : {};
  const ownTracksLastMs = Number.isFinite(drive.lastPointAtMs) ? drive.lastPointAtMs : null;

  const ref = db.collection(COL_HEARTBEATS).doc(member.email);
  const prevSnap = await ref.get().catch(() => null);
  const prev = prevSnap && prevSnap.exists ? prevSnap.data() || {} : {};
  let delta = 0;
  if (latest) {
    delta = prev.healthStepsDateKey === latest.dateKey ? Math.max(0, latest.total - (Number(prev.healthStepsTotal) || 0)) : latest.total;
  }
  // OwnTracks reported since the last export: the count starts over from this export's steps.
  const ownTracksReported = ownTracksLastMs !== (Number.isFinite(prev.healthOwnTracksLastAtMs) ? prev.healthOwnTracksLastAtMs : null);
  const stepsSinceOwnTracks = ownTracksReported ? delta : (Number(prev.healthStepsSinceOwnTracks) || 0) + delta;
  const ownTracksQuietMs = ownTracksLastMs != null ? nowMs - ownTracksLastMs : null;
  const ownTracksStopped = stepsSinceOwnTracks >= STEPS_MOVED && ownTracksQuietMs != null && ownTracksQuietMs > HEALTH_OWNTRACKS_QUIET_MS;

  await ref.set(
    {
      memberEmail: member.email,
      healthLastAtMs: nowMs,
      healthLastAt: new Date(nowMs),
      healthStepsDateKey: latest ? latest.dateKey : prev.healthStepsDateKey || null,
      healthStepsTotal: latest ? latest.total : Number.isFinite(prev.healthStepsTotal) ? prev.healthStepsTotal : null,
      healthStepsSinceOwnTracks: stepsSinceOwnTracks,
      healthOwnTracksLastAtMs: ownTracksLastMs,
      healthCount: (Number(prev.healthCount) || 0) + 1,
      updatedAt: FieldValue && typeof FieldValue.serverTimestamp === "function" ? FieldValue.serverTimestamp() : new Date(nowMs),
    },
    { merge: true }
  );

  const result = { delta, stepsSinceOwnTracks, ownTracksQuietMs, ownTracksStopped, alerted: false };
  if (ownTracksStopped && typeof sendSms === "function") {
    const { alertIssueOnce } = require("./trackingHealth");
    const issue = {
      code: "owntracks_dead",
      text: `You have taken ${stepsSinceOwnTracks.toLocaleString("en-US")} steps since OwnTracks last reported (${formatWhen(ownTracksLastMs, timezone)}, ${Math.round(ownTracksQuietMs / 3600000)} h ago), so OwnTracks seems to have stopped. Open it once; location access should be Always.`,
    };
    result.alerted = await alertIssueOnce({ db, FieldValue, member, issue, sendSms, nowMs, logger });
  }
  if (logger && typeof logger.info === "function") logger.info("heartbeat: health export recorded", { memberEmail: member.email, delta, stepsSinceOwnTracks, ownTracksQuietMs, ownTracksStopped, alerted: result.alerted });
  return result;
}

module.exports = { COL_HEARTBEATS, MOVED_METERS, OWNTRACKS_QUIET_MS, STEPS_MOVED, STEPS_STAYED_PUT, HEALTH_OWNTRACKS_QUIET_MS, haversineMeters, heartbeatFromRequest, isHeartbeat, recordHeartbeat, recordHealthHeartbeat, stampHeartbeat };
