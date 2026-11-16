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

module.exports = { COL_HEARTBEATS, MOVED_METERS, OWNTRACKS_QUIET_MS, haversineMeters, isHeartbeat, recordHeartbeat, stampHeartbeat };
