// A stop nobody has named yet. When a drive ends somewhere that is not a Known Place, the spot is
// remembered (pendingStops). When the next evidence shows the member has left, a stay of 15 minutes or
// more becomes a visit: the spot is named from OpenStreetMap (placeNaming.js), saved as a Known Place with
// a suggested activity, logged through the usual pipeline (arrive_location + leave_location carrying the
// stay, delivered to Time Left), and the member gets one text. Replying OK keeps it; replying
// "PLACE <name>, <activity>" renames it, and the two events are re-sent to Time Left (handlePlaceReply, from
// the SMS webhook). A stop OwnTracks reported as a Home/Work/Gym crossing, or that the member logged by hand
// in the meantime, is left to those. See docs/tracking-reliability.md.
const { COL_KNOWN_PLACES, DEFAULT_RADIUS_METERS, findNearbyKnownPlace, haversineMeters, normalizePlaceName } = require("./placeLearning");
const { normalizePlaceActivity, suggestPlaceActivity, suggestedActivityFields } = require("./placeActivities");
const { departureFromEvidence } = require("./stayEvidence");
const { parseShortcutEventPayload } = require("./iosShortcutsIntegration");
const { nameStop } = require("./placeNaming");
const { upsertTimeLeftDeliveryState } = require("./timeLeftDeliveryRepository");

const COL_PENDING = "pendingStops";
const COL_CONFIRMATIONS = "placeConfirmations";
const COL_STATE = "unknownStopState";
const COL_SHORTCUT_EVENTS = "iosShortcutEvents";
const COL_LOG_ENTRIES = "logEntries";
const SOURCE = "owntracks_motion";
const DEFAULT_TIMEZONE = "America/Toronto";
// Shorter than this is a stop at the lights or a drive-through, not a visit.
const MIN_STAY_MS = 15 * 60 * 1000;
// Evidence within this of the stop (plus the fix's accuracy) means you are still there.
const STILL_THERE_METERS = 150;
// Beyond that plus this margin you have clearly left; in between is too vague to say.
const AWAY_MARGIN_METERS = 300;
const SEEN_WRITE_INTERVAL_MS = 5 * 60 * 1000;
// A Home/Work/Gym crossing this close to the stop's start means OwnTracks covered the stay.
const REGION_WINDOW_MS = 10 * 60 * 1000;
const REGION_ARRIVAL_TYPES = ["arrive_home", "arrive_work", "arrive_gym"];
const MAX_TEXTS_PER_DAY = 3;
const CONFIRMATION_TTL_MS = 48 * 3600 * 1000;
const ADOPT_METERS = 200;

function idempotentRequest(key) {
  return { get: (name) => (String(name).toLowerCase() === "idempotency-key" ? key : "") };
}

function memberTimezone(member) {
  const tz = member && member.data && (member.data.timezone || member.data.timeZone);
  return typeof tz === "string" && tz.trim() ? tz.trim() : DEFAULT_TIMEZONE;
}

function dateKey(ms, timezone) {
  try {
    return new Intl.DateTimeFormat("en-CA", { timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(ms));
  } catch {
    return new Date(ms).toISOString().slice(0, 10);
  }
}

function weekdayClock(ms, timezone) {
  try {
    return new Intl.DateTimeFormat("en-CA", { timeZone: timezone, weekday: "short", hour: "numeric", minute: "2-digit", hour12: true })
      .format(new Date(ms))
      .replace(/\./g, "")
      .replace(/,/g, "")
      .toLowerCase()
      .replace(/^./, (c) => c.toUpperCase());
  } catch {
    return new Date(ms).toISOString().slice(11, 16);
  }
}

function warnOf(deps) {
  return (message, extra) => deps && deps.logger && typeof deps.logger.warn === "function" && deps.logger.warn(message, { runId: deps.runId, ...extra });
}

async function findOpenStop(db, memberEmail) {
  const snap = await db.collection(COL_PENDING).where("memberEmail", "==", memberEmail).where("status", "==", "open").limit(1).get().catch(() => null);
  if (!snap || snap.empty) return null;
  const doc = snap.docs[0];
  return { id: doc.id, ...(doc.data() || {}) };
}

/**
 * Remembers where a drive ended when the spot is not a Known Place. `stop` is the finish emission from
 * driveDetection.js ({ driveId, latitude, longitude, atMs }). An older stop still open is superseded: its
 * departure was never seen, so nothing can be said about it.
 */
async function openPendingStop({ db, FieldValue, member, stop, deps = {} }) {
  const warn = warnOf(deps);
  try {
    if (!stop || !Number.isFinite(stop.latitude) || !Number.isFinite(stop.longitude) || !Number.isFinite(stop.atMs)) return null;
    const open = await findOpenStop(db, member.email);
    if (open) {
      if (open.driveId && stop.driveId && open.driveId === stop.driveId) return open;
      await db.collection(COL_PENDING).doc(open.id).set({ status: "superseded", supersededAtMs: stop.atMs, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
    }
    const ref = db.collection(COL_PENDING).doc();
    const doc = {
      memberEmail: member.email,
      status: "open",
      latitude: stop.latitude,
      longitude: stop.longitude,
      arrivedAtMs: stop.atMs,
      lastSeenAtMs: stop.atMs,
      driveStartedAtMs: null,
      driveId: stop.driveId || null,
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    };
    await ref.set(doc);
    return { id: ref.id, ...doc };
  } catch (err) {
    warn("unknownStops: could not remember the stop", { message: err && err.message });
    return null;
  }
}

/** "Log this place" at the stop: the member named it themselves, so that flow owns the visit from here. */
async function adoptPendingStopAt({ db, FieldValue, member, latitude, longitude, deps = {} }) {
  try {
    const open = await findOpenStop(db, member.email);
    if (!open || !Number.isFinite(latitude) || !Number.isFinite(longitude)) return false;
    if (haversineMeters(latitude, longitude, open.latitude, open.longitude) > ADOPT_METERS) return false;
    await db.collection(COL_PENDING).doc(open.id).set({ status: "named_manually", updatedAt: FieldValue.serverTimestamp() }, { merge: true });
    return true;
  } catch (err) {
    warnOf(deps)("unknownStops: could not adopt the stop", { message: err && err.message });
    return false;
  }
}

async function regionArrivalBetween(db, memberEmail, fromMs, toMs) {
  for (const eventType of REGION_ARRIVAL_TYPES) {
    const snap = await db
      .collection(COL_SHORTCUT_EVENTS)
      .where("memberEmail", "==", memberEmail)
      .where("eventType", "==", eventType)
      .where("eventAtMs", ">=", fromMs)
      .where("eventAtMs", "<=", toMs)
      .limit(1)
      .get()
      .catch(() => null);
    if (snap && !snap.empty) return snap.docs[0].id;
  }
  return null;
}

/** Whether another "new place" text may go out today (at most MAX_TEXTS_PER_DAY), counting it if so. */
async function claimDailyText(db, FieldValue, memberEmail, nowMs, timezone) {
  const ref = db.collection(COL_STATE).doc(memberEmail);
  const today = dateKey(nowMs, timezone);
  const snap = await ref.get().catch(() => null);
  const state = snap && snap.exists ? snap.data() || {} : {};
  const count = state.textsDateKey === today ? Number(state.textsToday) || 0 : 0;
  if (count >= MAX_TEXTS_PER_DAY) return false;
  await ref.set({ memberEmail, textsDateKey: today, textsToday: count + 1, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
  return true;
}

/** The text that asks the member about a place logged for them. */
function composePlaceText({ name, town, activity, arrivedAtMs, leftAtMs, durationMinutes, timezone, source }) {
  const { formatStay } = require("./stayClosing");
  const where = town && !name.includes(town) ? `${name} (${town})` : name;
  const start = weekdayClock(arrivedAtMs, timezone);
  const end = weekdayClock(leftAtMs, timezone).replace(/^[A-Za-z]{3} /, "");
  const named = source === "coordinates" ? " The map had no name for the spot." : "";
  return `New place logged: ${where}, ${start} to ${end} (${formatStay(durationMinutes)}), as "${activity}".${named} Reply OK to keep it, or PLACE name, activity to rename (e.g. PLACE Raging Bull, Darts).`.slice(0, 480);
}

/**
 * Turns a settled stop into a Known Place and a logged visit, and texts the member once. Returns what was
 * done. Called by applyEvidenceToPendingStop once the departure is known.
 */
async function finalizeStop({ db, FieldValue, member, stop, leftAtMs, basis, deps = {} }) {
  const warn = warnOf(deps);
  const { buildVisitBody, closedStayNote, recordAndDeliver } = require("./stayClosing");
  const timezone = memberTimezone(member);
  const nowMs = typeof deps.now === "function" ? deps.now() : Date.now();
  const stopRef = db.collection(COL_PENDING).doc(stop.id);
  const durationMs = leftAtMs - stop.arrivedAtMs;
  const done = async (status, extra = {}) => {
    await stopRef.set({ status, leftAtMs, durationMinutes: Math.round(durationMs / 60000), basis, ...extra, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
    return { status, ...extra };
  };

  if (durationMs < MIN_STAY_MS) return done("short");
  if (await regionArrivalBetween(db, member.email, stop.arrivedAtMs - REGION_WINDOW_MS, leftAtMs)) return done("region");
  const known = await findNearbyKnownPlace(db, member.email, stop.latitude, stop.longitude);
  if (known) return done("known", { placeId: known.id });

  const named = await nameStop({ latitude: stop.latitude, longitude: stop.longitude, fetchImpl: deps.fetchImpl || fetch, logger: deps.logger });
  const name = normalizePlaceName(named.name);
  const activityFields = suggestedActivityFields(name);
  const durationMinutes = Math.round(durationMs / 60000);
  const placeRef = db.collection(COL_KNOWN_PLACES).doc();
  await placeRef.set({
    memberEmail: member.email,
    name,
    latitude: stop.latitude,
    longitude: stop.longitude,
    radiusMeters: DEFAULT_RADIUS_METERS,
    ...activityFields,
    visitCount: 1,
    timedVisitCount: 1,
    totalMinutesSpent: durationMinutes,
    lastVisitDurationMinutes: durationMinutes,
    firstVisitAt: new Date(stop.arrivedAtMs),
    lastVisitAt: new Date(stop.arrivedAtMs),
    lastLeftAt: new Date(leftAtMs),
    lastLeaveEstimated: basis !== "drive",
    address: named.address || null,
    nameSource: named.source,
    namedFrom: "unknown_stop",
    confirmation: "pending",
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });

  const arrivedIso = new Date(stop.arrivedAtMs).toISOString();
  const leftAt = new Date(leftAtMs);
  const note = closedStayNote({ stay: { name, durationMinutes }, leftAt, basis, distanceMeters: stop.evidenceDistanceMeters || 0, evidenceLabel: stop.evidenceLabel || null, timezone });
  const whereFrom = named.source === "coordinates" ? "the map had no name for the spot" : `named from OpenStreetMap${named.address && named.address !== name ? `: ${named.address}` : ""}`;
  const common = { latitude: stop.latitude, longitude: stop.longitude, name, timezone, deviceName: null, activity: activityFields.activity };
  const arrive = parseShortcutEventPayload({ ...buildVisitBody({ ...common, eventType: "arrive_location", timestamp: arrivedIso, notes: `New place, ${whereFrom}; logged from the drive that ended here` }), source: SOURCE });
  const leave = parseShortcutEventPayload({ ...buildVisitBody({ ...common, eventType: "leave_location", timestamp: leftAt.toISOString(), notes: `${note} [new place]`, stayStartedAt: arrivedIso }), source: SOURCE });
  const eventIds = [];
  for (const [parsed, key] of [[arrive, `stop-${stop.id}-arrive`], [leave, `stop-${stop.id}-leave`]]) {
    if (!parsed.ok) {
      warn("unknownStops: visit payload rejected", { code: parsed.code });
      continue;
    }
    const result = await recordAndDeliver({ db, FieldValue, req: idempotentRequest(key), member, event: parsed.event, ...deps });
    if (result && result.shortcutEventId) eventIds.push(result.shortcutEventId);
  }

  let texted = false;
  const phone = member && member.data && member.data.approvedPhoneE164;
  if (typeof deps.sendSms === "function" && phone && (await claimDailyText(db, FieldValue, member.email, nowMs, timezone))) {
    try {
      await deps.sendSms(phone, composePlaceText({ name, town: named.town, activity: activityFields.activity, arrivedAtMs: stop.arrivedAtMs, leftAtMs, durationMinutes, timezone, source: named.source }));
      texted = true;
      await db.collection(COL_CONFIRMATIONS).doc(phone).set({
        memberEmail: member.email,
        phoneE164: phone,
        placeId: placeRef.id,
        placeName: name,
        activity: activityFields.activity,
        eventIds,
        stopId: stop.id,
        createdAtMs: nowMs,
        expiresAtMs: nowMs + CONFIRMATION_TTL_MS,
        createdAt: FieldValue.serverTimestamp(),
      });
    } catch (err) {
      warn("unknownStops: text failed", { message: err && err.message });
    }
  }
  if (deps.logger && typeof deps.logger.info === "function") {
    deps.logger.info("unknownStops: new place logged", { runId: deps.runId, name, source: named.source, durationMinutes, texted, eventIds });
  }
  return done("logged", { placeId: placeRef.id, placeName: name, activity: activityFields.activity, eventIds, texted, nameSource: named.source });
}

/**
 * Applies one piece of location evidence ({ latitude, longitude, atMs, accuracyMeters?, eventType?,
 * label? }) to the member's open stop: at the stop, it refreshes "last seen" (a drive starting there is the
 * departure); clearly away, it settles the stop and, for a stay of 15 minutes or more, logs the visit.
 * Returns null when there is nothing open, else what happened. Never throws.
 */
async function applyEvidenceToPendingStop({ db, FieldValue, member, evidence, deps = {} }) {
  const warn = warnOf(deps);
  try {
    if (!evidence || !Number.isFinite(evidence.latitude) || !Number.isFinite(evidence.longitude) || !Number.isFinite(evidence.atMs)) return null;
    if (evidence.latitude === 0 && evidence.longitude === 0) return null;
    const open = await findOpenStop(db, member.email);
    if (!open || evidence.atMs < open.arrivedAtMs) return null;
    const accuracy = Number.isFinite(evidence.accuracyMeters) && evidence.accuracyMeters > 0 ? evidence.accuracyMeters : 0;
    const distance = haversineMeters(evidence.latitude, evidence.longitude, open.latitude, open.longitude);
    const ref = db.collection(COL_PENDING).doc(open.id);

    if (distance <= STILL_THERE_METERS + accuracy) {
      const update = {};
      if (evidence.eventType === "start_drive") update.driveStartedAtMs = evidence.atMs;
      else if (!Number.isFinite(open.lastSeenAtMs) || evidence.atMs - open.lastSeenAtMs >= SEEN_WRITE_INTERVAL_MS) update.lastSeenAtMs = evidence.atMs;
      if (Object.keys(update).length) await ref.set({ ...update, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
      return { status: "open", distanceMeters: Math.round(distance) };
    }
    if (distance <= STILL_THERE_METERS + accuracy + AWAY_MARGIN_METERS) return { status: "open", distanceMeters: Math.round(distance), vague: true };

    // A drive that started within a short walk of the stop is the departure itself.
    const nearbyDriveStart = evidence.eventType === "start_drive" && distance <= 1500;
    const departure = nearbyDriveStart
      ? { leftMs: evidence.atMs, estimated: false, basis: "drive" }
      : departureFromEvidence({ startMs: open.arrivedAtMs, lastSeenMs: open.lastSeenAtMs, driveStartMs: open.driveStartedAtMs, evidenceMs: evidence.atMs, distanceMeters: distance });

    // Only one piece of evidence may settle a stop; a second ping arriving at the same time finds it closed.
    let claimed = false;
    if (typeof db.runTransaction === "function") {
      claimed = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists || (snap.data() || {}).status !== "open") return false;
        tx.set(ref, { status: "settling", updatedAt: FieldValue.serverTimestamp() }, { merge: true });
        return true;
      });
    } else {
      await ref.set({ status: "settling" }, { merge: true });
      claimed = true;
    }
    if (!claimed) return null;

    const stop = { ...open, evidenceDistanceMeters: Math.round(distance), evidenceLabel: evidence.label || null };
    return finalizeStop({ db, FieldValue, member, stop, leftAtMs: departure.leftMs, basis: departure.basis, deps });
  } catch (err) {
    warn("unknownStops: could not apply evidence", { message: err && err.message });
    return null;
  }
}

/** "OK" keeps the place; "PLACE <name>, <activity>" renames it. Null when the text is neither. */
function parsePlaceReply(body) {
  const text = String(body || "").trim();
  if (!text) return null;
  if (/^(?:ok|okay|yes|yep|keep|fine|good|correct|right|perfect|👍)[.!\s]*$/i.test(text)) return { kind: "keep" };
  const match = text.match(/^(?:place|name|rename|call it)\s*[:\-]?\s*(.+)$/i);
  if (!match) return null;
  const rest = match[1].trim();
  const comma = rest.lastIndexOf(",");
  const name = normalizePlaceName(comma >= 0 ? rest.slice(0, comma) : rest);
  const activity = comma >= 0 ? normalizePlaceActivity(rest.slice(comma + 1)) : "";
  if (!name) return null;
  return { kind: "rename", name, activity };
}

/**
 * Handles a text from the member about the last place logged for them. Returns { handled, reply } so the
 * SMS webhook can answer and stop; { handled: false } hands the message on to the assistant.
 */
async function handlePlaceReply({ db, FieldValue, from, body, deps = {} }) {
  const warn = warnOf(deps);
  try {
    const phone = String(from || "").trim();
    if (!phone) return { handled: false };
    const parsed = parsePlaceReply(body);
    if (!parsed) return { handled: false };
    const ref = db.collection(COL_CONFIRMATIONS).doc(phone);
    const snap = await ref.get();
    if (!snap.exists) return { handled: false };
    const confirmation = snap.data() || {};
    const nowMs = typeof deps.now === "function" ? deps.now() : Date.now();
    if (Number.isFinite(confirmation.expiresAtMs) && nowMs > confirmation.expiresAtMs) {
      await ref.delete();
      return { handled: false };
    }
    const placeRef = db.collection(COL_KNOWN_PLACES).doc(confirmation.placeId);
    const placeSnap = await placeRef.get();
    if (!placeSnap.exists) {
      await ref.delete();
      return { handled: true, reply: "That place is no longer saved." };
    }
    const place = placeSnap.data() || {};

    if (parsed.kind === "keep") {
      await placeRef.set({ confirmation: "confirmed", confirmedAt: FieldValue.serverTimestamp(), updatedAt: FieldValue.serverTimestamp() }, { merge: true });
      await ref.delete();
      return { handled: true, reply: `Kept: ${place.name} (${normalizePlaceActivity(place.activity) || suggestPlaceActivity(place.name)}). Future visits there log themselves.` };
    }

    const activity = parsed.activity || (place.activitySource === "set" ? normalizePlaceActivity(place.activity) : suggestPlaceActivity(parsed.name));
    await placeRef.set({
      name: parsed.name,
      activity,
      activitySource: parsed.activity ? "set" : "suggested",
      confirmation: "renamed",
      renamedFrom: place.name,
      confirmedAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    }, { merge: true });
    const eventIds = Array.isArray(confirmation.eventIds) ? confirmation.eventIds : [];
    for (const eventId of eventIds) {
      const eventRef = db.collection(COL_SHORTCUT_EVENTS).doc(eventId);
      const eventSnap = await eventRef.get().catch(() => null);
      if (!eventSnap || !eventSnap.exists) continue;
      const event = eventSnap.data() || {};
      await eventRef.set({ locationLabel: parsed.name, placeActivity: activity, renamedFrom: event.locationLabel || null, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
      // The 15-minute retry re-sends it; Time Left updates a place visit from the same source record.
      await upsertTimeLeftDeliveryState(db, FieldValue, eventId, { status: "retryable_failure", retryable: true, mode: "production", targetProjectId: "timelefttolive", lastErrorCode: "renamed", lastErrorSummary: `re-sent after the place was renamed to ${parsed.name}` });
      if (event.logEntryId && place.name) {
        const logRef = db.collection(COL_LOG_ENTRIES).doc(event.logEntryId);
        const logSnap = await logRef.get().catch(() => null);
        if (logSnap && logSnap.exists) {
          const log = logSnap.data() || {};
          const swap = (text) => (typeof text === "string" ? text.split(place.name).join(parsed.name) : text);
          await logRef.set({ rawText: swap(log.rawText), normalizedText: swap(log.normalizedText), shortcutLocationLabel: parsed.name, updatedAt: FieldValue.serverTimestamp() }, { merge: true });
        }
      }
    }
    await ref.delete();
    return { handled: true, reply: `Saved: ${parsed.name} (${activity}). Time Left updates within 15 minutes, and future visits there log themselves.` };
  } catch (err) {
    warn("unknownStops: could not handle the reply", { message: err && err.message });
    return { handled: true, reply: "I could not save that. Edit the place on the Known Places page instead." };
  }
}

module.exports = {
  COL_PENDING,
  COL_CONFIRMATIONS,
  MIN_STAY_MS,
  MAX_TEXTS_PER_DAY,
  adoptPendingStopAt,
  applyEvidenceToPendingStop,
  composePlaceText,
  finalizeStop,
  handlePlaceReply,
  openPendingStop,
  parsePlaceReply,
};
