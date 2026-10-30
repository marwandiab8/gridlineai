// "Hey Siri, leaving work" -> this endpoint works out where you are, how long the drive home will take
// in current traffic, and texts your contact from the gridlineai Twilio number:
//
//   "Marwan leaving work, will be home in 42 minutes, at 4:51 PM."
//
// Siri is the only trigger for that text on purpose. An OwnTracks "Work" region exit also means
// leave_work, but it fires every time you drive out for material and come back, which is exactly the
// false alarm this replaces. OwnTracks exits keep being logged; they never text on their own.
//
// The one exception is the fallback for days you forget to say it: an OwnTracks Work exit after
// `afterLocalHour` arms a pending check, and a later ping that shows you several kilometres closer
// to home than the site sends the same text (evaluateEtaFallbackOnPing). Returning to the site,
// driving away from home, or an hour passing disarms it.
//
// Per-member config lives on appMembers/{email}.etaNotify:
//   {
//     enabled: true,
//     contactName: "Ashley",
//     contactPhoneE164: "+1519...",
//     firstName: "Marwan",                  // optional, else the first word of displayName / the email
//     home: { latitude, longitude },        // optional; else homePlaceId, else the known place named "Home"
//     homePlaceId: "<knownPlaces doc id>",  // optional
//     minGapMinutes: 45,                    // one text per departure
//     fallback: { enabled: true, afterLocalHour: 15, waitMinutes: 8, minProgressMeters: 4000, expireMinutes: 60 }
//   }
// Runtime state lives in etaNotifications/{email} (last send, pending fallback); each text is also
// logged to `messages` so it shows in the dashboard thread.
const {
  extractShortcutToken,
  hashShortcutToken,
  checkShortcutRateLimit,
  findShortcutMemberByTokenHash,
  parseShortcutEventPayload,
  COL_SHORTCUT_EVENTS,
} = require("./iosShortcutsIntegration");
const { COL_KNOWN_PLACES, haversineMeters, placeNameKey } = require("./placeLearning");

const COL_ETA_NOTIFICATIONS = "etaNotifications";
const COL_APP_MEMBERS = "appMembers";
const COL_DRIVE_DETECTION = "driveDetection";
const COL_MESSAGES = "messages";

const DEFAULT_TIMEZONE = "America/Toronto";
const ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes";
const ROUTES_TIMEOUT_MS = 8000;
/** When no route can be fetched: straight-line distance x road factor at this average speed. */
const ESTIMATE_ROAD_FACTOR = 1.3;
const ESTIMATE_KMH = 60;
/** A drive-detection fix older than this is not "where you are now". */
const MAX_LAST_FIX_AGE_MS = 10 * 60 * 1000;
/** A departure already in the log this close to now is the same departure, not a second one. */
const DEPARTURE_DEDUPE_MS = 20 * 60 * 1000;
/** Fallback pings less accurate than this are not trusted for the heading check. */
const FALLBACK_MAX_ACCURACY_METERS = 150;
/** Within this distance of home the drive is over; no point texting. */
const FALLBACK_HOME_RADIUS_METERS = 1500;
/** This much farther from home than the site counts as driving away, not home. */
const FALLBACK_AWAY_MARGIN_METERS = 1500;
/** This close to the exit point again counts as back at the site. */
const FALLBACK_RETURNED_RADIUS_METERS = 400;
/** A ping older than this (delivered late from OwnTracks' queue) is not where you are now. */
const FALLBACK_MAX_PING_AGE_MS = 5 * 60 * 1000;

const DEFAULT_CONFIG = {
  minGapMinutes: 45,
  fallback: { enabled: true, afterLocalHour: 15, waitMinutes: 8, minProgressMeters: 4000, expireMinutes: 60 },
};

function jsonError(res, status, code, message, extra = {}) {
  res.status(status).json({ ok: false, error: code, message, ...extra });
}

function finiteOr(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toMs(value) {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (value instanceof Date) return Number.isFinite(value.getTime()) ? value.getTime() : null;
  if (typeof value.toMillis === "function") return value.toMillis();
  if (typeof value.toDate === "function") return value.toDate().getTime();
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function memberTimezone(member) {
  const tz = member && member.data && (member.data.timezone || member.data.timeZone);
  return typeof tz === "string" && tz.trim() ? tz.trim() : DEFAULT_TIMEZONE;
}

/** The hour of day (0-23) at `date` in `timezone`. */
function localHour(date, timezone) {
  try {
    const parts = new Intl.DateTimeFormat("en-CA", { hour: "numeric", hour12: false, timeZone: timezone || DEFAULT_TIMEZONE }).formatToParts(date);
    const hour = Number((parts.find((p) => p.type === "hour") || {}).value);
    return Number.isFinite(hour) ? hour % 24 : date.getUTCHours();
  } catch {
    return date.getUTCHours();
  }
}

/** "4:51 PM" in `timezone`. */
function formatClockUpper(date, timezone) {
  try {
    return new Intl.DateTimeFormat("en-CA", { hour: "numeric", minute: "2-digit", hour12: true, timeZone: timezone || DEFAULT_TIMEZONE })
      .format(date)
      .replace(/\./g, "")
      .replace(/\s+/g, " ")
      .trim()
      .toUpperCase();
  } catch {
    return date.toISOString().slice(11, 16);
  }
}

function firstNameFor(member, config) {
  const explicit = String((config && config.firstName) || "").trim();
  if (explicit) return explicit;
  const display = String((member && member.data && (member.data.displayName || member.data.name)) || "").trim();
  if (display) return display.split(/\s+/)[0];
  const email = String((member && member.email) || "").trim();
  const local = email.split("@")[0] || "I";
  return local ? local.charAt(0).toUpperCase() + local.slice(1) : "I";
}

/** The member's etaNotify config with defaults filled in; `configured` is false when no contact is set. */
function resolveEtaConfig(member) {
  const raw = (member && member.data && member.data.etaNotify) || {};
  const fallbackRaw = raw.fallback && typeof raw.fallback === "object" ? raw.fallback : {};
  const contactPhoneE164 = String(raw.contactPhoneE164 || raw.contactPhone || "").trim();
  const home =
    raw.home && Number.isFinite(Number(raw.home.latitude)) && Number.isFinite(Number(raw.home.longitude))
      ? { latitude: Number(raw.home.latitude), longitude: Number(raw.home.longitude) }
      : null;
  return {
    enabled: raw.enabled !== false,
    configured: Boolean(contactPhoneE164),
    contactName: String(raw.contactName || "").trim() || null,
    contactPhoneE164: contactPhoneE164 || null,
    firstName: firstNameFor(member, raw),
    home,
    homePlaceId: String(raw.homePlaceId || "").trim() || null,
    minGapMinutes: Math.max(1, finiteOr(raw.minGapMinutes, DEFAULT_CONFIG.minGapMinutes)),
    fallback: {
      enabled: fallbackRaw.enabled !== false && DEFAULT_CONFIG.fallback.enabled,
      afterLocalHour: finiteOr(fallbackRaw.afterLocalHour, DEFAULT_CONFIG.fallback.afterLocalHour),
      waitMinutes: Math.max(1, finiteOr(fallbackRaw.waitMinutes, DEFAULT_CONFIG.fallback.waitMinutes)),
      minProgressMeters: Math.max(500, finiteOr(fallbackRaw.minProgressMeters, DEFAULT_CONFIG.fallback.minProgressMeters)),
      expireMinutes: Math.max(5, finiteOr(fallbackRaw.expireMinutes, DEFAULT_CONFIG.fallback.expireMinutes)),
    },
  };
}

function placeCoords(doc) {
  const data = doc && typeof doc.data === "function" ? doc.data() || {} : doc || {};
  if (!Number.isFinite(data.latitude) || !Number.isFinite(data.longitude)) return null;
  return { latitude: data.latitude, longitude: data.longitude, name: data.name || null, radiusMeters: data.radiusMeters || null };
}

/** Where home is: etaNotify.home, then etaNotify.homePlaceId, then the known place named "Home". */
async function resolveHome({ db, member, config }) {
  if (config.home) return { ...config.home, source: "etaNotify.home", name: "Home" };
  if (config.homePlaceId) {
    const snap = await db.collection(COL_KNOWN_PLACES).doc(config.homePlaceId).get().catch(() => null);
    const coords = snap && snap.exists ? placeCoords(snap) : null;
    if (coords) return { ...coords, source: "etaNotify.homePlaceId" };
  }
  const snap = await db.collection(COL_KNOWN_PLACES).where("memberEmail", "==", member.email).get().catch(() => null);
  if (!snap || snap.empty) return null;
  let named = null;
  let byActivity = null;
  snap.forEach((doc) => {
    const data = doc.data() || {};
    const coords = placeCoords(data);
    if (!coords) return;
    const key = placeNameKey(data.name);
    if (!named && (key === "home" || key === "my house")) named = { ...coords, source: "knownPlaces.name" };
    if (!byActivity && String(data.activity || "").toLowerCase() === "home") byActivity = { ...coords, source: "knownPlaces.activity" };
  });
  return named || byActivity || null;
}

/** The known place that is the site, if one is named/tagged Work (used only as the last-resort origin). */
async function resolveWorkPlace({ db, member }) {
  const snap = await db.collection(COL_KNOWN_PLACES).where("memberEmail", "==", member.email).get().catch(() => null);
  if (!snap || snap.empty) return null;
  let found = null;
  snap.forEach((doc) => {
    if (found) return;
    const data = doc.data() || {};
    const coords = placeCoords(data);
    if (!coords) return;
    const key = placeNameKey(data.name);
    if (key === "work" || key === "office" || key === "site" || String(data.activity || "").toLowerCase() === "work") {
      found = { ...coords, source: "knownPlaces.work" };
    }
  });
  return found;
}

/** The newest drive-detection fix, if it is recent enough to still be where you are. */
async function recentDriveFix({ db, member, nowMs }) {
  const snap = await db.collection(COL_DRIVE_DETECTION).doc(member.email).get().catch(() => null);
  if (!snap || !snap.exists) return null;
  const data = snap.data() || {};
  const last = data.state && data.state.last;
  if (!last || !Number.isFinite(last.latitude) || !Number.isFinite(last.longitude) || !Number.isFinite(last.atMs)) return null;
  const ageMs = nowMs - last.atMs;
  if (ageMs < 0 || ageMs > MAX_LAST_FIX_AGE_MS) return null;
  return { latitude: last.latitude, longitude: last.longitude, ageMs, source: "owntracks.last" };
}

/**
 * Where to drive from: the coordinates the Shortcut sent (best - it is literally where you stand),
 * then OwnTracks' last fix if it is recent, then the site itself.
 */
async function resolveOrigin({ db, member, event, nowMs }) {
  if (event && Number.isFinite(event.latitude) && Number.isFinite(event.longitude)) {
    return { latitude: event.latitude, longitude: event.longitude, ageMs: 0, source: "shortcut" };
  }
  const fix = await recentDriveFix({ db, member, nowMs });
  if (fix) return fix;
  const work = await resolveWorkPlace({ db, member });
  if (work) return { latitude: work.latitude, longitude: work.longitude, ageMs: null, source: work.source };
  return null;
}

/** Parses a Routes API duration ("1832s" or {seconds}) into seconds. */
function parseRouteDuration(value) {
  if (value == null) return null;
  if (typeof value === "object") return finiteOr(value.seconds, null);
  const m = String(value).match(/^(\d+(?:\.\d+)?)s$/);
  return m ? Number(m[1]) : finiteOr(value, null);
}

/** Straight-line fallback when the Routes API is unavailable. */
function estimateDrive(origin, destination) {
  const line = haversineMeters(origin.latitude, origin.longitude, destination.latitude, destination.longitude);
  const distanceMeters = Math.round(line * ESTIMATE_ROAD_FACTOR);
  const seconds = Math.round((distanceMeters / 1000 / ESTIMATE_KMH) * 3600);
  return { seconds, distanceMeters, source: "estimate" };
}

/**
 * Traffic-aware drive time from `origin` to `destination` via Google Routes computeRoutes. Falls back
 * to a straight-line estimate when there is no key, the call fails, or it takes too long - a rough
 * text beats no text. Returns { seconds, distanceMeters, staticSeconds?, source }.
 */
async function fetchDrivingEta({ origin, destination, apiKey, fetchImpl = globalThis.fetch, logger, timeoutMs = ROUTES_TIMEOUT_MS }) {
  if (!apiKey || typeof fetchImpl !== "function") return estimateDrive(origin, destination);
  const controller = typeof AbortController === "function" ? new AbortController() : null;
  const timer = controller ? setTimeout(() => controller.abort(), timeoutMs) : null;
  try {
    const response = await fetchImpl(ROUTES_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": apiKey,
        "X-Goog-FieldMask": "routes.duration,routes.staticDuration,routes.distanceMeters",
      },
      body: JSON.stringify({
        origin: { location: { latLng: { latitude: origin.latitude, longitude: origin.longitude } } },
        destination: { location: { latLng: { latitude: destination.latitude, longitude: destination.longitude } } },
        travelMode: "DRIVE",
        routingPreference: "TRAFFIC_AWARE",
        units: "METRIC",
      }),
      signal: controller ? controller.signal : undefined,
    });
    const json = await response.json().catch(() => null);
    if (!response.ok) {
      const detail = json && json.error ? `${json.error.status || response.status}: ${json.error.message || ""}` : `HTTP ${response.status}`;
      throw new Error(`Routes API ${detail}`.trim());
    }
    const route = json && Array.isArray(json.routes) ? json.routes[0] : null;
    const seconds = route ? parseRouteDuration(route.duration) : null;
    if (!Number.isFinite(seconds)) throw new Error("Routes API returned no route");
    return {
      seconds,
      distanceMeters: finiteOr(route.distanceMeters, null),
      staticSeconds: parseRouteDuration(route.staticDuration),
      source: "google_routes",
    };
  } catch (err) {
    if (logger && typeof logger.warn === "function") {
      logger.warn("leavingWorkEta: route lookup failed, using estimate", { message: err && err.message });
    }
    return estimateDrive(origin, destination);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** "Marwan leaving work, will be home in 42 minutes, at 4:51 PM." */
function buildEtaMessage({ firstName, minutes, arriveAt, timezone, estimated = false }) {
  const mins = Math.max(1, Math.round(minutes));
  const about = estimated ? "about " : "";
  return `${firstName} leaving work, will be home in ${about}${mins} ${mins === 1 ? "minute" : "minutes"}, at ${formatClockUpper(arriveAt, timezone)}.`;
}

/** What Siri reads back. */
function buildSpokenReply({ contactName, minutes, arriveAt, timezone, estimated = false }) {
  const mins = Math.max(1, Math.round(minutes));
  const who = contactName || "your contact";
  return `Told ${who} you'll be home in ${estimated ? "about " : ""}${mins} minutes, around ${formatClockUpper(arriveAt, timezone)}.`;
}

/** Sends one text from the gridlineai number and logs it to `messages`. Throws when Twilio rejects it. */
async function sendEtaText({ db, FieldValue, smsClient, fromPhone, to, body, memberEmail, meta = {}, logger }) {
  const tw = await smsClient.messages.create({ to, from: fromPhone, body });
  try {
    await db.collection(COL_MESSAGES).add({
      direction: "outbound",
      from: fromPhone,
      to,
      body,
      messageSid: tw.sid || null,
      delivery: "twilio_api_notification",
      threadKey: to,
      phoneE164: to,
      channel: "sms",
      command: "leaving_work_eta",
      memberEmail,
      etaNotify: meta,
      twilioMessageStatus: tw.status || null,
      twilioSenderPhoneE164: fromPhone,
      twilioDestinationPhoneE164: to,
      createdAt: FieldValue.serverTimestamp(),
    });
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: could not log the sent text", { message: err && err.message });
  }
  return { sid: tw.sid || null, status: tw.status || null };
}

function stateRef(db, memberEmail) {
  return db.collection(COL_ETA_NOTIFICATIONS).doc(memberEmail);
}

async function readState(db, memberEmail) {
  const snap = await stateRef(db, memberEmail).get().catch(() => null);
  return snap && snap.exists ? snap.data() || {} : {};
}

/**
 * Claims the right to send: false when a text went out within `minGapMinutes`, when the same
 * Idempotency-Key was already handled, or when another request claimed within the last minute (Siri
 * hearing you twice). Uses a transaction when the db offers one.
 */
async function claimSend({ db, memberEmail, nowMs, minGapMinutes, idempotencyKey, trigger }) {
  const ref = stateRef(db, memberEmail);
  const decide = (data) => {
    const lastSentAtMs = toMs(data.lastSentAtMs);
    if (idempotencyKey && data.lastIdempotencyKey && data.lastIdempotencyKey === idempotencyKey) {
      return { ok: false, reason: "idempotent_replay", last: data.lastSent || null };
    }
    if (Number.isFinite(lastSentAtMs) && nowMs - lastSentAtMs < minGapMinutes * 60 * 1000) {
      return { ok: false, reason: "sent_recently", last: data.lastSent || null };
    }
    const claimedAtMs = toMs(data.claimedAtMs);
    if (Number.isFinite(claimedAtMs) && nowMs - claimedAtMs < 60 * 1000) {
      return { ok: false, reason: "in_progress", last: data.lastSent || null };
    }
    return { ok: true };
  };
  const claimData = { memberEmail, claimedAtMs: nowMs, claimTrigger: trigger, updatedAtMs: nowMs };
  if (typeof db.runTransaction === "function") {
    return db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const decision = decide(snap.exists ? snap.data() || {} : {});
      if (decision.ok) tx.set(ref, claimData, { merge: true });
      return decision;
    });
  }
  const decision = decide(await readState(db, memberEmail));
  if (decision.ok) await ref.set(claimData, { merge: true });
  return decision;
}

async function releaseClaim(db, memberEmail) {
  await stateRef(db, memberEmail).set({ claimedAtMs: null }, { merge: true }).catch(() => {});
}

/** A leave_work already recorded within DEPARTURE_DEDUPE_MS of now (by Siri or OwnTracks). */
async function recentDepartureEvent({ db, memberEmail, nowMs, windowMs = DEPARTURE_DEDUPE_MS }) {
  const snap = await db
    .collection(COL_SHORTCUT_EVENTS)
    .where("memberEmail", "==", memberEmail)
    .where("eventType", "==", "leave_work")
    .where("eventAtMs", ">=", nowMs - windowMs)
    .where("eventAtMs", "<=", nowMs + windowMs)
    .limit(1)
    .get()
    .catch(() => null);
  if (!snap || snap.empty) return null;
  const doc = snap.docs[0];
  return { id: doc.id, data: doc.data() || {} };
}

/**
 * The whole send: route, text, state, log. Shared by the Siri endpoint and the OwnTracks fallback.
 * Assumes the caller already holds the claim. Returns the details Siri and the state doc need.
 */
async function computeAndSendEta({
  db,
  FieldValue,
  member,
  config,
  origin,
  home,
  now,
  getTwilio,
  mapsApiKey,
  fetchImpl,
  logger,
  trigger,
  idempotencyKey = null,
}) {
  const timezone = memberTimezone(member);
  const drive = await fetchDrivingEta({ origin, destination: home, apiKey: mapsApiKey, fetchImpl, logger });
  const minutes = Math.max(1, Math.round(drive.seconds / 60));
  const arriveAt = new Date(now.getTime() + minutes * 60 * 1000);
  const estimated = drive.source !== "google_routes";
  const body = buildEtaMessage({ firstName: config.firstName, minutes, arriveAt, timezone, estimated });

  const { smsClient, fromPhone } = getTwilio();
  if (!smsClient || !fromPhone) {
    const err = new Error("Twilio is not configured.");
    err.status = 503;
    err.code = "sms_not_configured";
    throw err;
  }
  const sent = await sendEtaText({
    db,
    FieldValue,
    smsClient,
    fromPhone,
    to: config.contactPhoneE164,
    body,
    memberEmail: member.email,
    meta: { trigger, minutes, arriveAtIso: arriveAt.toISOString(), routeSource: drive.source, originSource: origin.source },
    logger,
  });

  const record = {
    trigger,
    sentAtMs: now.getTime(),
    sentAtIso: now.toISOString(),
    minutes,
    arriveAtMs: arriveAt.getTime(),
    arriveAtIso: arriveAt.toISOString(),
    arriveAtClock: formatClockUpper(arriveAt, timezone),
    distanceMeters: drive.distanceMeters != null ? drive.distanceMeters : null,
    routeSource: drive.source,
    staticSeconds: drive.staticSeconds != null ? drive.staticSeconds : null,
    origin: { latitude: origin.latitude, longitude: origin.longitude, source: origin.source, ageMs: origin.ageMs == null ? null : origin.ageMs },
    home: { latitude: home.latitude, longitude: home.longitude, source: home.source },
    to: config.contactPhoneE164,
    contactName: config.contactName,
    body,
    messageSid: sent.sid,
  };
  await stateRef(db, member.email).set(
    {
      memberEmail: member.email,
      lastSentAtMs: now.getTime(),
      lastIdempotencyKey: idempotencyKey || null,
      lastSent: record,
      pendingFallback: null,
      claimedAtMs: null,
      updatedAtMs: now.getTime(),
    },
    { merge: true }
  );
  try {
    await stateRef(db, member.email).collection("sends").add({ ...record, createdAt: FieldValue.serverTimestamp() });
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: could not log the send history", { message: err && err.message });
  }

  return {
    minutes,
    arriveAt,
    arriveAtClock: record.arriveAtClock,
    distanceMeters: record.distanceMeters,
    routeSource: drive.source,
    estimated,
    body,
    messageSid: sent.sid,
    spoken: buildSpokenReply({ contactName: config.contactName, minutes, arriveAt, timezone, estimated }),
  };
}

/**
 * Records the departure in the shared Shortcuts pipeline (dashboard log + TimeLeft) as a leave_work
 * event, unless one is already in the log for this departure. Never throws.
 */
async function recordDepartureSafely({ db, member, event, nowMs, recordDeparture, logger, note }) {
  if (typeof recordDeparture !== "function" || !event) return { recorded: false, reason: "no_recorder" };
  try {
    const existing = await recentDepartureEvent({ db, memberEmail: member.email, nowMs });
    if (existing) return { recorded: false, reason: "already_logged", eventId: existing.id };
    const result = await recordDeparture({ member, event: { ...event, notes: [event.notes, note].filter(Boolean).join(" ") } });
    return { recorded: !(result && result.duplicate), eventId: result ? result.shortcutEventId || result.eventId || null : null };
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: could not record the departure", { message: err && err.message });
    return { recorded: false, reason: "failed" };
  }
}

/**
 * HTTP handler for POST /api/integrations/ios-shortcuts/leaving-work. Same token as the other Shortcut
 * endpoints. Body: anything parseShortcutEventPayload accepts; event_type defaults to leaving_work,
 * and latitude/longitude from the Shortcut's Get Current Location are what make the ETA accurate.
 *
 * Reply: { ok, sent, eta_minutes, arrive_at, arrive_at_iso, distance_km, route_source, message, spoken }.
 * `spoken` is a sentence for the Shortcut's Show Result, which Siri reads aloud.
 */
async function handleLeavingWorkEtaRequest({
  db,
  FieldValue,
  req,
  res,
  logger,
  getTwilio,
  mapsApiKey = null,
  fetchImpl = globalThis.fetch,
  recordDeparture = null,
  now = new Date(),
}) {
  const runId = `leaving-work-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  let member = null;
  let claimed = false;
  try {
    if (req.method !== "POST") {
      res.status(405).set("Allow", "POST").json({ ok: false, error: "method_not_allowed", message: "Use POST." });
      return;
    }
    const rawToken = extractShortcutToken(req);
    if (!rawToken) return jsonError(res, 401, "missing_token", "Missing Shortcuts integration token.");
    const tokenHash = hashShortcutToken(rawToken);
    if (!checkShortcutRateLimit(tokenHash)) return jsonError(res, 429, "rate_limited", "Too many requests. Try again shortly.");
    member = await findShortcutMemberByTokenHash(db, tokenHash);
    if (!member) return jsonError(res, 401, "invalid_token", "Invalid Shortcuts integration token.");

    const config = resolveEtaConfig(member);
    if (!config.configured) {
      return jsonError(res, 409, "eta_not_configured", "Set etaNotify.contactPhoneE164 on your appMembers document first.", {
        spoken: "Your ETA contact isn't set up yet.",
      });
    }
    if (!config.enabled) return jsonError(res, 409, "eta_disabled", "etaNotify.enabled is false.", { spoken: "ETA texts are turned off." });

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const parsed = parseShortcutEventPayload({ source: "ios_shortcuts", location_label: "Work", ...body, event_type: body.event_type || "leaving_work" }, now);
    if (!parsed.ok) return jsonError(res, parsed.status, parsed.code, parsed.message);
    const event = parsed.event;
    if (event.eventType !== "leave_work") {
      return jsonError(res, 400, "invalid_event_type", "This endpoint only takes leaving_work.");
    }
    const timezone = memberTimezone(member);

    const home = await resolveHome({ db, member, config });
    if (!home) {
      return jsonError(res, 409, "home_not_configured", "No home location: set etaNotify.home {latitude, longitude}, etaNotify.homePlaceId, or name a known place \"Home\".", {
        spoken: "I don't know where home is yet.",
      });
    }
    const origin = await resolveOrigin({ db, member, event, nowMs: now.getTime() });
    if (!origin) {
      return jsonError(res, 409, "origin_unknown", "No location: send latitude/longitude from Get Current Location, or name a known place \"Work\".", {
        spoken: "I couldn't tell where you are.",
      });
    }

    const idempotencyKey = String(req.get("idempotency-key") || "").trim().slice(0, 200) || null;
    const claim = await claimSend({ db, memberEmail: member.email, nowMs: now.getTime(), minGapMinutes: config.minGapMinutes, idempotencyKey, trigger: "siri" });
    if (!claim.ok) {
      const last = claim.last || {};
      const minutesAgo = Number.isFinite(toMs(last.sentAtMs)) ? Math.round((now.getTime() - toMs(last.sentAtMs)) / 60000) : null;
      const spoken = last.arriveAtClock
        ? `Already told ${config.contactName || "your contact"}${minutesAgo != null ? ` ${minutesAgo} minutes ago` : ""}: home around ${last.arriveAtClock}.`
        : "That text already went out.";
      res.status(200).json({
        ok: true,
        sent: false,
        duplicate: true,
        reason: claim.reason,
        eta_minutes: last.minutes != null ? last.minutes : null,
        arrive_at: last.arriveAtClock || null,
        arrive_at_iso: last.arriveAtIso || null,
        message: last.body || null,
        spoken,
      });
      return;
    }
    claimed = true;

    const result = await computeAndSendEta({
      db, FieldValue, member, config, origin, home, now, getTwilio, mapsApiKey, fetchImpl, logger, trigger: "siri", idempotencyKey,
    });
    claimed = false; // computeAndSendEta cleared the claim as part of the state write

    const departure = await recordDepartureSafely({
      db, member, event, nowMs: now.getTime(), recordDeparture, logger,
      note: `Told ${config.contactName || "contact"}: home in ${result.minutes} min (${result.arriveAtClock}).`,
    });

    if (logger && typeof logger.info === "function") {
      logger.info("leavingWorkEta: sent", {
        runId,
        memberEmail: member.email,
        minutes: result.minutes,
        routeSource: result.routeSource,
        originSource: origin.source,
        homeSource: home.source,
        departure,
      });
    }
    res.status(200).json({
      ok: true,
      sent: true,
      duplicate: false,
      eta_minutes: result.minutes,
      arrive_at: result.arriveAtClock,
      arrive_at_iso: result.arriveAt.toISOString(),
      distance_km: result.distanceMeters != null ? Math.round(result.distanceMeters / 100) / 10 : null,
      route_source: result.routeSource,
      origin_source: origin.source,
      timezone,
      message: result.body,
      message_sid: result.messageSid,
      departure_logged: departure.recorded,
      spoken: result.spoken,
    });
  } catch (err) {
    if (claimed && member) await releaseClaim(db, member.email);
    const status = Number(err && err.status) || 500;
    const code = String((err && err.code) || "leaving_work_eta_failed");
    if (logger && typeof logger.error === "function") {
      logger.error("leavingWorkEta: failed", { runId, code, status, message: err && err.message, stack: err && err.stack });
    }
    jsonError(res, status >= 400 && status < 600 ? status : 500, code, err && err.message ? err.message : "Could not send the ETA.", {
      spoken: "Sorry, the ETA text didn't go out.",
    });
  }
}

/**
 * OwnTracks said you left the Work region. If it's late enough in the day and nothing was texted yet,
 * arm the fallback: a later ping that shows real progress toward home sends the text. Never throws.
 */
async function noteWorkExitForEtaFallback({ db, FieldValue, member, exitAt, latitude, longitude, logger }) {
  try {
    const config = resolveEtaConfig(member);
    if (!config.configured || !config.enabled || !config.fallback.enabled) return { armed: false, reason: "disabled" };
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return { armed: false, reason: "no_exit_position" };
    const at = exitAt instanceof Date ? exitAt : new Date(toMs(exitAt) || Date.now());
    const hour = localHour(at, memberTimezone(member));
    if (hour < config.fallback.afterLocalHour) return { armed: false, reason: "too_early", hour };
    const state = await readState(db, member.email);
    const lastSentAtMs = toMs(state.lastSentAtMs);
    if (Number.isFinite(lastSentAtMs) && at.getTime() - lastSentAtMs < config.minGapMinutes * 60 * 1000) {
      return { armed: false, reason: "sent_recently" };
    }
    const home = await resolveHome({ db, member, config });
    if (!home) return { armed: false, reason: "home_unknown" };
    const exitDistanceToHomeMeters = Math.round(haversineMeters(latitude, longitude, home.latitude, home.longitude));
    const pendingFallback = {
      armedAtMs: at.getTime(),
      armedAtIso: at.toISOString(),
      exitLatitude: latitude,
      exitLongitude: longitude,
      exitDistanceToHomeMeters,
      home: { latitude: home.latitude, longitude: home.longitude, source: home.source },
      expiresAtMs: at.getTime() + config.fallback.expireMinutes * 60 * 1000,
    };
    await stateRef(db, member.email).set({ memberEmail: member.email, pendingFallback, updatedAtMs: Date.now() }, { merge: true });
    if (logger && typeof logger.info === "function") logger.info("leavingWorkEta: fallback armed", { memberEmail: member.email, exitDistanceToHomeMeters });
    return { armed: true, pendingFallback };
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: could not arm the fallback", { message: err && err.message });
    return { armed: false, reason: "failed" };
  }
}

/**
 * True when Siri sent the ETA text within DEPARTURE_DEDUPE_MS: the OwnTracks Work exit that follows is
 * the same departure Siri already logged, so the OwnTracks path widens its dedupe window instead of
 * writing a second leave_work. Never throws.
 */
async function siriSentRecently({ db, member, nowMs, windowMs = DEPARTURE_DEDUPE_MS }) {
  try {
    const state = await readState(db, member.email);
    const lastSentAtMs = toMs(state.lastSentAtMs);
    const trigger = state.lastSent && state.lastSent.trigger;
    return trigger === "siri" && Number.isFinite(lastSentAtMs) && Math.abs(nowMs - lastSentAtMs) <= windowMs;
  } catch {
    return false;
  }
}

/** Disarms a pending fallback (you came back to the site, or Siri already sent). Never throws. */
async function clearEtaFallback({ db, member, reason, logger }) {
  try {
    const state = await readState(db, member.email);
    if (!state.pendingFallback) return { cleared: false };
    await stateRef(db, member.email).set({ pendingFallback: null, lastFallbackOutcome: { reason, atMs: Date.now() }, updatedAtMs: Date.now() }, { merge: true });
    if (logger && typeof logger.info === "function") logger.info("leavingWorkEta: fallback cleared", { memberEmail: member.email, reason });
    return { cleared: true };
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: could not clear the fallback", { message: err && err.message });
    return { cleared: false };
  }
}

/**
 * Pure decision for one ping against a pending fallback. Returns { action, ... } where action is one of
 * waiting | inaccurate | expired | arrived | returned | away | send.
 */
function decideFallback({ pending, point, nowMs, config }) {
  const armedAtMs = toMs(pending.armedAtMs);
  const expiresAtMs = toMs(pending.expiresAtMs);
  if (Number.isFinite(expiresAtMs) && nowMs > expiresAtMs) return { action: "expired" };
  if (!Number.isFinite(armedAtMs) || nowMs - armedAtMs < config.fallback.waitMinutes * 60 * 1000) return { action: "waiting" };
  if (!Number.isFinite(point.latitude) || !Number.isFinite(point.longitude)) return { action: "waiting" };
  if (Number.isFinite(point.accuracyMeters) && point.accuracyMeters > FALLBACK_MAX_ACCURACY_METERS) return { action: "inaccurate" };
  const home = pending.home || {};
  if (!Number.isFinite(home.latitude) || !Number.isFinite(home.longitude)) return { action: "expired" };
  const distanceToHome = haversineMeters(point.latitude, point.longitude, home.latitude, home.longitude);
  const fromExit = haversineMeters(point.latitude, point.longitude, pending.exitLatitude, pending.exitLongitude);
  const exitDistance = finiteOr(pending.exitDistanceToHomeMeters, null);
  if (distanceToHome <= FALLBACK_HOME_RADIUS_METERS) return { action: "arrived", distanceToHome };
  if (fromExit <= FALLBACK_RETURNED_RADIUS_METERS) return { action: "returned", fromExit };
  if (exitDistance == null) return { action: "expired" };
  const progress = exitDistance - distanceToHome;
  if (progress >= config.fallback.minProgressMeters) return { action: "send", progress, distanceToHome };
  if (distanceToHome >= exitDistance + FALLBACK_AWAY_MARGIN_METERS) return { action: "away", progress, distanceToHome };
  return { action: "waiting", progress, distanceToHome };
}

/**
 * Called on every OwnTracks ping. Does nothing unless a fallback is pending; then applies decideFallback
 * and, on "send", texts from the ping's position. Never throws.
 */
async function evaluateEtaFallbackOnPing({
  db,
  FieldValue,
  member,
  point,
  getTwilio,
  mapsApiKey = null,
  fetchImpl = globalThis.fetch,
  logger,
  recordDeparture = null,
  now = new Date(),
}) {
  try {
    const state = await readState(db, member.email);
    const pending = state.pendingFallback;
    if (!pending) return { action: "none" };
    const config = resolveEtaConfig(member);
    // Timing goes by the ping's own clock (OwnTracks can deliver a queue of them late), but a ping that
    // is itself old says nothing about where you are now, so it never triggers a text.
    const pointAtMs = toMs(point && point.atMs);
    if (Number.isFinite(pointAtMs) && now.getTime() - pointAtMs > FALLBACK_MAX_PING_AGE_MS) return { action: "stale_ping" };
    const decision = decideFallback({ pending, point, nowMs: Number.isFinite(pointAtMs) ? pointAtMs : now.getTime(), config });
    if (decision.action === "waiting" || decision.action === "inaccurate") return decision;
    if (decision.action !== "send") {
      await clearEtaFallback({ db, member, reason: decision.action, logger });
      return decision;
    }
    const claim = await claimSend({ db, memberEmail: member.email, nowMs: now.getTime(), minGapMinutes: config.minGapMinutes, idempotencyKey: null, trigger: "owntracks_fallback" });
    if (!claim.ok) {
      await clearEtaFallback({ db, member, reason: claim.reason, logger });
      return { action: "skipped", reason: claim.reason };
    }
    const origin = { latitude: point.latitude, longitude: point.longitude, ageMs: Math.max(0, now.getTime() - (toMs(point.atMs) || now.getTime())), source: "owntracks_fallback" };
    const home = { ...pending.home };
    let result;
    try {
      result = await computeAndSendEta({ db, FieldValue, member, config, origin, home, now, getTwilio, mapsApiKey, fetchImpl, logger, trigger: "owntracks_fallback" });
    } catch (err) {
      await releaseClaim(db, member.email);
      throw err;
    }
    const departureEvent = buildFallbackDepartureEvent({ pending, member, now });
    const departure = await recordDepartureSafely({
      db, member, event: departureEvent, nowMs: now.getTime(), recordDeparture, logger,
      note: `Told ${config.contactName || "contact"} automatically: home in ${result.minutes} min (${result.arriveAtClock}).`,
    });
    if (logger && typeof logger.info === "function") {
      logger.info("leavingWorkEta: fallback sent", { memberEmail: member.email, minutes: result.minutes, progress: Math.round(decision.progress), departure });
    }
    return { action: "sent", minutes: result.minutes, arriveAtClock: result.arriveAtClock, progress: decision.progress };
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: fallback failed", { message: err && err.message });
    return { action: "failed", message: err && err.message };
  }
}

/** The leave_work event to log for a fallback send: at the exit time OwnTracks reported, at the exit point. */
function buildFallbackDepartureEvent({ pending, member, now }) {
  const parsed = parseShortcutEventPayload(
    {
      event_type: "leaving_work",
      timestamp: pending.armedAtIso || new Date(toMs(pending.armedAtMs) || now.getTime()).toISOString(),
      timezone: memberTimezone(member),
      latitude: pending.exitLatitude,
      longitude: pending.exitLongitude,
      location_label: "Work",
      source: "owntracks",
    },
    now
  );
  return parsed.ok ? parsed.event : null;
}

/**
 * Inbound SMS from a member's ETA contact ("ok", "drive safe") is relayed to that member's own phone as
 * "Ashley: ok" instead of being answered by the assistant. Returns { handled, relayed }. `handled`
 * means inboundSms should reply with empty TwiML and stop.
 */
async function relayEtaContactReply({ db, FieldValue, from, body, mediaCount = 0, smsClient = null, getSmsClient = null, fromPhone, logger, runId }) {
  const sender = String(from || "").trim();
  if (!sender) return { handled: false };
  let snap;
  try {
    snap = await db.collection(COL_APP_MEMBERS).where("etaNotify.contactPhoneE164", "==", sender).limit(1).get();
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: contact lookup failed", { runId, message: err && err.message });
    return { handled: false };
  }
  if (!snap || snap.empty) return { handled: false };
  const doc = snap.docs[0];
  const member = { email: doc.id, data: doc.data() || {} };
  const config = resolveEtaConfig(member);
  const memberPhone = String(member.data.approvedPhoneE164 || "").trim();
  // The contact set to the member's own phone (e.g. while testing) is just the member texting the
  // assistant; swallowing it would silence every text they send.
  if (memberPhone === sender) return { handled: false };
  if (!memberPhone) return { handled: true, relayed: false, reason: "member_phone_unavailable" };
  const text = String(body || "").trim();
  const relayBody = `${config.contactName || "Your ETA contact"}: ${text || (mediaCount > 0 ? "[sent an attachment]" : "[empty message]")}`.slice(0, 1500);
  try {
    const client = smsClient || (typeof getSmsClient === "function" ? getSmsClient() : null);
    if (!client) throw new Error("no Twilio client");
    await sendEtaText({ db, FieldValue, smsClient: client, fromPhone, to: memberPhone, body: relayBody, memberEmail: member.email, meta: { trigger: "contact_reply_relay", from: sender }, logger });
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("leavingWorkEta: could not relay the contact's reply", { runId, message: err && err.message });
    return { handled: true, relayed: false, reason: "relay_failed" };
  }
  return { handled: true, relayed: true, memberEmail: member.email };
}

module.exports = {
  COL_ETA_NOTIFICATIONS,
  DEPARTURE_DEDUPE_MS,
  resolveEtaConfig,
  resolveHome,
  resolveOrigin,
  fetchDrivingEta,
  estimateDrive,
  parseRouteDuration,
  buildEtaMessage,
  buildSpokenReply,
  formatClockUpper,
  localHour,
  claimSend,
  recentDepartureEvent,
  computeAndSendEta,
  handleLeavingWorkEtaRequest,
  noteWorkExitForEtaFallback,
  siriSentRecently,
  clearEtaFallback,
  decideFallback,
  evaluateEtaFallbackOnPing,
  relayEtaContactReply,
};
