const crypto = require("crypto");
const { normalizeProjectSlug } = require("./projectAccess");

const COL_APP_MEMBERS = "appMembers";
const COL_SHORTCUT_EVENTS = "iosShortcutEvents";
const TOKEN_PREFIX = "glsc_";
const SUPPORTED_EVENT_TYPES = new Set([
  "arrive_work",
  "leave_work",
  "arrive_home",
  "leave_home",
  "arrive_gym",
  "leave_gym",
  "start_workout",
  "finish_workout",
  "start_spotify",
  "arrive_location",
  "leave_location",
]);
const EVENT_LABELS = {
  arrive_work: "Arrived at work",
  leave_work: "Left work",
  arrive_home: "Arrived home",
  leave_home: "Left home",
  arrive_gym: "Arrived at the gym",
  leave_gym: "Left the gym",
  start_workout: "Started workout",
  finish_workout: "Finished workout",
  start_spotify: "Started listening to Spotify",
  arrive_location: "Arrived at location",
  leave_location: "Left location",
};
const EVENT_TYPE_ALIASES = {
  arrive_at_work: "arrive_work",
  arrived_work: "arrive_work",
  arrived_at_work: "arrive_work",
  leaving_work: "leave_work",
  left_work: "leave_work",
  depart_work: "leave_work",
  departed_work: "leave_work",
  arrive_at_home: "arrive_home",
  arrived_home: "arrive_home",
  arrived_at_home: "arrive_home",
  leaving_home: "leave_home",
  left_home: "leave_home",
  depart_home: "leave_home",
  departed_home: "leave_home",
  arrive_at_gym: "arrive_gym",
  arrive_the_gym: "arrive_gym",
  arrive_at_the_gym: "arrive_gym",
  arrived_gym: "arrive_gym",
  arrived_at_gym: "arrive_gym",
  arrived_at_the_gym: "arrive_gym",
  arrive_gym: "arrive_gym",
  leaving_gym: "leave_gym",
  leave_the_gym: "leave_gym",
  left_gym: "leave_gym",
  left_the_gym: "leave_gym",
  depart_gym: "leave_gym",
  departed_gym: "leave_gym",
  start_workout: "start_workout",
  started_workout: "start_workout",
  workout_started: "start_workout",
  begin_workout: "start_workout",
  began_workout: "start_workout",
  finish_workout: "finish_workout",
  finished_workout: "finish_workout",
  workout_finished: "finish_workout",
  end_workout: "finish_workout",
  ended_workout: "finish_workout",
  complete_workout: "finish_workout",
  completed_workout: "finish_workout",
  start_spotify: "start_spotify",
  started_spotify: "start_spotify",
  spotify_started: "start_spotify",
  start_listening_to_spotify: "start_spotify",
  started_listening_to_spotify: "start_spotify",
  listening_to_spotify: "start_spotify",
  spotify_listening_started: "start_spotify",
  arrive_at_location: "arrive_location",
  arrived_location: "arrive_location",
  arrived_at_location: "arrive_location",
  leaving_location: "leave_location",
  left_location: "leave_location",
  depart_location: "leave_location",
  departed_location: "leave_location",
};
const rateLimitBuckets = new Map();

function parseShortcutCoordinateValue(value) {
  if (value == null) return null;
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  const text = String(value).trim();
  if (!text) return null;
  const numeric = Number(text);
  return Number.isFinite(numeric) ? numeric : null;
}

function extractShortcutCoordinates(payload) {
  if (!payload || typeof payload !== "object") {
    return { latitude: null, longitude: null };
  }

  const direct = () => {
    const latitude = parseShortcutCoordinateValue(payload.latitude);
    const longitude = parseShortcutCoordinateValue(payload.longitude);
    if (latitude === null && longitude === null) return null;
    if (latitude === null || longitude === null) return null;
    return { latitude, longitude };
  };

  const directResult = direct();
  if (directResult) return directResult;

  const altDirect = () => {
    const latitude = parseShortcutCoordinateValue(payload.lat);
    const longitude = parseShortcutCoordinateValue(payload.lng);
    if (latitude === null && longitude === null) return null;
    if (latitude === null || longitude === null) return null;
    return { latitude, longitude };
  };
  const altDirectResult = altDirect();
  if (altDirectResult) return altDirectResult;

  const short = parseShortcutCoordinateValue(payload.lon);
  const shortAlternative = parseShortcutCoordinateValue(payload.long);
  const longAlternative = parseShortcutCoordinateValue(payload.longitudeE7);
  if (short !== null || shortAlternative !== null || longAlternative !== null) {
    const latitude = parseShortcutCoordinateValue(payload.latitude_e7);
    const longitude = short !== null ? short : shortAlternative !== null ? shortAlternative : longAlternative;
    if (latitude !== null && longitude !== null) return { latitude, longitude };
  }

  const objectCandidates = [
    payload.location,
    payload.place,
    payload.currentLocation,
    payload.current_location,
    payload.locationObj,
    payload.location_data,
    payload.locationLabel,
    payload.placeName,
    payload.place_name,
    payload.location_name,
    payload.locationLabel,
  ].filter((item) => item && typeof item === "object");

  for (const obj of objectCandidates) {
    const latFromObj = parseShortcutCoordinateValue(obj.latitude || obj.lat || obj.y);
    const lonFromObj =
      parseShortcutCoordinateValue(obj.longitude || obj.lon || obj.lng || obj.long || obj.x);
    if (latFromObj !== null && lonFromObj !== null) return { latitude: latFromObj, longitude: lonFromObj };

    if (Array.isArray(obj?.coordinates) && obj.coordinates.length >= 2) {
      const candidateLat = parseShortcutCoordinateValue(obj.coordinates[1]);
      const candidateLon = parseShortcutCoordinateValue(obj.coordinates[0]);
      if (candidateLat !== null && candidateLon !== null) return { latitude: candidateLat, longitude: candidateLon };
    }

    if (Array.isArray(obj?.center) && obj.center.length >= 2) {
      const candidateLat = parseShortcutCoordinateValue(obj.center[1]);
      const candidateLon = parseShortcutCoordinateValue(obj.center[0]);
      if (candidateLat !== null && candidateLon !== null) return { latitude: candidateLat, longitude: candidateLon };
    }
  }

  return { latitude: null, longitude: null };
}

function sha256Hex(value) {
  return crypto.createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

function generateShortcutToken() {
  return `${TOKEN_PREFIX}${crypto.randomBytes(32).toString("base64url")}`;
}

function hashShortcutToken(token) {
  const raw = String(token || "").trim();
  return raw ? sha256Hex(raw) : "";
}

function tokenLast4(token) {
  const raw = String(token || "").trim();
  return raw ? raw.slice(-4) : "";
}

function extractShortcutToken(req) {
  const customHeader = String(req.get("x-gridline-shortcut-token") || "").trim();
  if (customHeader) return customHeader;
  const authHeader = String(req.get("authorization") || "").trim();
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function jsonError(res, status, code, message) {
  res.status(status).json({ ok: false, error: code, message });
}

function isValidTimezone(value) {
  const tz = String(value || "").trim();
  if (!tz) return true;
  try {
    new Intl.DateTimeFormat("en-CA", { timeZone: tz }).format(new Date());
    return true;
  } catch (_) {
    return false;
  }
}

function pickLocationField(value) {
  if (value == null) return null;
  if (typeof value === "object") return null;
  const text = String(value || "").trim();
  return text.length ? text.slice(0, 120) : null;
}

function firstTruthy(values) {
  for (const value of values) {
    const next = pickLocationField(value);
    if (next) return next;
  }
  return null;
}

function extractLocationFromObject(value) {
  if (!value || typeof value !== "object") return null;
  const obj = value;
  const direct = firstTruthy([
    obj.locationName,
    obj.location_name,
    obj.placeName,
    obj.place_name,
    obj.name,
    obj.label,
    obj.title,
    obj.subtitle,
    obj.description,
    obj.formattedAddress,
    obj.formatted_address,
    obj.address,
    obj.addressText,
    obj.fullAddress,
    obj.city,
    obj.region,
    obj.state,
    obj.town,
    obj.county,
    obj.country,
  ]);
  if (direct) return direct;

  const street = firstTruthy([obj.street, obj.thoroughfare, obj.road, obj.route]);
  const locality = firstTruthy([obj.locality, obj.city, obj.town, obj.neighborhood]);
  const region = firstTruthy([obj.administrativeArea, obj.region, obj.state]);
  const parts = [street, locality, region]
    .map((p) => p)
    .filter(Boolean);
  if (parts.length) {
    const joined = parts.join(", ").slice(0, 120);
    if (joined) return joined;
  }

  if (obj.formattedAddress || obj.formatted_address || obj.address) {
    return firstTruthy([obj.formattedAddress, obj.formatted_address, obj.address]);
  }

  if (Number.isFinite(Number(obj.latitude)) && Number.isFinite(Number(obj.longitude))) {
    return firstTruthy([`${obj.latitude},${obj.longitude}`]);
  }
  return null;
}

function extractShortcutLocation(payload) {
  if (!payload || typeof payload !== "object") return null;
  const direct = firstTruthy([
    payload.locationLabel,
    payload.location_label,
    payload.location,
    payload.locationName,
    payload.location_name,
    payload.place,
    payload.placeName,
    payload.place_name,
    payload.label,
  ]);
  if (direct) return direct;

  const firstObj =
    (typeof payload.location === "object" && payload.location) ||
    (typeof payload.place === "object" && payload.place) ||
    (typeof payload.locationLabel === "object" && payload.locationLabel) ||
    (typeof payload.location_name === "object" && payload.location_name) ||
    (typeof payload.locationName === "object" && payload.locationName) ||
    (typeof payload.placeName === "object" && payload.placeName) ||
    null;

  if (firstObj) {
    const extracted = extractLocationFromObject(firstObj);
    if (extracted) return extracted;
  }

  return extractLocationFromObject(payload);
}

function extractShortcutTimezone(payload) {
  if (!payload || typeof payload !== "object") return "America/Toronto";
  return pickLocationField(
    payload.timezone ||
      payload.time_zone ||
      payload.timeZone ||
      payload.timezone_name ||
      payload.timeZoneName ||
      ""
  ) || "America/Toronto";
}

function formatShortcutEventLocalTime(event) {
  const timeZone = String(event && event.timezone ? event.timezone : "America/Toronto").trim() || "America/Toronto";
  const eventTimeMs = Number.isFinite(Number(event && event.eventAtMs))
    ? Number(event.eventAtMs)
    : Number(new Date(event && event.eventAtIso ? String(event.eventAtIso) : "").getTime());
  if (!Number.isFinite(eventTimeMs)) return "";

  try {
    return new Intl.DateTimeFormat("en-US", {
      timeZone,
      hour: "numeric",
      minute: "2-digit",
      timeZoneName: "short",
      hour12: true,
    }).format(new Date(eventTimeMs));
  } catch (_) {
    return "";
  }
}

function normalizeShortcutEventType(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return EVENT_TYPE_ALIASES[normalized] || normalized;
}

function dateKeyForTimezone(date, timezone) {
  const tz = String(timezone || "").trim() || "America/Toronto";
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: tz,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    })
      .formatToParts(date)
      .reduce((acc, part) => {
        acc[part.type] = part.value;
        return acc;
      }, {});
    if (parts.year && parts.month && parts.day) {
      return `${parts.year}-${parts.month}-${parts.day}`;
    }
  } catch (_) {}
  return date.toISOString().slice(0, 10);
}

function parseTimezoneOffsetMinutes(timezone, date) {
  const d = date instanceof Date ? date : new Date(date);
  if (!Number.isFinite(d.getTime())) return 0;
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      timeZoneName: "shortOffset",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).formatToParts(d);
    const tzPart = parts.find((part) => part.type === "timeZoneName");
    const match = String(tzPart && tzPart.value ? tzPart.value : "").match(
      /GMT([+-])(\d{1,2})(?::?(\d{2}))?/
    );
    if (!match) return 0;
    const sign = match[1] === "-" ? -1 : 1;
    const hours = Number(match[2] || 0);
    const mins = Number(match[3] || 0);
    if (!Number.isFinite(hours) || !Number.isFinite(mins)) return 0;
    return sign * (hours * 60 + mins);
  } catch (_) {
    return 0;
  }
}

function parseTimezoneNaiveTimestamp(raw, timezone) {
  const m = String(raw)
    .trim()
    .match(
      /^(\d{4})-(\d{2})-(\d{2})[T\s](\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/
    );
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]) - 1;
  const day = Number(m[3]);
  const hour = Number(m[4]);
  const minute = Number(m[5]);
  const second = Number(m[6] || 0);
  const ms = Number(m[7] || 0);

  if (
    !Number.isFinite(year) ||
    !Number.isFinite(month) ||
    !Number.isFinite(day) ||
    !Number.isFinite(hour) ||
    !Number.isFinite(minute) ||
    !Number.isFinite(second) ||
    !Number.isFinite(ms)
  ) {
    return null;
  }

  if (
    month < 0 ||
    month > 11 ||
    day < 1 ||
    day > 31 ||
    hour < 0 ||
    hour > 23 ||
    minute < 0 ||
    minute > 59 ||
    second < 0 ||
    second > 59 ||
    ms < 0 ||
    ms > 999
  ) {
    return null;
  }

  let eventMs = Date.UTC(year, month, day, hour, minute, second, ms);
  if (!Number.isFinite(eventMs)) return null;

  // Convert zone-local wall time into UTC by iterating for the zone offset at that instant.
  for (let i = 0; i < 4; i += 1) {
    const offsetMinutes = parseTimezoneOffsetMinutes(timezone, new Date(eventMs));
    const nextEventMs = Date.UTC(year, month, day, hour, minute, second, ms) - offsetMinutes * 60 * 1000;
    if (nextEventMs === eventMs) break;
    eventMs = nextEventMs;
  }

  return new Date(eventMs);
}

function parseShortcutEventPayload(body, now = new Date()) {
  const payload = body && typeof body === "object" ? body : {};
  const eventType = normalizeShortcutEventType(payload.event_type);
  if (!SUPPORTED_EVENT_TYPES.has(eventType)) {
    return {
      ok: false,
      status: 400,
      code: "invalid_event_type",
      message: `Unsupported event_type. Supported values: ${[...SUPPORTED_EVENT_TYPES].join(", ")}.`,
    };
  }

  const timezone = extractShortcutTimezone(payload);
  if (!isValidTimezone(timezone)) {
    return {
      ok: false,
      status: 400,
      code: "invalid_timezone",
      message: "timezone must be a valid IANA timezone name.",
    };
  }

  const timestampRaw = payload.timestamp == null ? "" : String(payload.timestamp).trim();
  let eventDate;
  if (!timestampRaw) {
    eventDate = now;
  } else if (/([zZ]|[+\-]\d{2}:?\d{2})$/.test(timestampRaw)) {
    eventDate = new Date(timestampRaw);
  } else {
    eventDate = parseTimezoneNaiveTimestamp(timestampRaw, timezone) || new Date(timestampRaw);
  }
  if (!Number.isFinite(eventDate.getTime())) {
    return {
      ok: false,
      status: 400,
      code: "invalid_timestamp",
      message: "timestamp must be a valid ISO date/time string.",
    };
  }

  const { latitude, longitude } = extractShortcutCoordinates(payload);
  if (latitude != null && (!Number.isFinite(latitude) || latitude < -90 || latitude > 90)) {
    return { ok: false, status: 400, code: "invalid_latitude", message: "latitude must be between -90 and 90." };
  }
  if (longitude != null && (!Number.isFinite(longitude) || longitude < -180 || longitude > 180)) {
    return { ok: false, status: 400, code: "invalid_longitude", message: "longitude must be between -180 and 180." };
  }

  return {
    ok: true,
    event: {
      eventType,
      eventLabel: EVENT_LABELS[eventType] || eventType,
      timestampRaw: timestampRaw || null,
      eventDate,
      eventAtIso: eventDate.toISOString(),
      eventAtMs: eventDate.getTime(),
      timezone,
      reportDateKey: dateKeyForTimezone(eventDate, timezone),
      locationLabel: extractShortcutLocation(payload),
      projectSlug: normalizeProjectSlug(
        String(payload.project_slug || payload.projectSlug || payload.project || "").trim()
      ) || null,
      source: String(payload.source || "ios_shortcuts").trim().slice(0, 80) || "ios_shortcuts",
      deviceName: String(payload.device_name || "").trim().slice(0, 120),
      latitude,
      longitude,
      notes: String(payload.notes || "").trim().slice(0, 500),
    },
  };
}

function summarizeShortcutRequestBody(body) {
  const payload = body && typeof body === "object" ? body : {};
  const summaryLocation = extractShortcutLocation(payload);
  return {
    bodyType: Array.isArray(body) ? "array" : typeof body,
    bodyKeys: Object.keys(payload).slice(0, 20),
    eventTypeRaw:
      payload.event_type == null
        ? null
        : String(payload.event_type).trim().slice(0, 120),
    timestampType: payload.timestamp == null ? "missing" : typeof payload.timestamp,
    timezone:
      payload.timezone == null
        ? null
        : String(payload.timezone).trim().slice(0, 120),
    locationLabel: summaryLocation,
    source:
      payload.source == null
        ? null
        : String(payload.source).trim().slice(0, 120),
  };
}

function shortcutEventToAssistantBody(event) {
  const eventTimeLabel = formatShortcutEventLocalTime(event) || event.eventAtIso;
  const parts = [
    `log note (${event.reportDateKey}): iOS Shortcuts tracking event - ${event.eventLabel}.`,
    `Event type: ${event.eventType}.`,
    `Event time: ${eventTimeLabel}.`,
  ];
  if (event.projectSlug) parts.push(`Project: ${event.projectSlug}.`);
  if (event.timezone) parts.push(`Timezone: ${event.timezone}.`);
  if (event.locationLabel) parts.push(`Location: ${event.locationLabel}.`);
  if (event.deviceName) parts.push(`Device: ${event.deviceName}.`);
  if (event.latitude != null && event.longitude != null) {
    parts.push(`Coordinates: ${event.latitude}, ${event.longitude}.`);
  }
  if (event.notes) parts.push(`Notes: ${event.notes}.`);
  return parts.join(" ");
}

function shortcutEventDocId(memberEmail, key) {
  return sha256Hex(`${String(memberEmail || "").toLowerCase()}|${String(key || "")}`).slice(0, 48);
}

function checkShortcutRateLimit(bucketKey, nowMs = Date.now()) {
  const key = String(bucketKey || "").slice(0, 64);
  if (!key) return false;
  const windowMs = 60 * 1000;
  const max = 30;
  const bucket = rateLimitBuckets.get(key) || { startMs: nowMs, count: 0 };
  if (nowMs - bucket.startMs >= windowMs) {
    bucket.startMs = nowMs;
    bucket.count = 0;
  }
  bucket.count += 1;
  rateLimitBuckets.set(key, bucket);
  return bucket.count <= max;
}

async function findShortcutMemberByTokenHash(db, tokenHash) {
  if (!tokenHash) return null;
  const snap = await db
    .collection(COL_APP_MEMBERS)
    .where("shortcutIntegration.tokenHash", "==", tokenHash)
    .where("shortcutIntegration.enabled", "==", true)
    .limit(1)
    .get();
  if (snap.empty) return null;
  const doc = snap.docs[0];
  const data = doc.data() || {};
  if (data.active === false) return null;
  return { email: doc.id, data };
}

async function findDuplicateShortcutEvent(db, memberEmail, event) {
  const lowerMs = event.eventAtMs - 60 * 1000;
  const upperMs = event.eventAtMs + 60 * 1000;
  const snap = await db
    .collection(COL_SHORTCUT_EVENTS)
    .where("memberEmail", "==", memberEmail)
    .where("eventType", "==", event.eventType)
    .where("eventAtMs", ">=", lowerMs)
    .where("eventAtMs", "<=", upperMs)
    .limit(1)
    .get()
    .catch(() => null);
  if (!snap || snap.empty) return null;
  const doc = snap.docs[0];
  return { id: doc.id, data: doc.data() || {} };
}

function normalizeProjectSlugList(values) {
  if (!Array.isArray(values)) return [];
  return [...new Set(values.map((v) => normalizeProjectSlug(v)).filter(Boolean))];
}

async function resolveShortcutProjectSlug(db, member, approvedPhoneE164, requestedProjectSlug) {
  const memberData = member && member.data ? member.data : {};
  let activeProjectSlug = normalizeProjectSlug(memberData.activeProjectSlug) || null;
  let allowedProjectSlugs = normalizeProjectSlugList(memberData.projectSlugs);
  const allProjects = memberData.allProjects === true || memberData.role === "admin" || memberData.role === "owner";

  if (approvedPhoneE164) {
    const smsSnap = await db.collection("smsUsers").doc(approvedPhoneE164).get().catch(() => null);
    if (smsSnap && smsSnap.exists) {
      const smsUser = smsSnap.data() || {};
      activeProjectSlug = activeProjectSlug || normalizeProjectSlug(smsUser.activeProjectSlug) || null;
      allowedProjectSlugs = normalizeProjectSlugList([
        ...allowedProjectSlugs,
        ...(Array.isArray(smsUser.projectSlugs) ? smsUser.projectSlugs : []),
        smsUser.activeProjectSlug,
      ]);
    }
  }

  const requested = normalizeProjectSlug(requestedProjectSlug) || null;
  if (requested) {
    if (!allProjects && !allowedProjectSlugs.includes(requested)) {
      const err = new Error("Shortcut token is not allowed to log to that project.");
      err.status = 403;
      err.code = "project_not_allowed";
      throw err;
    }
    return requested;
  }

  if (activeProjectSlug) return activeProjectSlug;
  return allowedProjectSlugs.length === 1 ? allowedProjectSlugs[0] : null;
}

async function recordShortcutEvent({
  db,
  FieldValue,
  req,
  member,
  event,
  processAssistantMessage,
  openaiKey,
  runId,
}) {
  const idempotencyKey = String(req.get("idempotency-key") || "").trim().slice(0, 200);
  let eventRef;
  let existing = null;

  if (idempotencyKey) {
    eventRef = db.collection(COL_SHORTCUT_EVENTS).doc(shortcutEventDocId(member.email, idempotencyKey));
    if (typeof db.runTransaction !== "function") {
      throw new Error("Idempotent Shortcut events require transactional Firestore support.");
    }
    const claim = await db.runTransaction(async (tx) => {
      const existingSnap = await tx.get(eventRef);
      if (existingSnap.exists) return { existing: true, data: existingSnap.data() || {} };
      const claimData = {
        status: "processing",
        memberEmail: member.email,
        idempotencyKey,
        claimedAt: FieldValue.serverTimestamp(),
      };
      if (typeof tx.create === "function") tx.create(eventRef, claimData);
      else tx.set(eventRef, claimData);
      return { existing: false };
    });
    if (claim.existing) existing = { id: eventRef.id, data: claim.data };
  } else {
    existing = await findDuplicateShortcutEvent(db, member.email, event);
    eventRef = existing
      ? db.collection(COL_SHORTCUT_EVENTS).doc(existing.id)
      : db.collection(COL_SHORTCUT_EVENTS).doc();
  }

  if (existing) {
    return {
      duplicate: true,
      eventId: existing.data.logEntryId || existing.id,
      shortcutEventId: existing.id,
      message: "Event already recorded",
    };
  }

  const approvedPhoneE164 = String(member.data.approvedPhoneE164 || "").trim();
  if (!approvedPhoneE164) {
    const err = new Error("Shortcut token is not linked to an approved phone.");
    err.status = 403;
    err.code = "token_not_linked";
    throw err;
  }
  const targetProjectSlug = await resolveShortcutProjectSlug(db, member, approvedPhoneE164, event.projectSlug);
  event.projectSlug = targetProjectSlug;

  await eventRef.set({
    status: "processing",
    memberEmail: member.email,
    phoneE164: approvedPhoneE164,
    eventType: event.eventType,
    eventAtIso: event.eventAtIso,
    eventAtMs: event.eventAtMs,
    reportDateKey: event.reportDateKey,
    projectSlug: targetProjectSlug || null,
    timezone: event.timezone,
    locationLabel: event.locationLabel || null,
    source: event.source,
    deviceName: event.deviceName || null,
    latitude: event.latitude,
    longitude: event.longitude,
    notes: event.notes || null,
    idempotencyKey: idempotencyKey || null,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });

  const assistantBody = shortcutEventToAssistantBody(event);
  const processed = await processAssistantMessage({
    phoneE164: approvedPhoneE164,
    body: assistantBody,
    channel: "ios_shortcuts",
    replyFrom: "ios_shortcuts",
    openaiKey,
    runId,
  });

  const logEntryId = processed.outboundMeta && processed.outboundMeta.logEntryId ? processed.outboundMeta.logEntryId : null;
  const projectSlug = processed.outboundMeta && processed.outboundMeta.projectSlug ? processed.outboundMeta.projectSlug : null;
  if (logEntryId) {
    await db.collection("logEntries").doc(logEntryId).set(
      {
        source: "ios_shortcuts",
        shortcutEventId: eventRef.id,
        shortcutEventType: event.eventType,
        shortcutEventAtIso: event.eventAtIso,
        shortcutEventAtMs: event.eventAtMs,
        shortcutTimestampProvided: Boolean(event.timestampRaw),
        shortcutTimezone: event.timezone,
        shortcutLocationLabel: event.locationLabel || null,
        shortcutDeviceName: event.deviceName || null,
        shortcutLatitude: event.latitude,
        shortcutLongitude: event.longitude,
        reportDateKey: event.reportDateKey,
        dateKey: event.reportDateKey,
        ...(targetProjectSlug ? { projectSlug: targetProjectSlug, projectId: targetProjectSlug } : {}),
        tags: FieldValue.arrayUnion("ios_shortcuts", "location_tracking", event.eventType),
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  }

  await eventRef.set(
    {
      status: "recorded",
      logEntryId,
      projectSlug: targetProjectSlug || projectSlug || null,
      inboundMessageId: processed.inboundRef ? processed.inboundRef.id : null,
      outboundMessageId: processed.outboundRef ? processed.outboundRef.id : null,
      assistantCommand: processed.outboundMeta ? processed.outboundMeta.command || null : null,
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  return {
    duplicate: false,
    eventId: logEntryId || eventRef.id,
    shortcutEventId: eventRef.id,
    message: "Event recorded",
  };
}

async function handleShortcutEventRequest({
  db,
  FieldValue,
  req,
  res,
  logger,
  processAssistantMessage,
  openaiKey,
  timeLeftLifeEventDelivery,
}) {
  const runId = `ios-shortcuts-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  try {
    if (req.method !== "POST") {
      res.status(405).set("Allow", "POST").json({ ok: false, error: "method_not_allowed", message: "Use POST." });
      return;
    }

    const rawToken = extractShortcutToken(req);
    if (!rawToken) {
      jsonError(res, 401, "missing_token", "Missing Shortcuts integration token.");
      return;
    }
    const tokenHash = hashShortcutToken(rawToken);
    if (!checkShortcutRateLimit(tokenHash)) {
      jsonError(res, 429, "rate_limited", "Too many Shortcut events. Try again shortly.");
      return;
    }
    const member = await findShortcutMemberByTokenHash(db, tokenHash);
    if (!member) {
      jsonError(res, 401, "invalid_token", "Invalid Shortcuts integration token.");
      return;
    }

    const requestSummary = summarizeShortcutRequestBody(req.body);
    if (logger && typeof logger.info === "function") {
      logger.info("iosShortcutsEvents: received event payload", {
        runId,
        contentType: req.get("content-type") || "",
        idempotencyKeyPresent: Boolean(String(req.get("idempotency-key") || "").trim()),
        ...requestSummary,
      });
    }

    const parsed = parseShortcutEventPayload(req.body);
    if (!parsed.ok) {
      if (logger && typeof logger.warn === "function") {
        logger.warn("iosShortcutsEvents: rejected event payload", {
          runId,
          code: parsed.code,
          status: parsed.status,
          eventTypeRaw: requestSummary.eventTypeRaw,
          bodyKeys: requestSummary.bodyKeys,
        });
      }
      jsonError(res, parsed.status, parsed.code, parsed.message);
      return;
    }

    if (logger && typeof logger.info === "function") {
      logger.info("iosShortcutsEvents: parsed event payload", {
        runId,
        eventType: parsed.event.eventType,
        eventAtIso: parsed.event.eventAtIso,
        reportDateKey: parsed.event.reportDateKey,
        timezone: parsed.event.timezone,
        latitude: parsed.event.latitude,
        longitude: parsed.event.longitude,
      });
    }

    const result = await recordShortcutEvent({
      db,
      FieldValue,
      req,
      member,
      event: parsed.event,
      processAssistantMessage,
      openaiKey,
      runId,
    });

    if (!result.duplicate && typeof timeLeftLifeEventDelivery === "function") {
      try {
        await timeLeftLifeEventDelivery({
          event: {
            ...parsed.event,
            id: result.shortcutEventId,
          },
          eventId: result.shortcutEventId,
        });
      } catch (err) {
        if (logger && typeof logger.warn === "function") {
          logger.warn("iosShortcutsEvents: TimeLeft delivery failed", {
            runId,
            shortcutEventId: result.shortcutEventId,
            message: err && err.message,
            code: err && err.code,
          });
        }
      }
    }

    res.status(200).json({
      ok: true,
      event_id: result.eventId,
      shortcut_event_id: result.shortcutEventId,
      duplicate: result.duplicate,
      message: result.message,
    });
  } catch (err) {
    const status = Number(err && err.status) || 500;
    const code = String((err && err.code) || "shortcut_event_failed");
    if (logger) {
      logger.error("iosShortcutsEvents: failed", {
        runId,
        code,
        status,
        message: err && err.message,
        stack: err && err.stack,
      });
    }
    jsonError(
      res,
      status >= 400 && status < 600 ? status : 500,
      code,
      status === 401 ? "Invalid Shortcuts integration token." : err.message || "Could not record Shortcut event."
    );
  }
}

module.exports = {
  COL_SHORTCUT_EVENTS,
  SUPPORTED_EVENT_TYPES,
  generateShortcutToken,
  hashShortcutToken,
  tokenLast4,
  extractShortcutToken,
  parseShortcutEventPayload,
  summarizeShortcutRequestBody,
  shortcutEventToAssistantBody,
  shortcutEventDocId,
  checkShortcutRateLimit,
  findShortcutMemberByTokenHash,
  recordShortcutEvent,
  handleShortcutEventRequest,
};
