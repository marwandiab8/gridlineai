const STABLE_LOCATION_KEYS = ["location", "shortcutLocation", "locationLabel", "location_label"];
const SECRET_LIKE_KEYS = [
  "token",
  "accessToken",
  "apiKey",
  "secret",
  "password",
  "authorization",
  "bearer",
  "webhook",
  "webhookUrl",
];

const DEFAULT_TIMEZONE = "America/Toronto";
const DEFAULT_SOURCE_APP = "gridlineai";
const DEFAULT_SOURCE_FIREBASE_PROJECT_ID = "gridlineai";
const SCHEMA_VERSION = 1;

const SHORTCUT_EVENT_RULES = {
  arrive_work: {
    eventClass: "activity_boundary",
    activityFamily: "work",
    categoryId: "work",
    title: "Arrived at work",
  },
  leave_work: {
    eventClass: "activity_boundary",
    activityFamily: "work",
    categoryId: "work",
    title: "Left work",
  },
  arrive_home: {
    eventClass: "activity_boundary",
    activityFamily: "home",
    categoryId: "home",
    title: "Arrived home",
  },
  leave_home: {
    eventClass: "activity_boundary",
    activityFamily: "home",
    categoryId: "home",
    title: "Left home",
  },
  arrive_gym: {
    eventClass: "activity_boundary",
    activityFamily: "gym",
    categoryId: "gym",
    title: "Arrived at the gym",
  },
  leave_gym: {
    eventClass: "activity_boundary",
    activityFamily: "gym",
    categoryId: "gym",
    title: "Left the gym",
  },
  start_workout: {
    eventClass: "activity_boundary",
    activityFamily: "workout",
    categoryId: "workout",
    title: "Started workout",
  },
  finish_workout: {
    eventClass: "activity_boundary",
    activityFamily: "workout",
    categoryId: "workout",
    title: "Finished workout",
  },
  start_spotify: {
    eventClass: "activity_boundary",
    activityFamily: "spotify",
    categoryId: "spotify",
    title: "Started listening to Spotify",
  },
  arrive_location: {
    eventClass: "location",
    activityFamily: "location",
    categoryId: "other_location",
    title: "Arrived at location",
  },
  leave_location: {
    eventClass: "location",
    activityFamily: "location",
    categoryId: "other_location",
    title: "Left location",
  },
};

function trimOrEmpty(value) {
  const text = String(value == null ? "" : value).trim();
  return text;
}

function normalizeEventType(value) {
  return trimOrEmpty(value);
}

function normalizeTimezone(value) {
  const tz = trimOrEmpty(value);
  if (!tz) return DEFAULT_TIMEZONE;
  try {
    new Intl.DateTimeFormat("en-CA", { timeZone: tz });
    return tz;
  } catch (_) {
    return null;
  }
}

function parseIso(value) {
  const text = trimOrEmpty(value);
  if (!text) return null;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function parseEventAtMs(value) {
  if (!Number.isFinite(Number(value))) return null;
  const ms = Number(value);
  if (!Number.isFinite(ms) || ms < 0) return null;
  return new Date(ms).toISOString();
}

function parseOccurredAt(event) {
  return (
    parseIso(event && event.eventAtIso) ||
    parseEventAtMs(event && event.eventAtMs) ||
    null
  );
}

function safeMetadataFromValue(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const out = {};
  const seen = new Set();

  for (const key of Object.keys(value)) {
    if (seen.has(key)) continue;
    seen.add(key);

    const lowered = key.toLowerCase();
    const isSecret = SECRET_LIKE_KEYS.some((secret) => lowered.includes(secret.toLowerCase()));
    if (isSecret) continue;
    if (key === "memberEmail") continue;

    const entry = value[key];
    if (entry == null) continue;
    if (typeof entry === "string" || typeof entry === "number" || typeof entry === "boolean") {
      out[key] = entry;
      continue;
    }
    if (entry instanceof Date) {
      out[key] = entry.toISOString();
      continue;
    }
    if (Array.isArray(entry)) {
      out[key] = entry
        .filter((item) => item != null)
        .map((item) => (typeof item === "string" ? item : JSON.stringify(item)))
        .filter(Boolean);
      continue;
    }
    if (typeof entry === "object") {
      out[key] = safeMetadataFromValue(entry);
    }
  }

  return Object.keys(out).length ? out : null;
}

function buildMetadata(event) {
  let eventMetadata = safeMetadataFromValue(event && event.metadata);
  if (event && event.projectSlug) {
    (eventMetadata || (eventMetadata = {})).projectSlug = trimOrEmpty(event.projectSlug);
  }
  if (event && event.reportDateKey) {
    (eventMetadata || (eventMetadata = {})).reportDateKey = trimOrEmpty(event.reportDateKey);
  }
  return eventMetadata && Object.keys(eventMetadata).length ? eventMetadata : null;
}

function extractCoordinate(value) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(value);
}

function buildLocation(event) {
  const labelRaw =
    event && (event.locationLabel || event.shortcutLocationLabel || event.location_label);
  const label = trimOrEmpty(labelRaw || STABLE_LOCATION_KEYS
    .map((key) => (event && event[key]))
    .find(Boolean));
  const latitude = extractCoordinate(event && event.latitude);
  const longitude = extractCoordinate(event && event.longitude);
  const hasLat = Number.isFinite(latitude);
  const hasLon = Number.isFinite(longitude);
  if (!Number.isFinite(latitude) && !Number.isFinite(longitude) && !label) return null;
  if ((hasLat && !hasLon) || (hasLon && !hasLat)) {
    return label ? { label } : null;
  }
  if (hasLat && hasLon) {
    if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
      return label ? { label } : null;
    }
    if (label) {
      return { label, latitude, longitude };
    }
    return { latitude, longitude };
  }
  return label ? { label } : null;
}

function resultForUnsupportedEvent(event) {
  const eventType = trimOrEmpty(event && event.eventType);
  return {
    ok: false,
    status: "unsupported_event_type",
    sourceEventType: eventType || null,
    reason: eventType
      ? `Unsupported source eventType: ${eventType}`
      : "Missing source eventType",
  };
}

function mapShortcutEventToTimeLeftLifeEvent(event) {
  if (!event || typeof event !== "object") {
    return resultForUnsupportedEvent({});
  }
  const sourceRecordId = trimOrEmpty(event.id || event.sourceRecordId);
  const eventType = normalizeEventType(event.eventType);
  const rule = SHORTCUT_EVENT_RULES[eventType];
  if (!rule) return resultForUnsupportedEvent(event);

  const timezone = normalizeTimezone(event.timezone);
  if (!timezone) {
    return {
      ok: false,
      status: "invalid_timezone",
      sourceEventType: eventType,
    };
  }

  const occurredAt = parseOccurredAt(event);
  if (!occurredAt) {
    return {
      ok: false,
      status: "missing_event_time",
      sourceEventType: eventType,
    };
  }

  if (!sourceRecordId) {
    return {
      ok: false,
      status: "missing_source_record_id",
      sourceEventType: eventType,
    };
  }

  const eventBody = {
    ok: true,
    event: {
      schemaVersion: SCHEMA_VERSION,
      sourceApp: DEFAULT_SOURCE_APP,
      sourceFirebaseProjectId: DEFAULT_SOURCE_FIREBASE_PROJECT_ID,
      sourceRecordId,
      eventType,
      eventClass: rule.eventClass,
      activityFamily: rule.activityFamily,
      categoryId: rule.categoryId,
      title: rule.title,
      occurredAt,
      timezone,
      privacyLevel: "ownerOnly",
    },
  };

  if (trimOrEmpty(event.sourceEventId)) {
    eventBody.event.sourceEventId = trimOrEmpty(event.sourceEventId);
  }
  if (trimOrEmpty(event.sourceProjectId || event.projectSlug)) {
    eventBody.event.sourceProjectId = trimOrEmpty(event.sourceProjectId || event.projectSlug);
  }

  const location = buildLocation(event);
  if (location) {
    eventBody.event.location = location;
  }

  const metadata = buildMetadata(event);
  if (metadata) {
    eventBody.event.metadata = metadata;
  }

  return eventBody;
}

module.exports = {
  SCHEMA_VERSION,
  DEFAULT_TIMEZONE,
  DEFAULT_SOURCE_APP,
  DEFAULT_SOURCE_FIREBASE_PROJECT_ID,
  mapShortcutEventToTimeLeftLifeEvent,
  parseOccurredAt,
  parseEventAtMs,
  parseIso,
};
