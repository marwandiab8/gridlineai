const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_MODE = "off";

const DISALLOWED_TARGET_PROJECT_IDS = new Set([
  "timelefttolive",
  "timelefttolive-stg-marwan",
]);

const STAGING_TIMELEFT_ORIGIN = "timelefttolive-stg-go.web.app";
const STAGING_LIFE_EVENTS_PATH = "/api/v1/life-events";

function normalizeMode(value) {
  const candidate = String(value || "").trim().toLowerCase();
  if (candidate === "production") return "production";
  if (candidate === "staging") return "staging";
  return DEFAULT_MODE;
}

function trimOrEmpty(value) {
  const text = String(value == null ? "" : value).trim();
  return text;
}

function parseTimeout(value) {
  const timeoutMsRaw = trimOrEmpty(value);
  if (!timeoutMsRaw) return DEFAULT_TIMEOUT_MS;
  const parsed = Number(timeoutMsRaw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_TIMEOUT_MS;
  return Math.max(1, Math.min(120_000, Math.floor(parsed)));
}

function isValidProjectId(projectId) {
  const id = trimOrEmpty(projectId).toLowerCase();
  return Boolean(id) && !DISALLOWED_TARGET_PROJECT_IDS.has(id);
}

function parseUrl(rawUrl) {
  const url = trimOrEmpty(rawUrl);
  if (!url) return null;
  try {
    return new URL(url);
  } catch (_) {
    return null;
  }
}

function isStagingLifeEventsUrl(urlValue) {
  const url = parseUrl(urlValue);
  if (!url) return false;
  if (url.protocol !== "https:") return false;
  if (url.hostname !== STAGING_TIMELEFT_ORIGIN) return false;
  return url.pathname === STAGING_LIFE_EVENTS_PATH;
}

function readTimeLeftLifeEventConfigFromEnv(env = process.env) {
  return {
    mode: env.TLTL_DUAL_WRITE_MODE,
    endpointUrl: env.TLTL_LIFE_EVENTS_URL,
    calendarId: env.TLTL_CALENDAR_ID,
    connectionId: env.TLTL_CONNECTION_ID,
    integrationId: env.TLTL_INTEGRATION_ID,
    bearerToken: env.TLTL_BEARER_TOKEN,
    targetProjectId: env.TLTL_TARGET_PROJECT_ID,
    timeoutMs: env.TLTL_REQUEST_TIMEOUT_MS,
  };
}

function validateTimeLeftLifeEventConfig(rawConfig = {}) {
  const provided = {
    mode: normalizeMode(rawConfig.mode),
    endpointUrl: trimOrEmpty(rawConfig.endpointUrl),
    calendarId: trimOrEmpty(rawConfig.calendarId),
    connectionId: trimOrEmpty(rawConfig.connectionId),
    integrationId: trimOrEmpty(rawConfig.integrationId),
    bearerToken: trimOrEmpty(rawConfig.bearerToken),
    targetProjectId: trimOrEmpty(rawConfig.targetProjectId),
    timeoutMs: parseTimeout(rawConfig.timeoutMs),
  };

  const mode = provided.mode;
  const errors = [];

  if (!isValidProjectId(provided.targetProjectId) && provided.targetProjectId) {
    errors.push("TLTL_TARGET_PROJECT_ID is not allowed.");
  } else if (mode !== "off" && !provided.targetProjectId) {
    errors.push("TLTL_TARGET_PROJECT_ID is required.");
  }

  if (mode === "production") {
    errors.push("TLTL_DUAL_WRITE_MODE=production is disabled during Phase A.");
  }

  if (mode === "staging") {
    if (!provided.endpointUrl) {
      errors.push("TLTL_LIFE_EVENTS_URL is required in staging mode.");
    } else if (!isStagingLifeEventsUrl(provided.endpointUrl)) {
      errors.push("TLTL_LIFE_EVENTS_URL must target https://timelefttolive-stg-go.web.app/api/v1/life-events.");
    }
    if (!provided.calendarId) errors.push("TLTL_CALENDAR_ID is required in staging mode.");
    if (!provided.connectionId) errors.push("TLTL_CONNECTION_ID is required in staging mode.");
    if (!provided.integrationId) errors.push("TLTL_INTEGRATION_ID is required in staging mode.");
    if (!provided.bearerToken) errors.push("TLTL_BEARER_TOKEN is required in staging mode.");
  }

  return {
    valid: errors.length === 0,
    errors,
    config: {
      mode,
      endpointUrl: provided.endpointUrl,
      calendarId: provided.calendarId,
      connectionId: provided.connectionId,
      integrationId: provided.integrationId,
      bearerToken: provided.bearerToken,
      targetProjectId: provided.targetProjectId,
      timeoutMs: provided.timeoutMs,
    },
  };
}

function requireValidTimeLeftLifeEventConfig(rawConfig = {}) {
  const result = validateTimeLeftLifeEventConfig(rawConfig);
  if (!result.valid) {
    const error = new Error(
      `TimeLeft to LifeEvent configuration is not valid: ${result.errors.join("; ")}`
    );
    error.code = "timelefttolive-config-invalid";
    throw error;
  }
  return result.config;
}

module.exports = {
  DEFAULT_MODE,
  DEFAULT_TIMEOUT_MS,
  DISALLOWED_TARGET_PROJECT_IDS,
  STAGING_LIFE_EVENTS_PATH,
  STAGING_TIMELEFT_ORIGIN,
  isStagingLifeEventsUrl,
  normalizeMode,
  parseTimeout,
  readTimeLeftLifeEventConfigFromEnv,
  requireValidTimeLeftLifeEventConfig,
  validateTimeLeftLifeEventConfig,
};
