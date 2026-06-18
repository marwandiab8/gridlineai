const logger = require("firebase-functions/logger");
const {
  GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS,
  TIME_LEFT_BATCH_INGEST_ENDPOINT,
  TIME_LEFT_CALENDAR_ID,
  TIME_LEFT_CONNECTION_ID,
  TIME_LEFT_INGESTION_TOKEN,
  TIME_LEFT_SINGLE_INGEST_ENDPOINT,
  commaList,
} = require("./config");

function readRuntimeConfig() {
  return {
    batchEndpoint: TIME_LEFT_BATCH_INGEST_ENDPOINT.value(),
    calendarId: TIME_LEFT_CALENDAR_ID.value(),
    connectionId: TIME_LEFT_CONNECTION_ID.value(),
    singleEndpoint: TIME_LEFT_SINGLE_INGEST_ENDPOINT.value(),
    token: TIME_LEFT_INGESTION_TOKEN.value(),
    allowedSourceProjectIds: commaList(GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS.value()),
  };
}

function assertRuntimeConfig(config, batch = false) {
  const missing = [];
  if (!(batch ? config.batchEndpoint : config.singleEndpoint)) {
    missing.push(batch ? "TIME_LEFT_BATCH_INGEST_ENDPOINT" : "TIME_LEFT_SINGLE_INGEST_ENDPOINT");
  }
  if (!config.calendarId) missing.push("TIME_LEFT_CALENDAR_ID");
  if (!config.connectionId) missing.push("TIME_LEFT_CONNECTION_ID");
  if (!config.token) missing.push("TIME_LEFT_INGESTION_TOKEN");
  if (missing.length) {
    const error = new Error(`Time Left gridlineai sync is not configured: ${missing.join(", ")}`);
    error.code = "time-left-gridlineai-not-configured";
    throw error;
  }
}

function validateAllowedSourceProject(item, allowedSourceProjectIds) {
  if (!allowedSourceProjectIds.length || !item || !item.sourceProjectId) return;
  if (allowedSourceProjectIds.includes(item.sourceProjectId)) return;
  const error = new Error(`sourceProjectId is not locally allowlisted: ${item.sourceProjectId}`);
  error.code = "source-project-not-allowed";
  throw error;
}

async function parseResponse(response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_) {
    return { raw: text.slice(0, 1000) };
  }
}

async function postToTimeLeft(url, token, payload, options = {}) {
  const tokenHeader = options.tokenHeader || "Authorization";
  const headers = {
    "Content-Type": "application/json",
  };
  headers[tokenHeader] = tokenHeader.toLowerCase() === "authorization" ? `Bearer ${token}` : token;

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  const body = await parseResponse(response);
  if (!response.ok) {
    const error = new Error(body.error || body.message || `Time Left ingestion failed with HTTP ${response.status}`);
    error.status = response.status;
    error.body = body;
    throw error;
  }
  return body;
}

async function sendTimeLeftDailyItem(item, options = {}) {
  const config = readRuntimeConfig();
  try {
    assertRuntimeConfig(config, false);
    validateAllowedSourceProject(item, config.allowedSourceProjectIds);
    return await postToTimeLeft(
      config.singleEndpoint,
      config.token,
      {
        calendarId: config.calendarId,
        connectionId: config.connectionId,
        item,
      },
      options
    );
  } catch (error) {
    logger.warn("timeLeft gridlineai single ingestion failed", {
      sourceDocumentPath: item && item.sourceDocumentPath,
      sourceStoragePath: item && item.sourceStoragePath,
      sourceProjectId: item && item.sourceProjectId,
      category: item && item.category,
      status: error.status || null,
      code: error.code || null,
      message: String(error.message || error).slice(0, 300),
    });
    if (options.throwOnError) throw error;
    return { ok: false, error: error.message, status: error.status || null };
  }
}

async function sendTimeLeftDailyItemsBatch(items, options = {}) {
  const config = readRuntimeConfig();
  try {
    assertRuntimeConfig(config, true);
    for (const item of items || []) validateAllowedSourceProject(item, config.allowedSourceProjectIds);
    return await postToTimeLeft(
      config.batchEndpoint,
      config.token,
      {
        calendarId: config.calendarId,
        connectionId: config.connectionId,
        items,
      },
      options
    );
  } catch (error) {
    logger.warn("timeLeft gridlineai batch ingestion failed", {
      itemCount: Array.isArray(items) ? items.length : 0,
      status: error.status || null,
      code: error.code || null,
      message: String(error.message || error).slice(0, 300),
    });
    if (options.throwOnError) throw error;
    return { ok: false, error: error.message, status: error.status || null };
  }
}

module.exports = {
  postToTimeLeft,
  readRuntimeConfig,
  sendTimeLeftDailyItem,
  sendTimeLeftDailyItemsBatch,
};
