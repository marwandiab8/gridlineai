const { defineSecret, defineString } = require("firebase-functions/params");

const TIME_LEFT_INGESTION_TOKEN = defineSecret("TIME_LEFT_INGESTION_TOKEN");

const TIME_LEFT_SINGLE_INGEST_ENDPOINT = defineString("TIME_LEFT_SINGLE_INGEST_ENDPOINT", {
  default: "https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItem",
});

const TIME_LEFT_BATCH_INGEST_ENDPOINT = defineString("TIME_LEFT_BATCH_INGEST_ENDPOINT", {
  default: "https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItemsBatch",
});

const TIME_LEFT_CALENDAR_ID = defineString("TIME_LEFT_CALENDAR_ID", { default: "" });
const TIME_LEFT_CONNECTION_ID = defineString("TIME_LEFT_CONNECTION_ID", { default: "" });
const GRIDLINE_FIREBASE_PROJECT_ID = defineString("GRIDLINE_FIREBASE_PROJECT_ID", { default: "gridlineai" });
const GRIDLINE_APP_BASE_URL = defineString("GRIDLINE_APP_BASE_URL", { default: "" });
const GRIDLINE_DEFAULT_TIME_ZONE = defineString("GRIDLINE_DEFAULT_TIME_ZONE", { default: "America/Toronto" });
const GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS = defineString("GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS", { default: "" });

function commaList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

module.exports = {
  GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS,
  GRIDLINE_APP_BASE_URL,
  GRIDLINE_DEFAULT_TIME_ZONE,
  GRIDLINE_FIREBASE_PROJECT_ID,
  TIME_LEFT_BATCH_INGEST_ENDPOINT,
  TIME_LEFT_CALENDAR_ID,
  TIME_LEFT_CONNECTION_ID,
  TIME_LEFT_INGESTION_TOKEN,
  TIME_LEFT_SINGLE_INGEST_ENDPOINT,
  commaList,
};
