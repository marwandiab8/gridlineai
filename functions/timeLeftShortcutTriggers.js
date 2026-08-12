const admin = require("firebase-admin");
const logger = require("firebase-functions/logger");
const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const { onSchedule } = require("firebase-functions/v2/scheduler");

const {
  GRIDLINE_FIREBASE_PROJECT_ID,
  TIME_LEFT_CALENDAR_ID,
  TIME_LEFT_CONNECTION_ID,
  TIME_LEFT_INGESTION_TOKEN,
} = require("./timeLeftGridline/config");
const { assertFirebaseProjectId } = require("./timeLeftGridline/policy");
const { createTimeLeftDeliveryRepository } = require("./timeLeftDeliveryRepository");
const { createTimeLeftLifeEventClient } = require("./timeLeftLifeEventClient");
const {
  readTimeLeftLifeEventConfigFromEnv,
  requireValidTimeLeftLifeEventConfig,
} = require("./timeLeftLifeEventConfig");
const { createTimeLeftLifeEventDelivery } = require("./timeLeftLifeEventDelivery");
const { mapShortcutEventToTimeLeftLifeEvent } = require("./timeLeftLifeEventMapper");
const { replayRecentShortcutEvents } = require("./timeLeftShortcutReplay");

if (!admin.apps.length) admin.initializeApp();

const triggerOptions = {
  region: "northamerica-northeast1",
  memory: "256MiB",
  secrets: [TIME_LEFT_INGESTION_TOKEN],
};

function buildRuntimeConfig(env = process.env) {
  const sourceProjectId = assertFirebaseProjectId(
    GRIDLINE_FIREBASE_PROJECT_ID.value() || env.GRIDLINE_FIREBASE_PROJECT_ID || "gridlineai"
  );
  if (sourceProjectId !== "gridlineai") {
    throw new Error("GRIDLINE_FIREBASE_PROJECT_ID must equal gridlineai for Shortcut delivery.");
  }
  return requireValidTimeLeftLifeEventConfig(
    readTimeLeftLifeEventConfigFromEnv({
      ...env,
      TIME_LEFT_CALENDAR_ID: TIME_LEFT_CALENDAR_ID.value(),
      TIME_LEFT_CONNECTION_ID: TIME_LEFT_CONNECTION_ID.value(),
      TIME_LEFT_INGESTION_TOKEN: TIME_LEFT_INGESTION_TOKEN.value(),
    })
  );
}

function buildDelivery({ db = admin.firestore(), FieldValue = admin.firestore.FieldValue } = {}) {
  const config = buildRuntimeConfig();
  return createTimeLeftLifeEventDelivery({
    mapper: mapShortcutEventToTimeLeftLifeEvent,
    client: createTimeLeftLifeEventClient(config, { logger }),
    repository: createTimeLeftDeliveryRepository({ db, FieldValue }),
    logger,
  });
}

exports.syncIosShortcutEventToTimeLeft = onDocumentCreated(
  {
    ...triggerOptions,
    document: "iosShortcutEvents/{eventId}",
    timeoutSeconds: 60,
    retry: false,
  },
  async (event) => {
    const eventId = String(event.params && event.params.eventId || "").trim();
    const source = event.data && event.data.exists ? event.data.data() || {} : null;
    if (!eventId || !source) return null;
    const result = await buildDelivery()({ event: { ...source, id: eventId }, eventId });
    logger.info("TimeLeft Shortcut create delivery completed", {
      eventId,
      status: result.status || null,
      retryable: Boolean(result.retryable),
    });
    return result;
  }
);

exports.retryIosShortcutEventsToTimeLeft = onSchedule(
  {
    ...triggerOptions,
    schedule: "every 15 minutes",
    timeZone: "America/Toronto",
    timeoutSeconds: 300,
  },
  async () => {
    const config = buildRuntimeConfig();
    const summary = await replayRecentShortcutEvents({
      db: admin.firestore(),
      deliver: buildDelivery(),
      currentMode: config.mode,
      lookbackDays: 7,
      scanLimit: 500,
      attemptLimit: 100,
    });
    logger.info("TimeLeft Shortcut retry summary", summary);
    return summary;
  }
);

module.exports.buildDelivery = buildDelivery;
module.exports.buildRuntimeConfig = buildRuntimeConfig;
