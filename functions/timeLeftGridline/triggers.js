const { onDocumentWritten } = require("firebase-functions/v2/firestore");
const { onRequest } = require("firebase-functions/v2/https");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const {
  GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS,
  GRIDLINE_APP_BASE_URL,
  GRIDLINE_DEFAULT_TIME_ZONE,
  GRIDLINE_FIREBASE_PROJECT_ID,
  TIME_LEFT_INGESTION_TOKEN,
  commaList,
} = require("./config");
const { sendTimeLeftDailyItem, sendTimeLeftDailyItemsBatch } = require("./ingestionClient");
const {
  mapJournalEntryToTimeLeft,
  mapMediaToTimeLeft,
  mapProjectRecordToTimeLeft,
  mapReportToTimeLeft,
} = require("./mappers");

const triggerOptions = {
  region: "northamerica-northeast1",
  timeoutSeconds: 60,
  memory: "256MiB",
  retry: false,
  secrets: [TIME_LEFT_INGESTION_TOKEN],
};

const BACKFILL_SOURCES = {
  projects: { collection: "projects", mapper: mapProjectRecordToTimeLeft },
  dailyReports: { collection: "dailyReports", mapper: mapReportToTimeLeft },
  logEntries: { collection: "logEntries", mapper: mapJournalEntryToTimeLeft },
  media: { collection: "media", mapper: mapMediaToTimeLeft },
};

function mediaDownloadUrl(mediaId, storagePath) {
  const id = encodeURIComponent(String(mediaId || "").trim());
  const path = encodeURIComponent(String(storagePath || "").trim());
  return id && path
    ? `https://northamerica-northeast1-gridlineai.cloudfunctions.net/timeLeftGridlineMediaDownload?m=${id}&p=${path}`
    : "";
}

function snapshotData(snapshot) {
  return snapshot && snapshot.exists ? snapshot.data() || {} : null;
}

function writeOptions(collectionName, id, syncStatus) {
  return {
    id,
    path: `${collectionName}/${id}`,
    appBaseUrl: GRIDLINE_APP_BASE_URL.value(),
    sourceFirebaseProjectId: GRIDLINE_FIREBASE_PROJECT_ID.value() || "gridlineai",
    syncStatus,
    timeZone: GRIDLINE_DEFAULT_TIME_ZONE.value() || "America/Toronto",
  };
}

async function syncWrittenDocument(event, collectionName, paramName, mapper) {
  const id = event.params && event.params[paramName];
  const before = snapshotData(event.data && event.data.before);
  const after = snapshotData(event.data && event.data.after);
  const deleted = before && !after;
  const source = after || before;
  if (!source || !id) return null;

  const options = writeOptions(collectionName, id, deleted ? "deletedFromSource" : "active");
  const item = mapper(source, options);
  if (!item.sourceDocumentPath) item.sourceDocumentPath = `${collectionName}/${id}`;
  if (collectionName === "media" && item.sourceStoragePath && !item.fileUrl) {
    const url = mediaDownloadUrl(id, item.sourceStoragePath);
    item.fileUrl = url || item.fileUrl;
    if (item.category === "image" && !item.thumbnailUrl) item.thumbnailUrl = url;
  }

  const result = await sendTimeLeftDailyItem(item);
  logger.info("timeLeft gridlineai sync attempted", {
    functionName: `syncGridline${collectionName}ToTimeLeft`,
    sourceDocumentPath: item.sourceDocumentPath,
    sourceStoragePath: item.sourceStoragePath || null,
    sourceProjectId: item.sourceProjectId || null,
    category: item.category,
    dateId: item.dateId || null,
    ok: !(result && result.ok === false),
    status: result && result.status ? result.status : null,
  });
  return result;
}

function mapBackfillItem(collectionName, id, source) {
  const def = BACKFILL_SOURCES[collectionName];
  const options = writeOptions(collectionName, id, "active");
  const item = def.mapper(source || {}, options);
  if (!item.sourceDocumentPath) item.sourceDocumentPath = `${collectionName}/${id}`;
  if (collectionName === "media" && item.sourceStoragePath && !item.fileUrl) {
    const url = mediaDownloadUrl(id, item.sourceStoragePath);
    item.fileUrl = url || item.fileUrl;
    if (item.category === "image" && !item.thumbnailUrl) item.thumbnailUrl = url;
  }
  return item;
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function isLocallyAllowedSourceProject(item) {
  const allowed = commaList(GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS.value());
  if (!allowed.length || !item || !item.sourceProjectId) return true;
  return allowed.includes(item.sourceProjectId);
}

function assertBackfillAuth(req) {
  const header = String(req.get("authorization") || "");
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match || match[1].trim() !== TIME_LEFT_INGESTION_TOKEN.value()) {
    const error = new Error("Forbidden.");
    error.status = 403;
    throw error;
  }
}

async function backfillCollection(collectionName, limit) {
  const def = BACKFILL_SOURCES[collectionName];
  const snap = await admin.firestore().collection(def.collection).limit(limit).get();
  const items = snap.docs.map((doc) => mapBackfillItem(collectionName, doc.id, doc.data() || {}));
  const allowedItems = [];
  const skippedLocalProjectIds = {};
  for (const item of items) {
    if (isLocallyAllowedSourceProject(item)) {
      allowedItems.push(item);
    } else {
      const key = item.sourceProjectId || "_missing";
      skippedLocalProjectIds[key] = (skippedLocalProjectIds[key] || 0) + 1;
    }
  }
  const result = {
    source: collectionName,
    scanned: snap.size,
    skippedLocal: items.length - allowedItems.length,
    skippedLocalProjectIds,
    sent: 0,
    failed: 0,
    batches: [],
  };
  for (const batch of chunk(allowedItems, 100)) {
    const response = await sendTimeLeftDailyItemsBatch(batch, { throwOnError: true });
    result.sent += batch.length;
    result.failed += Array.isArray(response.errors) ? response.errors.length : 0;
    result.batches.push({
      count: batch.length,
      created: response.created || 0,
      updated: response.updated || 0,
      moved: response.moved || 0,
      needsDateReview: response.needsDateReview || 0,
      errors: Array.isArray(response.errors) ? response.errors.length : 0,
    });
  }
  return result;
}

exports.syncGridlineProjectRecordToTimeLeft = onDocumentWritten(
  {
    ...triggerOptions,
    document: "projects/{projectId}",
  },
  (event) => syncWrittenDocument(event, "projects", "projectId", mapProjectRecordToTimeLeft)
);

exports.syncGridlineReportToTimeLeft = onDocumentWritten(
  {
    ...triggerOptions,
    document: "dailyReports/{reportId}",
  },
  (event) => syncWrittenDocument(event, "dailyReports", "reportId", mapReportToTimeLeft)
);

exports.syncGridlineJournalEntryToTimeLeft = onDocumentWritten(
  {
    ...triggerOptions,
    document: "logEntries/{entryId}",
  },
  (event) => syncWrittenDocument(event, "logEntries", "entryId", mapJournalEntryToTimeLeft)
);

exports.syncGridlineFileToTimeLeft = onDocumentWritten(
  {
    ...triggerOptions,
    document: "media/{mediaId}",
  },
  (event) => syncWrittenDocument(event, "media", "mediaId", mapMediaToTimeLeft)
);

exports.backfillGridlineTimeLeft = onRequest(
  {
    region: "northamerica-northeast1",
    invoker: "public",
    timeoutSeconds: 300,
    memory: "512MiB",
    secrets: [TIME_LEFT_INGESTION_TOKEN],
  },
  async (req, res) => {
    if (req.method !== "POST") {
      res.status(405).set("Allow", "POST").json({ ok: false, error: "Use POST." });
      return;
    }
    try {
      assertBackfillAuth(req);
      const source = String(req.query.source || req.body?.source || "all");
      const limit = Math.max(1, Math.min(1000, Number(req.query.limit || req.body?.limit || 500) || 500));
      const names = source === "all" ? Object.keys(BACKFILL_SOURCES) : [source];
      for (const name of names) {
        if (!BACKFILL_SOURCES[name]) {
          res.status(400).json({ ok: false, error: `Unknown source: ${name}` });
          return;
        }
      }
      const results = [];
      for (const name of names) results.push(await backfillCollection(name, limit));
      res.json({ ok: true, limit, results });
    } catch (error) {
      logger.warn("timeLeft gridlineai backfill failed", {
        status: error.status || null,
        message: String(error.message || error).slice(0, 300),
      });
      res.status(error.status || 500).json({ ok: false, error: error.message || "Backfill failed." });
    }
  }
);

exports.timeLeftGridlineMediaDownload = onRequest(
  {
    region: "northamerica-northeast1",
    invoker: "public",
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (req, res) => {
    if (req.method !== "GET") {
      res.status(405).set("Allow", "GET").send("Method Not Allowed");
      return;
    }

    const mediaId = String(req.query && req.query.m ? req.query.m : "").trim();
    const requestedPath = String(req.query && req.query.p ? req.query.p : "").trim();
    if (!mediaId || !requestedPath) {
      res.status(400).type("text/plain").send("Missing media id or storage path.");
      return;
    }

    try {
      const mediaSnap = await admin.firestore().collection("media").doc(mediaId).get();
      if (!mediaSnap.exists) {
        res.status(404).type("text/plain").send("Media not found.");
        return;
      }
      const media = mediaSnap.data() || {};
      const storagePath = String(media.storagePath || "").trim();
      if (!storagePath || storagePath !== requestedPath) {
        res.status(403).type("text/plain").send("Media path is not valid for this item.");
        return;
      }

      const file = admin.storage().bucket().file(storagePath);
      const [exists] = await file.exists();
      if (!exists) {
        res.status(404).type("text/plain").send("File not found.");
        return;
      }
      const fileName = String(media.fileName || storagePath.split("/").pop() || "media").replace(/"/g, "");
      const [url] = await file.getSignedUrl({
        action: "read",
        expires: Date.now() + 1000 * 60 * 5,
        responseDisposition: `inline; filename="${fileName}"`,
      });
      res.redirect(302, url);
    } catch (error) {
      logger.warn("timeLeft gridlineai media download failed", {
        mediaId,
        storagePath: requestedPath,
        message: String(error.message || error).slice(0, 300),
      });
      res.status(500).type("text/plain").send("Could not open media.");
    }
  }
);
