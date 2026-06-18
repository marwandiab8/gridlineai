#!/usr/bin/env node
const admin = require("firebase-admin");
const {
  mapJournalEntryToTimeLeft,
  mapMediaToTimeLeft,
  mapProjectRecordToTimeLeft,
  mapReportToTimeLeft,
} = require("./mappers");
const { sendTimeLeftDailyItemsBatch } = require("./ingestionClient");

if (!admin.apps.length) admin.initializeApp();

const COLLECTIONS = {
  projects: mapProjectRecordToTimeLeft,
  dailyReports: mapReportToTimeLeft,
  logEntries: mapJournalEntryToTimeLeft,
  media: mapMediaToTimeLeft,
};

function parseArgs(argv) {
  const args = {};
  for (const raw of argv.slice(2)) {
    if (!raw.startsWith("--")) continue;
    const [key, value] = raw.slice(2).split("=");
    args[key] = value == null ? true : value;
  }
  return args;
}

function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

async function loadCollection(db, collectionName, limit) {
  let query = db.collection(collectionName)
    .orderBy(admin.firestore.FieldPath.documentId())
    .limit(Math.min(limit || 100, 100));
  let scanned = 0;
  const rows = [];
  let cursor = null;
  while (scanned < limit) {
    let page = query;
    if (cursor) page = page.startAfter(cursor);
    const snap = await page.get();
    if (snap.empty) break;
    for (const doc of snap.docs) {
      rows.push({ id: doc.id, data: doc.data() || {} });
      scanned += 1;
      if (scanned >= limit) break;
    }
    cursor = snap.docs[snap.docs.length - 1];
    if (snap.docs.length < Math.min(limit || 100, 100)) break;
  }
  return rows;
}

async function main() {
  const args = parseArgs(process.argv);
  const source = String(args.source || "all");
  const dryRun = Boolean(args.dryRun || args["dry-run"]);
  const limit = Math.max(1, Number(args.limit || 50) || 50);
  const batchSize = Math.min(100, Math.max(1, Number(args.batchSize || 100) || 100));
  const appBaseUrl = String(process.env.GRIDLINE_APP_BASE_URL || "");
  const sourceFirebaseProjectId = String(process.env.GRIDLINE_FIREBASE_PROJECT_ID || "gridlineai");
  const timeZone = String(process.env.GRIDLINE_DEFAULT_TIME_ZONE || "America/Toronto");

  const names = source === "all" ? Object.keys(COLLECTIONS) : [source];
  const db = admin.firestore();
  const totals = { scanned: 0, mapped: 0, missingDate: 0, sent: 0, failed: 0 };

  for (const collectionName of names) {
    const mapper = COLLECTIONS[collectionName];
    if (!mapper) throw new Error(`Unknown source: ${collectionName}`);
    const rows = await loadCollection(db, collectionName, limit);
    totals.scanned += rows.length;
    const items = rows.map(({ id, data }) => mapper(data, {
      id,
      path: `${collectionName}/${id}`,
      appBaseUrl,
      sourceFirebaseProjectId,
      syncStatus: "active",
      timeZone,
    }));
    totals.mapped += items.length;
    totals.missingDate += items.filter((item) => !item.dateId).length;
    console.log(`[${collectionName}] scanned=${rows.length} mapped=${items.length} missingDate=${items.filter((item) => !item.dateId).length}`);
    if (dryRun || !items.length) continue;
    for (const batch of chunk(items, batchSize)) {
      const result = await sendTimeLeftDailyItemsBatch(batch, { throwOnError: true });
      totals.sent += batch.length;
      console.log(`[${collectionName}] sent batch=${batch.length} result=${JSON.stringify(result).slice(0, 500)}`);
    }
  }
  console.log(JSON.stringify(totals, null, 2));
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
