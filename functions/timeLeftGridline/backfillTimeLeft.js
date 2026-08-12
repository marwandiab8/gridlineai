#!/usr/bin/env node
const admin = require("firebase-admin");
const {
  mapJournalEntryToTimeLeft,
  mapMediaToTimeLeft,
  mapProjectRecordToTimeLeft,
  mapReportToTimeLeft,
} = require("./mappers");
const { sendTimeLeftDailyItemsBatch } = require("./ingestionClient");
const { assertFirebaseProjectId, isIosShortcutLegacyLogEntry } = require("./policy");

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
  const apply = Boolean(args.apply);
  const dryRun = !apply;
  const limit = Math.max(1, Number(args.limit || 50) || 50);
  const batchSize = Math.min(100, Math.max(1, Number(args.batchSize || 100) || 100));
  const appBaseUrl = String(process.env.GRIDLINE_APP_BASE_URL || "");
  const sourceFirebaseProjectId = assertFirebaseProjectId(
    process.env.GRIDLINE_FIREBASE_PROJECT_ID || "gridlineai"
  );
  const timeZone = String(process.env.GRIDLINE_DEFAULT_TIME_ZONE || "America/Toronto");
  const startDate = String(args.startDate || args["start-date"] || args.date || "").trim();
  const endDate = String(args.endDate || args["end-date"] || args.date || startDate).trim();
  if (apply && (!startDate || !endDate)) {
    throw new Error("--apply requires --date or both --start-date and --end-date.");
  }

  const names = source === "all" ? Object.keys(COLLECTIONS) : [source];
  const db = admin.firestore();
  const totals = { scanned: 0, mapped: 0, eligible: 0, skippedShortcut: 0, skippedDate: 0, missingDate: 0, sent: 0, failed: 0 };

  for (const collectionName of names) {
    const mapper = COLLECTIONS[collectionName];
    if (!mapper) throw new Error(`Unknown source: ${collectionName}`);
    const rows = await loadCollection(db, collectionName, limit);
    totals.scanned += rows.length;
    const sourceRows = rows.filter(({ data }) => {
      const skip = collectionName === "logEntries" && isIosShortcutLegacyLogEntry(data);
      if (skip) totals.skippedShortcut += 1;
      return !skip;
    });
    const items = sourceRows.map(({ id, data }) => mapper(data, {
      id,
      path: `${collectionName}/${id}`,
      appBaseUrl,
      sourceFirebaseProjectId,
      syncStatus: "active",
      timeZone,
    }));
    totals.mapped += items.length;
    totals.missingDate += items.filter((item) => !item.dateId).length;
    const eligible = items.filter((item) => {
      if (!startDate && !endDate) return true;
      return Boolean(item.dateId && item.dateId >= startDate && item.dateId <= endDate);
    });
    totals.eligible += eligible.length;
    totals.skippedDate += items.length - eligible.length;
    console.log(`[${collectionName}] scanned=${rows.length} eligible=${eligible.length} skippedShortcut=${rows.length - sourceRows.length} missingDate=${items.filter((item) => !item.dateId).length}`);
    if (dryRun || !eligible.length) continue;
    for (const batch of chunk(eligible, batchSize)) {
      const result = await sendTimeLeftDailyItemsBatch(batch, { throwOnError: true });
      totals.sent += batch.length;
      console.log(`[${collectionName}] sent batch=${batch.length} result=${JSON.stringify(result).slice(0, 500)}`);
    }
  }
  console.log(JSON.stringify({ dryRun, startDate: startDate || null, endDate: endDate || null, ...totals }, null, 2));
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
