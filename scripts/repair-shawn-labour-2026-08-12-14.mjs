import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const admin = require("../functions/node_modules/firebase-admin");
const { labourHoursFromStoredValue } = require("../functions/labourRepository.js");

const PROJECT_ID = "gridlineai";
const LABOURER_NAME = "Shawn Jones";
const LABOURER_PHONE = "+12893385196";
const PROJECT_SLUG = "docksteader";
const EXPECTED = {
  "2026-08-12": {
    notes: "8.5 hours 1 hour safety tape and signs 6 hours building wall on exterior stair case 1.5 housekeeping",
    minutesWorked: 420,
    workOn: "1h safety tape and signs - 6h building wall on exterior stair case 1.5 housekeeping",
    correctedWorkOn: "1h safety tape and signs - 6h building wall on exterior stair case - 1.5h housekeeping",
  },
  "2026-08-14": {
    notes: "8.5 hours for 2026-08-14 - 6.5 hours for safety railing install and foam install on treads - 2 hours for housekeeping",
    minutesWorked: 1020,
    workOn: "8.5h for 2026-08-14 - 6.5h for safety railing install and foam install on treads - 2h for housekeeping",
    correctedWorkOn: "6.5h for safety railing install and foam install on treads - 2h for housekeeping",
  },
};

const canonical = (value) => String(value || "")
  .toLowerCase()
  .replace(/[^a-z0-9.]+/g, " ")
  .replace(/\s+/g, " ")
  .trim();
const displayValue = (value) => String(value || "").replace(/\s+/g, " ").trim();
const timestamp = (value) => {
  if (!value) return null;
  if (typeof value.toDate === "function") return value.toDate().toISOString();
  return String(value);
};
const hasOwn = (object, key) => Object.prototype.hasOwnProperty.call(object || {}, key);
const fail = (message) => { throw new Error(`GUARDED_REPAIR_ABORTED: ${message}`); };

function preview(id, data) {
  return {
    documentId: id,
    labourerName: data.labourerName || null,
    labourerPhone: data.labourerPhone || null,
    projectSlug: data.projectSlug || null,
    reportDateKey: data.reportDateKey || null,
    minutesWorked: Number(data.minutesWorked) || null,
    derivedHours: labourHoursFromStoredValue(data),
    workOn: displayValue(data.workOn),
    notes: displayValue(data.notes),
    hasLegacyHours: hasOwn(data, "hours"),
    source: data.source || null,
    createdAt: timestamp(data.createdAt),
    updatedAt: timestamp(data.updatedAt),
  };
}

async function findVerifiedCandidates(db, dateKey) {
  const expected = EXPECTED[dateKey];
  const snap = await db.collection("labourEntries")
    .where("labourerPhone", "==", LABOURER_PHONE)
    .where("projectSlug", "==", PROJECT_SLUG)
    .get();
  const candidates = snap.docs
    .filter((doc) => doc.data()?.reportDateKey === dateKey)
    .map((doc) => ({ id: doc.id, ref: doc.ref, data: doc.data() || {} }));
  if (candidates.length !== 1) fail(`${dateKey} has ${candidates.length} date-matched documents; refusing to guess.`);
  const candidate = candidates[0];
  const data = candidate.data;
  if (data.labourerName !== LABOURER_NAME) fail(`${dateKey} worker identity mismatch.`);
  if (data.labourerPhone !== LABOURER_PHONE) fail(`${dateKey} phone mismatch.`);
  if (data.projectSlug !== PROJECT_SLUG) fail(`${dateKey} project mismatch.`);
  if (canonical(data.notes) !== canonical(expected.notes)) fail(`${dateKey} notes fingerprint mismatch.`);
  if (Number(data.minutesWorked) !== expected.minutesWorked) fail(`${dateKey} expected ${expected.minutesWorked} minutes, found ${data.minutesWorked}.`);
  if (labourHoursFromStoredValue(data) !== expected.minutesWorked / 60) fail(`${dateKey} stored value does not resolve to the expected incorrect hours.`);
  if (canonical(data.workOn) !== canonical(expected.workOn)) fail(`${dateKey} workOn fingerprint mismatch.`);
  return candidate;
}

const apply = process.argv.includes("--apply");
admin.initializeApp({ projectId: PROJECT_ID });
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

try {
  const candidates = {};
  for (const dateKey of Object.keys(EXPECTED)) candidates[dateKey] = await findVerifiedCandidates(db, dateKey);
  console.log(JSON.stringify({
    mode: apply ? "apply" : "dry-run",
    projectId: PROJECT_ID,
    candidates: Object.fromEntries(Object.entries(candidates).map(([dateKey, candidate]) => [dateKey, preview(candidate.id, candidate.data)])),
  }, null, 2));
  if (!apply) {
    console.log("Dry run only. Re-run with --apply to perform the two guarded transaction updates.");
    process.exitCode = 0;
  } else {
    const transactionResult = await db.runTransaction(async (tx) => {
      const snapshots = {};
      for (const dateKey of Object.keys(EXPECTED)) {
        const candidate = candidates[dateKey];
        const snap = await tx.get(candidate.ref);
        if (!snap.exists || snap.id !== candidate.id) fail(`${dateKey} document changed or disappeared.`);
        const data = snap.data() || {};
        const expected = EXPECTED[dateKey];
        if (data.labourerName !== LABOURER_NAME || data.labourerPhone !== LABOURER_PHONE || data.projectSlug !== PROJECT_SLUG || data.reportDateKey !== dateKey) fail(`${dateKey} identity changed immediately before update.`);
        if (canonical(data.notes) !== canonical(expected.notes)) fail(`${dateKey} notes changed immediately before update.`);
        if (Number(data.minutesWorked) !== expected.minutesWorked || labourHoursFromStoredValue(data) !== expected.minutesWorked / 60) fail(`${dateKey} current hours changed immediately before update.`);
        if (canonical(data.workOn) !== canonical(expected.workOn)) fail(`${dateKey} workOn changed immediately before update.`);
        snapshots[dateKey] = { before: data, afterWorkOn: expected.correctedWorkOn };
      }
      for (const dateKey of Object.keys(EXPECTED)) {
        const candidate = candidates[dateKey];
        const expected = EXPECTED[dateKey];
        tx.update(candidate.ref, {
          minutesWorked: 510,
          hours: FieldValue.delete(),
          workOn: expected.correctedWorkOn,
          updatedAt: FieldValue.serverTimestamp(),
        });
      }
      return snapshots;
    });

    const verified = {};
    for (const dateKey of Object.keys(EXPECTED)) {
      const candidate = candidates[dateKey];
      const snap = await candidate.ref.get();
      if (!snap.exists) fail(`${dateKey} missing after update.`);
      const data = snap.data() || {};
      if (Number(data.minutesWorked) !== 510 || labourHoursFromStoredValue(data) !== 8.5 || hasOwn(data, "hours")) fail(`${dateKey} failed post-update value verification.`);
      if (canonical(data.notes) !== canonical(EXPECTED[dateKey].notes)) fail(`${dateKey} original notes were not preserved.`);
      if (data.labourerName !== LABOURER_NAME || data.labourerPhone !== LABOURER_PHONE || data.projectSlug !== PROJECT_SLUG || data.reportDateKey !== dateKey) fail(`${dateKey} identity changed after update.`);
      verified[dateKey] = preview(snap.id, data);
    }
    console.log(JSON.stringify({ mode: "applied", verified }, null, 2));
  }
} finally {
  await admin.app().delete();
}
