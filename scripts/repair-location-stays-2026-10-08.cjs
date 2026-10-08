// One-off repair of two wrong "Log this place" departures (Oct 8, 2026). Dry run unless --apply.
// Run from functions/:   node ../scripts/repair-location-stays-2026-10-08.cjs            (shows the plan)
//                        node ../scripts/repair-location-stays-2026-10-08.cjs --apply    (backs up, then applies)
//  1. Raging Bull: left ~10:22 pm Oct 3 (estimated from arriving home at 10:45 pm, 19 km away), not 6:34 am Oct 8.
//  2. Quick Oil Change: the Oct 3 "245 h" departure duplicates the recorded 2:38 pm Sep 23 one - remove it
//     from Time Left (with a tombstone, as deleteActivityEntry does) and void it in gridlineai.
// Every touched document is written to /home/marwan/Documents/backups/location-repair-2026-10-08.json first.
const fs = require("fs");
const admin = require(process.cwd() + "/node_modules/firebase-admin");
const { departureFromEvidence } = require(process.cwd() + "/stayEvidence");
const { haversineMeters } = require(process.cwd() + "/placeLearning");
const { closedStayNote, formatClock } = require(process.cwd() + "/stayClosing");

const APPLY = process.argv.includes("--apply");
const g = admin.initializeApp({ projectId: "gridlineai" }, "g").firestore();
const t = admin.initializeApp({ projectId: "timelefttolive" }, "t").firestore();
const { Timestamp, FieldValue } = admin.firestore;
const CAL = "cvJjrQfCbzwjRU11zSvX";
const BACKUP = "/home/marwan/Documents/backups/location-repair-2026-10-08.json";
const ser = (o) =>
  JSON.parse(JSON.stringify(o, (k, v) => (v && v._seconds != null ? { __ts: new Date(v._seconds * 1000 + Math.round(v._nanoseconds / 1e6)).toISOString() } : v)));

(async () => {
  const lifeEvents = t.collection("lifeCalendars").doc(CAL).collection("lifeEvents");
  const refs = {
    rbEvent: g.collection("iosShortcutEvents").doc("VZeHBtuTw96mpOBPvegv"),
    rbLog: g.collection("logEntries").doc("pz8cXK6eTprNdVfNie1O"),
    rbPlace: g.collection("knownPlaces").doc("G8IVl9KbioBurxchhzk3"),
    rbLife: lifeEvents.doc("4c4f195057dcd6bf6c47461c22387d7ba16975c12a5475e24d99c7841341b7cb"),
    qocEvent: g.collection("iosShortcutEvents").doc("f9AC85JmAIh6N0ePV5oq"),
    qocLog: g.collection("logEntries").doc("sFpc2wMr7SMqUVHfoCIl"),
    qocPlace: g.collection("knownPlaces").doc("ShVExU5HTc5LXGOcemsc"),
    qocLife: lifeEvents.doc("7484a695e62c5a6b8aa197f7d4674cfe4ce211f999746fbd73a8b65bb7cde542"),
  };
  const docs = {};
  for (const [k, ref] of Object.entries(refs)) {
    const snap = await ref.get();
    if (!snap.exists) throw new Error(`${k} missing: ${ref.path}`);
    docs[k] = snap.data();
  }

  // Raging Bull's real departure, from the evidence the new code would have used: OwnTracks arrive_home.
  const place = docs.rbPlace;
  const startMs = place.lastVisitAt.toMillis();
  const home = { latitude: 43.7064, longitude: -80.3934, at: Date.parse("2026-10-04T02:45:48Z") };
  const distance = haversineMeters(home.latitude, home.longitude, place.latitude, place.longitude);
  const dep = departureFromEvidence({ startMs, evidenceMs: home.at, distanceMeters: distance });
  const leftAt = new Date(dep.leftMs);
  const minutes = Math.round((dep.leftMs - startMs) / 60000);
  const note = closedStayNote({ stay: { name: "Raging Bull", durationMinutes: minutes }, leftAt, basis: dep.basis, distanceMeters: distance, evidenceLabel: "home", timezone: "America/Toronto" });
  const clock = `${formatClock(leftAt, "America/Toronto").toUpperCase()} EDT`;
  const fix = (s) =>
    String(s)
      .replace("log note (2026-10-08)", "log note (2026-10-03)")
      .replace("Event time: 6:34 AM EDT", `Event time: ${clock}`)
      .replace(
        /Notes: Stayed 107 h 32 min at Raging Bull \(ended automatically when you arrived at GoodLife Newmarket\)\./,
        `Notes: ${note} [corrected Oct 8: was logged as 107 h 32 min when you arrived at GoodLife Newmarket].`
      );
  const qocDupNote =
    "Duplicate departure removed Oct 8: you left Quick Oil Change at 2:38 pm on Sep 23 (46 min stay); this 245 h entry was logged by mistake when you arrived at Raging Bull.";

  const plan = [
    ["update", refs.rbEvent, { eventAtIso: leftAt.toISOString(), eventAtMs: dep.leftMs, reportDateKey: "2026-10-03", notes: `${note} [corrected Oct 8]`, correctedAt: FieldValue.serverTimestamp(), correctedFrom: { eventAtIso: docs.rbEvent.eventAtIso, notes: docs.rbEvent.notes } }],
    ["update", refs.rbLog, { dateKey: "2026-10-03", reportDateKey: "2026-10-03", rawText: fix(docs.rbLog.rawText), normalizedText: fix(docs.rbLog.normalizedText), correctedAt: FieldValue.serverTimestamp() }],
    ["update", refs.rbPlace, { lastLeftAt: Timestamp.fromDate(leftAt), lastVisitDurationMinutes: minutes, totalMinutesSpent: minutes, timedVisitCount: 1, lastLeaveEstimated: true, updatedAt: FieldValue.serverTimestamp() }],
    ["update", refs.rbLife, { occurredAt: Timestamp.fromDate(leftAt), metadata: { ...(docs.rbLife.metadata || {}), reportDateKey: "2026-10-03", estimated: true, correctedFrom: docs.rbLife.occurredAt.toDate().toISOString() }, manualOverride: true, updatedBy: "repair-2026-10-08", updatedAt: FieldValue.serverTimestamp() }],
    ["update", refs.qocEvent, { status: "voided", voidReason: qocDupNote, correctedAt: FieldValue.serverTimestamp() }],
    ["update", refs.qocLog, { includeInDailySummary: false, rawText: `${docs.qocLog.rawText} [${qocDupNote}]`, normalizedText: `${docs.qocLog.normalizedText} [${qocDupNote}]`, correctedAt: FieldValue.serverTimestamp() }],
    ["update", refs.qocPlace, { lastLeftAt: Timestamp.fromDate(new Date("2026-09-23T18:38:00Z")), lastVisitDurationMinutes: 46, totalMinutesSpent: 46, timedVisitCount: 1, lastLeaveEstimated: false, updatedAt: FieldValue.serverTimestamp() }],
    ["tombstone+delete", refs.qocLife, null],
  ];

  console.log(`Raging Bull: arrived ${new Date(startMs).toISOString()}, left ${leftAt.toISOString()} (${dep.basis}, ${Math.round(distance)} m), ${minutes} min`);
  console.log("note:", note);
  for (const [op, ref, data] of plan) console.log(op.padEnd(17), ref.path, data ? JSON.stringify(ser(data)).slice(0, 300) : "");
  if (!APPLY) {
    console.log("\nDry run - nothing changed. Re-run with --apply.");
    return;
  }

  fs.writeFileSync(BACKUP, JSON.stringify(Object.fromEntries(Object.entries(refs).map(([k, r]) => [r.path, ser(docs[k])])), null, 2));
  console.log("backup written:", BACKUP);
  for (const [op, ref, data] of plan) {
    if (op === "update") await ref.update(data);
    else {
      const d = docs.qocLife;
      await t.collection("lifeCalendars").doc(CAL).collection("lifeEventTombstones").doc(ref.id).set(
        {
          eventId: ref.id,
          idempotencyKey: d.idempotencyKey || ref.id,
          sourceApp: d.sourceApp || "",
          sourceRecordId: d.sourceRecordId || "",
          sourceEventId: d.sourceEventId || "",
          deletedAt: FieldValue.serverTimestamp(),
          deletedBy: "repair-2026-10-08",
          deletedByUid: "repair-2026-10-08",
        },
        { merge: true }
      );
      await ref.delete();
    }
    console.log("done:", op, ref.path);
  }
})()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("ERR", e.message);
    process.exit(1);
  });
