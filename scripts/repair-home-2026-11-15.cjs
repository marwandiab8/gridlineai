// One-off repair of Friday Nov 13, 2026 (evening). Dry run unless --apply.
// Run from functions/:   node ../scripts/repair-home-2026-11-15.cjs            (shows the plan)
//                        node ../scripts/repair-home-2026-11-15.cjs --apply    (backs up, then applies)
//
// What happened: Marwan left home at 6:58 pm, dropped off at Anytime Fitness Fergus, and the OwnTracks drive
// finished at 7:34 pm at the Raging Bull (Guelph, 20 m from the saved Known Place). He left there at 9:57 pm
// (the drive that started there), picked up at Anytime Fitness at 10:13 pm, and OwnTracks recorded him arriving
// home at 10:17:49 pm. The pub visit itself was never logged ("Log this place" was not run and OwnTracks has no
// region for it), so only the drives appeared. On Nov 15 at 3:19 pm the arrival home was deleted from the Time
// Left dashboard, which left a tombstone: from then on Home was missing from Friday night onwards and GridlineAI
// could not re-send the arrival (Time Left answers "deleted" for a tombstoned key).
//
// This script:
//  1. Time Left: removes that tombstone and re-creates the arrive_home life event from GridlineAI's own record
//     (iosShortcutEvents/0dNF0AFKvS5IDbuo9NoO), through Time Left's normal normalize + upsert code. The
//     idempotency key and content hash must come out identical to the original delivery, or nothing is written.
//  2. GridlineAI + Time Left: adds the Raging Bull visit as the "Log this place" flow would have: arrive_location
//     at 7:34:19 pm and leave_location at 9:57:10 pm with the stay, activity Darts (the Known Place's activity),
//     and updates the Known Place's visit counters. No dashboard log notes are written (the assistant is not run).
// Every touched document is written to /home/marwan/Documents/backups/home-repair-2026-11-15.json first.
//
// Afterwards (same day): writing the two iosShortcutEvents fired GridlineAI's syncIosShortcutEventToTimeLeft create
// trigger, which re-sent them and got HTTP 409 (idempotency_conflict). That trigger and retryIosShortcutEventsToTimeLeft
// were last deployed on 2026-10-08 and still run the mapper without the Oct 11 "activity at place" block (title
// "<activity> at <place>", startAt/endAt on the departure), so their payload hashes differ from the inline delivery's
// for every place visit. Time Left keeps the first (correct) record; the two delivery states were set back to
// "delivered". Redeploying those two functions from current main removes the mismatch.
const fs = require("fs");
const gAdmin = require(process.cwd() + "/node_modules/firebase-admin");
const tlAdmin = require("/home/marwan/Documents/timelefttolive/functions/node_modules/firebase-admin");
const { mapShortcutEventToTimeLeftLifeEvent } = require(process.cwd() + "/timeLeftLifeEventMapper");
const { buildVisitBody, closedStayNote } = require(process.cwd() + "/stayClosing");
const { parseShortcutEventPayload } = require(process.cwd() + "/iosShortcutsIntegration");
const { haversineMeters } = require(process.cwd() + "/placeLearning");
const { upsertTimeLeftDeliveryState } = require(process.cwd() + "/timeLeftDeliveryRepository");
const { normalizeLifeEventRecord, upsertLifeEventRecord } = require("/home/marwan/Documents/timelefttolive/functions/src/ingestion/lifeEventFoundation");

const APPLY = process.argv.includes("--apply");
const g = gAdmin.initializeApp({ projectId: "gridlineai" }, "g").firestore();
const t = tlAdmin.initializeApp({ projectId: "timelefttolive" }, "t").firestore();
const GField = gAdmin.firestore.FieldValue;
const GTimestamp = gAdmin.firestore.Timestamp;
const TField = tlAdmin.firestore.FieldValue;

const CAL = "cvJjrQfCbzwjRU11zSvX";
const CONNECTION = "E0zb3yWAJtWpKk57vY1d";
const OWNER = "lIFgBluamLU69QHeSAaSRJMGbx52";
const MEMBER = "marwandiab8@gmail.com";
const TZ = "America/Toronto";
const REPAIR = "repair-2026-11-15";
const BACKUP = "/home/marwan/Documents/backups/home-repair-2026-11-15.json";
const ARRIVE_HOME_EVENT = "0dNF0AFKvS5IDbuo9NoO";
const ARRIVE_HOME_KEY = "a15622467e7cb3ac6f97f33e5f1476b705c4bc3c97645e75fc138ef08471fd5a";
const RAGING_BULL_PLACE = "G8IVl9KbioBurxchhzk3";
const ARRIVED = new Date("2026-11-14T00:34:19Z"); // finish_drive at the pub, iosShortcutEvents/69e75c1a51d582b3c9036f
const LEFT = new Date("2026-11-14T02:57:10Z"); // start_drive from the pub, iosShortcutEvents/5a047f81eef0ef29c2621f
const DRIVE_END = { latitude: 43.546936, longitude: -80.296852 };

const ser = (o) => JSON.parse(JSON.stringify(o, (k, v) => (v && typeof v.toDate === "function" ? v.toDate().toISOString() : v && v._seconds != null ? new Date(v._seconds * 1000).toISOString() : v)));
const brief = (p) => JSON.stringify(ser({ eventType: p.eventType, title: p.title, occurredAt: p.occurredAt, startAt: p.startAt, endAt: p.endAt, durationSeconds: p.durationSeconds, location: p.location, metadata: p.metadata, sourceRecordId: p.sourceRecordId, idempotencyKey: p.idempotencyKey }));

(async () => {
  const cal = t.collection("lifeCalendars").doc(CAL);
  const lifeEvents = cal.collection("lifeEvents");
  const tombstoneRef = cal.collection("lifeEventTombstones").doc(ARRIVE_HOME_KEY);
  const rawRef = cal.collection("rawIngestionPayloads").doc(ARRIVE_HOME_KEY);
  const placeRef = g.collection("knownPlaces").doc(RAGING_BULL_PLACE);
  const homeEventRef = g.collection("iosShortcutEvents").doc(ARRIVE_HOME_EVENT);

  const [connection, tombstone, existingLife, raw, place, homeEvent] = await Promise.all([
    cal.collection("sourceConnections").doc(CONNECTION).get(), tombstoneRef.get(), lifeEvents.doc(ARRIVE_HOME_KEY).get(), rawRef.get(), placeRef.get(), homeEventRef.get(),
  ]);
  if (!connection.exists) throw new Error("source connection missing");
  if (!tombstone.exists) throw new Error("no tombstone for the arrival: has it been restored already?");
  if (existingLife.exists) throw new Error("the arrival life event already exists");
  if (!place.exists) throw new Error("Raging Bull Known Place missing");
  if (!homeEvent.exists) throw new Error("GridlineAI arrive_home record missing");
  const context = { calendarId: CAL, connectionId: CONNECTION, integrationId: CONNECTION, timeLeftUserId: OWNER, connection: connection.data() };

  // 1. The arrival home, rebuilt from GridlineAI's record exactly as it was delivered on the night.
  const timeLeftPayloadFor = (eventDoc, id) => {
    const mapped = mapShortcutEventToTimeLeftLifeEvent({ ...eventDoc, id });
    if (!mapped.ok) throw new Error(`mapper refused ${id}: ${mapped.status}`);
    return normalizeLifeEventRecord(JSON.parse(JSON.stringify(mapped.event)), context); // as it travels over HTTP
  };
  const arrival = timeLeftPayloadFor(homeEvent.data(), ARRIVE_HOME_EVENT);
  if (arrival.idempotencyKey !== ARRIVE_HOME_KEY) throw new Error(`rebuilt key ${arrival.idempotencyKey} differs from the deleted event's`);
  const originalHash = raw.exists ? raw.data().payloadHash : null;
  if (originalHash && arrival.contentHash !== originalHash) throw new Error(`rebuilt content hash ${arrival.contentHash} differs from the original delivery ${originalHash}`);
  console.log(`arrival home: key and content hash match the original delivery (${originalHash ? "hash verified" : "no raw audit record to compare"})`);
  console.log("  ", brief(arrival));

  // 2. The Raging Bull visit, as "Log this place" + the stay closing would have logged it.
  const p = place.data();
  const distance = haversineMeters(DRIVE_END.latitude, DRIVE_END.longitude, p.latitude, p.longitude);
  if (distance > (p.radiusMeters || 120)) throw new Error(`the drive ended ${Math.round(distance)} m from the place`);
  const minutes = Math.round((LEFT - ARRIVED) / 60000);
  const leaveNote = `${closedStayNote({ stay: { name: p.name, durationMinutes: minutes }, leftAt: LEFT, basis: "drive", distanceMeters: 0, evidenceLabel: null, timezone: TZ })} [added Nov 15: the visit was not logged on the night; times from the OwnTracks drives]`;
  const arriveNote = "Arrived when the drive from Anytime Fitness Fergus finished here [added Nov 15: the visit was not logged on the night; time from the OwnTracks drive]";
  const common = { latitude: p.latitude, longitude: p.longitude, name: p.name, timezone: TZ, deviceName: null, activity: p.activity };
  const parsedArrive = parseShortcutEventPayload({ ...buildVisitBody({ ...common, eventType: "arrive_location", timestamp: ARRIVED.toISOString(), notes: arriveNote }), source: REPAIR });
  const parsedLeave = parseShortcutEventPayload({ ...buildVisitBody({ ...common, eventType: "leave_location", timestamp: LEFT.toISOString(), notes: leaveNote, stayStartedAt: ARRIVED.toISOString() }), source: REPAIR });
  if (!parsedArrive.ok || !parsedLeave.ok) throw new Error("visit payloads rejected: " + JSON.stringify([parsedArrive, parsedLeave]));

  // Nothing of this visit may exist already.
  const sameNight = await g.collection("iosShortcutEvents").where("eventAtMs", ">=", Date.parse("2026-11-13T22:00:00Z")).where("eventAtMs", "<", Date.parse("2026-11-14T05:00:00Z")).get();
  const visits = sameNight.docs.map((d) => d.data()).filter((e) => /location$/.test(e.eventType) && /raging/i.test(e.locationLabel || ""));
  if (visits.length) throw new Error("a Raging Bull visit is already recorded that night: " + JSON.stringify(ser(visits)));
  console.log(`that night GridlineAI has ${sameNight.size} events and no Raging Bull visit; the drive ended ${Math.round(distance)} m from the place`);

  const arriveRef = g.collection("iosShortcutEvents").doc();
  const leaveRef = g.collection("iosShortcutEvents").doc();
  const shortcutEventDoc = (event, basis) => ({
    status: "recorded", memberEmail: MEMBER, phoneE164: homeEvent.data().phoneE164, eventType: event.eventType, eventAtIso: event.eventAtIso, eventAtMs: event.eventAtMs,
    reportDateKey: event.reportDateKey, projectSlug: "home", timezone: event.timezone, locationLabel: event.locationLabel || null, source: event.source, deviceName: event.deviceName || null,
    latitude: event.latitude, longitude: event.longitude, notes: event.notes || null, placeActivity: event.placeActivity || null, stayStartedAtIso: event.stayStartedAtIso || null,
    idempotencyKey: null, logEntryId: null, inboundMessageId: null, outboundMessageId: null, assistantCommand: null,
    repair: { by: REPAIR, reason: "Raging Bull darts visit of Nov 13 was never logged; added from the OwnTracks drives", basis },
  });
  const arriveDoc = shortcutEventDoc(parsedArrive.event, "finish_drive 69e75c1a51d582b3c9036f at 2026-11-14T00:34:19Z, 20 m from the place");
  const leaveDoc = shortcutEventDoc(parsedLeave.event, "start_drive 5a047f81eef0ef29c2621f at 2026-11-14T02:57:10Z");
  const arrivePayload = timeLeftPayloadFor(arriveDoc, arriveRef.id);
  const leavePayload = timeLeftPayloadFor(leaveDoc, leaveRef.id);
  console.log("visit arrive:", brief(arrivePayload));
  console.log("visit leave: ", brief(leavePayload));
  const placeUpdate = {
    visitCount: (Number.isFinite(p.visitCount) ? p.visitCount : 0) + 1, timedVisitCount: (Number.isFinite(p.timedVisitCount) ? p.timedVisitCount : 0) + 1,
    lastVisitAt: GTimestamp.fromDate(ARRIVED), lastLeftAt: GTimestamp.fromDate(LEFT), lastVisitDurationMinutes: minutes,
    totalMinutesSpent: (Number.isFinite(p.totalMinutesSpent) ? p.totalMinutesSpent : 0) + minutes, lastLeaveEstimated: false,
  };
  console.log("known place update:", JSON.stringify(ser(placeUpdate)));
  console.log("new GridlineAI events:", arriveRef.path, leaveRef.path);
  console.log("tombstone to remove:", tombstoneRef.path, JSON.stringify(ser(tombstone.data())));

  if (!APPLY) { console.log("\nDry run - nothing changed. Re-run with --apply."); return; }

  fs.writeFileSync(BACKUP, JSON.stringify({ [tombstoneRef.path]: ser(tombstone.data()), [rawRef.path]: raw.exists ? ser(raw.data()) : null, [placeRef.path]: ser(p), [homeEventRef.path]: ser(homeEvent.data()) }, null, 2));
  console.log("backup written:", BACKUP);

  await tombstoneRef.delete();
  console.log("done: tombstone removed");
  const r1 = await upsertLifeEventRecord(t, arrival);
  if (r1.status !== "created") throw new Error("arrival not created: " + JSON.stringify(r1));
  await lifeEvents.doc(r1.lifeEventId).update({ updatedBy: REPAIR, restoredAt: TField.serverTimestamp(), restoredFrom: "deleted from the Time Left dashboard on 2026-11-15T20:19:20Z" });
  console.log("done: arrival home restored", r1.lifeEventId);

  await arriveRef.set({ ...arriveDoc, createdAt: GField.serverTimestamp(), updatedAt: GField.serverTimestamp() });
  await leaveRef.set({ ...leaveDoc, createdAt: GField.serverTimestamp(), updatedAt: GField.serverTimestamp() });
  console.log("done: GridlineAI visit events written");
  for (const [ref, payload] of [[arriveRef, arrivePayload], [leaveRef, leavePayload]]) {
    const r = await upsertLifeEventRecord(t, payload);
    if (r.status !== "created") throw new Error(`${ref.id} not created in Time Left: ` + JSON.stringify(r));
    await lifeEvents.doc(r.lifeEventId).update({ updatedBy: REPAIR });
    const d = await upsertTimeLeftDeliveryState(g, GField, ref.id, { status: "delivered", mode: "production", targetProjectId: "timelefttolive", lifeEventId: r.lifeEventId, idempotencyKey: r.idempotencyKey, retryable: false });
    console.log("done: Time Left", payload.eventType, r.lifeEventId, "delivery state", d.ok ? "recorded" : d.error);
  }
  await placeRef.set({ ...placeUpdate, updatedAt: GField.serverTimestamp() }, { merge: true });
  console.log("done: Known Place updated");

  const check = await lifeEvents.where("occurredAt", ">=", new Date("2026-11-13T23:00:00Z")).where("occurredAt", "<", new Date("2026-11-14T05:00:00Z")).orderBy("occurredAt").get();
  check.docs.map((d) => d.data()).filter((e) => e.eventType !== "journal").forEach((e) => console.log("  now:", e.occurredAt.toDate().toISOString(), e.eventType, "|", e.title, "|", e.startAt ? `${e.startAt.toDate().toISOString()} -> ${e.endAt.toDate().toISOString()}` : "", e.updatedBy || ""));
})()
  .then(() => process.exit(0))
  .catch((e) => { console.error("ERR", e.message); process.exit(1); });
