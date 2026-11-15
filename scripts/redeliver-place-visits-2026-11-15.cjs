// Re-sends the place visits that Time Left holds in the pre-Oct-11 shape (title "Arrived at location" /
// "Left location", no activity, no stay length) because the Shortcut delivery trigger that reached Time
// Left first was running the Oct 8 mapper (docs/time-left-gridlineai-sync.md). Dry run unless --apply.
// Run from functions/:   node ../scripts/redeliver-place-visits-2026-11-15.cjs            (shows the plan)
//                        node ../scripts/redeliver-place-visits-2026-11-15.cjs --apply    (backs up, then applies)
// For each such event: the GridlineAI record is mapped with the current mapper and normalised with Time
// Left's own code; when that differs from what Time Left holds, the old record is removed (no tombstone,
// so nothing blocks it) and the new one written, and the GridlineAI delivery state set to delivered.
// Ashley's parents' house (Oct 18) came from an OwnTracks region before region visits carried an activity:
// its two records get activity "Visit" and the departure gets the stay's start, as the region logic would
// set today, so the 4-hour visit counts.
const fs = require("fs");
const gAdmin = require(process.cwd() + "/node_modules/firebase-admin");
const tlAdmin = require("/home/marwan/Documents/timelefttolive/functions/node_modules/firebase-admin");
const { mapShortcutEventToTimeLeftLifeEvent } = require(process.cwd() + "/timeLeftLifeEventMapper");
const { upsertTimeLeftDeliveryState } = require(process.cwd() + "/timeLeftDeliveryRepository");
const { normalizeLifeEventRecord, upsertLifeEventRecord } = require("/home/marwan/Documents/timelefttolive/functions/src/ingestion/lifeEventFoundation");

const APPLY = process.argv.includes("--apply");
const g = gAdmin.initializeApp({ projectId: "gridlineai" }, "g").firestore();
const t = tlAdmin.initializeApp({ projectId: "timelefttolive" }, "t").firestore();
const GField = gAdmin.firestore.FieldValue;
const CAL = "cvJjrQfCbzwjRU11zSvX", CONNECTION = "E0zb3yWAJtWpKk57vY1d", OWNER = "lIFgBluamLU69QHeSAaSRJMGbx52";
const REPAIR = "redeliver-2026-11-15";
const BACKUP = "/home/marwan/Documents/backups/place-visits-redeliver-2026-11-15.json";
const SINCE = new Date("2026-10-11T00:00:00Z");
const ser = (o) => JSON.parse(JSON.stringify(o, (k, v) => (v && typeof v.toDate === "function" ? v.toDate().toISOString() : v && v._seconds != null ? new Date(v._seconds * 1000).toISOString() : v)));

(async () => {
  const cal = t.collection("lifeCalendars").doc(CAL);
  const lifeEvents = cal.collection("lifeEvents");
  const connection = (await cal.collection("sourceConnections").doc(CONNECTION).get()).data();
  const context = { calendarId: CAL, connectionId: CONNECTION, integrationId: CONNECTION, timeLeftUserId: OWNER, connection };

  const stale = [];
  for (const eventType of ["arrive_location", "leave_location"]) {
    const snap = await lifeEvents.where("eventType", "==", eventType).get();
    snap.forEach((doc) => {
      const d = doc.data();
      if (d.occurredAt.toDate() < SINCE) return;
      const md = d.metadata || {};
      if (!("placeActivity" in md) || (eventType === "leave_location" && !d.startAt)) stale.push({ id: doc.id, data: d });
    });
  }
  stale.sort((a, b) => a.data.occurredAt.toMillis() - b.data.occurredAt.toMillis());
  console.log(`${stale.length} place visit records in the old shape since Oct 11`);

  // The Oct 18 region visit: arrival and departure share the region name; the departure learns the arrival time.
  const byLabel = {};
  for (const s of stale) (byLabel[(s.data.location || {}).label] ||= []).push(s);
  const plan = [];
  const backup = {};
  for (const s of stale) {
    const gid = s.data.sourceRecordId;
    const gsnap = await g.collection("iosShortcutEvents").doc(gid).get();
    if (!gsnap.exists) { console.log("  skip, no GridlineAI record:", s.id.slice(0, 10), gid); continue; }
    const gdoc = gsnap.data();
    const patch = {};
    if (!gdoc.placeActivity && /parent/i.test(gdoc.locationLabel || "")) {
      patch.placeActivity = "Visit";
      if (gdoc.eventType === "leave_location") {
        const arrival = (byLabel[(s.data.location || {}).label] || []).find((x) => x.data.eventType === "arrive_location");
        if (arrival) patch.stayStartedAtIso = arrival.data.occurredAt.toDate().toISOString();
      }
      patch.repair = { by: REPAIR, reason: "region visit recorded before region visits carried the place's activity and stay" };
    }
    const mapped = mapShortcutEventToTimeLeftLifeEvent({ ...gdoc, ...patch, id: gid });
    if (!mapped.ok) { console.log("  skip, mapper refused:", gid, mapped.status); continue; }
    const normalized = normalizeLifeEventRecord(JSON.parse(JSON.stringify(mapped.event)), context);
    if (normalized.idempotencyKey !== s.id) throw new Error(`key mismatch for ${gid}: ${normalized.idempotencyKey} vs ${s.id}`);
    if (normalized.contentHash === s.data.contentHash) { console.log(`  same as before, nothing to do: ${s.data.occurredAt.toDate().toISOString().slice(0, 16)} ${s.data.title}`); continue; }
    plan.push({ s, gid, patch, normalized });
    backup[`timelefttolive:${lifeEvents.doc(s.id).path}`] = ser(s.data);
    backup[`gridlineai:iosShortcutEvents/${gid}`] = ser(gdoc);
    const dur = normalized.durationSeconds != null ? ` ${Math.round(normalized.durationSeconds / 60)} min` : "";
    console.log(`  ${s.data.occurredAt.toDate().toISOString().slice(0, 16)} ${s.data.eventType.padEnd(15)} "${s.data.title}" -> "${normalized.title}"${dur}${Object.keys(patch).length ? "  (GridlineAI record gets " + Object.keys(patch).filter((k) => k !== "repair").join(", ") + ")" : ""}`);
  }
  if (!APPLY) { console.log(`\nDry run - ${plan.length} to re-send. Re-run with --apply.`); return; }
  fs.writeFileSync(BACKUP, JSON.stringify(backup, null, 2));
  console.log("backup written:", BACKUP);
  for (const { s, gid, patch, normalized } of plan) {
    if (Object.keys(patch).length) await g.collection("iosShortcutEvents").doc(gid).set({ ...patch, updatedAt: GField.serverTimestamp() }, { merge: true });
    await lifeEvents.doc(s.id).delete();
    const r = await upsertLifeEventRecord(t, normalized);
    if (r.status !== "created") throw new Error(`${s.id} not re-created: ${JSON.stringify(r)}`);
    await lifeEvents.doc(r.lifeEventId).update({ updatedBy: REPAIR });
    const d = await upsertTimeLeftDeliveryState(g, GField, gid, { status: "delivered", mode: "production", targetProjectId: "timelefttolive", lifeEventId: r.lifeEventId, idempotencyKey: r.idempotencyKey, retryable: false, lastErrorCode: null, lastErrorSummary: `re-sent by ${REPAIR} after the trigger redeploy` });
    console.log("done:", s.data.occurredAt.toDate().toISOString().slice(0, 16), normalized.title, "| delivery", d.ok ? "delivered" : d.error);
  }
})().then(() => process.exit(0)).catch((e) => { console.error("ERR", e.message); process.exit(1); });
