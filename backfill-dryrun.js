
const admin = require("./functions/node_modules/firebase-admin");

admin.initializeApp();
const db = admin.firestore();

(async () => {
  const phoneE164 = "+14165189628";
  const projectSlug = "docksteader";
  const startKey = "2026-04-27";
  const endKey = "2026-04-27";
  const sourceMessageIds = new Set([
    "yq3q93dE9B4pGHcJhIbz",
    "UDK1qFUR60SK1yNX7vTS",
  ]);

  const dryRun = false; // change to false to execute
  const forceReassign = true;

  const logSnap = await db.collection("logEntries").where("senderPhone", "==", phoneE164).limit(5000).get();
  const logs = logSnap.docs.map(d => ({ id: d.id, ref: d.ref, ...d.data() })).filter(r => {
    const dk = String(r.reportDateKey || r.dateKey || "").trim();
    const sid = String(r.sourceMessageId || "").trim();
    const inDate = dk >= startKey && dk <= endKey;
    return inDate && (sourceMessageIds.size === 0 || sourceMessageIds.has(sid));
  });

  const rowsToUpdate = logs.filter(r => {
    const current = String(r.projectSlug || r.projectId || "").trim().toLowerCase();
    if (forceReassign) return current !== projectSlug;
    return !current || current === "_unassigned";
  });

  const changedLogIds = new Set(rowsToUpdate.map(r => r.id));

  const mediaSnap = await db.collection("media").where("senderPhone", "==", phoneE164).limit(5000).get();
  const mediaToUpdate = mediaSnap.docs.map(d => ({ id: d.id, ref: d.ref, ...d.data() })).filter(m => {
    const dk = String(m.reportDateKey || m.dateKey || "").trim();
    const sid = String(m.sourceMessageId || "").trim();
    const linked = String(m.linkedLogEntryId || "").trim();
    const inDate = dk >= startKey && dk <= endKey;
    const bySid = sourceMessageIds.size === 0 || sourceMessageIds.has(sid);
    const byLink = linked && changedLogIds.has(linked);
    if (!inDate || !(bySid || byLink)) return false;
    const current = String(m.projectId || m.projectSlug || "").trim().toLowerCase();
    if (forceReassign) return current !== projectSlug;
    return !current || current === "_unassigned";
  });

  console.log({
    dryRun,
    matchedLogEntries: logs.length,
    wouldUpdateLogEntries: rowsToUpdate.length,
    wouldUpdateMediaDocs: mediaToUpdate.length,
    sampleLogIds: rowsToUpdate.slice(0, 20).map(r => r.id),
    sampleMediaIds: mediaToUpdate.slice(0, 20).map(m => m.id),
  });

  if (!dryRun) {
    let batch = db.batch();
    let ops = 0;
    const commit = async () => {
      if (!ops) return;
      await batch.commit();
      batch = db.batch();
      ops = 0;
    };

    for (const r of rowsToUpdate) {
      batch.update(r.ref, {
        projectSlug,
        projectId: projectSlug,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      if (++ops >= 400) await commit();
    }

    for (const m of mediaToUpdate) {
      batch.update(m.ref, {
        projectId: projectSlug,
        projectSlug,
        includeInDailyReport: true,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      if (++ops >= 400) await commit();
    }

    await commit();
    console.log("Backfill applied.");
  }
})();

