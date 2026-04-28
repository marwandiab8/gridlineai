const { FieldValue } = require("firebase-admin/firestore");

function normalizeProjectScope(projectSlug) {
  const slug = String(projectSlug || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug || "_unassigned";
}

function snapshotDocId(storagePath) {
  return encodeURIComponent(String(storagePath || "").trim());
}

function sanitizeTask(task) {
  return {
    rowNumber: Number(task && task.rowNumber) || 0,
    section: task && task.section ? String(task.section).trim() : null,
    activity: task && task.activity ? String(task.activity).trim() : "",
    actionBy: task && task.actionBy ? String(task.actionBy).trim() : null,
    durationDays: Number.isFinite(Number(task && task.durationDays)) ? Number(task.durationDays) : null,
    durationText: task && task.durationText ? String(task.durationText).trim() : null,
    startDate: task && task.startDate ? String(task.startDate).trim() : null,
    finishDate: task && task.finishDate ? String(task.finishDate).trim() : null,
    hidden: task && task.hidden === true,
    completed: task && task.completed === true,
    scheduledDateKeys: Array.isArray(task && task.scheduledDateKeys)
      ? task.scheduledDateKeys.map((value) => String(value).trim()).filter(Boolean)
      : [],
  };
}

async function saveLookaheadSnapshot({
  db,
  phoneE164,
  projectSlug,
  projectName,
  storagePath,
  parsed,
}) {
  const scope = normalizeProjectScope(projectSlug);
  const ref = db
    .collection("projects")
    .doc(scope)
    .collection("lookaheadSchedules")
    .doc(snapshotDocId(storagePath));

  const tasks = Array.isArray(parsed && parsed.tasks) ? parsed.tasks.map(sanitizeTask) : [];
  await ref.set(
    {
      phoneE164: String(phoneE164 || "").trim() || null,
      projectSlug: scope,
      projectName: String(projectName || "").trim() || null,
      storagePath: String(storagePath || "").trim() || null,
      fileName: parsed && parsed.fileName ? String(parsed.fileName).trim() : null,
      window: {
        startDateKey:
          parsed && parsed.window && parsed.window.startDateKey
            ? String(parsed.window.startDateKey).trim()
            : null,
        endDateKey:
          parsed && parsed.window && parsed.window.endDateKey
            ? String(parsed.window.endDateKey).trim()
            : null,
      },
      taskCount: tasks.length,
      tasks,
      updatedAt: FieldValue.serverTimestamp(),
      createdAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
  return { snapshotRef: ref, projectScope: scope };
}

async function loadPreviousLookaheadSnapshot({
  db,
  projectSlug,
  excludeStoragePath,
}) {
  const scope = normalizeProjectScope(projectSlug);
  const snap = await db
    .collection("projects")
    .doc(scope)
    .collection("lookaheadSchedules")
    .orderBy("updatedAt", "desc")
    .limit(10)
    .get();

  const excluded = String(excludeStoragePath || "").trim();
  for (const doc of snap.docs) {
    const data = doc.data() || {};
    if (String(data.storagePath || "").trim() === excluded) continue;
    return {
      id: doc.id,
      ...data,
    };
  }
  return null;
}

module.exports = {
  saveLookaheadSnapshot,
  loadPreviousLookaheadSnapshot,
  normalizeProjectScope,
};
