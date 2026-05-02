/**
 * Firestore `media` collection + canonical Storage paths for Twilio MMS.
 * Deterministic paths: projects/{projectId}/media/{dateKey}/{messageSid}/image-{i}.ext
 *
 * Data integrity (daily PDF): unlinked media is not included on project match alone.
 * Optional `includeInDailyReport: true` on a doc forces inclusion when unlinked (manual override).
 */

const path = require("path");
const { FieldValue, Timestamp } = require("firebase-admin/firestore");
const {
  dateKeyEastern,
  startOfEasternDayForDateKey,
  addCalendarDaysToDateKey,
  extractExplicitReportDate,
} = require("./logClassifier");
const {
  fetchTwilioMediaBuffer,
  guessExtension,
  TwilioMediaFetchError,
} = require("./twilioMediaFetch");
const { normalizeProjectSlug } = require("./projectAccess");

function logMediaQueryErr(ctx, err) {
  try {
    require("firebase-functions").logger.error(ctx, {
      message: err && err.message,
      code: err && err.code,
    });
  } catch (_) {}
}

function sanitizeDiagForFirestore(d) {
  const o = {};
  for (const [k, v] of Object.entries(d || {})) {
    if (v !== undefined) o[k] = v;
  }
  return o;
}

const COL = "media";

function safePathSegment(s) {
  return String(s || "unknown").replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120);
}

function getMediaEffectiveDateKey(media) {
  const storedReportDate =
    media && typeof media.reportDateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(media.reportDateKey.trim())
      ? media.reportDateKey.trim()
      : null;
  if (storedReportDate) return storedReportDate;

  const parsed = extractExplicitReportDate(media && media.captionText ? media.captionText : "");
  if (parsed.reportDateKey) return parsed.reportDateKey;
  return media && typeof media.dateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(media.dateKey.trim())
    ? media.dateKey.trim()
    : null;
}

function dedupeMedia(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows || []) {
    const id = row && row.id ? String(row.id) : "";
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(row);
  }
  return out;
}

async function optimizeImageForStorage(buffer, contentType) {
  return { buffer, contentType };
}

/**
 * Download one Twilio media URL, upload to Storage, write `media` doc.
 * @returns {Promise<{ storagePath: string, downloadURL: string|null, fileName: string, mediaId: string, mimeType: string } | null>}
 */
async function saveOneInboundMedia({
  db,
  bucket,
  FieldValue,
  accountSid,
  authToken,
  mediaUrl,
  contentType,
  mediaIndex,
  messageSidTwilio,
  sourceMessageId,
  senderPhone,
  projectSlug,
  reportDateKey = null,
  captionText,
  linkedLogEntryId,
  fileStem = "image",
  sourceLabel = "sms",
  storageSource = "twilio_mms",
  issueCollection,
  issueId,
  uploadedByPhone,
  logger,
  runId,
}) {
  const projectId = projectSlug || "_unassigned";
  const dk =
    typeof reportDateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(reportDateKey.trim())
      ? reportDateKey.trim()
      : dateKeyEastern(new Date());
  const sid = safePathSegment(messageSidTwilio || "no-sid");
  const requestedType = contentType || "application/octet-stream";
  const ext = guessExtension(requestedType);
  let fileName = `${safePathSegment(fileStem || "image")}-${mediaIndex}.${ext}`;
  let storagePath = path.posix.join(
    "projects",
    safePathSegment(projectId),
    "media",
    dk,
    sid,
    fileName
  );

  let buf;
  try {
    buf = await fetchTwilioMediaBuffer(mediaUrl, accountSid, authToken, {
      logger,
      runId,
    });
  } catch (e) {
    const diag =
      e instanceof TwilioMediaFetchError && e.diagnostics
        ? { ...e.diagnostics }
        : {};
    logger.error("mediaRepository: download failed", {
      runId,
      mediaIndex,
      message: e.message,
      ...sanitizeDiagForFirestore(diag),
    });
    await db.collection(COL).add({
      projectId,
      dateKey: dk,
      reportDateKey: dk,
      senderPhone,
      sourceMessageId: sourceMessageId || null,
      messageSid: messageSidTwilio || null,
      twilioMediaUrl: mediaUrl,
      contentType: requestedType,
      storagePath: null,
      downloadURL: null,
      captionText: captionText || null,
      linkedLogEntryId: linkedLogEntryId || null,
      issueCollection: issueCollection || null,
      issueId: issueId || null,
      mediaIndex,
      downloadError: String(e.message).slice(0, 500),
      twilioFetchDiagnostics: sanitizeDiagForFirestore(diag),
      createdAt: FieldValue.serverTimestamp(),
      aiAnalyzed: false,
      aiError: null,
      aiTags: [],
      aiSummary: null,
    });
    return null;
  }

  if (!buf || !buf.length) {
    logger.warn("mediaRepository: empty buffer after download", {
      runId,
      mediaIndex,
      mediaUrl: (mediaUrl || "").slice(0, 120),
    });
    await db.collection(COL).add({
      projectId,
      dateKey: dk,
      reportDateKey: dk,
      senderPhone,
      sourceMessageId: sourceMessageId || null,
      messageSid: messageSidTwilio || null,
      twilioMediaUrl: mediaUrl,
      contentType: requestedType,
      storagePath: null,
      downloadURL: null,
      captionText: captionText || null,
      linkedLogEntryId: linkedLogEntryId || null,
      issueCollection: issueCollection || null,
      issueId: issueId || null,
      mediaIndex,
      downloadError: "empty response body from Twilio media URL",
      createdAt: FieldValue.serverTimestamp(),
      aiAnalyzed: false,
      aiError: null,
      aiTags: [],
      aiSummary: null,
    });
    return null;
  }

  const optimized = await optimizeImageForStorage(buf, requestedType);
  const finalContentType = optimized.contentType || requestedType;
  const finalExt = guessExtension(finalContentType);
  if (finalExt !== ext) {
    fileName = `${safePathSegment(fileStem || "image")}-${mediaIndex}.${finalExt}`;
    storagePath = path.posix.join(
      "projects",
      safePathSegment(projectId),
      "media",
      dk,
      sid,
      fileName
    );
  }
  const file = bucket.file(storagePath);
  try {
    await file.save(optimized.buffer, {
      metadata: {
        contentType: finalContentType,
          metadata: {
          source: storageSource,
          messageSid: messageSidTwilio || "",
          sourceMessageId: sourceMessageId || "",
          projectId,
        },
      },
    });
    logger.info("mediaRepository: GCS upload ok", {
      runId,
      mediaIndex,
      bucket: bucket.name,
      storagePath,
      bytesOriginal: buf.length,
      bytesStored: optimized.buffer.length,
    });
  } catch (e) {
    logger.error("mediaRepository: storage upload failed", {
      runId,
      storagePath,
      message: e.message,
    });
    await db.collection(COL).add({
      projectId,
      dateKey: dk,
      reportDateKey: dk,
      senderPhone,
      sourceMessageId: sourceMessageId || null,
      messageSid: messageSidTwilio || null,
      twilioMediaUrl: mediaUrl,
      contentType: finalContentType,
      storagePath: null,
      downloadURL: null,
      captionText: captionText || null,
      linkedLogEntryId: linkedLogEntryId || null,
      issueCollection: issueCollection || null,
      issueId: issueId || null,
      mediaIndex,
      uploadError: String(e.message).slice(0, 500),
      createdAt: FieldValue.serverTimestamp(),
      aiAnalyzed: false,
      aiError: null,
      aiTags: [],
      aiSummary: null,
    });
    return null;
  }

  /* Signed URLs often fail if the Functions SA lacks signBlob IAM; dashboard resolves via Storage SDK + getDownloadURL(storagePath). */
  const downloadURL = null;

  const mediaRef = await db.collection(COL).add({
    projectId,
    dateKey: dk,
    reportDateKey: dk,
    senderPhone,
    sourceMessageId: sourceMessageId || null,
    messageSid: messageSidTwilio || null,
    twilioMediaUrl: mediaUrl,
    contentType: finalContentType,
    storagePath,
    downloadURL,
    captionText: captionText || null,
    linkedLogEntryId: linkedLogEntryId || null,
    issueCollection: issueCollection || null,
    issueId: issueId || null,
    mediaIndex,
      uploadedBy: uploadedByPhone || senderPhone,
      source: sourceLabel,
      includeInDailyReport: true,
      createdAt: FieldValue.serverTimestamp(),
    aiAnalyzed: false,
    aiError: null,
    aiTags: [],
    aiSummary: null,
  });

  return {
    storagePath,
    downloadURL,
    fileName,
    mediaId: mediaRef.id,
    mimeType: finalContentType,
  };
}

/**
 * Media for a given Eastern `dateKey` + phone. Narrow to one project in memory:
 * `projectId` (canonical) or legacy `projectSlug` must match slug or "_unassigned".
 *
 * Daily PDF may merge an adjacent Eastern `dateKey` (see dailyReportPdf) so MMS tied to the
 * prior calendar day still appear when the report day and storage `dateKey` differ slightly.
 */
async function loadMediaForDailyReport(db, phoneE164, dateKey, projectSlug) {
  const wantPid = projectSlug || "_unassigned";
  const snap = await db
    .collection(COL)
    .where("senderPhone", "==", phoneE164)
    .where("dateKey", "==", dateKey)
    .get()
    .catch(() => null);
  const exactRows = snap
    ? snap.docs
    .map((d) => ({ id: d.id, ...d.data() }))
    .filter((m) => {
      if (!m.storagePath) return false;
      const pid =
        m.projectId != null && String(m.projectId).trim() !== ""
          ? String(m.projectId).trim()
          : m.projectSlug != null && String(m.projectSlug).trim() !== ""
            ? String(m.projectSlug).trim()
            : "";
      return pid === wantPid;
    })
    : [];

  const nextDayStart = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 1));
  const lookaheadEnd = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 3));
  const legacySnap = await db
    .collection(COL)
    .where("senderPhone", "==", phoneE164)
    .where("createdAt", ">=", nextDayStart)
    .where("createdAt", "<", lookaheadEnd)
    .orderBy("createdAt", "asc")
    .limit(120)
    .get()
    .catch(() => null);

  const legacyRows = legacySnap
    ? legacySnap.docs
        .map((d) => ({ id: d.id, ...d.data() }))
        .filter((m) => {
          if (!m.storagePath) return false;
          const pid =
            m.projectId != null && String(m.projectId).trim() !== ""
              ? String(m.projectId).trim()
              : m.projectSlug != null && String(m.projectSlug).trim() !== ""
                ? String(m.projectSlug).trim()
                : "";
          if (pid !== wantPid) return false;
          if (m.dateKey === dateKey || m.reportDateKey === dateKey) return false;
          return getMediaEffectiveDateKey(m) === dateKey;
        })
    : [];

  return dedupeMedia([...exactRows, ...legacyRows]);
}

function mediaProjectIdNorm(m) {
  const raw =
    m.projectId != null && String(m.projectId).trim() !== ""
      ? String(m.projectId).trim()
      : m.projectSlug != null && String(m.projectSlug).trim() !== ""
        ? String(m.projectSlug).trim()
        : "";
  return raw ? normalizeProjectSlug(raw) || raw : "";
}

/**
 * Media for one project + Eastern `dateKey`, all senders (daily PDF / management rollup).
 * Mirrors `loadMediaForDailyReport` lookahead for MMS whose stored `dateKey` is adjacent to report day.
 */
async function loadMediaForProjectDailyReport(db, dateKey, projectSlug) {
  const rawTrim = String(projectSlug || "").trim();
  const reportNorm =
    normalizeProjectSlug(projectSlug) || normalizeProjectSlug(rawTrim) || rawTrim || "_unassigned";
  const idVariants = [...new Set([reportNorm, rawTrim].filter(Boolean))];

  const fetchByProjectId = async (wantPid) => {
    const snap = await db
      .collection(COL)
      .where("projectId", "==", wantPid)
      .where("dateKey", "==", dateKey)
      .limit(500)
      .get()
      .catch((err) => {
        logMediaQueryErr("mediaRepository:projectMediaByDateKey", err);
        return null;
      });
    return snap
      ? snap.docs
          .map((d) => ({ id: d.id, ...d.data() }))
          .filter((m) => m.storagePath && mediaProjectIdNorm(m) === reportNorm)
      : [];
  };

  const fromProjectIdChunks = await Promise.all(idVariants.map((v) => fetchByProjectId(v)));
  const fromProjectId = dedupeMedia(fromProjectIdChunks.flat());

  const nextDayStart = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 1));
  const lookaheadEnd = startOfEasternDayForDateKey(addCalendarDaysToDateKey(dateKey, 3));

  const fetchLegacy = async (wantPid) => {
    const legacySnap = await db
      .collection(COL)
      .where("projectId", "==", wantPid)
      .where("createdAt", ">=", nextDayStart)
      .where("createdAt", "<", lookaheadEnd)
      .orderBy("createdAt", "asc")
      .limit(200)
      .get()
      .catch((err) => {
        logMediaQueryErr("mediaRepository:projectMediaLegacy", err);
        return null;
      });
    return legacySnap
      ? legacySnap.docs
          .map((d) => ({ id: d.id, ...d.data() }))
          .filter((m) => {
            if (!m.storagePath) return false;
            if (mediaProjectIdNorm(m) !== reportNorm) return false;
            if (m.dateKey === dateKey || m.reportDateKey === dateKey) return false;
            return getMediaEffectiveDateKey(m) === dateKey;
          })
      : [];
  };

  const legacyChunks = await Promise.all(idVariants.map((v) => fetchLegacy(v)));
  const legacyRows = legacyChunks.flat();

  let slugOnlyRows = [];
  if (reportNorm !== "_unassigned" && rawTrim) {
    const slugVariants = [...new Set([reportNorm, rawTrim])];
    const slugChunks = await Promise.all(
      slugVariants.map((ps) =>
        db
          .collection(COL)
          .where("projectSlug", "==", ps)
          .where("dateKey", "==", dateKey)
          .limit(200)
          .get()
          .catch((err) => {
            logMediaQueryErr("mediaRepository:projectMediaBySlug", err);
            return null;
          })
      )
    );
    for (const slugSnap of slugChunks) {
      if (!slugSnap) continue;
      slugOnlyRows.push(
        ...slugSnap.docs
          .map((d) => ({ id: d.id, ...d.data() }))
          .filter((m) => m.storagePath && mediaProjectIdNorm(m) === reportNorm)
      );
    }
  }

  return dedupeMedia([...fromProjectId, ...legacyRows, ...slugOnlyRows]);
}

async function registerUploadedMedia({
  db,
  FieldValue,
  phoneE164,
  projectSlug,
  reportDateKey,
  sourceMessageId,
  linkedLogEntryId,
  captionText,
  uploadedBy,
  files,
}) {
  const dk =
    typeof reportDateKey === "string" && /^\d{4}-\d{2}-\d{2}$/.test(reportDateKey.trim())
      ? reportDateKey.trim()
      : dateKeyEastern(new Date());
  const projectId = projectSlug || "_unassigned";
  const senderPhone = phoneE164 || null;
  const uploaded = [];

  for (let i = 0; i < (files || []).length; i += 1) {
    const file = files[i] || {};
    const storagePath = String(file.storagePath || "").trim();
    if (!storagePath) continue;
    const contentType = String(file.contentType || "application/octet-stream").trim();
    const fileName = String(file.fileName || path.posix.basename(storagePath) || `image-${i}`).trim();
    const mediaRef = await db.collection(COL).add({
      projectId,
      dateKey: dk,
      reportDateKey: dk,
      senderPhone,
      sourceMessageId: sourceMessageId || null,
      messageSid: null,
      twilioMediaUrl: null,
      contentType,
      storagePath,
      downloadURL: null,
      captionText: captionText || null,
      linkedLogEntryId: linkedLogEntryId || null,
      issueCollection: null,
      issueId: null,
      mediaIndex: i,
      uploadedBy: uploadedBy || senderPhone,
      source: "dashboard",
      fileName,
      includeInDailyReport: true,
      createdAt: FieldValue.serverTimestamp(),
      aiAnalyzed: false,
      aiError: null,
      aiTags: [],
      aiSummary: null,
    });
    uploaded.push({
      mediaId: mediaRef.id,
      storagePath,
      fileName,
      mimeType: contentType,
    });
  }

  return uploaded;
}

async function attachExistingMediaToIssueBySourceMessages({
  db,
  FieldValue,
  issueCollection,
  issueId,
  sourceMessageIds,
  changedBy,
  projectSlug,
}) {
  const ids = [...new Set((sourceMessageIds || []).map((v) => String(v || "").trim()).filter(Boolean))];
  if (!issueCollection || !issueId || !ids.length) {
    return { attached: 0, mediaIds: [], storagePaths: [], photos: [] };
  }

  const issueRef = db.collection(issueCollection).doc(issueId);
  const issueSnap = await issueRef.get();
  if (!issueSnap.exists) {
    return { attached: 0, mediaIds: [], storagePaths: [], photos: [] };
  }

  const issue = issueSnap.data() || {};
  const wantedProject = String(projectSlug || issue.projectId || issue.projectSlug || "").trim();
  const prevPhotos = Array.isArray(issue.photos) ? issue.photos : [];
  const prevHistory = Array.isArray(issue.history) ? issue.history : [];
  const seenPaths = new Set(prevPhotos.map((p) => p && p.storagePath).filter(Boolean));
  const photosToAdd = [];
  const historyToAdd = [];
  const mediaIds = [];
  const storagePaths = [];

  for (const sourceMessageId of ids) {
    const snap = await db
      .collection(COL)
      .where("sourceMessageId", "==", sourceMessageId)
      .get()
      .catch(() => null);
    if (!snap || snap.empty) continue;

    for (const doc of snap.docs) {
      const media = doc.data() || {};
      const mediaProject = String(media.projectId || media.projectSlug || "").trim();
      if (wantedProject && mediaProject && mediaProject !== wantedProject && mediaProject !== "_unassigned") {
        continue;
      }
      if (media.issueId && String(media.issueId) !== String(issueId)) {
        continue;
      }

      await doc.ref.update({
        issueCollection,
        issueId,
        projectId: wantedProject || media.projectId || media.projectSlug || null,
        linkedAt: FieldValue.serverTimestamp(),
      });

      mediaIds.push(doc.id);
      if (media.storagePath) {
        storagePaths.push(media.storagePath);
      }

      if (media.storagePath && !seenPaths.has(media.storagePath)) {
        seenPaths.add(media.storagePath);
        photosToAdd.push({
          mediaId: doc.id,
          storagePath: media.storagePath,
          downloadURL: media.downloadURL || null,
          fileName: media.fileName || null,
          uploadedAt:
            media.createdAt && typeof media.createdAt.toMillis === "function"
              ? media.createdAt
              : Timestamp.now(),
          uploadedBy: media.uploadedBy || media.senderPhone || changedBy || null,
          source: "sms",
          mimeType: media.contentType || media.mimeType || "application/octet-stream",
        });
        historyToAdd.push({
          at: Timestamp.now(),
          action: "photo_linked",
          changedBy: changedBy || "system",
          field: "photos",
          oldValue: null,
          newValue: media.storagePath || null,
          note: `Linked SMS media from message ${sourceMessageId}`,
        });
      }
    }
  }

  if (photosToAdd.length) {
    await issueRef.update({
      photos: [...prevPhotos, ...photosToAdd],
      history: [...prevHistory, ...historyToAdd],
      updatedAt: FieldValue.serverTimestamp(),
    });
  }

  return {
    attached: photosToAdd.length,
    mediaIds: [...new Set(mediaIds)],
    storagePaths: [...new Set(storagePaths)],
    photos: photosToAdd,
  };
}

module.exports = {
  COL_MEDIA: COL,
  saveOneInboundMedia,
  loadMediaForDailyReport,
  loadMediaForProjectDailyReport,
  attachExistingMediaToIssueBySourceMessages,
  getMediaEffectiveDateKey,
  registerUploadedMedia,
};
