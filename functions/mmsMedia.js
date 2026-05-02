/**
 * Twilio MMS → Storage + `media` docs + issue photo arrays (Admin SDK).
 * Media is saved even if issueId is missing (orphan link) or issue doc is missing.
 */

const { Timestamp, FieldValue } = require("firebase-admin/firestore");
const { saveOneInboundMedia } = require("./mediaRepository");
const { countTwilioMediaParams } = require("./twilioMediaFetch");

function classifyInboundMediaLabel(contentType) {
  const type = String(contentType || "").trim().toLowerCase();
  if (type.startsWith("audio/")) {
    return {
      fileStem: "voice-note",
      sourceLabel: "voice",
      historyLabel: "voice note",
    };
  }
  if (type.startsWith("video/")) {
    return {
      fileStem: "video",
      sourceLabel: "sms",
      historyLabel: "video",
    };
  }
  return {
    fileStem: "image",
    sourceLabel: "sms",
    historyLabel: "image",
  };
}

/**
 * @param {import('firebase-admin').firestore.Firestore} db
 * @param {import('firebase-admin').storage.Storage} storage
 */
async function attachTwilioMediaToIssue({
  db,
  storage,
  FieldValue,
  accountSid,
  authToken,
  params,
  issueCollection,
  issueId,
  uploadedByPhone,
  logger,
  runId,
  messageSidTwilio,
  sourceMessageId,
  projectSlug,
  reportDateKey,
  captionText,
  linkedLogEntryId,
}) {
  const n = countTwilioMediaParams(params);
  if (n <= 0) return { attached: 0, mediaIds: [], photos: [], skippedUrls: 0 };

  const canLinkIssue = Boolean(issueCollection && issueId);
  let issueRef = null;
  let snap = null;
  let linkIssueDoc = false;

  if (canLinkIssue) {
    issueRef = db.collection(issueCollection).doc(issueId);
    snap = await issueRef.get();
    if (snap.exists) {
      linkIssueDoc = true;
    } else {
      logger.error(
        "mmsMedia: issue doc missing — media will still reference issueId; issue array not updated",
        { runId, issueCollection, issueId }
      );
    }
  } else {
    logger.warn("mmsMedia: no issue id — saving media with orphan issue link", {
      runId,
      sourceMessageId,
    });
  }

  const existing = linkIssueDoc && snap ? snap.data() || {} : {};
  const prevPhotos = Array.isArray(existing.photos) ? existing.photos : [];
  const prevHistory = Array.isArray(existing.history) ? existing.history : [];

  const bucket = storage.bucket();
  const newPhotos = [];
  const newHistory = [];
  const mediaIds = [];
  let skippedUrls = 0;

  for (let i = 0; i < n; i++) {
    const mediaUrl = params[`MediaUrl${i}`];
    const contentType =
      params[`MediaContentType${i}`] || "application/octet-stream";
    const mediaKind = classifyInboundMediaLabel(contentType);
    if (!mediaUrl || !String(mediaUrl).trim()) {
      skippedUrls += 1;
      logger.warn("mmsMedia: missing MediaUrl for index (expected by count)", {
        runId,
        mediaIndex: i,
        n,
      });
      continue;
    }

    const saved = await saveOneInboundMedia({
      db,
      bucket,
      FieldValue,
      accountSid,
      authToken,
      mediaUrl,
      contentType,
      mediaIndex: i,
      messageSidTwilio: messageSidTwilio || params.MessageSid || "",
      sourceMessageId: sourceMessageId || null,
      senderPhone: uploadedByPhone,
      projectSlug: projectSlug || null,
      reportDateKey: reportDateKey || null,
      captionText: captionText || null,
      linkedLogEntryId: linkedLogEntryId || null,
      fileStem: mediaKind.fileStem,
      sourceLabel: mediaKind.sourceLabel,
      issueCollection: canLinkIssue ? issueCollection : null,
      issueId: canLinkIssue ? issueId : null,
      uploadedByPhone,
      logger,
      runId,
    });

    if (!saved) continue;

    mediaIds.push(saved.mediaId);

    const photo = {
      mediaId: saved.mediaId,
      storagePath: saved.storagePath,
      downloadURL: saved.downloadURL,
      fileName: saved.fileName,
      uploadedAt: Timestamp.now(),
      uploadedBy: uploadedByPhone,
      source: "sms",
      mimeType: saved.mimeType,
    };
    newPhotos.push(photo);

    if (linkIssueDoc) {
      newHistory.push({
        at: Timestamp.now(),
        action: "photo_added",
        changedBy: uploadedByPhone,
        field: "photos",
        oldValue: null,
        newValue: saved.storagePath,
        note: `MMS ${mediaKind.historyLabel}-${i} (${saved.fileName})`,
      });
    }
  }

  if (!newPhotos.length) {
    return { attached: 0, mediaIds: [], photos: [], skippedUrls };
  }

  if (linkIssueDoc && issueRef) {
    await issueRef.update({
      photos: [...prevPhotos, ...newPhotos],
      history: [...prevHistory, ...newHistory],
      updatedAt: FieldValue.serverTimestamp(),
    });
  }

  logger.info("mmsMedia: attached photos", {
    runId,
    issueId: issueId || null,
    linkedToIssueDoc: linkIssueDoc,
    count: newPhotos.length,
    mediaIds: mediaIds.length,
    skippedUrls,
  });
  return {
    attached: newPhotos.length,
    mediaIds,
    photos: newPhotos,
    skippedUrls,
  };
}

module.exports = {
  attachTwilioMediaToIssue,
};
