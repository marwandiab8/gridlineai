const { resolveDateId, toIsoTimestamp } = require("./date");
const { buildReportAppUrl } = require("../reportPushConfig");

function text(value, max = 500) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, max);
}

function compactObject(source) {
  return Object.fromEntries(Object.entries(source || {}).filter(([, value]) => value !== undefined));
}

function sourceUrl(baseUrl, path) {
  const base = text(baseUrl || "").replace(/\/+$/, "");
  return base && path ? `${base}/${String(path).replace(/^\/+/, "")}` : "";
}

function reportSourceUrl(baseUrl, reportId) {
  return buildReportAppUrl({
    baseUrl,
    reportId,
    openPdf: true,
  });
}

function commonFields(record, options) {
  const projectId = text(record.projectId || record.projectSlug || options.projectId || options.id || "");
  const timeZone = text(record.timeZone || record.projectTimeZone || options.timeZone || "America/Toronto");
  const dateId = resolveDateId(record, { timeZone });
  return compactObject({
    dateId: dateId || undefined,
    sourceApp: "gridlineai",
    sourceFirebaseProjectId: options.sourceFirebaseProjectId || "gridlineai",
    sourceProjectName: text(record.projectName || record.name || record.projectTitle || projectId, 180),
    sourceProjectId: projectId,
    sourceDocumentId: options.id || record.id || "",
    sourceDocumentPath: options.path || "",
    sourceStoragePath: record.storagePath || null,
    sourceUrl: options.sourceUrl || "",
    originalCreatedAt: toIsoTimestamp(record.createdAt),
    originalUpdatedAt: toIsoTimestamp(record.updatedAt),
    capturedAt: toIsoTimestamp(record.capturedAt || record.takenAt || record.occurredAt || record.reportDate || record.dateKey),
    visibility: "ownerOnly",
    syncStatus: options.syncStatus || "active",
  });
}

function mapProjectRecordToTimeLeft(project, options = {}) {
  const item = commonFields(project, {
    ...options,
    projectId: options.id || project.projectId || project.projectSlug,
    sourceUrl: sourceUrl(options.appBaseUrl, `#projects/${encodeURIComponent(options.id || project.projectSlug || "")}`),
  });
  return {
    ...item,
    category: "projectRecord",
    title: text(project.name || project.projectName || options.id || "Project", 180),
    summary: text(project.location || project.address || project.notes || "", 700),
    description: text(project.notes || project.instructions || project.description || "", 2000),
    sourceCollection: "projects",
    metadata: compactObject({
      projectSlug: options.id || project.projectSlug || null,
      ownerPhoneE164: project.ownerPhoneE164 || null,
      location: project.location || project.address || null,
      status: project.status || null,
      source: "gridlineai",
    }),
  };
}

function mapReportToTimeLeft(report, options = {}) {
  const item = commonFields(report, {
    ...options,
    sourceUrl: reportSourceUrl(options.appBaseUrl, options.id || ""),
  });
  return {
    ...item,
    category: "projectReport",
    title: text(report.reportTitle || report.title || report.reportFileName || "Project Report", 180),
    summary: text(report.summary || report.aiNarrative || report.weatherSummary || report.reportTitle || "", 700),
    description: text(report.description || report.notes || report.aiNarrative || "", 2000),
    sourceCollection: "dailyReports",
    fileUrl: report.downloadURL || report.accessURL || null,
    thumbnailUrl: null,
    contentType: report.storagePath || report.downloadURL ? "application/pdf" : null,
    fileName: text(report.reportFileName || report.fileName || ""),
    fileSize: Number.isFinite(Number(report.fileSize || report.size)) ? Number(report.fileSize || report.size) : null,
    metadata: compactObject({
      projectId: report.projectId || report.projectSlug || null,
      reportDateKey: report.reportDateKey || report.dateKey || null,
      reportType: report.reportType || null,
      taskCount: report.taskCount || null,
      weatherSummary: report.weatherSummary || null,
      source: "gridlineai",
    }),
  };
}

function mapJournalEntryToTimeLeft(entry, options = {}) {
  const body = text(entry.rawText || entry.normalizedText || entry.body || entry.text || entry.summaryText || "", 2000);
  const item = commonFields(entry, {
    ...options,
    sourceUrl: sourceUrl(options.appBaseUrl, `#logEntries/${encodeURIComponent(options.id || "")}`),
  });
  return {
    ...item,
    category: "journalEntry",
    title: text(entry.title || "Journal Entry", 180),
    summary: text(entry.summaryText || entry.normalizedText || entry.rawText || entry.body || entry.text || "", 700),
    description: body,
    sourceCollection: "logEntries",
    sourceStoragePath: null,
    fileUrl: null,
    thumbnailUrl: null,
    contentType: null,
    fileName: null,
    fileSize: null,
    metadata: compactObject({
      projectId: entry.projectId || entry.projectSlug || null,
      category: entry.category || null,
      subtype: entry.subtype || null,
      tags: Array.isArray(entry.tags) ? entry.tags : [],
      linkedMediaIds: Array.isArray(entry.linkedMediaIds) ? entry.linkedMediaIds : [],
      sourceMessageId: entry.sourceMessageId || null,
      source: "gridlineai",
    }),
  };
}

function mapMediaToTimeLeft(media, options = {}) {
  const contentType = text(media.contentType || media.mimeType || "");
  const nameOrPath = text(media.fileName || media.name || media.storagePath || "");
  const isImage =
    contentType.toLowerCase().startsWith("image/") ||
    /\.(jpe?g|png|gif|webp|heic|bmp|tiff?)$/i.test(nameOrPath);
  const projectId = text(media.projectId || media.projectSlug || options.projectId || "gridlineai");
  const mediaForDate = {
    ...media,
    uploadedAt: media.uploadedAt || media.createdAt || media.capturedAt || media.dateKey || media.reportDateKey,
  };
  const item = commonFields(mediaForDate, {
    ...options,
    projectId,
    sourceUrl: sourceUrl(options.appBaseUrl, `#media/${encodeURIComponent(options.id || "")}`),
  });
  return {
    ...item,
    category: isImage ? "image" : "file",
    title: text(media.title || media.fileName || media.name || (isImage ? "Uploaded image" : "Uploaded file"), 180),
    summary: text(media.captionText || media.caption || media.description || "", 700),
    description: text(media.description || media.captionText || media.caption || "", 2000),
    sourceCollection: "media",
    fileUrl: media.downloadURL || media.url || null,
    thumbnailUrl: media.thumbnailURL || media.thumbnailUrl || media.thumbUrl || null,
    contentType: contentType || null,
    fileName: text(media.fileName || media.name || String(media.storagePath || "").split("/").pop() || ""),
    fileSize: Number.isFinite(Number(media.fileSize || media.size)) ? Number(media.fileSize || media.size) : null,
    metadata: compactObject({
      projectId: media.projectId || media.projectSlug || null,
      storageBucket: media.storageBucket || null,
      storagePath: media.storagePath || null,
      linkedLogEntryId: media.linkedLogEntryId || null,
      sourceMessageId: media.sourceMessageId || null,
      messageSid: media.messageSid || null,
      uploadedBy: media.uploadedBy || media.senderPhone || null,
      source: "gridlineai",
    }),
  };
}

module.exports = {
  mapJournalEntryToTimeLeft,
  mapMediaToTimeLeft,
  mapProjectRecordToTimeLeft,
  mapReportToTimeLeft,
};
