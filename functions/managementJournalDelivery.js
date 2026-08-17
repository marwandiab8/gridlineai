const { normalizeProjectSlug } = require("./projectAccess");

const DATE_KEY_RE = /^\d{4}-\d{2}-\d{2}$/;
const REPORT_TYPES = new Set(["journal", "dailySiteLog"]);
const AUDIENCES = new Set(["management", "project_users"]);

function parseManagementJournalQueueId(docId) {
  const parts = String(docId || "").split("__");
  if (parts.length !== 4) return null;
  const [reportDateKey, projectPart, reportType, audience] = parts;
  const projectSlug = normalizeProjectSlug(projectPart);
  if (
    !DATE_KEY_RE.test(reportDateKey) ||
    !projectSlug ||
    projectSlug !== projectPart ||
    !REPORT_TYPES.has(reportType) ||
    !AUDIENCES.has(audience)
  ) {
    return null;
  }
  return { reportDateKey, projectSlug, reportType, audience };
}

function resolveManagementJournalDeliveryContext(docId, data) {
  const queueIdentity = parseManagementJournalQueueId(docId);
  if (!queueIdentity) throw new Error("invalid management journal queue document id");
  const record = data && typeof data === "object" ? data : {};
  const recordIdentity = {
    reportDateKey: String(record.reportDateKey || "").trim(),
    projectSlug: normalizeProjectSlug(record.projectSlug),
    reportType: String(record.reportType || "").trim(),
    audience: String(record.audience || "").trim(),
  };
  for (const field of ["reportDateKey", "projectSlug", "reportType", "audience"]) {
    if (recordIdentity[field] !== queueIdentity[field]) {
      throw new Error(`management journal queue ${field} mismatch`);
    }
  }
  return {
    ...queueIdentity,
    runId: String(record.runId || `mgmt-journal-delivery-${docId}`).trim(),
  };
}

function shouldSkipManagementJournalDelivery(data) {
  return String((data && data.status) || "").trim() === "sent";
}

function assertGeneratedReportMatchesDelivery(report, deliveryContext) {
  const actual = report && typeof report === "object" ? report : {};
  const expected = deliveryContext && typeof deliveryContext === "object"
    ? deliveryContext
    : {};
  if (normalizeProjectSlug(actual.projectSlug) !== expected.projectSlug) {
    throw new Error("generated report project mismatch");
  }
  if (String(actual.reportDateKey || "").trim() !== expected.reportDateKey) {
    throw new Error("generated report date mismatch");
  }
  if (String(actual.reportType || "").trim() !== expected.reportType) {
    throw new Error("generated report type mismatch");
  }
  if (!String(actual.reportId || "").trim() || !String(actual.storagePath || "").trim()) {
    throw new Error("generated report identity missing");
  }
  return actual;
}

module.exports = {
  assertGeneratedReportMatchesDelivery,
  parseManagementJournalQueueId,
  resolveManagementJournalDeliveryContext,
  shouldSkipManagementJournalDelivery,
};
