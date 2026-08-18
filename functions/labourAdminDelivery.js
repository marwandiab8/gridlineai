const {
  aggregateCanonicalLabour,
  isValidDateKey,
} = require("./labourAdminQuery");
const { isDeepStrictEqual } = require("node:util");

const ADMIN_LABOUR_PDF_MAX_ATTEMPTS = 3;
const ADMIN_LABOUR_PDF_QUEUE_FAILURE_TEXT =
  "I couldn’t queue the labour PDF. No report was sent. Please try again.";

function formatLongDateRange(startKey, endKey) {
  if (!isValidDateKey(startKey) || !isValidDateKey(endKey) || startKey > endKey) {
    throw new Error("Administrator labour PDF date range is invalid.");
  }
  const start = new Date(`${startKey}T12:00:00.000Z`);
  const end = new Date(`${endKey}T12:00:00.000Z`);
  const monthName = (date) => new Intl.DateTimeFormat("en-CA", {
    timeZone: "UTC",
    month: "long",
  }).format(date);
  const startDay = start.getUTCDate();
  const endDay = end.getUTCDate();
  const startYear = start.getUTCFullYear();
  const endYear = end.getUTCFullYear();
  if (startKey === endKey) {
    return `${monthName(start)} ${startDay}, ${startYear}`;
  }
  const sameYear = startYear === endYear;
  const sameMonth = sameYear && start.getUTCMonth() === end.getUTCMonth();
  if (sameMonth) return `${monthName(start)} ${startDay}–${endDay}, ${startYear}`;
  if (sameYear) return `${monthName(start)} ${startDay}–${monthName(end)} ${endDay}, ${startYear}`;
  return `${monthName(start)} ${startDay}, ${startYear}–${monthName(end)} ${endDay}, ${endYear}`;
}

function labourPdfScopeName(request) {
  return String(request && (request.projectName || request.workerName) || "all work projects").trim();
}

function formatAdminLabourPdfAcknowledgement(request) {
  return `Okay. Generating the administrator labour PDF for ${labourPdfScopeName(request)}, ${formatLongDateRange(
    request && request.startKey,
    request && request.endKey,
  )}. A protected link will follow shortly.`;
}

function formatAdminLabourPdfLinkMessage(request, accessURL) {
  const url = String(accessURL || "").trim();
  if (!url) throw new Error("Administrator labour PDF protected URL is missing.");
  return `${labourPdfScopeName(request)} labour report — ${formatLongDateRange(
    request && request.startKey,
    request && request.endKey,
  )}: ${url}`;
}

function formatAdminLabourPdfFailureMessage(request) {
  const scope = labourPdfScopeName(request);
  return `The ${scope} labour PDF could not be delivered. No labour records were changed. Please try again.`;
}

async function persistAdminLabourPdfJob({ queueWriter, requestKey, payload }) {
  if (typeof queueWriter !== "function") throw new Error("Administrator labour PDF queue writer is required.");
  const queued = await queueWriter({ requestKey, payload });
  if (!queued || !queued.ref) throw new Error("Administrator labour PDF queue was not persisted.");
  return {
    ...queued,
    acknowledgement: formatAdminLabourPdfAcknowledgement(payload && payload.query),
  };
}

async function dispatchAdministratorLabourPdfQueue({ snapshot, deliverAdministrator }) {
  const data = snapshot && typeof snapshot.data === "function" ? snapshot.data() || {} : {};
  if (data.adminQuery !== true) return false;
  if (typeof deliverAdministrator !== "function") {
    throw new Error("Administrator labour PDF delivery handler is required.");
  }
  await deliverAdministrator(snapshot);
  return true;
}

function shouldRetryAdminLabourPdfClaim(claim) {
  return Boolean(claim && claim.claimed === false && claim.status === "processing");
}

function validatePersistedAdminLabourAggregation(query, result) {
  if (!query || typeof query !== "object" || !result || typeof result !== "object") {
    throw new Error("Administrator labour PDF persisted aggregation is missing.");
  }
  const request = result.request && typeof result.request === "object" ? result.request : {};
  for (const key of ["startKey", "endKey", "projectSlug", "workerId"]) {
    if (String(request[key] || "") !== String(query[key] || "")) {
      throw new Error(`Administrator labour PDF aggregation ${key} mismatch.`);
    }
  }
  if (!isValidDateKey(request.startKey) || !isValidDateKey(request.endKey) || request.startKey > request.endKey) {
    throw new Error("Administrator labour PDF persisted date range is invalid.");
  }
  if (!Array.isArray(result.entries) || !Array.isArray(result.sections) || !Array.isArray(result.documentIds)) {
    throw new Error("Administrator labour PDF persisted aggregation is incomplete.");
  }
  const entryIds = new Set();
  for (const entry of result.entries) {
    const id = String(entry && entry.id || "").trim();
    const minutes = Number(entry && entry.minutesWorked);
    if (!id || entryIds.has(id) || !Number.isSafeInteger(minutes) || minutes <= 0) {
      throw new Error("Administrator labour PDF persisted entry is invalid.");
    }
    entryIds.add(id);
    if (!isValidDateKey(entry.reportDateKey) || entry.reportDateKey < request.startKey || entry.reportDateKey > request.endKey) {
      throw new Error("Administrator labour PDF persisted entry date is outside the request.");
    }
    if (request.projectSlug && entry.projectSlug !== request.projectSlug) {
      throw new Error("Administrator labour PDF persisted entry crossed the project boundary.");
    }
    if (request.workerId && entry.workerId !== request.workerId) {
      throw new Error("Administrator labour PDF persisted entry crossed the worker boundary.");
    }
  }
  const rebuilt = aggregateCanonicalLabour({
    canonical: {
      included: result.entries,
      excludedCount: Number(result.excludedCount || 0),
      excludedReasons: result.excludedReasons || {},
      auditFlags: result.auditFlags || {},
    },
    request: query,
  });
  if (
    rebuilt.totalMinutes !== result.totalMinutes ||
    rebuilt.entryCount !== result.entryCount ||
    rebuilt.workerCount !== result.workerCount ||
    rebuilt.projectCount !== result.projectCount ||
    !isDeepStrictEqual(rebuilt.documentIds, result.documentIds) ||
    !isDeepStrictEqual(rebuilt.sections, result.sections)
  ) {
    throw new Error("Administrator labour PDF persisted aggregation failed integrity validation.");
  }
  return result;
}

async function executeAdminLabourPdfDeliveryOnce({
  identity,
  query,
  result,
  findReport,
  findExistingArtifact,
  beginReport,
  generateReport,
  finishReport,
  ensureAccessURL,
  claimSend,
  sendLinkMessage,
  recordSent,
  recordAuditMessage,
  onAuditFailure,
}) {
  const aggregation = validatePersistedAdminLabourAggregation(query, result);
  let report = await findReport();
  if (report && (
    report.requestKey !== identity.requestKey ||
    report.storagePath !== identity.storagePath ||
    report.type !== "administratorLabourQuery"
  )) {
    throw new Error("Administrator labour report identity mismatch.");
  }
  if (!report || report.status !== "ready") {
    await beginReport(report);
    const existingArtifact = typeof findExistingArtifact === "function"
      ? await findExistingArtifact()
      : null;
    const generated = existingArtifact || await generateReport(aggregation);
    report = await finishReport(generated, aggregation);
  }
  const accessURL = await ensureAccessURL(report, aggregation);
  if (!accessURL) throw new Error("Administrator labour report access URL could not be created.");
  if (!(await claimSend())) return { status: "suppressed" };

  const body = formatAdminLabourPdfLinkMessage(query, accessURL);
  const sent = await sendLinkMessage({ body, accessURL });
  const messageSid = String(sent && sent.messageSid || "").trim();
  const providerStatus = String(sent && sent.providerStatus || "").trim() || null;
  if (!messageSid) throw new Error("Administrator labour PDF link was not accepted by Twilio.");
  await recordSent({ messageSid, providerStatus, body, report, aggregation });
  if (typeof recordAuditMessage === "function") {
    try {
      await recordAuditMessage({ messageSid, providerStatus, body, report, aggregation });
    } catch (error) {
      if (typeof onAuditFailure === "function") await onAuditFailure(error);
    }
  }
  return { status: "sent", messageSid, providerStatus, body, accessURL };
}

async function executeAdminLabourPdfFailureNotificationOnce({
  query,
  sendFailureMessage,
  recordFailureSent,
  recordAuditMessage,
  onAuditFailure,
}) {
  const body = formatAdminLabourPdfFailureMessage(query);
  const sent = await sendFailureMessage({ body });
  const messageSid = String(sent && sent.messageSid || "").trim();
  const providerStatus = String(sent && sent.providerStatus || "").trim() || null;
  if (!messageSid) throw new Error("Labour PDF failure notification was not accepted by Twilio.");
  await recordFailureSent({ messageSid, providerStatus, body });
  if (typeof recordAuditMessage === "function") {
    try {
      await recordAuditMessage({ messageSid, providerStatus, body });
    } catch (error) {
      if (typeof onAuditFailure === "function") await onAuditFailure(error);
    }
  }
  return { status: "failed_notified", messageSid, providerStatus, body };
}

module.exports = {
  ADMIN_LABOUR_PDF_MAX_ATTEMPTS,
  ADMIN_LABOUR_PDF_QUEUE_FAILURE_TEXT,
  dispatchAdministratorLabourPdfQueue,
  executeAdminLabourPdfFailureNotificationOnce,
  executeAdminLabourPdfDeliveryOnce,
  formatAdminLabourPdfAcknowledgement,
  formatAdminLabourPdfFailureMessage,
  formatAdminLabourPdfLinkMessage,
  formatLongDateRange,
  persistAdminLabourPdfJob,
  shouldRetryAdminLabourPdfClaim,
  validatePersistedAdminLabourAggregation,
};
