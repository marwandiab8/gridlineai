function timestampMillis(value) {
  if (value && typeof value.toMillis === "function") return value.toMillis();
  if (value instanceof Date) return value.getTime();
  return Number.isFinite(Number(value)) ? Number(value) : 0;
}

function validateReportAccessGrant({ grant, collectionName, reportId, storagePath, nowMs = Date.now() }) {
  const record = grant && typeof grant === "object" ? grant : null;
  if (!record) return { valid: false, reason: "missing" };
  const expiresAtMs = timestampMillis(record.expiresAt);
  if (
    record.collectionName !== collectionName ||
    record.reportId !== reportId ||
    record.storagePath !== storagePath
  ) {
    return { valid: false, reason: "mismatched" };
  }
  if (!expiresAtMs || expiresAtMs < nowMs) return { valid: false, reason: "expired" };
  return { valid: true, reason: null, expiresAtMs };
}

module.exports = {
  validateReportAccessGrant,
};
