const DATE_ID_RE = /^\d{4}-\d{2}-\d{2}$/;

function isDateId(value) {
  return typeof value === "string" && DATE_ID_RE.test(value.trim());
}

function timestampToDate(value) {
  if (!value) return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  if (typeof value.toDate === "function") {
    const date = value.toDate();
    return date instanceof Date && !Number.isNaN(date.getTime()) ? date : null;
  }
  if (typeof value.seconds === "number") {
    const date = new Date(value.seconds * 1000);
    return Number.isNaN(date.getTime()) ? null : date;
  }
  if (typeof value === "string") {
    if (isDateId(value)) return parseDateId(value);
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
  }
  return null;
}

function parseDateId(value) {
  if (!isDateId(value)) return null;
  const [year, month, day] = value.trim().split("-").map(Number);
  return new Date(year, month - 1, day, 12, 0, 0, 0);
}

function formatDateIdInTimeZone(value, timeZone = "America/Toronto") {
  if (isDateId(value)) return value.trim();
  const date = timestampToDate(value);
  if (!date) return "";
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timeZone || "America/Toronto",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

function toIsoTimestamp(value) {
  if (!value) return null;
  if (isDateId(value)) return `${value.trim()}T12:00:00.000`;
  const date = timestampToDate(value);
  return date ? date.toISOString() : null;
}

function resolveDateId(record, options = {}) {
  const timeZone = options.timeZone || "America/Toronto";
  const fields = options.fields || [
    "dateId",
    "entryDate",
    "journalDate",
    "reportDateKey",
    "dateKey",
    "reportDate",
    "capturedAt",
    "takenAt",
    "exifTakenAt",
    "occurredAt",
    "documentDate",
    "uploadedAt",
  ];
  for (const field of fields) {
    const value = record && record[field];
    if (value == null || value === "") continue;
    const dateId = formatDateIdInTimeZone(value, timeZone);
    if (dateId) return dateId;
  }
  return "";
}

module.exports = {
  formatDateIdInTimeZone,
  isDateId,
  parseDateId,
  resolveDateId,
  timestampToDate,
  toIsoTimestamp,
};
