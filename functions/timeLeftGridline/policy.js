const FIREBASE_WEB_APP_ID_RE = /^1:\d+:web:[a-z0-9]+$/i;

function normalizeText(value) {
  const text = String(value == null ? "" : value).trim();
  return text || null;
}

function assertFirebaseProjectId(value, label = "GRIDLINE_FIREBASE_PROJECT_ID") {
  const projectId = normalizeText(value);
  if (!projectId) {
    throw new Error(`${label} is required.`);
  }
  if (FIREBASE_WEB_APP_ID_RE.test(projectId) || projectId.includes(":web:")) {
    throw new Error(`${label} must be a Firebase project ID, not a Firebase web app ID.`);
  }
  if (!/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(projectId)) {
    throw new Error(`${label} is not a valid Firebase project ID.`);
  }
  return projectId;
}

function isIosShortcutLegacyLogEntry(record) {
  if (!record || typeof record !== "object") return false;
  return (
    normalizeText(record.source)?.toLowerCase() === "ios_shortcuts" &&
    Boolean(normalizeText(record.shortcutEventId))
  );
}

module.exports = {
  FIREBASE_WEB_APP_ID_RE,
  assertFirebaseProjectId,
  isIosShortcutLegacyLogEntry,
  normalizeText,
};
