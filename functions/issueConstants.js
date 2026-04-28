/**
 * Construction issue logging — shared constants (SMS + dashboard + exports).
 */

const ISSUE_STATUSES = [
  "Open",
  "In Progress",
  "Pending Review",
  "Waiting on Trade",
  "Waiting on Consultant",
  "Waiting on Owner",
  "Closed",
  "Archived",
];

const ISSUE_PRIORITIES = ["Low", "Medium", "High", "Critical"];

/** Canonical issue type keys stored on documents */
const ISSUE_TYPES = ["safety", "delay", "deficiency", "general"];

const COLLECTION_BY_TYPE = {
  safety: "safetyIssues",
  delay: "delayIssues",
  deficiency: "deficiencyIssues",
  general: "generalIssues",
};

const TYPE_BY_COLLECTION = Object.fromEntries(
  Object.entries(COLLECTION_BY_TYPE).map(([k, v]) => [v, k])
);

module.exports = {
  ISSUE_STATUSES,
  ISSUE_PRIORITIES,
  ISSUE_TYPES,
  COLLECTION_BY_TYPE,
  TYPE_BY_COLLECTION,
  ALL_ISSUE_COLLECTIONS: Object.values(COLLECTION_BY_TYPE),
};
