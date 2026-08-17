const { normalizeProjectSlug } = require("./projectAccess");
const UNASSIGNED_PROJECT_SENTINELS = new Set([
  "_unassigned",
  "unassigned",
  "unknown",
  "none",
  "null",
]);

function normalizeOwnershipField(value) {
  const raw = value == null ? "" : String(value).trim();
  if (!raw) {
    return { present: false, valid: true, projectSlug: null };
  }
  if (UNASSIGNED_PROJECT_SENTINELS.has(raw.toLowerCase())) {
    return { present: true, valid: false, projectSlug: null };
  }
  const projectSlug = normalizeProjectSlug(raw);
  return {
    present: true,
    valid: Boolean(projectSlug),
    projectSlug: projectSlug || null,
  };
}

/**
 * Canonical project ownership for report inputs.
 * Rows with malformed or contradictory ownership fail closed.
 */
function getReportRecordProjectOwnership(record) {
  const slug = normalizeOwnershipField(record && record.projectSlug);
  const id = normalizeOwnershipField(record && record.projectId);
  const malformed = (slug.present && !slug.valid) || (id.present && !id.valid);
  const contradictory = Boolean(
    slug.projectSlug && id.projectSlug && slug.projectSlug !== id.projectSlug
  );
  const consistent = !malformed && !contradictory;
  const projectSlug = consistent ? slug.projectSlug || id.projectSlug || null : null;
  return {
    projectSlug,
    assigned: Boolean(projectSlug),
    consistent,
    contradictory,
    malformed,
    hasProjectFields: slug.present || id.present,
  };
}

function reportRecordBelongsToProject(record, reportProjectSlug) {
  const wantedProject = normalizeProjectSlug(reportProjectSlug);
  if (!wantedProject) return false;
  const ownership = getReportRecordProjectOwnership(record);
  return ownership.consistent && ownership.projectSlug === wantedProject;
}

module.exports = {
  getReportRecordProjectOwnership,
  reportRecordBelongsToProject,
};
