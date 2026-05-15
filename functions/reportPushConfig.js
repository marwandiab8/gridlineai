const DEFAULT_REPORT_PUSH_TIME = "21:00";

function normalizeReportPushTime(value) {
  const raw = String(value || "").trim();
  return /^\d{2}:\d{2}$/.test(raw) ? raw : DEFAULT_REPORT_PUSH_TIME;
}

function normalizeReportPushAudience(value) {
  return String(value || "").trim() === "project_users" ? "project_users" : "management";
}

function normalizeReportPushType(value) {
  return String(value || "").trim() === "dailySiteLog" ? "dailySiteLog" : "journal";
}

function normalizePdfPushSettings(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  return {
    enabled: source.enabled !== false,
    reportType: normalizeReportPushType(source.reportType),
    scheduleTimeLocal: normalizeReportPushTime(source.scheduleTimeLocal),
    audience: normalizeReportPushAudience(source.audience),
  };
}

function resolveAppBaseUrl(projectId, configuredBaseUrl) {
  const direct = String(configuredBaseUrl || "").trim().replace(/\/+$/, "");
  if (direct) return direct;
  const safeProjectId = String(projectId || "").trim();
  if (!safeProjectId) return "";
  return `https://${safeProjectId}.web.app`;
}

function buildReportAppUrl({ baseUrl, reportId, openPdf = true }) {
  const root = String(baseUrl || "").trim().replace(/\/+$/, "");
  const id = String(reportId || "").trim();
  if (!root || !id) return "";
  const params = new URLSearchParams();
  params.set("view", "reports");
  params.set("reportId", id);
  if (openPdf) params.set("openPdf", "1");
  return `${root}/?${params.toString()}`;
}

module.exports = {
  DEFAULT_REPORT_PUSH_TIME,
  normalizeReportPushTime,
  normalizeReportPushAudience,
  normalizeReportPushType,
  normalizePdfPushSettings,
  resolveAppBaseUrl,
  buildReportAppUrl,
};
