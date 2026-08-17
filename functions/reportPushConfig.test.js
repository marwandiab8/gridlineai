const { test } = require("node:test");
const assert = require("node:assert/strict");

const {
  DEFAULT_REPORT_PUSH_TIME,
  buildReportAppUrl,
  canReceiveProjectReport,
  normalizePdfPushSettings,
  resolveAppBaseUrl,
} = require("./reportPushConfig");

test("normalizePdfPushSettings preserves defaults and valid overrides", () => {
  assert.deepEqual(normalizePdfPushSettings(null), {
    enabled: true,
    reportType: "journal",
    scheduleTimeLocal: DEFAULT_REPORT_PUSH_TIME,
    audience: "management",
  });

  assert.deepEqual(
    normalizePdfPushSettings({
      enabled: false,
      reportType: "dailySiteLog",
      scheduleTimeLocal: "18:35",
      audience: "project_users",
    }),
    {
      enabled: false,
      reportType: "dailySiteLog",
      scheduleTimeLocal: "18:35",
      audience: "project_users",
    }
  );
});

test("resolveAppBaseUrl and buildReportAppUrl create app deeplinks", () => {
  assert.equal(resolveAppBaseUrl("gridlineai", ""), "https://gridlineai.web.app");
  assert.equal(resolveAppBaseUrl("gridlineai", "https://example.com/"), "https://example.com");
  assert.equal(
    buildReportAppUrl({
      baseUrl: "https://gridlineai.web.app/",
      reportId: "abc123",
      openPdf: true,
    }),
    "https://gridlineai.web.app/?view=reports&reportId=abc123&openPdf=1"
  );
});

test("delivery links remain bound to each project's exact report id", () => {
  const homeLink = buildReportAppUrl({
    baseUrl: "https://gridlineai.web.app",
    reportId: "home-report-id",
    openPdf: true,
  });
  const dockLink = buildReportAppUrl({
    baseUrl: "https://gridlineai.web.app",
    reportId: "dock-report-id",
    openPdf: true,
  });
  assert.match(homeLink, /reportId=home-report-id/);
  assert.doesNotMatch(homeLink, /dock-report-id/);
  assert.match(dockLink, /reportId=dock-report-id/);
  assert.doesNotMatch(dockLink, /home-report-id/);
});

test("canReceiveProjectReport only allows assigned management scope", () => {
  assert.equal(
    canReceiveProjectReport(
      { projectSlugs: ["home"], allProjects: false },
      "home"
    ),
    true
  );
  assert.equal(
    canReceiveProjectReport(
      { projectSlugs: ["docksteader"], allProjects: false },
      "home"
    ),
    false
  );
  assert.equal(
    canReceiveProjectReport(
      { projectSlugs: ["Docksteader"], allProjects: true },
      "home"
    ),
    true
  );
  assert.equal(
    canReceiveProjectReport(
      { projectSlugs: ["home-renovation"], allProjects: false },
      "home"
    ),
    false
  );
});
