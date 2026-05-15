const { test } = require("node:test");
const assert = require("node:assert/strict");

const {
  DEFAULT_REPORT_PUSH_TIME,
  buildReportAppUrl,
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
