const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  assertGeneratedReportMatchesDelivery,
  resolveManagementJournalDeliveryContext,
  shouldSkipManagementJournalDelivery,
} = require("./managementJournalDelivery");

function queueData(projectSlug, status = "queued") {
  return {
    status,
    projectSlug,
    reportDateKey: "2026-08-16",
    reportType: "journal",
    audience: "management",
    runId: "mgmt-journal-push-2026-08-16-2100",
  };
}

test("same-cycle Home and Docksteader deliveries retain distinct queue identity", () => {
  const home = resolveManagementJournalDeliveryContext(
    "2026-08-16__home__journal__management",
    queueData("home")
  );
  const dock = resolveManagementJournalDeliveryContext(
    "2026-08-16__docksteader__journal__management",
    queueData("docksteader")
  );

  assert.equal(home.projectSlug, "home");
  assert.equal(dock.projectSlug, "docksteader");
  assert.notDeepEqual(home, dock);
});

test("queue data cannot claim another project's deterministic document id", () => {
  assert.throws(
    () =>
      resolveManagementJournalDeliveryContext(
        "2026-08-16__docksteader__journal__management",
        queueData("home")
      ),
    /projectSlug mismatch/
  );
});

test("retry skips sent queues and report payloads must match the exact queue", () => {
  const dock = resolveManagementJournalDeliveryContext(
    "2026-08-16__docksteader__journal__management",
    queueData("docksteader")
  );
  assert.equal(shouldSkipManagementJournalDelivery(queueData("docksteader", "sent")), true);
  assert.equal(shouldSkipManagementJournalDelivery(queueData("docksteader", "processing")), false);

  const report = {
    reportId: "dock-report-id",
    projectSlug: "docksteader",
    reportDateKey: "2026-08-16",
    reportType: "journal",
    storagePath: "dailyReports/redacted/2026-08-16/journal/dock.pdf",
  };
  assert.equal(assertGeneratedReportMatchesDelivery(report, dock), report);
  assert.throws(
    () => assertGeneratedReportMatchesDelivery({ ...report, projectSlug: "home" }, dock),
    /project mismatch/
  );
  assert.throws(
    () => assertGeneratedReportMatchesDelivery({ ...report, reportDateKey: "2026-08-15" }, dock),
    /date mismatch/
  );
});
