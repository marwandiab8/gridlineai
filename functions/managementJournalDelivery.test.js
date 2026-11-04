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

test("a day's journal can be queued again once its Procore daily log arrives (\"__procore\")", () => {
  const { parseManagementJournalQueueId } = require("./managementJournalDelivery");
  const id = "2026-11-02__docksteader__journal__management__procore";
  assert.deepEqual(parseManagementJournalQueueId(id), {
    reportDateKey: "2026-11-02",
    projectSlug: "docksteader",
    reportType: "journal",
    audience: "management",
    variant: "procore",
  });
  assert.equal(parseManagementJournalQueueId("2026-11-02__docksteader__journal__management__other"), null);
  assert.equal(parseManagementJournalQueueId("2026-11-02__docksteader__journal__management__procore__x"), null);

  const context = resolveManagementJournalDeliveryContext(id, { ...queueData("docksteader"), reportDateKey: "2026-11-02", variant: "procore" });
  assert.equal(context.variant, "procore");
  assert.throws(
    () => resolveManagementJournalDeliveryContext(id, { ...queueData("docksteader"), reportDateKey: "2026-11-02" }),
    /variant mismatch/
  );
  assert.throws(
    () => resolveManagementJournalDeliveryContext("2026-08-16__docksteader__journal__management", { ...queueData("docksteader"), variant: "procore" }),
    /variant mismatch/
  );
  assert.equal(resolveManagementJournalDeliveryContext("2026-08-16__docksteader__journal__management", queueData("docksteader")).variant, undefined);
});
