const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  ADMIN_LABOUR_REPORT_COLLECTION,
  buildAdminLabourPdfModel,
  canAccessAdminLabourReportMetadata,
  formatTorontoTimestamp,
  generateAdminLabourReportPdf,
} = require("./labourAdminReportPdf");

test("administrator report metadata uses a client-denied collection and exact admin access", () => {
  assert.equal(ADMIN_LABOUR_REPORT_COLLECTION, "adminLabourReports");
  assert.equal(canAccessAdminLabourReportMetadata({ role: "admin" }), true);
  assert.equal(canAccessAdminLabourReportMetadata({ role: "management", allProjects: true }), false);
  assert.equal(canAccessAdminLabourReportMetadata({ role: "viewer" }), false);
  assert.equal(canAccessAdminLabourReportMetadata(null), false);
});

function resultFixture() {
  return {
    request: {
      projectSlug: null,
      projectName: null,
      periodLabel: "Current pay period",
      startKey: "2026-08-15",
      endKey: "2026-08-28",
    },
    totalMinutes: 1020,
    workerCount: 2,
    entryCount: 2,
    projectCount: 2,
    excludedCount: 1,
    excludedReasons: { invalid_minutes: 1 },
    auditFlags: { contradictory_legacy_hours_ignored: 1 },
    sections: [
      {
        projectSlug: "docksteader",
        projectName: "Docksteader",
        totalMinutes: 510,
        workerCount: 1,
        entryCount: 1,
        workerTotals: [{ workerId: "worker-1", workerName: "Worker One", totalMinutes: 510 }],
        dayTotals: [{ reportDateKey: "2026-08-15", totalMinutes: 510 }],
        workerDayTotals: [{ workerId: "worker-1", workerName: "Worker One", reportDateKey: "2026-08-15", totalMinutes: 510 }],
      },
      {
        projectSlug: "other-project",
        projectName: "Other Project",
        totalMinutes: 510,
        workerCount: 1,
        entryCount: 1,
        workerTotals: [{ workerId: "worker-2", workerName: "Worker Two", totalMinutes: 510 }],
        dayTotals: [{ reportDateKey: "2026-08-16", totalMinutes: 510 }],
        workerDayTotals: [{ workerId: "worker-2", workerName: "Worker Two", reportDateKey: "2026-08-16", totalMinutes: 510 }],
      },
    ],
  };
}

test("PDF model carries canonical totals and isolated project sections", () => {
  const result = resultFixture();
  const model = buildAdminLabourPdfModel(result, new Date("2026-08-17T16:00:00.000Z"));
  assert.equal(model.totalMinutes, 1020);
  assert.equal(model.totalHours, "17");
  assert.equal(model.totalHoursMinutes, "17h 0m");
  assert.deepEqual(model.sections.map((section) => section.projectSlug), ["docksteader", "other-project"]);
  assert.equal(model.sections[0].totalMinutes + model.sections[1].totalMinutes, model.totalMinutes);
  assert.match(model.sourceStatement, /current canonical labourEntries/);
});

test("Toronto generation timestamp is deterministic and zone-labelled", () => {
  assert.match(
    formatTorontoTimestamp(new Date("2026-08-17T16:00:00.000Z")),
    /Aug 17, 2026, 12:00 p\.m\. EDT/
  );
});

test("administrator PDF is stored privately without a Firebase download token", async () => {
  const calls = [];
  const bucket = {
    file(path) {
      return {
        async save(bytes, options) {
          calls.push({ path, bytes, options });
        },
      };
    },
  };
  const path = `adminLabourReports/${"a".repeat(40)}.pdf`;
  const generated = await generateAdminLabourReportPdf({
    result: resultFixture(),
    generatedAt: new Date("2026-08-17T16:00:00.000Z"),
    storageBucket: bucket,
    storagePath: path,
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].path, path);
  assert.equal(calls[0].bytes.subarray(0, 4).toString("ascii"), "%PDF");
  assert.equal(calls[0].options.metadata.cacheControl, "private, max-age=0, no-store");
  assert.equal(
    JSON.stringify(calls[0].options).includes("firebaseStorageDownloadTokens"),
    false
  );
  assert.equal(generated.model.totalMinutes, resultFixture().totalMinutes);
});

test("administrator PDF rejects paths outside its private deterministic namespace", async () => {
  await assert.rejects(
    generateAdminLabourReportPdf({
      result: resultFixture(),
      storageBucket: { file() { throw new Error("must not be called"); } },
      storagePath: "public/report.pdf",
    }),
    /storage path is invalid/
  );
});
