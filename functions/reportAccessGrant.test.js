const { test } = require("node:test");
const assert = require("node:assert/strict");
const { validateReportAccessGrant } = require("./reportAccessGrant");

function validGrant() {
  return {
    collectionName: "adminLabourReports",
    reportId: "admin-report-1",
    storagePath: "adminLabourReports/report-1.pdf",
    expiresAt: { toMillis: () => 2_000 },
  };
}

test("protected labour report grants accept exact unexpired matches", () => {
  assert.deepEqual(validateReportAccessGrant({
    grant: validGrant(),
    collectionName: "adminLabourReports",
    reportId: "admin-report-1",
    storagePath: "adminLabourReports/report-1.pdf",
    nowMs: 1_000,
  }), { valid: true, reason: null, expiresAtMs: 2_000 });
});

test("protected labour report grants reject missing, expired, and mismatched tokens", () => {
  assert.equal(validateReportAccessGrant({
    grant: null,
    collectionName: "adminLabourReports",
    reportId: "admin-report-1",
    storagePath: "adminLabourReports/report-1.pdf",
    nowMs: 1_000,
  }).reason, "missing");
  assert.equal(validateReportAccessGrant({
    grant: validGrant(),
    collectionName: "adminLabourReports",
    reportId: "admin-report-1",
    storagePath: "adminLabourReports/report-1.pdf",
    nowMs: 2_001,
  }).reason, "expired");
  for (const patch of [
    { collectionName: "labourReports" },
    { reportId: "other-report" },
    { storagePath: "adminLabourReports/other.pdf" },
  ]) {
    assert.equal(validateReportAccessGrant({
      grant: { ...validGrant(), ...patch },
      collectionName: "adminLabourReports",
      reportId: "admin-report-1",
      storagePath: "adminLabourReports/report-1.pdf",
      nowMs: 1_000,
    }).reason, "mismatched");
  }
});

test("existing grant policy permits safe reuse until expiry", () => {
  const input = {
    grant: validGrant(),
    collectionName: "adminLabourReports",
    reportId: "admin-report-1",
    storagePath: "adminLabourReports/report-1.pdf",
    nowMs: 1_000,
  };
  assert.equal(validateReportAccessGrant(input).valid, true);
  assert.equal(validateReportAccessGrant(input).valid, true);
});
