const assert = require("node:assert/strict");
const test = require("node:test");
const {
  formatDateIdInTimeZone,
  resolveDateId,
} = require("./date");
const {
  mapJournalEntryToTimeLeft,
  mapMediaToTimeLeft,
  mapProjectRecordToTimeLeft,
  mapReportToTimeLeft,
} = require("./mappers");

test("formats dateId without UTC day shifting", () => {
  assert.equal(formatDateIdInTimeZone("2028-07-25", "America/Toronto"), "2028-07-25");
  assert.equal(formatDateIdInTimeZone("2028-07-25T02:00:00.000Z", "America/Toronto"), "2028-07-24");
});

test("omits dateId when no reliable date exists", () => {
  assert.equal(resolveDateId({ updatedAt: "2028-07-25T10:00:00.000Z" }), "");
});

test("maps report with stable idempotency fields", () => {
  const item = mapReportToTimeLeft({
    projectId: "docksteader",
    projectName: "Docksteader",
    reportDateKey: "2028-07-25",
    reportTitle: "Daily Report",
    storagePath: "dailyReports/r1.pdf",
    createdAt: "2028-07-25T11:30:00.000Z",
  }, { id: "r1", path: "dailyReports/r1", sourceFirebaseProjectId: "gridlineai" });
  assert.equal(item.sourceApp, "gridlineai");
  assert.equal(item.category, "projectReport");
  assert.equal(item.dateId, "2028-07-25");
  assert.equal(item.sourceDocumentPath, "dailyReports/r1");
  assert.equal(item.sourceStoragePath, "dailyReports/r1.pdf");
  assert.equal(item.addedAt, "2028-07-25T11:30:00.000Z");
});

test("maps report sourceUrl to the app report deeplink", () => {
  const item = mapReportToTimeLeft({
    projectId: "home",
    reportDateKey: "2028-07-25",
  }, {
    appBaseUrl: "https://gridlineai.web.app/",
    id: "report123",
    path: "dailyReports/report123",
  });

  assert.equal(
    item.sourceUrl,
    "https://gridlineai.web.app/?view=reports&reportId=report123&openPdf=1"
  );
});

test("maps journal entry", () => {
  const item = mapJournalEntryToTimeLeft({
    projectId: "docksteader",
    dateKey: "2028-07-25",
    rawText: "Crew completed framing.",
  }, { id: "l1", path: "logEntries/l1" });
  assert.equal(item.category, "journalEntry");
  assert.equal(item.summary, "Crew completed framing.");
  assert.equal(item.sourceDocumentPath, "logEntries/l1");
});

test("maps uploaded image and file categories", () => {
  const image = mapMediaToTimeLeft({
    projectId: "docksteader",
    reportDateKey: "2028-07-25",
    contentType: "image/jpeg",
    storagePath: "projects/docksteader/media/2028-07-25/msg/image-0.jpg",
  }, { id: "m1", path: "media/m1" });
  const file = mapMediaToTimeLeft({
    projectId: "docksteader",
    reportDateKey: "2028-07-25",
    contentType: "application/pdf",
    storagePath: "projects/docksteader/media/2028-07-25/msg/spec.pdf",
  }, { id: "m2", path: "media/m2" });
  assert.equal(image.category, "image");
  assert.equal(file.category, "file");
  assert.equal(image.sourceStoragePath, "projects/docksteader/media/2028-07-25/msg/image-0.jpg");
});

test("maps media library records with upload date and safe project fallback", () => {
  const item = mapMediaToTimeLeft({
    createdAt: "2028-07-25T14:00:00.000Z",
    fileName: "site-photo.jpg",
    storagePath: "tools/media/site-photo.jpg",
  }, {
    id: "m3",
    path: "media/m3",
    timeZone: "America/Toronto",
  });
  assert.equal(item.category, "image");
  assert.equal(item.dateId, "2028-07-25");
  assert.equal(item.sourceProjectId, "gridlineai");
});

test("maps project record", () => {
  const item = mapProjectRecordToTimeLeft({
    name: "Docksteader",
    location: "Vancouver",
    createdAt: "2028-07-25T10:00:00.000Z",
  }, { id: "docksteader", path: "projects/docksteader" });
  assert.equal(item.category, "projectRecord");
  assert.equal(item.sourceProjectId, "docksteader");
  assert.equal(item.sourceDocumentPath, "projects/docksteader");
});
