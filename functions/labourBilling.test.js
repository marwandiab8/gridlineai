const test = require("node:test");
const assert = require("node:assert/strict");
const ExcelJS = require("exceljs");

const { buildWinterHeatWorkbook, publishLabourDailySummary, withdrawLabourDailySummary } = require("./labourBilling");

const approved = (byEmail = "boss@example.com", byName) => ({ status: "approved", byEmail, ...(byName ? { byName } : {}), at: new Date("2026-10-03T13:00:00Z") });

const ENTRIES = [
  {
    id: "shawn-1002", labourerName: "Shawn Jones", reportDateKey: "2026-10-02", projectSlug: "docksteader", minutesWorked: 540, review: approved(), workOn: "",
    lines: [{ code: "WH-HOARD-PREP", minutes: 300, text: "east side" }, { code: "WH-HOARD-LIFT", minutes: 60, text: "east windows", location: "Z1 East Wing - L2" }, { code: "OT-GEN", minutes: 180, text: "" }],
  },
  {
    id: "kevin-1001", labourerName: "Kevin Ashdown", reportDateKey: "2026-10-01", projectSlug: "docksteader", minutesWorked: 540, review: approved(), workOn: "",
    lines: [{ code: "OT-MAT", minutes: 180, text: "Rebar" }, { code: "GC-PROTECT", minutes: 300, text: "stairs" }, { code: "OT-GEN", minutes: 60, text: "" }],
  },
  {
    id: "kevin-1002", labourerName: "Kevin Ashdown", reportDateKey: "2026-10-02", projectSlug: "docksteader", minutesWorked: 600, review: approved(), workOn: "",
    lines: [{ code: "WH-SNOW", minutes: 120, text: "" }, { code: "WH-FUEL", minutes: 90, text: "" }, { code: "GC-HOUSE", minutes: 390, text: "" }],
  },
  // Waiting for review: listed separately, counted nowhere.
  { id: "shawn-1003", labourerName: "Shawn Jones", reportDateKey: "2026-10-03", projectSlug: "docksteader", minutesWorked: 540, review: { status: "pending" }, workOn: "9h snow", lines: [{ code: "WH-SNOW", minutes: 540, text: "" }] },
  // Before the season, and an entry from before activity codes: neither is in the workbook.
  { id: "old", labourerName: "Shawn Jones", reportDateKey: "2026-09-30", minutesWorked: 540, review: approved(), lines: [{ code: "WH-SNOW", minutes: 540 }] },
  { id: "precode", labourerName: "Kevin Ashdown", reportDateKey: "2026-10-01", minutesWorked: 540, workOn: "9h general labour" },
];

async function load(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  return wb;
}

test("the Winter Heat workbook totals approved Winter Heat hours by day and labourer, as traceable formulas", async () => {
  const built = await buildWinterHeatWorkbook(ENTRIES, { projectSlug: "docksteader", seasonStartKey: "2026-10-01", now: new Date("2026-10-04T18:00:00Z") });
  assert.equal(built.winterHeatHours, 9.5); // Shawn 5 + 1, Kevin 2 + 1.5
  assert.equal(built.approvedEntries, 3);
  assert.equal(built.pendingEntries, 1);
  assert.equal(built.days, 2);

  const wb = await load(built.buffer);
  assert.deepEqual(wb.worksheets.map((s) => s.name), ["Daily Winter Heat", "By Labourer", "Detail", "Waiting for Review"]);

  const daily = wb.getWorksheet("Daily Winter Heat");
  assert.deepEqual(daily.getRow(4).values.slice(1), ["Date", "Day", "Kevin Ashdown", "Shawn Jones", "Day total", "Running total"]);
  const value = (cell) => (cell.value && typeof cell.value === "object" && "result" in cell.value ? cell.value.result : cell.value);
  // A formula whose value is 0 is saved without a cached result (the file recalculates on open).
  const row = (n) => daily.getRow(n).values.slice(1).map((v) => (v && typeof v === "object" && "formula" in v ? (v.result ?? 0) : v));
  assert.deepEqual(row(5), ["2026-10-01", "Thu", 0, 0, 0, 0]);
  assert.deepEqual(row(6), ["2026-10-02", "Fri", 3.5, 6, 9.5, 9.5]);
  assert.deepEqual(row(7), ["Total", "", 3.5, 6, 9.5, ""]);
  assert.match(daily.getCell("C6").value.formula, /^SUMIFS\(Detail!\$K\$2:\$K\$10,Detail!\$A\$2:\$A\$10,\$A6,Detail!\$B\$2:\$B\$10,C\$4,Detail!\$C\$2:\$C\$10,"Winter Heat"\)$/);
  assert.equal(daily.getCell("F6").value.formula, "F5+E6");
  assert.equal(value(daily.getCell("E6")), 9.5);

  const detail = wb.getWorksheet("Detail");
  assert.equal(detail.rowCount, 10); // header + 9 lines
  assert.deepEqual(detail.getRow(1).values.slice(1, 12), ["Date", "Labourer", "Category", "Extra", "Keyword", "Code", "Activity", "Description", "Location", "Note", "Hours"]);
  const lift = detail.getRows(2, 9).find((r) => r.getCell(6).value === "WH-HOARD-LIFT");
  assert.equal(lift.getCell(5).value, "hoarding lift");
  assert.equal(lift.getCell(4).value, "Yes");
  assert.match(lift.getCell(8).value, /47 ft scissor lift/);
  assert.equal(lift.getCell(9).value, "Z1 East Wing - L2");
  assert.equal(lift.getCell(10).value, "east windows");
  assert.equal(lift.getCell(11).value, 1);
  assert.equal(lift.getCell(12).value, "boss@example.com", "no name known: the email");

  const byLabourer = wb.getWorksheet("By Labourer");
  assert.deepEqual(byLabourer.getRow(2).values.slice(1).map((v) => (v && typeof v === "object" ? v.result : v)), ["Kevin Ashdown", 3.5, 11.5, 4, 19]);

  const waiting = wb.getWorksheet("Waiting for Review");
  assert.deepEqual(waiting.getRow(2).values.slice(1), ["2026-10-03", "Shawn Jones", 9, "9h snow", "shawn-1003"]);
  assert.equal(waiting.rowCount, 2);
});

test("the season starts on the day the extra work started (Oct 2), so earlier days aren't in the workbook", async () => {
  const built = await buildWinterHeatWorkbook(ENTRIES, { projectSlug: "docksteader" });
  const wb = await load(built.buffer);
  assert.equal(wb.getWorksheet("Daily Winter Heat").getCell("A5").value, "2026-10-02");
  assert.equal(built.approvedEntries, 2);
  assert.match(wb.getWorksheet("Daily Winter Heat").getCell("A2").value, /^Approved hours from 2026-10-02\./);
});

test("Approved by shows the approver's name, from the review or from earlier Daily Summaries", async () => {
  const entries = [
    { ...ENTRIES[0], review: approved("marwandiab8@gmail.com", "Marwan Diab") },
    { ...ENTRIES[2], review: approved("Marwandiab8@gmail.com") },
  ];
  const built = await buildWinterHeatWorkbook(entries, { projectSlug: "docksteader", approverNames: { "marwandiab8@gmail.com": "Marwan Diab" } });
  const detail = (await load(built.buffer)).getWorksheet("Detail");
  const names = new Set(detail.getRows(2, detail.rowCount - 1).map((r) => r.getCell(12).value));
  assert.deepEqual([...names], ["Marwan Diab"]);
});

test("an empty season still produces a readable workbook", async () => {
  const built = await buildWinterHeatWorkbook([], { projectSlug: "docksteader" });
  const wb = await load(built.buffer);
  assert.equal(wb.getWorksheet("Daily Winter Heat").getCell("A5").value, "No approved hours yet.");
});

function fakeStore() {
  const docs = new Map();
  const files = new Map();
  const db = {
    collection: () => ({
      doc: (id) => ({
        get: async () => ({ exists: docs.has(id), get: (k) => (docs.get(id) || {})[k] }),
        set: async (data) => { docs.set(id, data); },
        delete: async () => { docs.delete(id); },
      }),
    }),
  };
  const bucket = {
    file: (path) => ({
      save: async (data, options) => { files.set(path, { data, options }); },
      delete: async () => { files.delete(path); },
    }),
  };
  return { docs, files, db, bucket, FieldValue: { serverTimestamp: () => "now" } };
}

test("approving stores the day's Daily Summary; un-approving withdraws it", async () => {
  const store = fakeStore();
  const record = await publishLabourDailySummary({ ...store, entryId: "shawn-1002", entry: ENTRIES[0], supervisor: "Marwan Diab", approvedByEmail: "boss@example.com" });
  assert.equal(record.reportFileName, "Log_Shawn_Jones_2026-10-02.pdf");
  assert.equal(record.winterHeatHours, 6);
  assert.deepEqual(record.hoursByCategory, { "winter-heat": 6, "general-conditions": 0, other: 3 });
  const saved = store.files.get(record.storagePath);
  assert.equal(saved.options.contentType, "application/pdf");
  assert.equal(Buffer.from(saved.data).subarray(0, 5).toString(), "%PDF-");
  assert.equal(store.docs.get("daily-shawn-1002").type, "labourDailySummary");

  assert.equal(await withdrawLabourDailySummary({ ...store, entryId: "shawn-1002" }), true);
  assert.equal(store.docs.size, 0);
  assert.equal(store.files.size, 0);

  await assert.rejects(
    publishLabourDailySummary({ ...store, entryId: "shawn-1003", entry: ENTRIES[3] }),
    /not approved and fully coded/,
  );
});
