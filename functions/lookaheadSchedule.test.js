const { test } = require("node:test");
const assert = require("node:assert/strict");
const ExcelJS = require("exceljs");
const {
  inferDateColumns,
  parseLookaheadWorksheet,
  parseCliArgs,
} = require("./lookaheadSchedule");

function buildWorksheet() {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("3 Weeks Look Ahead Schedule");

  ws.getRow(1).values = [
    "Schedule",
    "",
    "",
    "",
    "",
    "",
    "Monday",
    "Tuesday",
    "Wednesday",
  ];
  ws.getRow(3).values = [
    "Activities",
    "Condition keep hidden",
    "Action By",
    "Duration",
    "Start",
    "Finish",
    new Date("2026-04-20T00:00:00Z"),
    new Date("2026-04-21T00:00:00Z"),
    new Date("2026-04-22T00:00:00Z"),
  ];
  ws.getRow(4).values = ["Activities", "", "Action By", "Duration", "Start", "Finish", "M", "T", "W"];

  ws.getRow(5).getCell(1).value = "Toe Walls";

  ws.getRow(6).values = [
    "Install forms",
    "",
    "ALC",
    3,
    new Date("2026-04-20T00:00:00Z"),
    new Date("2026-04-22T00:00:00Z"),
    { formula: "x", result: "l" },
    { formula: "x", result: "l" },
    { formula: "x", result: "n" },
  ];

  ws.getRow(7).values = [
    "Pour concrete",
    "",
    "ALC",
    1,
    new Date("2026-04-22T00:00:00Z"),
    new Date("2026-04-22T00:00:00Z"),
    "",
    "",
    { formula: "x", result: "n" },
  ];
  ws.getRow(7).hidden = true;

  ws.getRow(8).values = [
    "Completed task",
    "complete",
    "Coreydale",
    2,
    new Date("2026-04-20T00:00:00Z"),
    new Date("2026-04-21T00:00:00Z"),
    { formula: "x", result: "l" },
    { formula: "x", result: "n" },
    "",
  ];

  ws.getRow(9).values = [
    "Old task",
    "",
    "Matheson",
    2,
    new Date("2026-04-01T00:00:00Z"),
    new Date("2026-04-05T00:00:00Z"),
    "",
    "",
    "",
  ];

  return ws;
}

test("inferDateColumns reads the schedule window from row 3", () => {
  const ws = buildWorksheet();
  const columns = inferDateColumns(ws);
  assert.deepEqual(
    columns.map((item) => item.dateKey),
    ["2026-04-20", "2026-04-21", "2026-04-22"]
  );
});

test("parseLookaheadWorksheet groups tasks under the latest section and keeps hidden rows by default", () => {
  const ws = buildWorksheet();
  const parsed = parseLookaheadWorksheet(ws);
  assert.equal(parsed.taskCount, 2);
  assert.equal(parsed.tasks[0].section, "Toe Walls");
  assert.equal(parsed.tasks[0].activity, "Install forms");
  assert.deepEqual(parsed.tasks[0].scheduledDateKeys, [
    "2026-04-20",
    "2026-04-21",
    "2026-04-22",
  ]);
  assert.equal(parsed.tasks[1].hidden, true);
});

test("parseLookaheadWorksheet can exclude hidden and completed rows", () => {
  const ws = buildWorksheet();
  const parsed = parseLookaheadWorksheet(ws, {
    includeHidden: false,
    includeCompleted: false,
  });
  assert.equal(parsed.taskCount, 1);
  assert.equal(parsed.tasks[0].activity, "Install forms");
});

test("parseCliArgs supports json and window flags", () => {
  const args = parseCliArgs([
    "/tmp/file.xlsx",
    "--json",
    "--visible-only",
    "--start",
    "2026-04-21",
    "--end",
    "2026-04-22",
  ]);
  assert.deepEqual(args, {
    filePath: "/tmp/file.xlsx",
    format: "json",
    includeHidden: false,
    includeCompleted: false,
    startDateKey: "2026-04-21",
    endDateKey: "2026-04-22",
    companyName: "",
    projectName: "",
  });
});
