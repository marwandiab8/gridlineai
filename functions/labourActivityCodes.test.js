const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");

const {
  LABOUR_ACTIVITIES,
  LABOUR_CATEGORIES,
  describeLabourLinesShort,
  normalizeLabourLines,
  suggestLabourActivityCode,
} = require("./labourActivityCodes");
const { buildLabourLinesFromWorkOn, labourLinesForEntry, parseLabourHoursCommand } = require("./labourRepository");
const { buildCodedLabourReport, pdfSafe, renderCodedLabourReportPdf } = require("./labourCodedReportPdf");
const { readFormLines, workOnFromLines } = require("./labourPortal");

const sum = (lines) => lines.reduce((total, line) => total + line.minutes, 0);

test("codes say what the work was for, and unclear text is left for the supervisor", () => {
  assert.equal(suggestLabourActivityCode("build hoarding with lumber and tarps at curtain wall"), "WH-HOARD");
  assert.equal(suggestLabourActivityCode("prepairing wood for winter protection"), "WH-HOARD");
  assert.equal(suggestLabourActivityCode("rough carpentry for winter heat"), "WH-HOARD");
  assert.equal(suggestLabourActivityCode("rough carpentry safety railing"), "GC-CARP");
  assert.equal(suggestLabourActivityCode("Re And Re / Safety Rail"), "GC-RAIL");
  assert.equal(suggestLabourActivityCode("shovel snow"), "WH-SNOW");
  assert.equal(suggestLabourActivityCode("Setup pumps to remove melted snow and ice"), "WH-PUMP");
  assert.equal(suggestLabourActivityCode("refuel heaters"), "WH-FUEL");
  assert.equal(suggestLabourActivityCode("House Keeping"), "GC-HOUSE");
  assert.equal(suggestLabourActivityCode("General labouring"), "OT-GEN");
  // Could be winter heat or safety: the supervisor decides.
  assert.equal(suggestLabourActivityCode("Rough Carpentry"), null);
  // Pumping is only Winter Heat when it is melted snow or ice.
  assert.equal(suggestLabourActivityCode("pumping water"), null);
  // Two different activities in one line.
  assert.equal(suggestLabourActivityCode("general labor with removing and installing safety railing"), null);
  assert.equal(suggestLabourActivityCode("Supervision"), null);
});

test("lines from a texted breakdown always add up to the entry's hours", () => {
  const lines = buildLabourLinesFromWorkOn(
    "5 hours prepairing wood for winter protection\n\n1 hour installing winter protection\n\n3 hours general labor",
    540,
  );
  assert.deepEqual(lines.map((l) => [l.code, l.minutes]), [["WH-HOARD", 300], ["WH-HOARD", 60], ["OT-GEN", 180]]);
  assert.equal(lines[0].codedBy, "auto");

  // The text accounts for 8 of 9 hours: the last hour is kept, uncoded, rather than lost.
  const short = buildLabourLinesFromWorkOn("2h housekeeping - 6h general labour", 540);
  assert.equal(sum(short), 540);
  assert.deepEqual(short[2], { code: null, minutes: 60, text: "Not described", codedBy: null });

  // Parts adding up to more than the total can't be trusted: one uncoded line with the whole text.
  const over = buildLabourLinesFromWorkOn("6h housekeeping - 6h general labour", 540);
  assert.equal(over.length, 1);
  assert.equal(over[0].minutes, 540);

  const single = buildLabourLinesFromWorkOn("prep floor protection", 540);
  assert.deepEqual(single, [{ code: null, minutes: 540, text: "prep floor protection", codedBy: null }]);
});

test("an SMS entry is coded the same way it is saved", () => {
  const parsed = parseLabourHoursCommand("9h 3h hoarding at curtain wall - 2h shovel snow - 4h housekeeping");
  assert.ok(parsed);
  const lines = buildLabourLinesFromWorkOn(parsed.workOn, Math.round(parsed.hours * 60));
  assert.deepEqual(lines.map((l) => l.code), ["WH-HOARD", "WH-SNOW", "GC-HOUSE"]);
  assert.equal(describeLabourLinesShort(lines), "Winter Heat 5h, General Conditions 4h");
  assert.equal(describeLabourLinesShort([{ code: null, minutes: 90 }]), "1.5h for the supervisor to code");
});

test("reviewed lines must be coded to approve and must add up to the entry", () => {
  const ok = normalizeLabourLines([{ code: "wh-snow", hours: 2 }, { code: "GC-HOUSE", hours: 7, text: " north  side " }], { minutesWorked: 540, requireCodes: true });
  assert.deepEqual(ok, [
    { code: "WH-SNOW", minutes: 120, text: "", codedBy: "supervisor" },
    { code: "GC-HOUSE", minutes: 420, text: "north side", codedBy: "supervisor" },
  ]);
  assert.throws(() => normalizeLabourLines([{ code: "GC-HOUSE", hours: 8 }], { minutesWorked: 540 }), /add up to 8h but the entry is 9h/);
  assert.throws(() => normalizeLabourLines([{ code: "", hours: 9 }], { minutesWorked: 540, requireCodes: true }), /choose an activity/);
  assert.throws(() => normalizeLabourLines([{ code: "XX-1", hours: 9 }], { minutesWorked: 540 }), /unknown activity code/);
  assert.throws(() => normalizeLabourLines([{ code: "WH-OTHER", hours: 9 }], { minutesWorked: 540 }), /say what/);
  // Saving without approval may leave lines uncoded.
  assert.equal(normalizeLabourLines([{ code: "", hours: 9 }], { minutesWorked: 540 })[0].code, null);
});

test("the web form turns its rows into coded lines and a readable work text", () => {
  const rows = readFormLines({ line1Code: "WH-HOARD", line1Hours: "5", line1Note: "east windows", line2Code: "GC-HOUSE", line2Hours: "3" });
  assert.equal(rows.length, 6);
  const filled = rows.filter((r) => r.code || r.hours || r.note);
  const lines = normalizeLabourLines(filled.map((r) => ({ code: r.code, hours: r.hours, text: r.note })), { requireCodes: true, codedBy: "labourer" });
  assert.equal(workOnFromLines(lines), "5h Hoarding / winter protection (east windows) - 3h Site housekeeping");
});

test("the browser's code list matches the server's", async () => {
  const browser = await import(pathToFileURL(path.resolve(__dirname, "../public/labour-activity-codes.js")).href);
  assert.deepEqual(browser.LABOUR_CATEGORIES, LABOUR_CATEGORIES.map(({ id, label, chargeable, note }) => ({ id, label, chargeable, note })));
  assert.deepEqual(browser.LABOUR_ACTIVITIES, LABOUR_ACTIVITIES.map(({ code, category, label, description }) => ({ code, category, label, description })));
  assert.equal(new Set(LABOUR_ACTIVITIES.map((a) => a.code)).size, LABOUR_ACTIVITIES.length);
});

test("the activity report bills approved, fully coded entries only", async () => {
  const approved = { status: "approved" };
  const entries = [
    {
      labourerName: "Kevin Ashdown", labourerPhone: "+1", reportDateKey: "2026-11-02", minutesWorked: 540, review: approved,
      lines: [{ code: "WH-HOARD", minutes: 330, text: "east windows 😀" }, { code: "GC-HOUSE", minutes: 60, text: "" }, { code: "OT-GEN", minutes: 150, text: "" }],
    },
    { labourerName: "Shawn Jones", reportDateKey: "2026-11-02", minutesWorked: 600, review: { status: "pending" }, lines: [{ code: "WH-SNOW", minutes: 600 }] },
    { labourerName: "Joseph Diab", reportDateKey: "2026-11-01", minutesWorked: 480, review: approved, lines: [{ code: null, minutes: 480 }] },
    { labourerName: "Wael Ibrahim", reportDateKey: "2026-11-01", minutesWorked: 480, review: approved, lines: [{ code: "WH-FUEL", minutes: 420 }] },
  ];
  const report = buildCodedLabourReport(entries);
  assert.deepEqual(report.sheets.map((s) => s.labourer), ["Kevin Ashdown"]);
  assert.equal(report.totals.get("winter-heat"), 330);
  assert.equal(report.totals.get("general-conditions"), 60);
  assert.equal(report.totalMinutes, 540);
  // Not approved, approved but uncoded, and lines that don't add up are all left out and listed.
  assert.deepEqual(report.pending.map((p) => p.labourer), ["Joseph Diab", "Wael Ibrahim", "Shawn Jones"]);
  assert.deepEqual(report.sheets[0].rows[0], {
    category: "Winter Heat", activity: "Hoarding / winter protection", code: "WH-HOARD",
    description: LABOUR_ACTIVITIES[0].description, note: "east windows 😀", minutes: 330,
  });

  const bytes = await renderCodedLabourReportPdf(report, { rangeLabel: "2026-11-02", supervisor: "Marwan Diab" });
  assert.ok(Buffer.from(bytes).subarray(0, 5).toString() === "%PDF-");
  assert.equal(pdfSafe("east “windows” – 😀"), 'east "windows" - ');
});

test("entries saved before activity codes are coded from their text when reported", () => {
  const lines = labourLinesForEntry({ workOn: "7 hours general labor\n\n2 hours housekeeping", minutesWorked: 540 });
  assert.deepEqual(lines.map((l) => l.code), ["OT-GEN", "GC-HOUSE"]);
});
