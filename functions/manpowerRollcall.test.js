/**
 * Manpower roll-call parsing for daily PDF tables.
 * Run: npm test (from functions/)
 */

const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  parseManpowerRollcallLine,
  textContainsManpowerRollcall,
  tailAfterManpowerRollcall,
} = require("./manpowerRollcall");
const { buildDailyReportModel } = require("./dailyReportContent");

test("parses Docksteader-style preamble + roll call + tail", () => {
  const s =
    "Project Docksteader Monday April 06 2026 Manpower ALC 20 Matheson 7 MSB 4 Road-Ex 7 Coreydale 3 Structural Roofing 3 O'Connor 2 ALC working on line O between line 1 and line 4.";
  const pairs = parseManpowerRollcallLine(s);
  assert.equal(pairs.length, 7);
  assert.deepEqual(pairs[0], { trade: "ALC", workers: "20" });
  assert.deepEqual(pairs[6], { trade: "O'Connor", workers: "2" });
  assert.ok(tailAfterManpowerRollcall(s).toLowerCase().includes("working on line"));
  assert.equal(textContainsManpowerRollcall(s), true);
});

test("AI-extracted manpower rows take precedence over deterministic parsing", () => {
  const entries = [
    {
      id: "ai1",
      projectSlug: "docksteader",
      category: "journal",
      rawText: "Loose wording about crews on site without ALC 20 style tokens.",
      normalizedText: "Loose wording about crews on site without ALC 20 style tokens.",
      includeInDailySummary: true,
      aiEnhanced: true,
      aiReportExtract: {
        messageIntent: "Site-wide manpower snapshot.",
        manpowerRows: [
          ["ALC", "—", "20", "Formwork and stripping"],
          ["Matheson", "—", "7", "Pumping"],
        ],
      },
      dailySummarySections: ["dayLog", "journal", "manpower"],
    },
  ];
  const model = buildDailyReportModel(entries, [], { dayStart: new Date("2026-04-06T12:00:00Z") });
  const mrows = model.deterministic.manpowerRows;
  assert.equal(mrows.length, 2);
  assert.equal(mrows[0][0], "ALC");
  assert.equal(mrows[0][2], "20");
  assert.ok(model.deterministic.manpowerNarrative.toLowerCase().includes("snapshot"));
});

test("daily report model expands one manpower entry into multiple trade rows", () => {
  const entries = [
    {
      id: "1",
      projectSlug: "docksteader",
      category: "journal",
      rawText:
        "Manpower ALC 20 Matheson 7 MSB 4 Road-Ex 7 Coreydale 3 Structural Roofing 3 O'Connor 2\nALC working on formwork.\nMatheson Labourers pumping water.",
      normalizedText:
        "Manpower ALC 20 Matheson 7 MSB 4 Road-Ex 7 Coreydale 3 Structural Roofing 3 O'Connor 2\nALC working on formwork.\nMatheson Labourers pumping water.",
      includeInDailySummary: true,
      dailySummarySections: ["dayLog", "journal", "manpower"],
    },
  ];
  const model = buildDailyReportModel(entries, [], { dayStart: new Date("2026-04-06T12:00:00Z") });
  const rows = model.deterministic.manpowerRows;
  assert.equal(rows.length, 7);
  const alc = rows.find((r) => r[0] === "ALC");
  assert.ok(alc);
  assert.equal(alc[2], "20");
  assert.ok(String(alc[3]).toLowerCase().includes("formwork"));
  const mat = rows.find((r) => r[0] === "Matheson");
  assert.ok(String(mat[3]).toLowerCase().includes("pumping"));
});
