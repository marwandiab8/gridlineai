const test = require("node:test");
const assert = require("node:assert/strict");

const { buildActivitiesReportModel } = require("./lookaheadActivitiesPdf");

test("buildActivitiesReportModel organizes sections and milestone summaries", () => {
  const model = buildActivitiesReportModel({
    companyName: "Matheson",
    projectName: "Docksteader Paramedic Station",
    window: {
      startDateKey: "2026-04-20",
      endDateKey: "2026-04-24",
    },
    tasks: [
      {
        section: "FW & Piers",
        activity: "Install Forms",
        actionBy: "ALC",
        scheduledDateKeys: ["2026-04-20", "2026-04-21", "2026-04-22"],
      },
      {
        section: "FW & Piers",
        activity: "Pour Concrete - Balance Phase 1 FW & Piers",
        actionBy: "ALC",
        scheduledDateKeys: ["2026-04-24"],
      },
      {
        section: "Structural Steel Phase 1",
        activity: "Start Erecting Steel at Stair D",
        actionBy: "SteelCon",
        scheduledDateKeys: ["2026-04-22"],
      },
      {
        section: "Balance Phase 1 - Waterproofing & Backfills",
        activity: "Install Waterproofing Line 4 between L and O",
        actionBy: "SRWP",
        scheduledDateKeys: ["2026-04-20", "2026-04-21"],
      },
    ],
  });

  assert.equal(model.companyName, "Matheson");
  assert.match(model.activityHeadline, /Steel Erection Begins/);
  assert.match(model.activityHeadline, /Critical Concrete Pours/);
  assert.equal(model.progressLine, "0 / 4 Tasks (0%)");
  assert.equal(model.daysLine, "0 / 4 days (0%)");
  assert.equal(model.criticalPathItems.length, 1);
  assert.match(model.criticalPathItems[0], /Pour Concrete/);
  assert.ok(model.sections.some((section) => section.section === "FW & Piers"));
  assert.ok(model.sections.some((section) => section.section === "Structural Steel Phase 1"));
  assert.ok(model.weekGoalItems.includes("2 ALC tasks"));
  assert.ok(
    model.coordinationItems.some((item) => /Steel erection starts/i.test(item)),
    "expected steel coordination note"
  );
  assert.ok(
    model.coordinationItems.some((item) => /Waterproofing drives follow-on work/i.test(item)),
    "expected waterproofing coordination note"
  );
});
