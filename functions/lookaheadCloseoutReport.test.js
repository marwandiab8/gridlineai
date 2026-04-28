const test = require("node:test");
const assert = require("node:assert/strict");

const { classifyLookaheadDelta } = require("./lookaheadCloseoutReport");

test("classifyLookaheadDelta splits previous-week tasks into completed, ongoing, and delayed", () => {
  const previousSnapshot = {
    window: {
      startDateKey: "2026-04-13",
      endDateKey: "2026-05-01",
    },
    tasks: [
      {
        section: "Elevator Core",
        activity: "Install Rebar",
        actionBy: "ALC",
        scheduledDateKeys: ["2026-04-13"],
      },
      {
        section: "Phase 2/3",
        activity: "Excavate H-K 9-16",
        actionBy: "Coreydale",
        scheduledDateKeys: ["2026-04-16", "2026-04-17", "2026-04-20"],
      },
      {
        section: "FW & Piers",
        activity: "Pour Concrete",
        actionBy: "ALC",
        scheduledDateKeys: ["2026-04-16"],
      },
    ],
  };

  const currentParsed = {
    tasks: [
      {
        section: "Phase 2/3",
        activity: "Excavate H-K 9-16",
        actionBy: "Coreydale",
        scheduledDateKeys: ["2026-04-20", "2026-04-21"],
      },
      {
        section: "FW & Piers",
        activity: "Pour Concrete",
        actionBy: "ALC",
        scheduledDateKeys: ["2026-04-24"],
      },
    ],
  };

  const model = classifyLookaheadDelta(previousSnapshot, currentParsed);

  assert.equal(model.totalPlanned, 3);
  assert.equal(model.completed.length, 1);
  assert.equal(model.ongoing.length, 1);
  assert.equal(model.delayed.length, 1);
  assert.equal(model.completed[0].label, "Install Rebar");
  assert.equal(model.ongoing[0].label, "Excavate H-K 9-16");
  assert.equal(model.delayed[0].label, "Pour Concrete");
});
