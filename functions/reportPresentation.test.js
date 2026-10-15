const test = require("node:test");
const assert = require("node:assert/strict");

const {
  groupEquipment,
  isWeatherOnlySummary,
  resolvePeopleNames,
  shortDate,
  shortDateRange,
  siteFactsSummary,
  taskStatusText,
} = require("./reportPresentation");

test("shortDate and shortDateRange read like a schedule", () => {
  assert.equal(shortDate("2026-10-05"), "Oct 5");
  assert.equal(shortDate("2026-10-05", true), "Oct 5, 2026");
  assert.equal(shortDate("soon"), "soon");
  assert.equal(shortDateRange("2026-10-05", "2026-10-05"), "Oct 5");
  assert.equal(shortDateRange("2026-10-05", "2026-10-26"), "Oct 5 - 26");
  assert.equal(shortDateRange("2026-09-28", "2026-10-02"), "Sep 28 - Oct 2");
  assert.equal(shortDateRange("2026-12-30", "2027-01-04"), "Dec 30, 2026 - Jan 4, 2027");
  assert.equal(shortDateRange("", "2026-10-02"), "Oct 2");
});

test("groupEquipment gives one row per kind, owner and supplier, with off-rent items apart", () => {
  const rows = groupEquipment([
    { type: "Scissor lift", unit: "2041", crew: "Framing", rental: "Sunbelt" },
    { type: "Scissor lift", unit: "2042", crew: "Framing", rental: "Sunbelt" },
    { type: "Scissor lift", unit: "2043", crew: "Framing", rental: "Sunbelt", onRent: false },
    { type: "Bin", name: "Bin 7", unit: "7", crew: "", rental: "" },
    { type: "Bin", name: "7", unit: "7", crew: "", rental: "" },
  ]);
  assert.deepEqual(
    rows.map((r) => [r.type, r.count, r.units.join(", "), r.crew, r.supplier, r.offRent]),
    [
      ["Bin", 2, "#7", "", "", false],
      ["Scissor lift", 2, "#2041, #2042", "Framing", "Sunbelt", false],
      ["Scissor lift", 1, "#2043", "Framing", "Sunbelt", true],
    ]
  );
});

test("taskStatusText says who finished a task and how late an open one is", () => {
  assert.equal(taskStatusText({ status: "done", doneTime: "4:47 p.m.", doneBy: "Kevin Ashdown" }), "Done 4:47 p.m. - Kevin Ashdown");
  assert.equal(taskStatusText({ status: "done" }), "Done");
  assert.equal(taskStatusText({ status: "blocked", note: "gate locked" }), "Could not be done: gate locked");
  assert.equal(taskStatusText({ status: "blocked" }), "Could not be done: no reason given");
  assert.equal(taskStatusText({ status: "open", since: "2026-09-26", overdueDays: 3 }), "Open since Sep 26 - 3 days overdue");
  assert.equal(taskStatusText({ status: "open", since: "2026-09-26", overdueDays: 1 }), "Open since Sep 26 - 1 day overdue");
  assert.equal(taskStatusText({ status: "open" }), "Open");
});

test("siteFactsSummary states the day's numbers and leaves out what didn't happen", () => {
  assert.equal(
    siteFactsSummary({
      workers: 7,
      trades: 2,
      tasks: { total: 5, done: 3, blocked: 1, open: 1 },
      activities: [{ company: "Formwork" }, { company: "Formwork" }, { company: "Rebar" }],
      equipment: [{ type: "Lift" }, { type: "Bin", onRent: false }],
      issues: 1,
      pours: 2,
    }),
    "7 workers from 2 trades were on site. 3 of 5 Site Logistics tasks completed, 1 could not be done, 1 still open. " +
      "3 activities across 2 crews were scheduled in the look-ahead. 2 pieces of equipment on site, 1 off rent awaiting pickup. " +
      "Logged: 1 issue, 2 concrete pours."
  );
  assert.equal(siteFactsSummary({ workers: 1 }), "1 worker was on site.");
  assert.equal(siteFactsSummary({}), "");
});

test("isWeatherOnlySummary spots a summary that only describes the weather", () => {
  assert.equal(isWeatherOnlySummary("Overcast with a high of 12°C. Light rain in the afternoon."), true);
  assert.equal(isWeatherOnlySummary("Overcast with a high of 12°C. Formwork crew stripped Level 2 walls."), false);
  assert.equal(isWeatherOnlySummary(""), false);
});

function fakeDb({ labourers = [], members = {} }) {
  return {
    collection(name) {
      if (name === "labourers") return { get: async () => ({ docs: labourers.map((d) => ({ data: () => d })) }) };
      if (name === "appMembers") {
        return { doc: (id) => ({ get: async () => ({ exists: Boolean(members[id]), data: () => members[id] }) }) };
      }
      throw new Error(`unexpected collection ${name}`);
    },
  };
}

test("resolvePeopleNames names sign-ins from the Labourer manager, then app members", async () => {
  const db = fakeDb({
    labourers: [
      { displayName: "Kevin Ashdown", siteLogisticsIds: ["Kevin@Example.com", "+15550001111"] },
      { displayName: "Former Hand", active: false, siteLogisticsIds: ["old@example.com"] },
    ],
    members: { "pm@example.com": { displayName: "Pat Manager" }, "old@example.com": { displayName: "Old Member" } },
  });
  const names = await resolvePeopleNames(db, ["kevin@example.com", "+15550001111", "PM@example.com", "old@example.com", "nobody@example.com", ""]);
  assert.deepEqual(Object.fromEntries(names), {
    "kevin@example.com": "Kevin Ashdown",
    "+15550001111": "Kevin Ashdown",
    "pm@example.com": "Pat Manager",
    "old@example.com": "Old Member",
  });
  assert.equal((await resolvePeopleNames(db, [])).size, 0);
});
