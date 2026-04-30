const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  biweeklyPayPeriodStartKeyFromDateKey,
  buildLabourRollup,
  dayMultiplierFromDateKey,
  formatLabourBalanceReply,
  getDateKeyRangeForBalanceQuery,
  isPublicHolidayDateKey,
  monthKeyFromDateKey,
  parseLabourHoursBalanceQuery,
  parseLabourHoursCommand,
  weeklyKeyFromDateKey,
} = require("./labourRepository");

function ts(iso) {
  return {
    seconds: Math.floor(new Date(iso).getTime() / 1000),
    toDate() {
      return new Date(iso);
    },
  };
}

test("parseLabourHoursCommand parses shorthand labour hour texts", () => {
  const parsed = parseLabourHoursCommand("labour 8.5 framing cleanup");

  assert.ok(parsed);
  assert.equal(parsed.hours, 8.5);
  assert.equal(parsed.workOn, "framing cleanup");
  assert.equal(parsed.reportDateKey, null);
});

test("parseLabourHoursCommand keeps explicit report dates", () => {
  const parsed = parseLabourHoursCommand("worked 6 hours on drywall (2026-04-24)");

  assert.ok(parsed);
  assert.equal(parsed.hours, 6);
  assert.equal(parsed.workOn, "drywall");
  assert.equal(parsed.reportDateKey, "2026-04-24");
});

test("parseLabourHoursCommand parses total N H style field SMS", () => {
  const parsed = parseLabourHoursCommand(
    "Hi Sunday April 26- total 9 H . Pumping water all footings including tow wall Thanks Wael",
  );

  assert.ok(parsed);
  assert.equal(parsed.hours, 9);
  assert.ok(parsed.workOn.toLowerCase().includes("pumping water"));
  assert.ok(parsed.workOn.toLowerCase().includes("footings"));
  assert.equal(parsed.reportDateKey, null);
});

test("parseLabourHoursCommand parses total of N hours phrasing", () => {
  const parsed = parseLabourHoursCommand("Quick note — total of 7.5 hours forming basement walls");

  assert.ok(parsed);
  assert.equal(parsed.hours, 7.5);
  assert.ok(parsed.workOn.toLowerCase().includes("forming"));
});

test("parseLabourHoursCommand parses segmented breakdown with declared total", () => {
  const parsed = parseLabourHoursCommand("9 hours 6 hours pumping water 3 hours housekeeping");

  assert.ok(parsed);
  assert.equal(parsed.hours, 9);
  assert.ok(parsed.workOn.toLowerCase().includes("6h pumping water"));
  assert.ok(parsed.workOn.toLowerCase().includes("3h housekeeping"));
});

test("parseLabourHoursCommand parses segmented breakdown without declared total consistency", () => {
  const parsed = parseLabourHoursCommand("8 hours 6 hours pumping water 3 hours housekeeping");

  assert.ok(parsed);
  // falls back to summed segmented hours when declared total does not match
  assert.equal(parsed.hours, 9);
});

test("dayMultiplierFromDateKey applies holiday/weekend multipliers to the report date", () => {
  assert.equal(dayMultiplierFromDateKey("2026-04-24"), 1);
  assert.equal(dayMultiplierFromDateKey("2026-04-25"), 1.5);
  assert.equal(dayMultiplierFromDateKey("2026-04-26"), 2);
  assert.equal(dayMultiplierFromDateKey("2026-07-01"), 1.5);
  assert.equal(isPublicHolidayDateKey("2026-07-01"), true);
});

test("parseLabourHoursBalanceQuery distinguishes questions from hour submissions", () => {
  assert.equal(parseLabourHoursBalanceQuery("How many hours this week?")?.range, "week");
  assert.equal(parseLabourHoursBalanceQuery("my hours for this pay period")?.range, "pay");
  assert.equal(parseLabourHoursBalanceQuery("hours for today")?.range, "today");
  assert.equal(parseLabourHoursBalanceQuery("What is my time this month?")?.range, "month");
  assert.equal(parseLabourHoursBalanceQuery("How many hours?")?.range, "pay");
  assert.equal(parseLabourHoursBalanceQuery("my hours?")?.range, "pay");
  assert.equal(parseLabourHoursBalanceQuery("labour 8.0 framing"), null);
  assert.equal(parseLabourHoursBalanceQuery("worked 6 hours on drywall"), null);
  assert.equal(parseLabourHoursBalanceQuery("total 9 h pumping water"), null);
});

test("getDateKeyRangeForBalanceQuery matches Eastern calendar for a fixed now", () => {
  const fixed = new Date("2026-04-26T16:00:00.000Z");
  const day = getDateKeyRangeForBalanceQuery("today", fixed);
  assert.equal(day && day.startKey, "2026-04-26");
  assert.equal(day && day.endKey, "2026-04-26");
  const pay = getDateKeyRangeForBalanceQuery("pay", fixed);
  assert.equal(pay && pay.startKey, "2026-04-25");
  assert.equal(pay && pay.endKey, "2026-05-08");
});

test("formatLabourBalanceReply shows paid when weekend weighting applies", () => {
  const text = formatLabourBalanceReply({
    labourerName: "Wael",
    rangeLabel: "this pay period",
    startKey: "2026-04-25",
    endKey: "2026-05-08",
    totalHours: 9,
    totalPaidHours: 18,
    totalEntries: 1,
  });
  assert.match(text, /9h on site/);
  assert.match(text, /18h paid/);
  assert.match(text, /44h\/week/);
});

test("weekly and monthly rollups group labour entries by calendar ranges", () => {
  assert.equal(weeklyKeyFromDateKey("2026-04-24"), "2026-04-20");
  assert.equal(monthKeyFromDateKey("2026-04-24"), "2026-04");

  const rollup = buildLabourRollup([
    {
      id: "e1",
      createdAt: ts("2026-04-20T13:00:00Z"),
      reportDateKey: "2026-04-20",
      labourerName: "Marwan Diab",
      labourerPhone: "+14370000000",
      hours: 4,
      workOn: "site cleanup",
    },
    {
      id: "e2",
      createdAt: ts("2026-04-24T14:00:00Z"),
      reportDateKey: "2026-04-24",
      labourerName: "Ashley Trower",
      labourerPhone: "+15190000000",
      hours: 5.5,
      workOn: "shopping runs",
    },
  ]);

  assert.equal(rollup.totalHours, 9.5);
  assert.equal(rollup.totalEntries, 2);
  assert.equal(rollup.dailyTotals.length, 2);
  assert.equal(rollup.weeklyTotals.length, 1);
  assert.equal(rollup.weeklyTotals[0].weekStartKey, "2026-04-20");
  assert.equal(rollup.monthlyTotals.length, 1);
  assert.equal(rollup.monthlyTotals[0].monthKey, "2026-04");
  assert.equal(rollup.labourerTotals.length, 2);
});

test("paid period tally applies daily, weekly, weekend, and holiday rules without pyramiding", () => {
  assert.equal(biweeklyPayPeriodStartKeyFromDateKey("2026-04-25"), "2026-04-25");
  assert.equal(biweeklyPayPeriodStartKeyFromDateKey("2026-05-08"), "2026-04-25");
  assert.equal(biweeklyPayPeriodStartKeyFromDateKey("2026-05-09"), "2026-05-09");
  assert.equal(dayMultiplierFromDateKey("2026-05-02"), 1.5); // Saturday
  assert.equal(dayMultiplierFromDateKey("2026-05-03"), 2); // Sunday

  const rollup = buildLabourRollup([
    {
      id: "e1",
      createdAt: ts("2026-09-07T12:00:00Z"),
      reportDateKey: "2026-09-07",
      labourerName: "Sam",
      hours: 8,
      workOn: "holiday work",
    },
    {
      id: "e2",
      createdAt: ts("2026-09-08T12:00:00Z"),
      reportDateKey: "2026-09-08",
      labourerName: "Sam",
      hours: 12,
      workOn: "forming",
    },
    {
      id: "e3",
      createdAt: ts("2026-09-09T12:00:00Z"),
      reportDateKey: "2026-09-09",
      labourerName: "Sam",
      hours: 12,
      workOn: "forming",
    },
    {
      id: "e4",
      createdAt: ts("2026-09-10T12:00:00Z"),
      reportDateKey: "2026-09-10",
      labourerName: "Sam",
      hours: 14,
      workOn: "weekday overtime",
    },
    {
      id: "e5",
      createdAt: ts("2026-09-12T12:00:00Z"),
      reportDateKey: "2026-09-12",
      labourerName: "Sam",
      hours: 8,
      workOn: "saturday work",
    },
    {
      id: "e6",
      createdAt: ts("2026-09-13T12:00:00Z"),
      reportDateKey: "2026-09-13",
      labourerName: "Sam",
      hours: 8,
      workOn: "sunday work",
    },
  ]);

  assert.equal(rollup.totalHours, 62);
  assert.equal(rollup.totalPaidHours, 62);
  assert.equal(rollup.paidPeriodTotals.length, 2);
  assert.equal(rollup.paidPeriodTotals[0].periodStartKey, "2026-08-29");
  assert.equal(rollup.paidPeriodTotals[1].periodStartKey, "2026-09-12");
  const regularHours = rollup.paidPeriodTotals.reduce((total, period) => total + Number(period.regularHours || 0), 0);
  const overtimeHours = rollup.paidPeriodTotals.reduce((total, period) => total + Number(period.overtimeHours || 0), 0);
  const doubleTimeHours = rollup.paidPeriodTotals.reduce((total, period) => total + Number(period.doubleTimeHours || 0), 0);
  const totalPaidHours = rollup.paidPeriodTotals.reduce((total, period) => total + Number(period.totalPaidHours || 0), 0);
  const totalPayUnits = rollup.paidPeriodTotals.reduce((total, period) => total + Number(period.totalPayUnits || 0), 0);
  assert.equal(Math.round(regularHours * 10) / 10, 35.2);
  assert.equal(Math.round(overtimeHours * 10) / 10, 18.8);
  assert.equal(doubleTimeHours, 8);
  assert.equal(totalPaidHours, 62);
  assert.equal(Math.round(totalPayUnits * 10) / 10, 79.4);
});

test("weekday hours over 12 in a day are paid at time-and-a-half", () => {
  const rollup = buildLabourRollup([
    {
      id: "e1",
      createdAt: ts("2026-05-04T12:00:00Z"),
      reportDateKey: "2026-05-04",
      labourerName: "Sam",
      hours: 14,
      workOn: "weekday overtime",
    },
  ]);

  assert.equal(rollup.paidPeriodTotals[0].regularHours, 12);
  assert.equal(rollup.paidPeriodTotals[0].overtimeHours, 2);
  assert.equal(rollup.paidPeriodTotals[0].doubleTimeHours, 0);
  assert.equal(rollup.paidPeriodTotals[0].totalPayUnits, 15);
});

test("weekly hours above 44 become time-and-a-half on straight-time weekdays", () => {
  const rollup = buildLabourRollup([
    {
      id: "e1",
      createdAt: ts("2026-05-04T12:00:00Z"),
      reportDateKey: "2026-05-04",
      labourerName: "Sam",
      hours: 11,
      workOn: "weekday",
    },
    {
      id: "e2",
      createdAt: ts("2026-05-05T12:00:00Z"),
      reportDateKey: "2026-05-05",
      labourerName: "Sam",
      hours: 11,
      workOn: "weekday",
    },
    {
      id: "e3",
      createdAt: ts("2026-05-06T12:00:00Z"),
      reportDateKey: "2026-05-06",
      labourerName: "Sam",
      hours: 11,
      workOn: "weekday",
    },
    {
      id: "e4",
      createdAt: ts("2026-05-07T12:00:00Z"),
      reportDateKey: "2026-05-07",
      labourerName: "Sam",
      hours: 11,
      workOn: "weekday",
    },
    {
      id: "e5",
      createdAt: ts("2026-05-08T12:00:00Z"),
      reportDateKey: "2026-05-08",
      labourerName: "Sam",
      hours: 4,
      workOn: "weekday",
    },
  ]);

  assert.equal(rollup.paidPeriodTotals[0].regularHours, 44);
  assert.equal(rollup.paidPeriodTotals[0].overtimeHours, 4);
  assert.equal(rollup.paidPeriodTotals[0].doubleTimeHours, 0);
  assert.equal(rollup.paidPeriodTotals[0].totalPayUnits, 50);
});
