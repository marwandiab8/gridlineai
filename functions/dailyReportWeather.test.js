const test = require("node:test");
const assert = require("node:assert/strict");

const { summarizeWeeklyWeather, buildWeeklySummaryItems } = require("./dailyReportWeather");

test("summarizeWeeklyWeather highlights dry weeks and cold snaps", () => {
  const summary = summarizeWeeklyWeather([
    {
      dateKey: "2026-04-20",
      conditions: "Partly cloudy",
      highC: 12,
      lowC: 0,
      precipMm: 0,
      windMphMax: 14,
    },
    {
      dateKey: "2026-04-21",
      conditions: "Partly cloudy",
      highC: 14,
      lowC: 2,
      precipMm: 0,
      windMphMax: 16,
    },
    {
      dateKey: "2026-04-22",
      conditions: "Clear",
      highC: 15,
      lowC: 3,
      precipMm: 0.2,
      windMphMax: 18,
    },
  ]);

  assert.ok(
    summary.summaryItems.some((item) => /Highs around 12 to 15°C/i.test(item)),
    "expected temperature range summary"
  );
  assert.ok(
    summary.summaryItems.some((item) => /No meaningful rain/i.test(item)),
    "expected dry-week summary"
  );
  assert.ok(
    summary.summaryItems.some((item) => /overnight lows near 0°C/i.test(item)),
    "expected cold snap summary"
  );
});

test("summarizeWeeklyWeather highlights wet and windy windows", () => {
  const summary = summarizeWeeklyWeather([
    {
      dateKey: "2026-04-20",
      conditions: "Rain",
      highC: 8,
      lowC: 5,
      precipMm: 4,
      windMphMax: 22,
    },
    {
      dateKey: "2026-04-21",
      conditions: "Rain",
      highC: 9,
      lowC: 4,
      precipMm: 6,
      windMphMax: 24,
    },
    {
      dateKey: "2026-04-22",
      conditions: "Partly cloudy",
      highC: 11,
      lowC: 3,
      precipMm: 0,
      windMphMax: 18,
    },
  ]);

  assert.ok(
    summary.summaryItems.some((item) => /2 days show measurable precipitation/i.test(item)),
    "expected wet-week summary"
  );
  assert.ok(
    summary.summaryItems.some((item) => /24 mph/i.test(item)),
    "expected wind summary"
  );
});

test("buildWeeklySummaryItems summarizes long windows by week and notes clipped forecast coverage", () => {
  const rows = [];
  for (let i = 0; i < 14; i += 1) {
    rows.push({
      dateKey: `2026-04-${String(21 + i).padStart(2, "0")}`,
      conditions: i < 7 ? "Partly cloudy" : "Rain",
      highC: i < 7 ? 14 : 10,
      lowC: i < 7 ? 3 : 5,
      precipMm: i < 7 ? 0 : 3,
      windMphMax: i < 7 ? 14 : 22,
    });
  }

  const items = buildWeeklySummaryItems(rows, {
    requestedStartDateKey: "2026-04-20",
    requestedEndDateKey: "2026-05-10",
    clippedStartDateKey: "2026-04-21",
    clippedEndDateKey: "2026-05-04",
  });

  assert.ok(items.some((item) => /Forecast starts at 2026-04-21/i.test(item)));
  assert.ok(items.some((item) => /Week 1/i.test(item)));
  assert.ok(items.some((item) => /Week 2/i.test(item)));
  assert.ok(items.some((item) => /Forecast coverage ends at 2026-05-04/i.test(item)));
});
