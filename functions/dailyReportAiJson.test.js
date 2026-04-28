const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  sanitizeExecutiveSummary,
  sanitizeStructuredDailyReportJson,
} = require("./dailyReportAiJson");

test("sanitizeExecutiveSummary keeps long multi-sentence summaries intact beyond 520 chars", () => {
  const longSummary = [
    "Formwork crews completed slab edge prep at the south podium while waterproofing advanced along the east retaining wall with inspections coordinated in sequence.",
    "Electrical and mechanical rough-in continued through Levels 2 and 3, with access staggered around concrete curing areas to avoid rework and congestion.",
    "Road-Ex maintained excavation support and trucking logistics at the north approach, and deliveries were timed around crane activity to preserve site circulation.",
    "Consultant review of embedded items closed without major deficiencies, but follow-up was noted for isolated alignment checks before the next pour window.",
    "Open items remain focused on access coordination, material staging, and confirming manpower coverage for the next shift so progress can continue without interruption.",
  ].join(" ");

  const result = sanitizeExecutiveSummary(longSummary);

  assert.ok(longSummary.length > 520);
  assert.ok(result.length > 520);
  assert.equal(result, longSummary);
});

test("sanitizeStructuredDailyReportJson does not chop executive summary mid-sentence", () => {
  const executiveSummary = [
    "ALC completed waterproofing touch-ups and membrane protection at the west wall after morning cleanup and access setup.",
    "Coreydale advanced formwork and slab edge adjustments at the south podium while coordination continued with embedded items and upcoming pour checks.",
    "Electrical and mechanical crews rough-in progressed through active areas with sequencing adjusted around curing zones and inspection timing.",
    "Consultant review identified only minor follow-up items, and the remaining actions were limited to access planning, staging, and confirming manpower for the next shift.",
  ].join(" ");

  const parsed = sanitizeStructuredDailyReportJson({
    executiveSummary,
    weather: {},
    manpower: {},
    workCompletedInProgress: {},
    issuesDeficienciesDelays: {},
    inspections: {},
    concreteSummary: {},
    openItems: {},
  });

  assert.equal(parsed.executiveSummary, executiveSummary);
  assert.ok(parsed.executiveSummary.endsWith("next shift."));
});
