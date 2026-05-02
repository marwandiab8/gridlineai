const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  parseStructuredLog,
  parseDayRollupRequest,
  parseDailyReportRequest,
  isMetaInbound,
  extractProjectScopeHint,
  dateKeyEastern,
} = require("./logClassifier");

test("parseStructuredLog extracts an explicit backdated report date", () => {
  const parsed = parseStructuredLog(
    "log progress (2026-04-16) Toe wall east side after dewatering by Matheson Labourers"
  );

  assert.ok(parsed);
  assert.equal(parsed.category, "progress");
  assert.equal(parsed.reportDateKey, "2026-04-16");
  assert.equal(
    parsed.body,
    "Toe wall east side after dewatering by Matheson Labourers"
  );
});

test("parseStructuredLog extracts leading 'for YYYY-MM-DD' report dates", () => {
  const parsed = parseStructuredLog(
    "log note for 2026-04-16 photos - east entrance trench flooding"
  );

  assert.ok(parsed);
  assert.equal(parsed.category, "note");
  assert.equal(parsed.reportDateKey, "2026-04-16");
  assert.equal(parsed.body, "photos - east entrance trench flooding");
});

test("parseDayRollupRequest recognizes explicit dated log lookups", () => {
  const parsed = parseDayRollupRequest("show me what is logged for (2026-04-16)");

  assert.deepEqual(parsed, {
    reportDateKey: "2026-04-16",
    preferAiNarrative: false,
    normalizedText: "show me what is logged for (2026-04-16)",
  });
});

test("parseStructuredLog supports backdated manpower logs", () => {
  const parsed = parseStructuredLog(
    "log manpower (2026-04-16) ALC 16 Matheson 6 Coreydale 3"
  );

  assert.ok(parsed);
  assert.equal(parsed.category, "note");
  assert.equal(parsed.logParsedType, "manpower");
  assert.equal(parsed.reportDateKey, "2026-04-16");
  assert.equal(parsed.body, "ALC 16 Matheson 6 Coreydale 3");
});

test("parseStructuredLog normalizes common manpower command typos", () => {
  const parsed = parseStructuredLog(
    "load manpwer (2026-04016) ALC 16 Matheson 6 Coreydale 3"
  );

  assert.ok(parsed);
  assert.equal(parsed.logParsedType, "manpower");
  assert.equal(parsed.reportDateKey, "2026-04-16");
  assert.equal(parsed.body, "ALC 16 Matheson 6 Coreydale 3");
});

test("parseStructuredLog reclassifies note-prefixed manpower bodies", () => {
  const parsed = parseStructuredLog(
    "log note: Manpower (2026-04-16) ALC 16, Matheson 6, Coreydale 3"
  );

  assert.ok(parsed);
  assert.equal(parsed.category, "note");
  assert.equal(parsed.logParsedType, "manpower");
  assert.equal(parsed.reportDateKey, "2026-04-16");
  assert.equal(parsed.body, "ALC 16, Matheson 6, Coreydale 3");
});

test("parseStructuredLog supports punch shorthand as a deficiency", () => {
  const parsed = parseStructuredLog(
    "punch item missing baseboard at unit 204 bedroom"
  );

  assert.ok(parsed);
  assert.equal(parsed.category, "deficiency");
  assert.equal(parsed.logParsedType, "deficiency");
  assert.match(parsed.body, /missing baseboard/i);
});

test("isMetaInbound treats journal review follow-ups as conversation, not logs", () => {
  assert.equal(isMetaInbound("continue", "continue"), true);
  assert.equal(
    isMetaInbound("show me the journal input", "show me the journal input"),
    true
  );
});

test("parseDayRollupRequest recognizes dated activity lookups", () => {
  const parsed = parseDayRollupRequest("show me the activities for 2026-04-18");

  assert.deepEqual(parsed, {
    reportDateKey: "2026-04-18",
    preferAiNarrative: false,
    normalizedText: "show me the activities for 2026-04-18",
  });
});

test("parseDayRollupRequest does not treat plain activity notes as lookups", () => {
  const parsed = parseDayRollupRequest(
    "We did lots of activities today. Ashley and I went to the Restore place after breakfast and bought a desk for Myles"
  );

  assert.equal(parsed, null);
});

test("parseDayRollupRequest still recognizes direct activity lookup wording", () => {
  const parsed = parseDayRollupRequest("tell me the activities today");

  assert.deepEqual(parsed, {
    reportDateKey: dateKeyEastern(new Date()),
    preferAiNarrative: false,
    normalizedText: "tell me the activities today",
  });
});

test("extractProjectScopeHint recognizes terminal-injected project prefixes", () => {
  const parsed = extractProjectScopeHint(
    "project home Joseph's friends just got picked up by their parents."
  );

  assert.deepEqual(parsed, {
    projectSlug: "home",
    cleanedText: "Joseph's friends just got picked up by their parents.",
    scopeOnly: false,
    matchedText: "project home Joseph's friends just got picked up by their parents.",
  });
});

test("parseDailyReportRequest recognizes daily journal pdf requests", () => {
  const parsed = parseDailyReportRequest("daily journal pdf home");

  assert.ok(parsed);
  assert.equal(parsed.reportType, "journal");
  assert.equal(parsed.projectSlug, "home");
  assert.equal(parsed.invalidReason, null);
});
