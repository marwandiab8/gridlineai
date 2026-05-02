const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const { parseDayRollupRequest } = require("./logClassifier");
const {
  looksLikeExplicitAiChatRequest,
  isExplicitLabourEntryText,
  isExplicitLabourBalanceText,
  looksLikeNarrativeSaveCandidate,
  decideFallbackRouting,
} = require("./assistant");

const fixturesPath = path.join(__dirname, "testdata", "sms-routing-fixtures.json");
const fixtures = JSON.parse(fs.readFileSync(fixturesPath, "utf8"));

test("sms routing fixtures stay stable", () => {
  for (const fixture of fixtures) {
    const text = fixture.text;
    const expect = fixture.expect || {};

    if (Object.prototype.hasOwnProperty.call(expect, "dayRollup")) {
      assert.equal(
        Boolean(parseDayRollupRequest(text)),
        Boolean(expect.dayRollup),
        `${fixture.id}: dayRollup`
      );
    }

    if (Object.prototype.hasOwnProperty.call(expect, "explicitAiRequest")) {
      assert.equal(
        looksLikeExplicitAiChatRequest(text),
        Boolean(expect.explicitAiRequest),
        `${fixture.id}: explicitAiRequest`
      );
    }

    if (Object.prototype.hasOwnProperty.call(expect, "explicitLabourEntry")) {
      assert.equal(
        isExplicitLabourEntryText(text),
        Boolean(expect.explicitLabourEntry),
        `${fixture.id}: explicitLabourEntry`
      );
    }

    if (Object.prototype.hasOwnProperty.call(expect, "explicitLabourBalance")) {
      assert.equal(
        isExplicitLabourBalanceText(text),
        Boolean(expect.explicitLabourBalance),
        `${fixture.id}: explicitLabourBalance`
      );
    }

    if (Object.prototype.hasOwnProperty.call(expect, "narrativeSaveCandidate")) {
      assert.equal(
        looksLikeNarrativeSaveCandidate(text),
        Boolean(expect.narrativeSaveCandidate),
        `${fixture.id}: narrativeSaveCandidate`
      );
    }

    if (Object.prototype.hasOwnProperty.call(expect, "fallbackAction")) {
      const decision = decideFallbackRouting(
        { intent: "request", confidence: 0.55, reason: "fixture low confidence", source: "fixture" },
        text,
        looksLikeExplicitAiChatRequest(text)
      );
      assert.equal(decision.action, expect.fallbackAction, `${fixture.id}: fallbackAction`);
      if (Object.prototype.hasOwnProperty.call(expect, "safeFallbackUsed")) {
        assert.equal(
          decision.safeFallbackUsed,
          Boolean(expect.safeFallbackUsed),
          `${fixture.id}: safeFallbackUsed`
        );
      }
    }
  }
});
