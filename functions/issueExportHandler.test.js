const { test } = require("node:test");
const assert = require("node:assert/strict");
const { isAllowedIssueCollection } = require("./issueExportHandler");

test("isAllowedIssueCollection only permits typed issue collections", () => {
  assert.equal(isAllowedIssueCollection("generalIssues"), true);
  assert.equal(isAllowedIssueCollection("deficiencyIssues"), true);
  assert.equal(isAllowedIssueCollection("messages"), false);
  assert.equal(isAllowedIssueCollection("adminSettings"), false);
  assert.equal(isAllowedIssueCollection(""), false);
});
