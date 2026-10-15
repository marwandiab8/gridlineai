const test = require("node:test");
const assert = require("node:assert/strict");

const { REPLY_TIMEOUT_ERROR, awaitInboundReply } = require("./inboundReplyTiming");

const later = (ms, value, fail = false) => new Promise((resolve, reject) => setTimeout(() => (fail ? reject(new Error(value)) : resolve(value)), ms));
const timing = (calls) => ({ fastMs: 30, slowMs: 60, onSlow: () => calls.push("answered Twilio") });

test("a quick reply is used straight away and Twilio isn't answered early", async () => {
  const calls = [];
  assert.equal(await awaitInboundReply(later(5, "Logged."), timing(calls)), "Logged.");
  assert.deepEqual(calls, []);
});

test("a slow reply answers Twilio once and is still used when it's ready", async () => {
  const calls = [];
  assert.equal(await awaitInboundReply(later(50, "Logged your lunch."), timing(calls)), "Logged your lunch.");
  assert.deepEqual(calls, ["answered Twilio"]);
});

test("an error is passed on, whether it comes before or after Twilio was answered", async () => {
  const quick = [];
  await assert.rejects(awaitInboundReply(later(5, "boom", true), timing(quick)), /boom/);
  assert.deepEqual(quick, []);
  const slow = [];
  await assert.rejects(awaitInboundReply(later(50, "late boom", true), timing(slow)), /late boom/);
  assert.deepEqual(slow, ["answered Twilio"]);
});

test("a reply slower than both waits times out, as before", async () => {
  const calls = [];
  await assert.rejects(awaitInboundReply(later(200, "too late"), timing(calls)), new RegExp(REPLY_TIMEOUT_ERROR));
  assert.deepEqual(calls, ["answered Twilio"]);
  assert.equal(REPLY_TIMEOUT_ERROR, "__BUILD_REPLY_TIMEOUT__", "the handler's existing timeout branch still matches");
});
