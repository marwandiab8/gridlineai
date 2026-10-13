const test = require("node:test");
const assert = require("node:assert/strict");

const { createSiteLogisticsTaskNotifyHandler, parseTaskNotifyBody } = require("./siteLogisticsTaskNotify");

const FieldValue = { serverTimestamp: () => "NOW" };
const silent = { info() {}, warn() {}, error() {} };

function fakeDb() {
  const added = [];
  return { added, collection: (name) => ({ add: async (doc) => { added.push([name, doc]); return { id: `m${added.length}` }; } }) };
}

function fakeTwilio({ failTo = [] } = {}) {
  const calls = [];
  return {
    calls,
    messages: {
      create: async (payload) => {
        calls.push(payload);
        if (failTo.includes(payload.to)) throw new Error("Attempt to send to unsubscribed recipient");
        return { sid: `SM${calls.length}`, status: "queued" };
      },
    },
  };
}

function fakeRes() {
  const res = { statusCode: 0, body: null, headers: {} };
  res.status = (code) => { res.statusCode = code; return res; };
  res.set = (k, v) => { res.headers[k] = v; return res; };
  res.json = (body) => { res.body = body; return res; };
  return res;
}

const post = (body) => ({ method: "POST", headers: { authorization: "Bearer t" }, body });

function handler({ db = fakeDb(), sms = fakeTwilio(), fromPhone = "+16475550100", allow = true } = {}) {
  return {
    db,
    sms,
    run: createSiteLogisticsTaskNotifyHandler({
      db,
      FieldValue,
      logger: silent,
      audience: "https://example/siteLogisticsTaskNotify",
      getTwilio: () => ({ smsClient: sms, fromPhone }),
      verifyCaller: async (_req, aud) => allow && aud === "https://example/siteLogisticsTaskNotify",
      messageSchemaVersion: 3,
    }),
  };
}

test("each labourer's text goes out from our number, word for word, and is logged", async () => {
  const h = handler();
  const res = fakeRes();
  await h.run(post({
    siteId: "s1",
    siteName: "Docksteader",
    recipients: [
      { phoneE164: "+14165550123", label: "Sam", body: "New task on Docksteader:\n- Strip forms\nOpen your tasks: https://x" },
      { phoneE164: "+14165550124", label: "Ana", body: "2 new tasks on Docksteader:\n- A\n- B\nOpen your tasks: https://x" },
    ],
  }), res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body, { ok: true, sent: 2, failed: 0 });
  assert.deepEqual(h.sms.calls[0], { to: "+14165550123", from: "+16475550100", body: "New task on Docksteader:\n- Strip forms\nOpen your tasks: https://x" });
  assert.equal(h.db.added.length, 2);
  const [collection, logged] = h.db.added[0];
  assert.equal(collection, "messages");
  assert.equal(logged.direction, "outbound");
  assert.equal(logged.command, "site_logistics_task_notify");
  assert.equal(logged.schemaVersion, 3);
  assert.deepEqual(logged.siteLogistics, { siteId: "s1", siteName: "Docksteader" });
});

test("a text Twilio refuses is counted as failed; the others still go", async () => {
  const h = handler({ sms: fakeTwilio({ failTo: ["+14165550123"] }) });
  const res = fakeRes();
  await h.run(post({ recipients: [{ phoneE164: "+14165550123", body: "a" }, { phoneE164: "+14165550124", body: "b" }] }), res);
  assert.deepEqual(res.body, { ok: true, sent: 1, failed: 1 });
  assert.equal(h.db.added.length, 1);
});

test("only Site Logistics' server may send texts", async () => {
  const h = handler({ allow: false });
  const res = fakeRes();
  await h.run(post({ recipients: [{ phoneE164: "+14165550123", body: "a" }] }), res);
  assert.equal(res.statusCode, 401);
  assert.equal(h.sms.calls.length, 0);
});

test("no texts are sent when Twilio isn't configured", async () => {
  const h = handler({ fromPhone: "" });
  const res = fakeRes();
  await h.run(post({ recipients: [{ phoneE164: "+14165550123", body: "a" }] }), res);
  assert.equal(res.statusCode, 503);
  assert.equal(h.sms.calls.length, 0);
});

test("recipients are checked: real numbers, a message, one text per phone, a limit per call", () => {
  assert.deepEqual(parseTaskNotifyBody({}), { error: "Missing recipients." });
  assert.match(parseTaskNotifyBody({ recipients: Array(101).fill({ phoneE164: "+14165550123", body: "a" }) }).error, /At most 100/);
  const { recipients } = parseTaskNotifyBody({
    recipients: [
      { phoneE164: "+14165550123", body: "first" },
      { phoneE164: "+14165550123", body: "second" },
      { phoneE164: "4165550199", body: "not E.164" },
      { phoneE164: "+14165550124", body: "   " },
      { phoneE164: "+14165550125", body: "x".repeat(700) },
    ],
  });
  assert.deepEqual(recipients.map((r) => [r.phoneE164, r.body.length]), [["+14165550123", 5], ["+14165550125", 640]]);
  assert.equal(recipients[0].label, "+14165550123");
});
