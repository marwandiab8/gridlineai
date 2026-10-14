const test = require("node:test");
const assert = require("node:assert/strict");

const { createSiteLogisticsTaskNotifyHandler, parseTaskNotifyBody, phonesForSignIns } = require("./siteLogisticsTaskNotify");

const FieldValue = { serverTimestamp: () => "NOW" };
const silent = { info() {}, warn() {}, error() {} };

// messages (add) and labourers (where siteLogisticsIds array-contains). `labourers`: { phoneOrId: data }.
function fakeDb({ labourers = {} } = {}) {
  const added = [];
  return {
    added,
    collection: (name) => ({
      add: async (doc) => { added.push([name, doc]); return { id: `m${added.length}` }; },
      where: (field, op, value) => ({
        get: async () => ({
          docs: Object.entries(name === "labourers" ? labourers : {})
            .filter(([, d]) => op === "array-contains" && Array.isArray(d[field]) && d[field].includes(value))
            .map(([id, d]) => ({ id, get: (k) => d[k] })),
        }),
      }),
    }),
  };
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
  assert.deepEqual(res.body, { ok: true, sent: 2, failed: 0, unlinked: 0 });
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
  assert.deepEqual(res.body, { ok: true, sent: 1, failed: 1, unlinked: 0 });
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

const LABOURERS = {
  "+14165555196": { displayName: "Shawn Jones", phoneE164: "+14165555196", siteLogisticsIds: ["shawn83jones@gmail.com"] },
  "+14165554743": { displayName: "Kevin Ashdown", phoneE164: "+14165554743", siteLogisticsIds: ["ashdownk01@gmail.com"] },
  "+14165550001": { displayName: "Gone", phoneE164: "+14165550001", siteLogisticsIds: ["left@x.ca"], active: false },
};

test("a labourer Site Logistics has no number for is texted at the phone their sign-in is linked to here", async () => {
  const h = handler({ db: fakeDb({ labourers: LABOURERS }) });
  const res = fakeRes();
  await h.run(post({
    siteId: "s1",
    recipients: [
      { siteLogisticsId: "Shawn83Jones@gmail.com", label: "shawn83jones@gmail.com", body: "New task: sweep" },
      { siteLogisticsId: "ashdownk01@gmail.com", body: "New task: sweep" },
      { siteLogisticsId: "nobody@x.ca", body: "New task: sweep" },
      { siteLogisticsId: "left@x.ca", body: "New task: sweep" },
    ],
  }), res);
  assert.deepEqual(res.body, { ok: true, sent: 2, failed: 0, unlinked: 2 }, "an unknown or inactive sign-in isn't texted");
  assert.deepEqual(h.sms.calls.map((c) => [c.to, c.body]), [["+14165555196", "New task: sweep"], ["+14165554743", "New task: sweep"]]);
});

test("the same person by number and by sign-in gets one text", async () => {
  const { recipients, unlinked } = await phonesForSignIns(fakeDb({ labourers: LABOURERS }), [{ siteLogisticsId: "shawn83jones@gmail.com", label: "x", body: "b" }]);
  assert.deepEqual(recipients, [{ phoneE164: "+14165555196", label: "Shawn Jones", body: "b" }]);
  assert.equal(unlinked, 0);
  const h = handler({ db: fakeDb({ labourers: LABOURERS }) });
  const res = fakeRes();
  await h.run(post({ recipients: [{ phoneE164: "+14165555196", body: "by number" }, { siteLogisticsId: "shawn83jones@gmail.com", body: "by sign-in" }] }), res);
  assert.deepEqual(h.sms.calls.map((c) => c.body), ["by number"]);
});

test("sign-ins are read like the hours': an email, or a 10-digit phone", () => {
  const { recipients, bySignIn } = parseTaskNotifyBody({ recipients: [{ siteLogisticsId: " Kev@X.ca ", body: "a" }, { siteLogisticsId: "416-555-0123", body: "b" }, { siteLogisticsId: "not a sign-in", body: "c" }, { siteLogisticsId: "kev@x.ca", body: "dup" }] });
  assert.deepEqual(recipients, []);
  assert.deepEqual(bySignIn.map((r) => r.siteLogisticsId), ["kev@x.ca", "+14165550123"]);
});
