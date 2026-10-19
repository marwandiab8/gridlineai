const test = require("node:test");
const assert = require("node:assert/strict");
const {
  KINDS,
  addDays,
  dayKeyIn,
  checkBearer,
  selectSiteData,
  shapeTodoist,
  buildBriefingInput,
  parseBriefingJson,
  renderBriefingHtml,
  createBriefingRequestHandler,
  writeQueuedBriefing,
} = require("./docksteaderBriefing");

const FieldValue = { serverTimestamp: () => "SERVER_TIME", delete: () => "DELETE" };
const logger = { info() {}, warn() {}, error() {} };

class FakeDb {
  constructor() { this.rows = new Map(); }
  collection(col) {
    const db = this;
    return {
      doc: (id) => ({
        id,
        async get() {
          const data = db.rows.get(`${col}/${id}`);
          return { id, exists: Boolean(data), get: (k) => (data || {})[k], data: () => data };
        },
        async set(data) { db.rows.set(`${col}/${id}`, { ...data }); },
        async update(patch) {
          const next = { ...(db.rows.get(`${col}/${id}`) || {}) };
          Object.entries(patch).forEach(([k, v]) => { if (v === "DELETE") delete next[k]; else next[k] = v; });
          db.rows.set(`${col}/${id}`, next);
        },
      }),
    };
  }
}

function fakeRes() {
  const res = { statusCode: 0, body: null };
  res.status = (code) => { res.statusCode = code; return res; };
  res.json = (body) => { res.body = body; return res; };
  return res;
}
const fakeReq = ({ method = "POST", auth = "Bearer secret-token", body = {}, query = {} } = {}) => ({ method, body, query, get: (h) => (h.toLowerCase() === "authorization" ? auth : "") });

test("days are Toronto days, and the token must match exactly", () => {
  assert.equal(dayKeyIn(new Date("2026-10-20T03:30:00Z")), "2026-10-19", "11:30 PM on the 19th in Toronto");
  assert.equal(addDays("2026-10-31", 1), "2026-11-01");
  assert.equal(checkBearer("Bearer abc", "abc"), true);
  assert.equal(checkBearer("Bearer abd", "abc"), false);
  assert.equal(checkBearer("abc", "abc"), false);
  assert.equal(checkBearer("Bearer abc", ""), false);
});

test("Site Logistics: bookings and look-ahead around the day, delayed items, tasks done that day", () => {
  const site = selectSiteData({
    dayKey: "2026-10-19",
    siteName: "Docksteader",
    areas: { a1: "Z1 East Wing - L3" },
    bookings: [
      { company: "Alocon", trade: "Concrete", activity: "Pour housekeeping pads", areaId: "a1", start: "2026-10-21", end: "2026-10-21" },
      { company: "Legacy", trade: "Masonry", activity: "Masonry", start: "2026-09-01", end: "2026-09-30" },
    ],
    tasks: [
      { text: "First aid", status: "done", doneAt: "2026-10-19T15:12:00Z", doneBy: "marwandiab8@gmail.com" },
      { text: "Old one", status: "done", doneAt: "2026-10-16T15:00:00Z" },
      { text: "Clean roof", status: "open", day: "2026-10-20" },
    ],
    dayNote: "Legacy in zone 1.",
    labourHours: [{ byName: "Kevin Ashdown", status: "sent", lines: [{ hours: 4 }, { hours: 2.5 }] }],
    lookahead: {
      fileName: "3 Weeks.xlsx",
      activities: [
        { zone: "Vault", name: "Install Vault", company: "O'Connor", status: "scheduled", start: "2026-10-19", end: "2026-10-21" },
        { zone: "Roof D", name: "Roof buildup", company: "Triumph", status: "Delayed by solar wall deck", start: "2026-11-20", end: "2026-11-25" },
        { zone: "Z2", name: "Granular", company: "Coreydale", status: "scheduled", start: "2026-11-09", end: "2026-11-11" },
      ],
      undated: [{ zone: "Loading Dock", name: "Rebar", company: "RebarMaster", status: "delayed waiting for shop drawings approval" }],
    },
  });
  assert.deepEqual(site.bookings.map((b) => `${b.company} ${b.area}`), ["Alocon Z1 East Wing - L3"]);
  assert.deepEqual(site.tasksDoneToday.map((t) => t.text), ["First aid"]);
  assert.deepEqual(site.tasksOpen.map((t) => t.text), ["Clean roof"]);
  assert.deepEqual(site.lookahead.activities.map((a) => a.company), ["O'Connor", "Triumph"], "in the window, or delayed");
  assert.equal(site.lookahead.undated.length, 1);
  assert.equal(site.labourHours[0].hours, 6.5);
});

test("Todoist: a batch of tasks closed in one minute is housekeeping, not work done", () => {
  const closed = Array.from({ length: 6 }, (_, i) => ({ content: `old ${i}`, completedAt: "2026-10-19T10:55:12Z" }));
  const t = shapeTodoist({
    open: [
      { content: "Overdue", due: "2026-10-13" },
      { content: "Today", due: "2026-10-19" },
      { content: "Far", due: "2026-12-01" },
      { content: "No date" },
    ],
    completedToday: [...closed, { content: "Real", completedAt: "2026-10-19T14:02:00Z" }],
  }, "2026-10-19");
  assert.deepEqual(t.overdue.map((x) => x.task), ["Overdue"]);
  assert.deepEqual(t.dueToday.map((x) => x.task), ["Today"]);
  assert.equal(t.dueNextDays.length, 0);
  assert.equal(t.openWithoutDate, 1);
  assert.deepEqual(t.completedToday.map((x) => x.task), ["Real"]);
  assert.deepEqual(t.batchClosed, [{ at: "2026-10-19T10:55", count: 6 }]);
  assert.equal(shapeTodoist({ skipped: "no token" }, "2026-10-19").note, "no token");
});

test("the input is cut to size by shortening email bodies, keeping every thread", () => {
  const mail = Array.from({ length: 120 }, (_, i) => ({ subject: `Thread ${i}`, link: `https://mail.google.com/mail/u/0/#all/${i}`, messages: [{ from: "a", body: "x".repeat(2000) }] }));
  const input = buildBriefingInput({ kind: "wrapup", dayKey: "2026-10-19", nowLabel: "now", payload: { mail }, site: null });
  assert.ok(input.length <= 70000);
  assert.ok(input.includes("Thread 119"));
});

test("the model's JSON is held to the sections, and links not in the input are dropped", () => {
  const allowed = new Set(["https://mail.google.com/mail/u/0/#all/abc"]);
  const raw = 'Here you go: {"headline":"Vault dates won\'t hold","sections":[{"title":"Done today","items":[{"lead":"RFI 320","text":"Issued.","link":"https://mail.google.com/mail/u/0/#all/abc"},{"lead":"Made up","text":"x","link":"https://evil.example/"}]},{"title":"Something else","items":[{"lead":"","text":""}]}]}';
  const r = parseBriefingJson(raw, "wrapup", allowed);
  assert.equal(r.headline, "Vault dates won't hold");
  assert.deepEqual(r.sections.map((s) => s.title), KINDS.wrapup.sections);
  assert.equal(r.sections[0].items[0].link, "https://mail.google.com/mail/u/0/#all/abc");
  assert.equal(r.sections[0].items[1].link, "", "a link the model made up is removed");
  assert.equal(r.sections[1].items.length, 0, "empty items are dropped");
  assert.equal(parseBriefingJson("not json", "briefing", allowed), null);
});

test("the email HTML escapes everything it shows", () => {
  const html = renderBriefingHtml({
    title: "Docksteader evening wrap-up - Mon Oct 19",
    writtenAt: "Mon Oct 19, 6:31 p.m.",
    appUrl: "https://gridlineai.web.app/#reports",
    report: { headline: "<script>x</script>", sections: [{ title: "Done today", items: [{ lead: "A&B", text: "1 < 2", link: "https://mail.google.com/x?a=1&b=2" }] }, { title: "Needs your reply", items: [] }] },
  });
  assert.ok(html.includes("&lt;script&gt;x&lt;/script&gt;"));
  assert.ok(!html.includes("<script>"));
  assert.ok(html.includes("<b>A&amp;B</b>: 1 &lt; 2"));
  assert.ok(html.includes('href="https://mail.google.com/x?a=1&amp;b=2"'));
  assert.ok(html.includes("Nothing to report."));
  assert.ok(html.includes("<title>Docksteader evening wrap-up - Mon Oct 19</title>"));
});

test("a posted day is queued, written by the trigger, and returned when polled", async () => {
  const db = new FakeDb();
  const handler = createBriefingRequestHandler({ db, FieldValue, logger, token: () => "secret-token", now: () => new Date("2026-10-19T22:30:00Z") });

  let res = fakeRes();
  await handler(fakeReq({ auth: "Bearer wrong" }), res);
  assert.equal(res.statusCode, 401);
  res = fakeRes();
  await handler(fakeReq({ body: { kind: "lunch" } }), res);
  assert.equal(res.statusCode, 400);

  res = fakeRes();
  const mail = [{ subject: "RJC Site Review #4", link: "https://mail.google.com/mail/u/0/#all/rjc", messages: [{ from: "Dan Francis <dfrancis@rjc.ca>", to: "mdiab@mathesonconstructors.com", body: "Can RJC come Wednesday 10/21?" }] }];
  await handler(fakeReq({ body: { kind: "wrapup", windowStart: "2026-10-19T00:00", mail, todoist: { skipped: "no token" } } }), res);
  assert.equal(res.statusCode, 202);
  const { run, reportId } = res.body;
  assert.equal(reportId, "2026-10-19-wrapup");

  res = fakeRes();
  await handler(fakeReq({ method: "GET", query: { run } }), res);
  assert.equal(res.body.status, "queued");

  let sentParams = null;
  const runRef = db.collection("docksteaderBriefingQueue").doc(run);
  await writeQueuedBriefing({
    db, FieldValue, logger,
    siteLogisticsDb: { collection() { throw new Error("no access"); } },
    createCompletion: async (params) => {
      sentParams = params;
      return { choices: [{ message: { content: JSON.stringify({ headline: "Confirm RJC for Wednesday", sections: [{ title: "Needs your reply", items: [{ lead: "RJC Site Review #4", text: "Dan Francis can come Wednesday Oct 21 morning; confirm the time.", link: "https://mail.google.com/mail/u/0/#all/rjc" }] }] }) } }] };
    },
    model: "gpt-5.5",
    appUrl: "https://gridlineai.web.app/#reports",
    runRef,
    run: (await runRef.get()).data(),
    now: () => new Date("2026-10-19T22:31:00Z"),
  });
  assert.equal(sentParams.temperature, undefined, "GPT-5 models get no temperature");
  assert.ok(sentParams.messages[1].content.includes("Site Logistics could not be read"), "a Site Logistics failure is said, not fatal");
  const report = db.rows.get("docksteaderBriefings/2026-10-19-wrapup");
  assert.equal(report.memberEmail, "marwandiab8@gmail.com");
  assert.equal(report.title, "Docksteader evening wrap-up - Mon Oct 19");
  assert.equal(report.sections[2].items[0].lead, "RJC Site Review #4", "placed in its own section even when the model skips others");
  assert.equal(report.sources.emailThreads, 1);
  assert.equal(db.rows.get(`docksteaderBriefingQueue/${run}`).payload, undefined, "the raw day is not kept once written");

  res = fakeRes();
  await handler(fakeReq({ method: "GET", query: { run } }), res);
  assert.equal(res.body.status, "ready");
  assert.equal(res.body.subject, "Docksteader evening wrap-up - Mon Oct 19");
  assert.ok(res.body.html.includes("Confirm RJC for Wednesday"));
});

test("a model failure marks the run failed with the reason", async () => {
  const db = new FakeDb();
  const runRef = db.collection("docksteaderBriefingQueue").doc("2026-10-19-briefing-1");
  await runRef.set({ status: "queued", kind: "briefing", dayKey: "2026-10-19", reportId: "2026-10-19-briefing", payload: "{}" });
  await writeQueuedBriefing({
    db, FieldValue, logger,
    siteLogisticsDb: { collection() { throw new Error("no access"); } },
    createCompletion: async () => ({ choices: [{ message: { content: "sorry" } }] }),
    model: "gpt-5.5", appUrl: "", runRef, run: (await runRef.get()).data(),
  });
  const run = db.rows.get("docksteaderBriefingQueue/2026-10-19-briefing-1");
  assert.equal(run.status, "failed");
  assert.match(run.error, /not valid JSON/);
  assert.equal(db.rows.get("docksteaderBriefings/2026-10-19-briefing"), undefined);
});
