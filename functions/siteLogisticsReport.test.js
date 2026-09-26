const test = require("node:test");
const assert = require("node:assert/strict");
const {
  activitiesForDay,
  buildSiteLogisticsDay,
  crewsFromDay,
  loadSiteLogisticsForReport,
  mergeManpowerRows,
  resolveSiteRef,
  tasksForReport,
} = require("./siteLogisticsReport");

const silent = { warn() {} };

test("crewsFromDay keeps only crews with workers entered", () => {
  const crews = crewsFromDay({
    crews: [
      { trade: "Electrical", company: "O'Connor", workers: 6 },
      { trade: "Masonry", company: "Legacy", workers: 0 },
      { trade: "Gemtec", company: "", workers: "4" },
      { trade: "electrical", company: "o'connor", workers: 9 },
      { trade: "", company: "", workers: 3 },
    ],
  });
  assert.deepEqual(crews, [
    { trade: "Electrical", company: "O'Connor", workers: 6 },
    { trade: "Gemtec", company: "", workers: 4 },
  ]);
  assert.deepEqual(crewsFromDay(null), []);
});

test("activitiesForDay picks bookings covering the day and names the area", () => {
  const items = [{ id: "a1", label: "Phase 1" }];
  const bookings = [
    { areaId: "a1", trade: "Electrical", company: "O'Connor", start: "2026-09-28", end: "2026-10-02", activity: "Install temporary lights" },
    { areaId: "a1", trade: "Masonry", company: "Legacy", start: "2026-10-05", end: "2026-10-09", notes: "later" },
    { areaId: "gone", trade: "Roofing", company: "Camio", start: "2026-09-30", end: "2026-09-30", notes: "Parapet" },
  ];
  assert.deepEqual(activitiesForDay(bookings, items, "2026-09-30"), [
    { trade: "Roofing", company: "Camio", activity: "Parapet", area: "", start: "2026-09-30", end: "2026-09-30" },
    { trade: "Electrical", company: "O'Connor", activity: "Install temporary lights", area: "Phase 1", start: "2026-09-28", end: "2026-10-02" },
  ]);
});

test("mergeManpowerRows adds crews, and Site Logistics wins when a row already exists", () => {
  const existing = [
    ["Electrical", "Sam", "5", "Rough-in"],
    ["Legacy", "Lou", "3", ""],
  ];
  const crews = [
    { trade: "Electrical", company: "O'Connor", workers: 6 },
    { trade: "Fire protection", company: "FirePro", workers: 2 },
  ];
  const merged = mergeManpowerRows(existing, crews);
  assert.deepEqual(merged, [
    ["Electrical", "Sam", "6", "Rough-in (count from Site Logistics)"],
    ["Legacy", "Lou", "3", ""],
    ["FirePro (Fire protection)", "-", "2", "Site Logistics"],
  ]);
  assert.equal(existing[0][2], "5", "input rows are not mutated");
  const total = merged.reduce((n, r) => n + Number(r[2]), 0);
  assert.equal(total, 11, "no double counting");
});

test("mergeManpowerRows replaces the 'not stated' placeholder and leaves rows alone without crews", () => {
  const placeholder = [["-", "-", "-", "Not stated in log entries for this report day."]];
  assert.deepEqual(mergeManpowerRows(placeholder, [{ trade: "Masonry", company: "Legacy", workers: 4 }]), [["Legacy (Masonry)", "-", "4", "Site Logistics"]]);
  assert.deepEqual(mergeManpowerRows(placeholder, []), placeholder);
});

test("one company matches at most one existing row", () => {
  const rows = [["Electrical", "", "2", ""], ["Electrical - temp power", "", "1", ""]];
  const merged = mergeManpowerRows(rows, [
    { trade: "Electrical", company: "A", workers: 5 },
    { trade: "Electrical", company: "B", workers: 3 },
  ]);
  assert.deepEqual(merged.map((r) => r[2]), ["5", "3"]);
});

test("resolveSiteRef only knows Docksteader and honours SITE_LOGISTICS_SITE_ID", () => {
  assert.equal(resolveSiteRef("other"), null);
  assert.deepEqual(resolveSiteRef("Docksteader", {}), { siteId: "", nameContains: "docksteader" });
  assert.equal(resolveSiteRef("docksteader", { SITE_LOGISTICS_SITE_ID: " abc123 " }).siteId, "abc123");
});

function fakeDb({ sites, days = {}, bookings = [], items = [], tasks = [], fail = false }) {
  const doc = (id, data) => ({ id, exists: data != null, data: () => data, get: (k) => (data ? data[k] : undefined) });
  return {
    collection(name) {
      if (fail) throw new Error("permission-denied");
      assert.equal(name, "sites");
      return {
        get: async () => ({ docs: sites.map((s) => doc(s.id, s)) }),
        doc: () => ({
          collection: (sub) => {
            if (sub === "days") return { doc: (k) => ({ get: async () => doc(k, days[k] || null) }) };
            if (sub === "bookings") return { where: () => ({ get: async () => ({ docs: bookings.map((b) => doc(b.id, b)) }) }) };
            if (sub === "tasks") return { where: (f, op, v) => ({ get: async () => ({ docs: tasks.filter((t) => t[f] === v).map((t) => doc(t.id, t)) }) }) };
            return { get: async () => ({ docs: items.map((i) => doc(i.id, i)) }) };
          },
        }),
      };
    },
  };
}

test("loadSiteLogisticsForReport reads the day and finds the site by name", async () => {
  const db = fakeDb({
    sites: [{ id: "s1", name: "Docksteader Rd" }, { id: "s2", name: "Other job" }],
    days: { "2026-09-30": { crews: [{ trade: "Masonry", company: "Legacy", workers: 4 }], notes: "Rain delay" } },
    bookings: [{ id: "b1", areaId: "a1", trade: "Masonry", company: "Legacy", start: "2026-09-21", end: "2026-10-19", activity: "Masonry walls" }],
    items: [{ id: "a1", label: "Phase 1" }],
  });
  const day = await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-09-30", logger: silent, db });
  assert.equal(day.totalWorkers, 4);
  assert.equal(day.notes, "Rain delay");
  assert.equal(day.activities[0].area, "Phase 1");
});

test("loadSiteLogisticsForReport never throws and returns null when nothing usable", async () => {
  assert.equal(await loadSiteLogisticsForReport({ projectSlug: "other", dateKey: "2026-09-30", logger: silent, db: fakeDb({ sites: [] }) }), null);
  assert.equal(await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-09-30", logger: silent, db: fakeDb({ sites: [] }) }), null, "no site found");
  const two = fakeDb({ sites: [{ id: "1", name: "Docksteader A" }, { id: "2", name: "Docksteader B" }] });
  assert.equal(await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-09-30", logger: silent, db: two }), null, "ambiguous name is not guessed");
  assert.equal(await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-09-30", logger: silent, db: fakeDb({ fail: true, sites: [] }) }), null, "permission error");
  const empty = fakeDb({ sites: [{ id: "1", name: "Docksteader" }] });
  assert.equal(await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-09-30", logger: silent, db: empty }), null, "nothing recorded that day");
});

test("buildSiteLogisticsDay totals workers", () => {
  const day = buildSiteLogisticsDay({ dayDoc: { crews: [{ trade: "A", company: "", workers: 2 }, { trade: "B", company: "", workers: 3 }] }, bookings: [], items: [], dateKey: "2026-09-30" });
  assert.equal(day.totalWorkers, 5);
});

test("tasksForReport counts and orders the day's tasks: done, then could-not-do with the reason, then open", () => {
  const items = [{ id: "a1", label: "Phase 1" }];
  const doneAt = new Date("2026-09-28T18:15:00Z").getTime(); // 2:15 PM Toronto
  const out = tasksForReport(
    [
      { day: "2026-09-28", text: "Sweep level 1", areaId: "a1", trade: "Masonry", company: "Legacy", status: "open" },
      { day: "2026-09-28", text: "Stack block", areaId: "a1", trade: "Masonry", company: "Legacy", status: "done", doneAt, doneBy: "crew@legacy.ca" },
      { day: "2026-09-28", text: "Wall check", areaId: "gone", trade: "Masonry", company: "Legacy", status: "blocked", note: "Gate locked" },
      { day: "2026-09-29", text: "Tomorrow", status: "open" },
      { day: "2026-09-28", text: "  ", status: "open" },
    ],
    items,
    "2026-09-28"
  );
  assert.deepEqual({ total: out.total, done: out.done, blocked: out.blocked, open: out.open }, { total: 3, done: 1, blocked: 1, open: 1 });
  assert.deepEqual(out.items.map((t) => [t.text, t.status]), [["Stack block", "done"], ["Wall check", "blocked"], ["Sweep level 1", "open"]]);
  assert.equal(out.items[0].doneBy, "crew");
  assert.match(out.items[0].doneTime, /2:15/);
  assert.equal(out.items[0].area, "Phase 1");
  assert.equal(out.items[1].note, "Gate locked");
  assert.equal(out.items[2].note, "", "a note only matters for could-not-do tasks");
  assert.deepEqual(tasksForReport([], [], "2026-09-28"), { total: 0, done: 0, blocked: 0, open: 0, items: [] });
});

test("loadSiteLogisticsForReport includes the day's tasks, and tasks alone are enough to make a section", async () => {
  const db = fakeDb({
    sites: [{ id: "s1", name: "Docksteader" }],
    tasks: [{ id: "t1", day: "2026-09-30", text: "Sweep", status: "done", doneAt: 1, doneBy: "a@b.ca", trade: "Masonry", company: "Legacy" }, { id: "t2", day: "2026-10-01", text: "Other day", status: "open" }],
  });
  const day = await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-09-30", logger: silent, db });
  assert.equal(day.tasks.total, 1);
  assert.equal(day.tasks.done, 1);
  assert.equal(day.crews.length, 0);
});
