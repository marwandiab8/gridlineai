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

function fakeDb({ sites, days = {}, bookings = [], items = [], tasks = [], records = null, fail = false }) {
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
            if (sub === "tasks") return { get: async () => ({ docs: tasks.map((t) => doc(t.id, t)) }) };
            if (sub === "dailyRecords") {
              if (!records) throw new Error("no dailyRecords here"); // like a project that has none
              return { doc: (k) => ({ get: async () => doc(k, records[k] || null) }) };
            }
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

test("tasksForReport: finished that day, could not do, and still open - tasks are not tied to a day", () => {
  const items = [{ id: "a1", label: "Phase 1" }];
  const at = (iso) => new Date(iso).getTime();
  const out = tasksForReport(
    [
      { text: "Sweep level 1", areaId: "a1", trade: "Masonry", company: "Legacy", status: "open", createdAt: at("2026-09-20T15:00:00Z"), day: "2026-09-25" },
      { text: "Stack block", areaId: "a1", trade: "Masonry", company: "Legacy", status: "done", createdAt: at("2026-09-22T15:00:00Z"), doneAt: at("2026-09-28T18:15:00Z"), doneBy: "crew@legacy.ca" }, // 2:15 PM
      { text: "Wall check", areaId: "gone", trade: "Masonry", company: "Legacy", status: "blocked", note: "Gate locked", createdAt: at("2026-09-24T15:00:00Z") },
      { text: "Done last week", status: "done", createdAt: at("2026-09-10T15:00:00Z"), doneAt: at("2026-09-21T15:00:00Z") },
      { text: "Added tomorrow", status: "open", createdAt: at("2026-09-29T15:00:00Z") },
      { text: "Finished tomorrow", status: "done", createdAt: at("2026-09-23T15:00:00Z"), doneAt: at("2026-09-29T15:00:00Z"), doneBy: "x@y.ca" },
      { text: "  ", status: "open", createdAt: at("2026-09-20T15:00:00Z") },
    ],
    items,
    "2026-09-28"
  );
  assert.deepEqual({ total: out.total, done: out.done, blocked: out.blocked, open: out.open }, { total: 4, done: 1, blocked: 1, open: 2 });
  assert.deepEqual(out.items.map((t) => [t.text, t.status]), [
    ["Stack block", "done"],
    ["Wall check", "blocked"],
    ["Finished tomorrow", "open"],
    ["Sweep level 1", "open"],
  ], "finished after the report day was still open on it; finished before, or added after, is left out");
  assert.equal(out.items[0].doneBy, "crew");
  assert.match(out.items[0].doneTime, /2:15/);
  assert.equal(out.items[0].area, "Phase 1");
  assert.equal(out.items[1].note, "Gate locked");
  const sweep = out.items.find((t) => t.text === "Sweep level 1");
  assert.equal(sweep.since, "2026-09-20");
  assert.equal(sweep.overdueDays, 3);
  assert.equal(sweep.note, "");
  assert.deepEqual(tasksForReport([], [], "2026-09-28"), { total: 0, done: 0, blocked: 0, open: 0, items: [] });
});

test("loadSiteLogisticsForReport includes the tasks, and tasks alone are enough to make a section", async () => {
  const db = fakeDb({
    sites: [{ id: "s1", name: "Docksteader" }],
    tasks: [
      { id: "t1", text: "Sweep", status: "done", doneAt: new Date("2026-09-30T18:00:00Z").getTime(), doneBy: "a@b.ca", trade: "Masonry", company: "Legacy", createdAt: new Date("2026-09-25T15:00:00Z").getTime() },
      { id: "t2", text: "Still to do", status: "open", createdAt: new Date("2026-09-26T15:00:00Z").getTime() },
    ],
  });
  const day = await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-09-30", logger: silent, db });
  assert.equal(day.tasks.total, 2);
  assert.equal(day.tasks.done, 1);
  assert.equal(day.tasks.open, 1);
  assert.equal(day.crews.length, 0);
});

test("equipment on site comes from Site Logistics' daily record, sorted, with the crew and rental", () => {
  const { buildSiteLogisticsDay, equipmentFromRecord } = require("./siteLogisticsReport");
  const record = {
    equipment: {
      list: [
        { typeLabel: "Scissor lift", label: "SL1", unitNumber: "SL-9", rentalCompany: "Sunbelt", trade: "Electrical", company: "Sparky Ltd", drawing: "Level 1", onRent: true },
        { typeLabel: "Heater 2.5M BTU", label: "", unitNumber: "", rentalCompany: "", trade: "", company: "", drawing: "Site plan", onRent: false },
      ],
    },
  };
  assert.deepEqual(equipmentFromRecord(record), [
    { type: "Heater 2.5M BTU", name: "", unit: "", rental: "", crew: "", drawing: "Site plan", onRent: false },
    { type: "Scissor lift", name: "SL1", unit: "SL-9", rental: "Sunbelt", crew: "Sparky Ltd (Electrical)", drawing: "Level 1", onRent: true },
  ]);
  assert.deepEqual(equipmentFromRecord(null), []);
  const day = buildSiteLogisticsDay({ dayDoc: null, bookings: [], items: [], tasks: [], record, dateKey: "2026-10-08" });
  assert.equal(day.equipment.length, 2, "a day with only equipment still has something for the report");
});

test("loadSiteLogisticsForReport adds the day's equipment, and carries on without a record", async () => {
  const base = { sites: [{ id: "s1", name: "Docksteader Rd" }], days: { "2026-10-08": { crews: [{ trade: "Electrical", company: "Sparky", workers: 5 }] } } };
  const records = { "2026-10-08": { equipment: { list: [{ typeLabel: "Scissor lift", label: "SL1", onRent: true }] } } };
  const withRecord = await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-10-08", logger: silent, db: fakeDb({ ...base, records }) });
  assert.deepEqual(withRecord.equipment.map((e) => e.type), ["Scissor lift"]);
  const without = await loadSiteLogisticsForReport({ projectSlug: "docksteader", dateKey: "2026-10-08", logger: silent, db: fakeDb(base) });
  assert.equal(without.totalWorkers, 5, "headcount still there");
  assert.deepEqual(without.equipment, []);
});
