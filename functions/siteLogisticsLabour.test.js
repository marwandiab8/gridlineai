const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");

const { LABOUR_ACTIVITIES, LABOUR_CATEGORIES } = require("./labourActivityCodes");
const {
  normalizeSiteLogisticsIds,
  projectSlugForSite,
  saveSiteLogisticsLabourDay,
  verifySiteLogisticsCaller,
} = require("./siteLogisticsLabour");

// A small in-memory Firestore: enough for where("==" / "array-contains" / ">=" / "<="), orderBy, limit, get, doc().set.
function fakeDb(collections) {
  const data = Object.fromEntries(Object.entries(collections).map(([k, v]) => [k, new Map(Object.entries(v))]));
  const snapDoc = (id, value) => ({ id, exists: true, data: () => ({ ...value }), get: (k) => value[k] });
  function query(name, filters = []) {
    return {
      where: (field, op, value) => query(name, [...filters, [field, op, value]]),
      orderBy: () => query(name, filters),
      limit: () => query(name, filters),
      get: async () => {
        const docs = [...(data[name] || new Map()).entries()].filter(([, v]) => filters.every(([f, op, val]) => {
          if (op === "==") return v[f] === val;
          if (op === "array-contains") return Array.isArray(v[f]) && v[f].includes(val);
          if (op === ">=") return v[f] >= val;
          if (op === "<=") return v[f] <= val;
          throw new Error(op);
        })).map(([id, v]) => snapDoc(id, v));
        return { docs, size: docs.length };
      },
    };
  }
  return {
    data,
    collection: (name) => ({
      ...query(name),
      doc: (id) => ({ set: async (value) => { (data[name] = data[name] || new Map()).set(id, value); } }),
    }),
  };
}

const FieldValue = { serverTimestamp: () => "SERVER_TIME" };
const NOW = new Date("2026-10-05T18:00:00Z");
const LABOURERS = {
  "+19057164743": { phoneE164: "+19057164743", name: "Kevin Ashdown", active: true, siteLogisticsIds: ["ashdownk01@gmail.com"] },
  "+12893385196": { phoneE164: "+12893385196", name: "Shawn Jones", active: true, siteLogisticsIds: ["shawn83jones@gmail.com"] },
};
const BODY = {
  siteId: "viWiEkc1KoPBnRds0zzh",
  siteName: "Docksteader",
  submissionId: "ashdownk01@gmail.com_2026-10-05",
  by: "AshdownK01@gmail.com",
  dateKey: "2026-10-05",
  lines: [
    { code: "WH-HOARD-LIFT", hours: 5, location: "Z1 East Wing - L2", note: "east windows" },
    { code: "GC-HOUSE", hours: 4, location: "Z2 Admin - L1", note: "" },
  ],
};

test("a linked labourer's day from Site Logistics becomes one pending entry with locations", async () => {
  const db = fakeDb({ labourers: LABOURERS, labourEntries: {} });
  const out = await saveSiteLogisticsLabourDay({ db, FieldValue, body: BODY, now: NOW, env: {} });
  assert.equal(out.status, 200, JSON.stringify(out.body));
  assert.equal(out.body.entryId, "sl_19057164743_2026-10-05");
  const saved = db.data.labourEntries.get("sl_19057164743_2026-10-05");
  assert.equal(saved.labourerName, "Kevin Ashdown");
  assert.equal(saved.projectSlug, "docksteader");
  assert.equal(saved.minutesWorked, 540);
  assert.equal(saved.source, "site-logistics");
  assert.deepEqual(saved.review, { status: "pending" });
  assert.deepEqual(saved.lines, [
    { code: "WH-HOARD-LIFT", minutes: 300, text: "east windows", codedBy: "labourer", location: "Z1 East Wing - L2" },
    { code: "GC-HOUSE", minutes: 240, text: "", codedBy: "labourer", location: "Z2 Admin - L1" },
  ]);
  assert.equal(saved.workOn, "5h Hoarding - install from scissor lift @ Z1 East Wing - L2 (east windows) - 4h Site housekeeping @ Z2 Admin - L1");

  // Sending again before approval replaces the same entry.
  const again = await saveSiteLogisticsLabourDay({ db, FieldValue, body: { ...BODY, lines: [{ code: "WH-SNOW", hours: 8, location: "Roof B" }] }, now: NOW, env: {} });
  assert.equal(again.status, 200);
  assert.equal(again.body.updated, true);
  assert.equal(db.data.labourEntries.size, 1);
  assert.equal(db.data.labourEntries.get("sl_19057164743_2026-10-05").minutesWorked, 480);
});

test("Site Logistics never overwrites a texted day or an approved day", async () => {
  const texted = fakeDb({ labourers: LABOURERS, labourEntries: { abc: { labourerPhone: "+19057164743", reportDateKey: "2026-10-05", source: "sms", minutesWorked: 540 } } });
  const out = await saveSiteLogisticsLabourDay({ db: texted, FieldValue, body: BODY, now: NOW, env: {} });
  assert.equal(out.status, 409);
  assert.equal(out.body.code, "already_entered");
  assert.match(out.body.message, /already sent by text/);

  const approved = fakeDb({ labourers: LABOURERS, labourEntries: { "sl_19057164743_2026-10-05": { labourerPhone: "+19057164743", reportDateKey: "2026-10-05", source: "site-logistics", review: { status: "approved" } } } });
  const locked = await saveSiteLogisticsLabourDay({ db: approved, FieldValue, body: BODY, now: NOW, env: {} });
  assert.equal(locked.status, 409);
  assert.equal(locked.body.code, "approved");

  const corrected = fakeDb({ labourers: LABOURERS, labourEntries: { "sl_19057164743_2026-10-05": { labourerPhone: "+19057164743", reportDateKey: "2026-10-05", source: "site-logistics", minutesWorked: 480, review: { status: "pending" }, hoursCorrection: { fromMinutes: 360, toMinutes: 480 } } } });
  const kept = await saveSiteLogisticsLabourDay({ db: corrected, FieldValue, body: BODY, now: NOW, env: {} });
  assert.equal(kept.status, 409, "the supervisor's correction isn't overwritten by sending again");
  assert.equal(kept.body.code, "corrected");
  assert.match(kept.body.message, /supervisor corrected your hours for 2026-10-05/);
  assert.equal(corrected.data.labourEntries.get("sl_19057164743_2026-10-05").minutesWorked, 480);
});

test("hours are refused with a clear reason when something is wrong", async () => {
  const db = fakeDb({ labourers: LABOURERS, labourEntries: {} });
  const send = (patch) => saveSiteLogisticsLabourDay({ db, FieldValue, body: { ...BODY, ...patch }, now: NOW, env: {} });
  assert.equal((await send({ by: "stranger@example.com" })).body.code, "not_linked");
  assert.equal((await send({ dateKey: "2026-10-07" })).body.code, "bad_date");
  assert.equal((await send({ siteName: "Some other job" })).body.code, "unknown_site");
  assert.equal((await send({ lines: [{ code: "", hours: 8 }] })).body.code, "bad_lines");
  assert.equal((await send({ lines: [{ code: "WH-SNOW", hours: 0.1 }] })).body.code, "bad_lines");
  assert.equal((await send({ lines: [{ code: "WH-SNOW", hours: 13 }, { code: "GC-HOUSE", hours: 12 }] })).body.code, "bad_lines");
  assert.equal(db.data.labourEntries.size, 0);
});

test("sign-ins are normalized, and the site maps to its project", () => {
  assert.deepEqual(normalizeSiteLogisticsIds([" Ashdownk01@Gmail.com ", "905-716-4743", "ashdownk01@gmail.com"]), ["ashdownk01@gmail.com", "+19057164743"]);
  assert.throws(() => normalizeSiteLogisticsIds(["not an id"]), /not an email/);
  assert.equal(projectSlugForSite({ siteId: "x", siteName: "Docksteader PRPS" }, {}), "docksteader");
  assert.equal(projectSlugForSite({ siteId: "x", siteName: "Other" }, {}), "");
  assert.equal(projectSlugForSite({ siteId: "pinned", siteName: "Other" }, { SITE_LOGISTICS_SITE_ID: "pinned" }), "docksteader");
});

test("only a request carrying a valid token is accepted", async () => {
  assert.equal(await verifySiteLogisticsCaller({ headers: {} }, "https://example.test"), false);
  assert.equal(await verifySiteLogisticsCaller({ headers: { authorization: "Bearer not-a-token" } }, "https://example.test"), false);
});

test("the keyword list published for Site Logistics matches the server's", () => {
  const published = JSON.parse(fs.readFileSync(path.resolve(__dirname, "../public/labour-activity-codes.json"), "utf8"));
  assert.deepEqual(published.categories, LABOUR_CATEGORIES.map(({ id, label }) => ({ id, label })));
  assert.deepEqual(published.activities, LABOUR_ACTIVITIES.map(({ code, category, keyword, label, hint }) => ({ code, category, keyword, label, hint })));
});
