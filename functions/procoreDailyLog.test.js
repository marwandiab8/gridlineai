const test = require("node:test");
const assert = require("node:assert/strict");
const {
  parseProcorePageSummary,
  parsePhotoCount,
  normalizeProcoreDailyLog,
  procoreDailyLogHash,
  procoreJournalQueueId,
  storeProcoreDailyLog,
  loadProcoreDailyLog,
  formatClock,
  describeProcoreLogFacts,
  describeProcoreWeather,
  describeProcoreVisitor,
  describeProcoreInspection,
} = require("./procoreDailyLog");

// The Daily Log page's summary as the export reads it (Procore writes degrees as "º").
const SUMMARY = [
  "Mon, November 2, 2026", "Daily log completed",
  "The daily log was completed by Pat Lee on Tue, November 3, 2026.", "Re-open day",
  "6 AM", "Clear", "9 ºC", "9 AM", "Partly cloudy", "12 ºC", "12 PM", "Partly cloudy", "15 ºC",
  "3 PM", "Partly cloudy", "17 ºC", "6 PM", "Partly cloudy", "15 ºC", "9 PM", "Partly cloudy", "15 ºC",
  "Humidity", "Low", "59 %", "Avg.", "75 %", "High", "86 %", "Dew", "8ºC",
  "Precipitation since", "Midnight", "0 cm", "Two days ago", "0.01 cm", "Three days ago", "0.02 cm",
  "Temperature", "Low", "8 ºC", "Avg.", "12 ºC", "High", "17 ºC",
  "Wind speed", "Avg.", "4.4 km/h", "Max", "6 km/h", "Gusts", "20 km/h",
].join(" / ");

function payload(overrides = {}) {
  return {
    schema: 1,
    source: "procore-daily-notes",
    procoreCompanyId: 51994,
    procoreProjectId: 3118812,
    dateKey: "2026-11-02",
    exportedAt: "2026-11-04T14:10:00.000Z",
    page: { summary: SUMMARY, photos: "Photos 1-30 of 104 Page: 1" },
    logs: {
      weather_logs: [{ time: "2026-11-02T08:10:00-05:00", time_hour: "8", time_minute: "10" }],
      manpower_logs: [
        { vendor: "General Contractor Ltd.", contact: "General Contractor Ltd.", num_workers: "5", num_hours: "8.0", man_hours: "40.0", notes: "PM, superintendent and field engineer." },
        { vendor: "Northside Electric Ltd.", num_workers: "5", num_hours: "8.0", man_hours: "40.0", notes: "5 workers on site. Foreman Kyle.  Conduit   rough-in on L3." },
        { vendor: "Block Masonry", num_workers: "5", num_hours: "10.0", notes: "Block wall at PH-3." },
      ],
      visitor_logs: [{ begin_hour: "12", begin_minute: "0", end_hour: "13", end_minute: "0", details: "Owner's rep reviewed progress for the October billing." }],
      inspection_logs: [{ vendor: "Englobe Corp.", inspecting_entity: "Englob", inspection_type: "Subgrade and compaction", inspector_name: "A. Sharma", start_hour: "9", start_minute: "0", end_hour: "10", end_minute: "0", comments: "Transformer pad and surrounding area." }],
      delivery_logs: [{ time_hour: "7", time_minute: "30", comments: "Rebar for the pad", tracking_number: "PO-118" }],
    },
    ...overrides,
  };
}

test("parseProcorePageSummary reads the day, who completed the log and the observed weather", () => {
  const s = parseProcorePageSummary(SUMMARY);
  assert.equal(s.dateKey, "2026-11-02");
  assert.equal(s.completed, true);
  assert.equal(s.completedBy, "Pat Lee");
  assert.equal(s.completedOn, "Tue, November 3, 2026");
  assert.deepEqual(s.weather.hourly[0], { time: "6 AM", sky: "Clear", tempC: 9 });
  assert.equal(s.weather.hourly.length, 6);
  assert.equal(s.weather.lowC, 8);
  assert.equal(s.weather.highC, 17);
  assert.equal(s.weather.precipCm, 0);
  assert.equal(s.weather.windAvgKmh, 4.4);
  assert.equal(s.weather.gustsKmh, 20);
  assert.equal(s.weather.humidityLow, 59);
  assert.equal(s.weather.humidityHigh, 86);
});

test("an unfinished log's summary isn't completed, and the photo count is read", () => {
  const s = parseProcorePageSummary("Mon, November 2, 2026 / Complete only / 6 AM / Clear / 9 ºC");
  assert.equal(s.completed, false);
  assert.equal(s.completedBy, "");
  assert.equal(parsePhotoCount("Photos 1-30 of 104 Page: 1"), 104);
  assert.equal(parsePhotoCount(""), null);
});

test("normalizeProcoreDailyLog shapes companies, totals, visitors, inspections and the other entries", () => {
  const { ok, log } = normalizeProcoreDailyLog(payload());
  assert.equal(ok, true);
  assert.equal(log.projectSlug, "docksteader");
  assert.equal(log.dateKey, "2026-11-02");
  assert.equal(log.photos, 104);
  assert.deepEqual(log.manpower.map((r) => [r.company, r.workers, r.manHours]), [
    ["General Contractor Ltd.", 5, 40],
    ["Northside Electric Ltd.", 5, 40],
    ["Block Masonry", 5, 50],
  ]);
  assert.equal(log.manpower[1].notes, "5 workers on site. Foreman Kyle. Conduit rough-in on L3.");
  assert.deepEqual(log.totals, { workers: 15, manHours: 130 });
  assert.deepEqual(log.visitors[0], { who: "", from: "12:00 PM", to: "1:00 PM", subject: "", details: "Owner's rep reviewed progress for the October billing." });
  assert.equal(log.inspections[0].entity, "Englobe Corp.");
  assert.equal(log.inspections[0].from, "9:00 AM");
  // A weather reading with nothing but its time is left to the page's weather; a delivery keeps its time and fields.
  assert.deepEqual(log.sections, [{ type: "delivery_logs", title: "Deliveries", entries: ["7:30 AM · Rebar for the pad · Tracking number: PO-118"] }]);
});

test("normalizeProcoreDailyLog refuses another project, a bad date, an unfinished log and the wrong day's page", () => {
  assert.equal(normalizeProcoreDailyLog(payload({ procoreProjectId: 999 })).error, "unknown_project");
  assert.equal(normalizeProcoreDailyLog(payload({ dateKey: "Nov 2" })).error, "bad_date");
  assert.equal(normalizeProcoreDailyLog(payload({ page: { summary: "Mon, November 2, 2026 / Complete only" } })).error, "not_completed");
  assert.equal(normalizeProcoreDailyLog(payload({ dateKey: "2026-11-03" })).error, "date_mismatch");
});

test("the journal's wording for the log", () => {
  const { log } = normalizeProcoreDailyLog(payload());
  assert.equal(describeProcoreLogFacts(log), "Completed by Pat Lee on Tue, November 3, 2026 · 15 workers, 130 hours · 104 photos in Procore");
  const weather = describeProcoreWeather(log.weather);
  assert.equal(weather.summary, "Clear, then partly cloudy, 8 to 17 °C, wind 4.4 km/h average with gusts to 20 km/h, no precipitation.");
  assert.equal(weather.hourly.split(" · ")[0], "6 AM 9 °C");
  assert.equal(describeProcoreWeather({ hourly: [], precipCm: 0.02 }).summary, "Trace precipitation.");
  assert.deepEqual(describeProcoreWeather(null), { summary: "", hourly: "" });
  assert.equal(describeProcoreVisitor(log.visitors[0]), "12:00 PM – 1:00 PM: Owner's rep reviewed progress for the October billing.");
  assert.equal(
    describeProcoreInspection(log.inspections[0]),
    "9:00 AM – 10:00 AM · Englobe Corp. (A. Sharma), Subgrade and compaction: Transformer pad and surrounding area."
  );
  assert.equal(formatClock("0", "5"), "12:05 AM");
  assert.equal(formatClock("", "0"), "");
});

test("the log's hash changes with its content, not with when it was sent", () => {
  const a = normalizeProcoreDailyLog(payload()).log;
  const b = normalizeProcoreDailyLog(payload({ exportedAt: "2026-11-05T09:00:00.000Z" })).log;
  assert.equal(procoreDailyLogHash(a), procoreDailyLogHash(b));
  const changed = payload();
  changed.logs.manpower_logs[0].num_workers = "6";
  assert.notEqual(procoreDailyLogHash(normalizeProcoreDailyLog(changed).log), procoreDailyLogHash(a));
});

// ----- Keeping the log -----

const FieldValue = { serverTimestamp: () => "SERVER_TIME" };
const logger = { info() {}, warn() {}, error() {} };

class FakeDb {
  constructor(rows = {}) { this.rows = new Map(Object.entries(rows)); }
  collection(col) {
    const db = this;
    return {
      doc: (id) => {
        const path = `${col}/${id}`;
        return {
          path,
          async get() {
            const data = db.rows.get(path);
            return { id, exists: Boolean(data), get: (k) => (data || {})[k], data: () => data };
          },
        };
      },
    };
  }
  async runTransaction(fn) {
    const writes = [];
    const tx = {
      get: (ref) => ref.get(),
      set: (ref, data) => writes.push(() => this.rows.set(ref.path, { ...data })),
      update: (ref, data) => writes.push(() => this.rows.set(ref.path, { ...this.rows.get(ref.path), ...data })),
    };
    const out = await fn(tx);
    writes.forEach((write) => write());
    return out;
  }
}

const store = (db, body, extra = {}) =>
  storeProcoreDailyLog({ db, FieldValue, logger, log: normalizeProcoreDailyLog(body).log, exportedAt: body.exportedAt, ...extra });

test("a day's first log is kept and queues that day's journal; the same log again changes nothing", async () => {
  const db = new FakeDb({ "projects/docksteader": { name: "Docksteader" } });

  const first = await store(db, payload());
  assert.deepEqual(first, { ok: true, id: "docksteader__2026-11-02", created: true, changed: true, queued: true , photosQueued: false });
  const stored = db.rows.get("procoreDailyLogs/docksteader__2026-11-02");
  assert.equal(stored.log.totals.workers, 15);
  assert.equal(stored.journalQueued, true);
  assert.equal(stored.exportedAt, "2026-11-04T14:10:00.000Z");
  assert.equal(procoreJournalQueueId("docksteader", "2026-11-02"), "2026-11-02__docksteader__journal__management__procore");
  const queue = db.rows.get("managementJournalPushQueue/2026-11-02__docksteader__journal__management__procore");
  assert.equal(queue.status, "queued");
  assert.equal(queue.variant, "procore");
  assert.equal(queue.projectName, "Docksteader");
  assert.equal(queue.reportType, "journal");
  assert.equal(queue.audience, "management");

  const again = await store(db, payload({ exportedAt: "2026-11-04T18:00:00.000Z" }));
  assert.deepEqual(again, { ok: true, id: "docksteader__2026-11-02", created: false, changed: false, queued: false , photosQueued: false });
});

test("a changed log is updated without texting the journal again; deliver:false keeps a past day's log only", async () => {
  const db = new FakeDb();
  await store(db, payload());
  db.rows.delete("managementJournalPushQueue/2026-11-02__docksteader__journal__management__procore");

  const changed = payload();
  changed.logs.manpower_logs[0].num_workers = "6";
  const updated = await store(db, changed);
  assert.deepEqual(updated, { ok: true, id: "docksteader__2026-11-02", created: false, changed: true, queued: false , photosQueued: false });
  assert.equal(db.rows.get("procoreDailyLogs/docksteader__2026-11-02").log.totals.workers, 16);
  assert.equal(db.rows.get("procoreDailyLogs/docksteader__2026-11-02").createdAt, "SERVER_TIME");
  assert.equal(db.rows.has("managementJournalPushQueue/2026-11-02__docksteader__journal__management__procore"), false);

  const pastDay = payload({ dateKey: "2026-10-30" });
  pastDay.page = { summary: SUMMARY.replace("Mon, November 2, 2026", "Fri, October 30, 2026"), photos: "" };
  const kept = await store(db, pastDay, { deliver: false });
  assert.deepEqual(kept, { ok: true, id: "docksteader__2026-10-30", created: true, changed: true, queued: false , photosQueued: false });
  assert.equal(db.rows.has("managementJournalPushQueue/2026-10-30__docksteader__journal__management__procore"), false);
  assert.equal(db.rows.get("procoreDailyLogs/docksteader__2026-10-30").journalQueued, false);
});

test("loadProcoreDailyLog gives the stored log with every part present, or null", async () => {
  const db = new FakeDb({ "procoreDailyLogs/docksteader__2026-11-02": { log: { manpower: [{ company: "A", workers: 1, manHours: 8 }] } } });
  const log = await loadProcoreDailyLog(db, "docksteader", "2026-11-02");
  assert.deepEqual(log.visitors, []);
  assert.deepEqual(log.sections, []);
  assert.deepEqual(log.totals, { workers: 0, manHours: 0 });
  assert.equal(await loadProcoreDailyLog(db, "docksteader", "2026-11-03"), null);
  assert.equal(await loadProcoreDailyLog(db, "", "2026-11-02"), null);
});

test("the day's photo list is queued for copying, kept out of the log, and not copied again once done", async () => {
  const files = "https://storage.procore.com/api/v5/files/ca-central-1/prostore";
  const withPhotos = (starred = false) =>
    payload({
      photos: [
        { id: 11, taken_at: "2026-11-02T14:00:00Z", uploader: "Pat Lee", starred, thumbnail_url: `${files}/t11?sig=a`, url: `${files}/f11?sig=a` },
        { id: 12, taken_at: "2026-11-02T15:00:00Z", uploader: "Pat Lee", starred: false, thumbnail_url: `${files}/t12?sig=a`, url: `${files}/f12?sig=a` },
      ],
    });
  const normalized = normalizeProcoreDailyLog(withPhotos());
  assert.equal(normalized.photoSources.length, 2);
  assert.equal(JSON.stringify(normalized.log).includes("storage.procore.com"), false, "the signed links aren't part of the log");

  const db = new FakeDb();
  const storeWith = (body) => {
    const n = normalizeProcoreDailyLog(body);
    return storeProcoreDailyLog({ db, FieldValue, logger, log: n.log, photoSources: n.photoSources });
  };
  const first = await storeWith(withPhotos());
  assert.equal(first.photosQueued, true);
  const queued = db.rows.get("procorePhotoQueue/docksteader__2026-11-02");
  assert.equal(queued.sources.length, 2);
  assert.equal(queued.dateKey, "2026-11-02");

  // Copied (the photo trigger records its photoSet and removes the queue entry): the same photos again aren't queued.
  db.rows.delete("procorePhotoQueue/docksteader__2026-11-02");
  const stored = db.rows.get("procoreDailyLogs/docksteader__2026-11-02");
  db.rows.set("procoreDailyLogs/docksteader__2026-11-02", { ...stored, photoSet: { status: "done", sourceHash: queued.sourceHash } });
  const again = await storeWith(withPhotos());
  assert.deepEqual({ changed: again.changed, photosQueued: again.photosQueued }, { changed: false, photosQueued: false });
  assert.equal(db.rows.has("procorePhotoQueue/docksteader__2026-11-02"), false);

  // A photo starred afterwards: the log is unchanged but the photos are picked again.
  const starred = await storeWith(withPhotos(true));
  assert.deepEqual({ changed: starred.changed, photosQueued: starred.photosQueued }, { changed: false, photosQueued: true });
  assert.equal(db.rows.get("procorePhotoQueue/docksteader__2026-11-02").sources[0].starred, true);
});
