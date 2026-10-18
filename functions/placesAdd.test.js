const test = require("node:test");
const assert = require("node:assert/strict");
const ExcelJS = require("exceljs");
const { HttpsError } = require("firebase-functions/v2/https");
const {
  MAX_IMPORT_LOOKUPS,
  parseCoordinatesText,
  lookupPlaceAddress,
  addKnownPlace,
  parsePlacesSpreadsheet,
  importKnownPlaces,
} = require("./placesAdd");

const FieldValue = { serverTimestamp: () => new Date("2026-10-18T12:00:00.000Z") };
const ME = "user@example.com";

class FakeDb {
  constructor(seed = {}) {
    this.rows = new Map(Object.entries(seed));
    this.nextId = 1;
  }
  collection(col) {
    if (col !== "knownPlaces") throw new Error(`unexpected collection ${col}`);
    const db = this;
    const docRef = (id) => ({
      id,
      async set(patch, options = {}) {
        const current = options.merge ? db.rows.get(id) || {} : {};
        db.rows.set(id, { ...current, ...patch });
      },
    });
    return {
      doc: (id) => docRef(id || `new-${db.nextId++}`),
      where(field, op, value) {
        return {
          async get() {
            const docs = [...db.rows.entries()]
              .filter(([, data]) => data[field] === value)
              .map(([id, data]) => ({ id, data: () => data }));
            return { forEach: (fn) => docs.forEach(fn) };
          },
        };
      },
    };
  }
  placesNamed(name) {
    return [...this.rows.entries()].filter(([, p]) => p.name === name).map(([id, p]) => ({ id, ...p }));
  }
}

function seeded() {
  return new FakeDb({
    gym: { memberEmail: ME, name: "GoodLife", latitude: 43.5, longitude: -80.25, radiusMeters: 120, visitCount: 7, activity: "Gym", activitySource: "suggested" },
    theirs: { memberEmail: "other@example.com", name: "Raging Bull", latitude: 43.547, longitude: -80.2968, radiusMeters: 120, visitCount: 3 },
  });
}

function fakeFetch(resultsByQuery) {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    const query = decodeURIComponent(new URL(url).searchParams.get("q"));
    return { ok: true, json: async () => resultsByQuery[query] || [] };
  };
  return { fetchImpl, calls };
}

async function xlsxBase64(rows) {
  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet("Places");
  rows.forEach((row) => sheet.addRow(row));
  return Buffer.from(await workbook.xlsx.writeBuffer()).toString("base64");
}

test("reads typed coordinates and Google Maps links, and nothing else", () => {
  assert.deepEqual(parseCoordinatesText("43.5470973, -80.2967997"), { latitude: 43.5470973, longitude: -80.2967997 });
  assert.deepEqual(parseCoordinatesText("43.547 -80.297"), { latitude: 43.547, longitude: -80.297 });
  assert.deepEqual(parseCoordinatesText("https://www.google.com/maps/place/X/@43.547,-80.297,17z"), { latitude: 43.547, longitude: -80.297 });
  assert.deepEqual(parseCoordinatesText("https://maps.google.com/?q=43.547,-80.297"), { latitude: 43.547, longitude: -80.297 });
  assert.equal(parseCoordinatesText("123 Main St, Guelph"), null);
  assert.equal(parseCoordinatesText("95, 200"), null, "longitude out of range");
  assert.equal(parseCoordinatesText("0, 0"), null);
});

test("an address lookup returns OpenStreetMap's matches and identifies the app", async () => {
  const { fetchImpl, calls } = fakeFetch({
    "Raging Bull, Guelph": [
      { display_name: "Raging Bull, Stone Road, Guelph, Ontario", lat: "43.5471", lon: "-80.2968" },
      { display_name: "No coordinates", lat: "x", lon: "y" },
    ],
  });
  const { candidates } = await lookupPlaceAddress({ query: " Raging Bull,  Guelph ", fetchImpl });
  assert.deepEqual(candidates, [{ label: "Raging Bull, Stone Road, Guelph, Ontario", latitude: 43.5471, longitude: -80.2968 }]);
  assert.match(calls[0].url, /^https:\/\/nominatim\.openstreetmap\.org\/search\?format=jsonv2&limit=5&q=/);
  assert.match(calls[0].options.headers["User-Agent"], /GridlineAI/);
});

test("typed coordinates skip the address lookup", async () => {
  const { fetchImpl, calls } = fakeFetch({});
  const { candidates } = await lookupPlaceAddress({ query: "43.547, -80.297", fetchImpl });
  assert.equal(candidates[0].fromCoordinates, true);
  assert.equal(calls.length, 0);
});

test("a failed lookup service is reported, not treated as no match", async () => {
  await assert.rejects(lookupPlaceAddress({ query: "Somewhere", fetchImpl: async () => ({ ok: false }) }), HttpsError);
});

test("adds a place with no visits, using the activity given", async () => {
  const db = seeded();
  const result = await addKnownPlace({
    db, FieldValue, memberEmail: ME, name: "Raging Bull", latitude: "43.5471", longitude: -80.2968, activity: "Eating out", address: "Stone Road, Guelph",
  });
  assert.equal(result.action, "added");
  const [place] = db.placesNamed("Raging Bull").filter((p) => p.memberEmail === ME);
  assert.equal(place.visitCount, 0);
  assert.equal(place.currentVisitStartedAt, undefined, "no stay is open");
  assert.equal(place.lastVisitAt, undefined);
  assert.equal(place.activity, "Eating out");
  assert.equal(place.activitySource, "set");
  assert.equal(place.radiusMeters, 120);
  assert.equal(place.address, "Stone Road, Guelph");
  assert.equal(place.source, "manual");
});

test("without an activity, the name suggests one", async () => {
  const db = seeded();
  await addKnownPlace({ db, FieldValue, memberEmail: ME, name: "Costco Guelph", latitude: 43.52, longitude: -80.23 });
  const [place] = db.placesNamed("Costco Guelph");
  assert.equal(place.activity, "Grocery shopping");
  assert.equal(place.activitySource, "suggested");
});

test("adding a name already saved at that spot updates it and keeps its visits", async () => {
  const db = seeded();
  const result = await addKnownPlace({ db, FieldValue, memberEmail: ME, name: "goodlife", latitude: 43.5003, longitude: -80.25, activity: "Workout", radiusMeters: 200 });
  assert.equal(result.action, "updated");
  assert.equal(result.id, "gym");
  const gym = db.rows.get("gym");
  assert.equal(gym.visitCount, 7);
  assert.equal(gym.activity, "Workout");
  assert.equal(gym.activitySource, "set");
  assert.equal(gym.radiusMeters, 200);
  assert.equal(gym.name, "GoodLife", "keeps the name it was saved with");
});

test("another member's place with the same name is never touched", async () => {
  const db = seeded();
  const result = await addKnownPlace({ db, FieldValue, memberEmail: ME, name: "Raging Bull", latitude: 43.547, longitude: -80.2968 });
  assert.equal(result.action, "added");
  assert.equal(db.rows.get("theirs").visitCount, 3);
  assert.equal(db.rows.get("theirs").memberEmail, "other@example.com");
});

test("rejects a place with no name, bad coordinates or an out-of-range radius", async () => {
  const db = seeded();
  await assert.rejects(addKnownPlace({ db, FieldValue, memberEmail: ME, name: " ", latitude: 43, longitude: -80 }), HttpsError);
  await assert.rejects(addKnownPlace({ db, FieldValue, memberEmail: ME, name: "X", latitude: 91, longitude: -80 }), HttpsError);
  await assert.rejects(addKnownPlace({ db, FieldValue, memberEmail: ME, name: "X", latitude: "abc", longitude: -80 }), HttpsError);
  await assert.rejects(addKnownPlace({ db, FieldValue, memberEmail: ME, name: "X", latitude: 43, longitude: -80, radiusMeters: 5 }), HttpsError);
});

test("reads an Excel sheet with a title row above the headers and flexible column names", async () => {
  const fileBase64 = await xlsxBase64([
    ["My places"],
    ["Place Name", "Lat", "Lng", "Activities", "Radius (m)", "Notes"],
    ["Raging Bull", 43.5471, -80.2968, "Eating out", 150, "steak"],
    [],
    [{ richText: [{ text: "Quick " }, { text: "Oil" }] }, "43.79362", "-79.76264", "", "", ""],
  ]);
  const rows = await parsePlacesSpreadsheet(Buffer.from(fileBase64, "base64"), "places.xlsx");
  assert.deepEqual(rows, [
    { rowNumber: 3, name: "Raging Bull", latitude: 43.5471, longitude: -80.2968, address: "", activity: "Eating out", radiusMeters: "150" },
    { rowNumber: 5, name: "Quick Oil", latitude: 43.79362, longitude: -79.76264, address: "", activity: "", radiusMeters: "" },
  ]);
});

test("reads a CSV with a combined coordinates column and quoted commas", async () => {
  const csv = '﻿Name,Coordinates,Address,Activity\r\n"Bells of Steel, Barrie","44.38, -79.69",,Gym\r\nCabin,,"12 Lake Rd, Muskoka",Visit\r\n';
  const rows = await parsePlacesSpreadsheet(Buffer.from(csv), "places.csv");
  assert.equal(rows[0].name, "Bells of Steel, Barrie");
  assert.equal(rows[0].latitude, 44.38);
  assert.equal(rows[0].longitude, -79.69);
  assert.equal(rows[1].latitude, null);
  assert.equal(rows[1].address, "12 Lake Rd, Muskoka");
});

test("a sheet without a name column or any location columns is rejected with a clear message", async () => {
  await assert.rejects(parsePlacesSpreadsheet(Buffer.from("Foo,Bar\n1,2\n"), "x.csv"), /Name column/);
  await assert.rejects(parsePlacesSpreadsheet(Buffer.from("Name,Activity\nX,Gym\n"), "x.csv"), /Latitude and Longitude/);
});

test("a preview saves nothing and says what each row would do", async () => {
  const db = seeded();
  const { fetchImpl, calls } = fakeFetch({});
  const csv = [
    "Name,Latitude,Longitude,Address,Activity",
    "GoodLife,43.5002,-80.25,,Workout",
    "Raging Bull,43.5471,-80.2968,,Eating out",
    "Cabin,,,12 Lake Rd Muskoka,",
    "Nowhere,,,,",
    "Bad,95,-80,,",
  ].join("\n");
  const result = await importKnownPlaces({ db, FieldValue, memberEmail: ME, fileName: "p.csv", fileBase64: Buffer.from(csv).toString("base64"), dryRun: true, fetchImpl });
  assert.deepEqual(result.rows.map((row) => row.status), ["update", "add", "lookup", "error", "error"]);
  assert.deepEqual(result.summary, { rows: 5, added: 0, updated: 0, toAdd: 1, toUpdate: 1, toLookUp: 1, errors: 2 });
  assert.equal(db.rows.size, 2, "nothing saved");
  assert.equal(calls.length, 0, "nothing looked up");
});

test("an import saves each row, looks up address-only rows one a second, and reports every outcome", async () => {
  const db = seeded();
  const { fetchImpl, calls } = fakeFetch({
    "12 Lake Rd Muskoka": [{ display_name: "12 Lake Road, Muskoka, Ontario", lat: "45.1", lon: "-79.3" }],
  });
  const slept = [];
  const fileBase64 = await xlsxBase64([
    ["Name", "Latitude", "Longitude", "Address", "Activity"],
    ["GoodLife", 43.5002, -80.25, "", "Workout"],
    ["Raging Bull", 43.5471, -80.2968, "", "Eating out"],
    ["Raging Bull", 43.5472, -80.2968, "", "Eating out"],
    ["Cabin", "", "", "12 Lake Rd Muskoka", ""],
    ["Lost", "", "", "Nowhere at all", ""],
  ]);
  const result = await importKnownPlaces({ db, FieldValue, memberEmail: ME, fileName: "p.xlsx", fileBase64, fetchImpl, sleep: async (ms) => slept.push(ms) });
  assert.deepEqual(result.rows.map((row) => row.status), ["updated", "added", "updated", "added", "error"]);
  assert.equal(result.rows[3].foundAddress, "12 Lake Road, Muskoka, Ontario");
  assert.match(result.rows[4].message, /Address not found/);
  assert.equal(db.placesNamed("Raging Bull").filter((p) => p.memberEmail === ME).length, 1, "a name listed twice is saved once");
  const [cabin] = db.placesNamed("Cabin");
  assert.equal(cabin.latitude, 45.1);
  assert.equal(cabin.source, "import");
  assert.equal(cabin.address, "12 Lake Rd Muskoka");
  assert.equal(db.rows.get("gym").activity, "Workout");
  assert.equal(calls.length, 2);
  assert.deepEqual(slept, [1100], "a pause between lookups, none before the first");
  assert.equal(result.summary.added, 2);
  assert.equal(result.summary.updated, 2);
  assert.equal(result.summary.errors, 1);
});

test("an import stops looking up addresses after the per-import limit", async () => {
  const db = seeded();
  const { fetchImpl, calls } = fakeFetch({});
  const lines = ["Name,Address"];
  for (let i = 0; i < MAX_IMPORT_LOOKUPS + 2; i += 1) lines.push(`Place ${i},Street ${i}`);
  const result = await importKnownPlaces({
    db, FieldValue, memberEmail: ME, fileName: "p.csv", fileBase64: Buffer.from(lines.join("\n")).toString("base64"), fetchImpl, sleep: async () => {},
  });
  assert.equal(calls.length, MAX_IMPORT_LOOKUPS);
  assert.match(result.rows.at(-1).message, /Only 40 addresses/);
});

test("rejects an empty, unreadable or oversized file", async () => {
  const db = seeded();
  await assert.rejects(importKnownPlaces({ db, FieldValue, memberEmail: ME, fileName: "p.xlsx", fileBase64: "" }), /empty/);
  await assert.rejects(
    importKnownPlaces({ db, FieldValue, memberEmail: ME, fileName: "p.xlsx", fileBase64: Buffer.from("not a workbook").toString("base64") }),
    /could not be read/
  );
  await assert.rejects(
    importKnownPlaces({ db, FieldValue, memberEmail: ME, fileName: "p.csv", fileBase64: Buffer.alloc(3 * 1024 * 1024, 65).toString("base64") }),
    /too large/
  );
});
