// Adding known places without visiting them: one at a time from an address or coordinates, or many at
// once from a spreadsheet (public/places.html). "Log this place" (placesIntegration.js) learns a place
// by being there; these save it ahead of time, so the first real visit is already recognized.
//
// A place saved here has no visits yet (visitCount 0, no stay open). Its activity is the one given, or
// the one its name suggests. Saving a name that already exists at that spot (within the saved place's
// radius) updates that place instead of adding a second one - the same rule "Log this place" uses.
//
// Addresses are looked up with OpenStreetMap's Nominatim service (no API key). Its usage policy allows
// at most one request a second, so a spreadsheet's address-only rows are looked up one by one and
// capped per import.
const { HttpsError } = require("firebase-functions/v2/https");
const ExcelJS = require("exceljs");
const {
  COL_KNOWN_PLACES,
  DEFAULT_RADIUS_METERS,
  haversineMeters,
  normalizePlaceName,
  placeNameKey,
} = require("./placeLearning");
const { normalizePlaceActivity, suggestedActivityFields } = require("./placeActivities");
const { MIN_RADIUS_METERS, MAX_RADIUS_METERS } = require("./placesAdmin");

const NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search";
const NOMINATIM_USER_AGENT = "GridlineAI-KnownPlaces/1.0 (+https://gridlineai.web.app)";
const GEOCODE_INTERVAL_MS = 1100;
const MAX_ADDRESS_LENGTH = 300;
const MAX_IMPORT_ROWS = 500;
const MAX_IMPORT_LOOKUPS = 40;
const MAX_IMPORT_FILE_BYTES = 2 * 1024 * 1024;

const sleepMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function cleanText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function validCoordinates(latitude, longitude) {
  return (
    Number.isFinite(latitude) &&
    Number.isFinite(longitude) &&
    latitude >= -90 &&
    latitude <= 90 &&
    longitude >= -180 &&
    longitude <= 180 &&
    !(latitude === 0 && longitude === 0)
  );
}

/**
 * Coordinates typed or pasted as text: "43.547, -80.297", "43.547 -80.297", or a Google Maps link
 * ("…/@43.547,-80.297,17z" or "…?q=43.547,-80.297"). Null when the text is not coordinates.
 */
function parseCoordinatesText(value) {
  const text = cleanText(value);
  if (!text) return null;
  const number = "(-?\\d{1,3}(?:\\.\\d+)?)";
  const patterns = [
    new RegExp(`^${number}\\s*[,;/ ]\\s*${number}$`),
    new RegExp(`@${number},${number}`),
    new RegExp(`[?&](?:q|query|ll|destination)=${number},\\s*${number}`),
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const latitude = Number(match[1]);
    const longitude = Number(match[2]);
    if (validCoordinates(latitude, longitude)) return { latitude, longitude };
  }
  return null;
}

function parseCoordinateValue(value) {
  if (value == null || value === "") return null;
  const number = typeof value === "number" ? value : Number(cleanText(value));
  return Number.isFinite(number) ? number : NaN;
}

/** Address matches from Nominatim, best first: [{ label, latitude, longitude }]. */
async function geocodeAddress(address, { fetchImpl = fetch, limit = 5 } = {}) {
  const query = cleanText(address).slice(0, MAX_ADDRESS_LENGTH);
  if (query.length < 3) return [];
  const url = `${NOMINATIM_SEARCH_URL}?format=jsonv2&limit=${limit}&q=${encodeURIComponent(query)}`;
  const response = await fetchImpl(url, {
    headers: { "User-Agent": NOMINATIM_USER_AGENT, "Accept-Language": "en", Accept: "application/json" },
  });
  if (!response || !response.ok) {
    throw new HttpsError("unavailable", "The address lookup service did not answer. Try again in a minute, or paste coordinates instead.");
  }
  const results = await response.json();
  return (Array.isArray(results) ? results : [])
    .map((result) => ({
      label: cleanText(result && result.display_name),
      latitude: Number(result && result.lat),
      longitude: Number(result && result.lon),
    }))
    .filter((candidate) => candidate.label && validCoordinates(candidate.latitude, candidate.longitude));
}

/** Matches for what was typed in "Address or coordinates": coordinates are used as they are. */
async function lookupPlaceAddress({ query, fetchImpl }) {
  const text = cleanText(query);
  if (!text) throw new HttpsError("invalid-argument", "Type an address or coordinates.");
  const coordinates = parseCoordinatesText(text);
  if (coordinates) {
    return { candidates: [{ label: `${coordinates.latitude}, ${coordinates.longitude}`, ...coordinates, fromCoordinates: true }] };
  }
  return { candidates: await geocodeAddress(text, { fetchImpl }) };
}

async function loadMemberPlaces(db, memberEmail) {
  const snap = await db.collection(COL_KNOWN_PLACES).where("memberEmail", "==", memberEmail).get();
  const places = [];
  snap.forEach((doc) => places.push({ id: doc.id, ...(doc.data() || {}) }));
  return places;
}

/** The member's place with this name whose radius covers (latitude, longitude), nearest first. */
function findSameNamedPlace(places, name, latitude, longitude) {
  const key = placeNameKey(name);
  return places
    .filter((place) => placeNameKey(place.name) === key && Number.isFinite(place.latitude) && Number.isFinite(place.longitude))
    .map((place) => ({ place, distance: haversineMeters(latitude, longitude, place.latitude, place.longitude) }))
    .filter(({ place, distance }) => {
      const radius = Number.isFinite(place.radiusMeters) && place.radiusMeters > 0 ? place.radiusMeters : DEFAULT_RADIUS_METERS;
      return distance <= radius;
    })
    .sort((a, b) => a.distance - b.distance)
    .map(({ place }) => place)[0] || null;
}

/**
 * Checks one place to save; returns { place } with clean fields, or { error } saying what is wrong.
 * Coordinates are required here - an address must already have been looked up.
 */
function validatePlaceInput({ name, latitude, longitude, activity, radiusMeters, address }) {
  const cleanName = normalizePlaceName(name);
  if (!cleanName) return { error: "A name is required." };
  if (!validCoordinates(latitude, longitude)) return { error: "Valid latitude and longitude are required." };
  let radius = null;
  if (radiusMeters != null && radiusMeters !== "") {
    radius = Number(radiusMeters);
    if (!Number.isFinite(radius) || radius < MIN_RADIUS_METERS || radius > MAX_RADIUS_METERS) {
      return { error: `Radius must be between ${MIN_RADIUS_METERS} and ${MAX_RADIUS_METERS} meters.` };
    }
    radius = Math.round(radius);
  }
  return {
    place: {
      name: cleanName,
      latitude,
      longitude,
      activity: normalizePlaceActivity(activity),
      radiusMeters: radius,
      address: cleanText(address).slice(0, MAX_ADDRESS_LENGTH),
    },
  };
}

/**
 * Saves a validated place: updates the same-named place already at that spot (its activity, radius and
 * address when given - visits and stays are kept), or adds a new one with no visits.
 * `places` is the member's current list and is kept up to date, so a spreadsheet that names the same
 * place twice updates it rather than adding it twice.
 */
async function savePlace({ db, FieldValue, memberEmail, places, place, source }) {
  const now = FieldValue.serverTimestamp();
  const existing = findSameNamedPlace(places, place.name, place.latitude, place.longitude);
  if (existing) {
    const update = {
      ...(place.activity ? { activity: place.activity, activitySource: "set" } : {}),
      ...(place.radiusMeters ? { radiusMeters: place.radiusMeters } : {}),
      ...(place.address ? { address: place.address } : {}),
      updatedAt: now,
    };
    await db.collection(COL_KNOWN_PLACES).doc(existing.id).set(update, { merge: true });
    Object.assign(existing, update);
    return { id: existing.id, name: existing.name, action: "updated", activity: existing.activity || "" };
  }
  const ref = db.collection(COL_KNOWN_PLACES).doc();
  const data = {
    memberEmail,
    name: place.name,
    latitude: place.latitude,
    longitude: place.longitude,
    radiusMeters: place.radiusMeters || DEFAULT_RADIUS_METERS,
    visitCount: 0,
    ...(place.activity ? { activity: place.activity, activitySource: "set" } : suggestedActivityFields(place.name)),
    ...(place.address ? { address: place.address } : {}),
    source,
    createdAt: now,
    updatedAt: now,
  };
  await ref.set(data);
  places.push({ id: ref.id, ...data });
  return { id: ref.id, name: place.name, action: "added", activity: data.activity };
}

/** Adds (or updates) one place from the "Add a place" form. */
async function addKnownPlace({ db, FieldValue, memberEmail, name, latitude, longitude, activity, radiusMeters, address }) {
  const checked = validatePlaceInput({
    name,
    latitude: parseCoordinateValue(latitude),
    longitude: parseCoordinateValue(longitude),
    activity,
    radiusMeters,
    address,
  });
  if (checked.error) throw new HttpsError("invalid-argument", checked.error);
  const places = await loadMemberPlaces(db, memberEmail);
  return savePlace({ db, FieldValue, memberEmail, places, place: checked.place, source: "manual" });
}

// Spreadsheet headers, matched ignoring case, spaces and punctuation.
const HEADER_ALIASES = {
  name: ["name", "place", "placename", "location", "locationname", "title", "business"],
  latitude: ["latitude", "lat"],
  longitude: ["longitude", "lng", "lon", "long"],
  coordinates: ["coordinates", "coords", "latlong", "latlng", "latlon", "gps", "position"],
  address: ["address", "streetaddress", "fulladdress"],
  activity: ["activity", "activities", "category", "type", "whatido", "whatidothere"],
  radiusMeters: ["radius", "radiusm", "radiusmeters", "radiusmetres", "matchingradius"],
};

function headerKey(value) {
  return cleanText(value).toLowerCase().replace(/[^a-z]/g, "");
}

/** Column index per field from a header row, or null when the row has no name column. */
function mapHeaderRow(cells) {
  const columns = {};
  cells.forEach((cell, index) => {
    const key = headerKey(cell);
    if (!key) return;
    for (const [field, aliases] of Object.entries(HEADER_ALIASES)) {
      if (columns[field] == null && aliases.includes(key)) columns[field] = index;
    }
  });
  return columns.name == null ? null : columns;
}

/** A plain value for an exceljs cell (rich text, hyperlinks and formulas included). */
function excelCellValue(value) {
  if (value == null) return "";
  if (typeof value === "number" || typeof value === "string" || typeof value === "boolean") return value;
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value.richText)) return value.richText.map((part) => part.text || "").join("");
  if (value.text != null) return excelCellValue(value.text);
  if (value.result != null) return excelCellValue(value.result);
  return "";
}

/** Rows of a CSV file as arrays of strings (quoted fields, commas, semicolons or tabs). */
function parseCsv(text) {
  const firstLine = text.split(/\r?\n/, 1)[0] || "";
  const delimiter = [",", ";", "\t"].sort((a, b) => firstLine.split(b).length - firstLine.split(a).length)[0];
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;
  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    if (quoted) {
      if (char === '"' && text[i + 1] === '"') {
        field += '"';
        i += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        field += char;
      }
    } else if (char === '"') {
      quoted = true;
    } else if (char === delimiter) {
      row.push(field);
      field = "";
    } else if (char === "\n" || char === "\r") {
      if (char === "\r" && text[i + 1] === "\n") i += 1;
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else {
      field += char;
    }
  }
  if (field || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

/** The first worksheet (or CSV) as arrays of cell values. */
async function readSpreadsheetRows(buffer, fileName) {
  if (/\.csv$/i.test(String(fileName || ""))) {
    return parseCsv(buffer.toString("utf8").replace(/^﻿/, ""));
  }
  const workbook = new ExcelJS.Workbook();
  try {
    await workbook.xlsx.load(buffer);
  } catch (_) {
    throw new HttpsError("invalid-argument", "That file could not be read. Save it as .xlsx or .csv and try again.");
  }
  const sheet = workbook.worksheets.find((candidate) => candidate.actualRowCount > 0);
  if (!sheet) return [];
  const rows = [];
  sheet.eachRow({ includeEmpty: true }, (row) => {
    const values = [];
    row.eachCell({ includeEmpty: true }, (cell, column) => {
      values[column - 1] = excelCellValue(cell.value);
    });
    rows[row.number - 1] = Array.from(values, (value) => (value == null ? "" : value));
  });
  return Array.from(rows, (row) => row || []);
}

/**
 * The spreadsheet's places: [{ rowNumber, name, latitude, longitude, address, activity, radiusMeters }],
 * with latitude/longitude null when the row only gives an address. The header row is the first one
 * with a name column (Name, Place, Location, ...).
 */
async function parsePlacesSpreadsheet(buffer, fileName) {
  const rows = await readSpreadsheetRows(buffer, fileName);
  const headerIndex = rows.findIndex((row) => mapHeaderRow(row));
  if (headerIndex < 0) {
    throw new HttpsError(
      "invalid-argument",
      "No header row with a Name column was found. Use columns like Name, Latitude, Longitude, Address, Activity."
    );
  }
  const columns = mapHeaderRow(rows[headerIndex]);
  if (columns.address == null && columns.coordinates == null && (columns.latitude == null || columns.longitude == null)) {
    throw new HttpsError("invalid-argument", "Add Latitude and Longitude columns (or a Coordinates or Address column).");
  }
  const cell = (row, field) => (columns[field] == null ? "" : row[columns[field]]);
  const places = [];
  rows.slice(headerIndex + 1).forEach((row, offset) => {
    if (!row.some((value) => cleanText(value))) return;
    let latitude = parseCoordinateValue(cell(row, "latitude"));
    let longitude = parseCoordinateValue(cell(row, "longitude"));
    if (latitude == null && longitude == null && cleanText(cell(row, "coordinates"))) {
      const parsed = parseCoordinatesText(cell(row, "coordinates"));
      latitude = parsed ? parsed.latitude : NaN;
      longitude = parsed ? parsed.longitude : NaN;
    }
    places.push({
      rowNumber: headerIndex + offset + 2,
      name: cleanText(cell(row, "name")),
      latitude,
      longitude,
      address: cleanText(cell(row, "address")),
      activity: cleanText(cell(row, "activity")),
      radiusMeters: cleanText(cell(row, "radiusMeters")),
    });
  });
  return places;
}

/**
 * Imports a spreadsheet of places (.xlsx or .csv, sent as base64). With dryRun nothing is saved or
 * looked up: each row comes back as what would happen - "add", "update", "lookup" (an address that will
 * be looked up) or "error". Without dryRun the rows are saved in order and each says what happened.
 */
async function importKnownPlaces({
  db,
  FieldValue,
  memberEmail,
  fileName,
  fileBase64,
  dryRun = false,
  fetchImpl,
  sleep = sleepMs,
}) {
  const buffer = Buffer.from(String(fileBase64 || ""), "base64");
  if (!buffer.length) throw new HttpsError("invalid-argument", "The file is empty.");
  if (buffer.length > MAX_IMPORT_FILE_BYTES) throw new HttpsError("invalid-argument", "The file is too large (2 MB at most).");
  const rows = await parsePlacesSpreadsheet(buffer, fileName);
  if (!rows.length) throw new HttpsError("invalid-argument", "The spreadsheet has no places under its header row.");
  if (rows.length > MAX_IMPORT_ROWS) {
    throw new HttpsError("invalid-argument", `The spreadsheet has ${rows.length} places; import at most ${MAX_IMPORT_ROWS} at a time.`);
  }

  const places = await loadMemberPlaces(db, memberEmail);
  const results = [];
  let lookups = 0;
  for (const row of rows) {
    const result = { rowNumber: row.rowNumber, name: row.name };
    const needsLookup = row.latitude == null && row.longitude == null;
    if (needsLookup && !row.address) {
      results.push({ ...result, status: "error", message: "No coordinates or address." });
      continue;
    }
    let { latitude, longitude, address } = row;
    if (needsLookup) {
      // The rest of the row is checked first, so a row skipped anyway uses none of the lookups - and the
      // preview counts lookups the same way, so it shows the rows past the limit as skipped too.
      const precheck = validatePlaceInput({ ...row, latitude: 0.0001, longitude: 0.0001 });
      if (precheck.error) {
        results.push({ ...result, status: "error", message: precheck.error });
        continue;
      }
      if (lookups >= MAX_IMPORT_LOOKUPS) {
        results.push({ ...result, status: "error", message: `Only ${MAX_IMPORT_LOOKUPS} addresses are looked up per import; add coordinates or import the rest separately.` });
        continue;
      }
      lookups += 1;
      if (dryRun) {
        results.push({ ...result, status: "lookup", address });
        continue;
      }
      if (lookups > 1) await sleep(GEOCODE_INTERVAL_MS);
      let candidates;
      try {
        candidates = await geocodeAddress(address, { fetchImpl, limit: 1 });
      } catch (_) {
        results.push({ ...result, status: "error", message: `The address lookup service did not answer for: ${address}. Try again later, or add coordinates.` });
        continue;
      }
      if (!candidates.length) {
        results.push({ ...result, status: "error", message: `Address not found: ${address}` });
        continue;
      }
      ({ latitude, longitude } = candidates[0]);
      result.foundAddress = candidates[0].label;
    }
    const checked = validatePlaceInput({ ...row, latitude, longitude, address });
    if (checked.error) {
      results.push({ ...result, status: "error", message: checked.error });
      continue;
    }
    if (dryRun) {
      const existing = findSameNamedPlace(places, checked.place.name, latitude, longitude);
      // Remembered as if added, so a later row naming it again previews as the update the import makes.
      if (!existing) {
        places.push({ name: checked.place.name, latitude, longitude, radiusMeters: checked.place.radiusMeters || DEFAULT_RADIUS_METERS });
      }
      results.push({ ...result, status: existing ? "update" : "add", latitude, longitude });
      continue;
    }
    const saved = await savePlace({ db, FieldValue, memberEmail, places, place: checked.place, source: "import" });
    results.push({ ...result, id: saved.id, status: saved.action, latitude, longitude, activity: saved.activity });
  }

  const count = (status) => results.filter((result) => result.status === status).length;
  return {
    dryRun: Boolean(dryRun),
    summary: { rows: results.length, added: count("added"), updated: count("updated"), toAdd: count("add"), toUpdate: count("update"), toLookUp: count("lookup"), errors: count("error") },
    rows: results,
  };
}

module.exports = {
  MAX_IMPORT_ROWS,
  MAX_IMPORT_LOOKUPS,
  parseCoordinatesText,
  geocodeAddress,
  lookupPlaceAddress,
  addKnownPlace,
  parsePlacesSpreadsheet,
  importKnownPlaces,
};
