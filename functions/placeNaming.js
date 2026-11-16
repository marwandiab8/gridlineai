// Names a stop nobody has named yet, from OpenStreetMap (Nominatim reverse geocoding): the business or
// amenity at the spot when the map knows one ("Raging Bull Sports Bar"), else the street address
// ("25 Macdonell St, Guelph"), else the coordinates. The same service and etiquette as placesAdd.js
// (one request per stop, an identifying User-Agent). Data (c) OpenStreetMap contributors.
const NOMINATIM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse";
const NOMINATIM_USER_AGENT = "GridlineAI-KnownPlaces/1.0 (+https://gridlineai.web.app)";
const TIMEOUT_MS = 8000;
// Features whose name is a street or an area, not somewhere you stop.
const NOT_A_PLACE = new Set(["highway", "boundary", "place", "landuse", "natural", "waterway", "railway", "administrative", "route"]);

function cleanText(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

/** "25 Macdonell St" and "Guelph" from a Nominatim address block, and the two joined. */
function addressLine(address = {}) {
  const street = [address.house_number, address.road || address.pedestrian || address.footway || address.residential].filter(Boolean).map(cleanText).join(" ").trim();
  const town = cleanText(address.town || address.city || address.village || address.hamlet || address.municipality || address.suburb || address.county || "");
  return { street, town, line: [street, town].filter(Boolean).join(", ") };
}

function coordinatesName(latitude, longitude) {
  return `Stop near ${Number(latitude).toFixed(4)}, ${Number(longitude).toFixed(4)}`;
}

/**
 * { name, address, town, source: "osm_name" | "osm_address" | "coordinates", ... }. Never throws: when the
 * lookup fails the stop is named after its coordinates, and the member's reply can rename it.
 */
async function nameStop({ latitude, longitude, fetchImpl = fetch, logger = null }) {
  const fallback = { name: coordinatesName(latitude, longitude), address: "", town: "", source: "coordinates" };
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return fallback;
  const url = `${NOMINATIM_REVERSE_URL}?format=jsonv2&zoom=18&addressdetails=1&namedetails=1&lat=${latitude}&lon=${longitude}`;
  let result;
  try {
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    const timer = controller ? setTimeout(() => controller.abort(), TIMEOUT_MS) : null;
    const response = await fetchImpl(url, {
      headers: { "User-Agent": NOMINATIM_USER_AGENT, "Accept-Language": "en", Accept: "application/json" },
      ...(controller ? { signal: controller.signal } : {}),
    });
    if (timer) clearTimeout(timer);
    if (!response || !response.ok) throw new Error(`HTTP ${response && response.status}`);
    result = await response.json();
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("placeNaming: reverse geocoding failed", { message: err && err.message });
    return fallback;
  }
  if (!result || typeof result !== "object" || result.error) return fallback;
  const { street, town, line } = addressLine(result.address || {});
  const named = result.namedetails && typeof result.namedetails === "object" ? result.namedetails : {};
  const name = cleanText(result.name || named["name:en"] || named.name);
  const category = cleanText(result.category || result.class);
  const detail = { category, kind: cleanText(result.type), osmType: result.osm_type || null, osmId: result.osm_id || null };
  if (name && !NOT_A_PLACE.has(category) && name !== street) {
    return { name: name.slice(0, 120), address: line, town, source: "osm_name", ...detail };
  }
  if (line) return { name: line.slice(0, 120), address: line, town, source: "osm_address", ...detail };
  const display = cleanText(result.display_name).split(",").slice(0, 2).join(",").trim();
  if (display) return { name: display.slice(0, 120), address: display, town, source: "osm_address", ...detail };
  return fallback;
}

module.exports = { NOMINATIM_REVERSE_URL, NOMINATIM_USER_AGENT, addressLine, coordinatesName, nameStop };
