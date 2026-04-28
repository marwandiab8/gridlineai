/**
 * Server-side daily weather for the report Eastern date (Open-Meteo, no API key).
 * Used only by the daily PDF path; does not change SMS behavior.
 */

const https = require("https");

/**
 * Cover PDF + human label when no project site is configured.
 * Weather uses fixed coordinates below — no geocoding API call for this path.
 */
const DEFAULT_WEATHER_LOCATION_LINE = "Brampton, Ontario, Canada";

/** WGS84 — Brampton, ON (approx. city centre). Deterministic; not resolved via geocoder. */
const DEFAULT_BRAMPTON_LAT = 43.6833;
const DEFAULT_BRAMPTON_LON = -79.7667;
const DEFAULT_BRAMPTON_RESOLVED_LABEL = "Brampton, Ontario, Canada";

/**
 * Project docs sometimes store the same string used on the PDF cover when no site is set.
 * That is not a geocodable address — treat as empty so we use fixed Brampton coordinates.
 */
function isDisplayOnlyDefaultLocation(s) {
  const t = String(s || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
  if (!t) return false;
  const def = DEFAULT_WEATHER_LOCATION_LINE.replace(/\s+/g, " ").toLowerCase();
  return t === `${def} (default)`;
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      { headers: { "User-Agent": "GridlineDailyReport/1.0 (contact: server)" } },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
          } catch (e) {
            reject(e);
          }
        });
      }
    );
    req.on("error", reject);
    req.setTimeout(12_000, () => {
      req.destroy(new Error("weather request timeout"));
    });
  });
}

/** Round Open-Meteo °C for display (daily max/min are already Celsius). */
function roundC(c) {
  if (c == null || Number.isNaN(Number(c))) return null;
  return Math.round(Number(c) * 10) / 10;
}

function mmToIn(mm) {
  if (mm == null || Number.isNaN(Number(mm))) return null;
  const x = Number(mm) * 0.0393701;
  if (x < 0.01) return "0";
  return x < 0.1 ? x.toFixed(2) : x.toFixed(2);
}

function kmhToMph(kmh) {
  if (kmh == null || Number.isNaN(Number(kmh))) return null;
  return Math.round(Number(kmh) * 0.621371);
}

/** WMO weather code → short English (subset). */
function weatherCodeSummary(code) {
  const c = Number(code);
  if (c === 0) return "Clear";
  if (c <= 3) return "Partly cloudy";
  if (c <= 48) return "Fog / low cloud";
  if (c <= 57) return "Drizzle / freezing drizzle";
  if (c <= 67) return "Rain";
  if (c <= 77) return "Snow / ice pellets";
  if (c <= 82) return "Rain showers";
  if (c <= 86) return "Snow showers";
  if (c <= 99) return "Thunderstorm";
  return "Mixed conditions";
}

/**
 * @param {string} addressLine
 * @returns {Promise<{ lat: number, lon: number, label: string }|null>}
 */
async function geocodeAddressLine(addressLine) {
  const q = String(addressLine || "").trim();
  if (q.length < 4) return null;
  const url = `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(q.slice(0, 200))}&count=1&language=en&format=json`;
  try {
    const j = await fetchJson(url);
    if (!j.results || !j.results.length) return null;
    const r = j.results[0];
    return {
      lat: r.latitude,
      lon: r.longitude,
      label: [r.name, r.admin1, r.country_code].filter(Boolean).join(", "),
    };
  } catch (_) {
    return null;
  }
}

/**
 * Open-Meteo search is place-name oriented; street addresses often return no hit.
 * Try shorter queries, then caller may use fixed Brampton coords.
 */
async function geocodeWithStreetAddressFallbacks(fullLine) {
  const primary = String(fullLine || "").trim();
  if (primary.length < 4) return null;
  let g = await geocodeAddressLine(primary);
  if (g) return g;
  const lower = primary.toLowerCase();
  if (/\bbrampton\b/i.test(primary)) {
    g = await geocodeAddressLine("Brampton");
    if (g) return g;
  }
  const m = primary.match(/\b([A-Z]\d[A-Z])\s*(\d[A-Z]\d)\b/i);
  if (m) {
    g = await geocodeAddressLine(`${m[1]} ${m[2]}`.toUpperCase());
    if (g) return g;
  }
  return null;
}

function safeDateKeyLike(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ""));
}

function addDaysToDateKey(dateKey, days) {
  const d = new Date(`${dateKey}T12:00:00Z`);
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCDate(d.getUTCDate() + Number(days || 0));
  return d.toISOString().slice(0, 10);
}

function maxDateKey(a, b) {
  if (!a) return b || null;
  if (!b) return a || null;
  return a >= b ? a : b;
}

function minDateKey(a, b) {
  if (!a) return b || null;
  if (!b) return a || null;
  return a <= b ? a : b;
}

function dateKeyUtcToday() {
  return new Date().toISOString().slice(0, 10);
}

async function resolveWeatherLocation(addressLine) {
  const rawLine = String(addressLine || "").trim();
  const trimmedProject = isDisplayOnlyDefaultLocation(rawLine) ? "" : rawLine;
  let usedFallbackLocation = !trimmedProject;

  let lat;
  let lon;
  let resolvedLabel;
  let locationQuery;
  let siteAddressUnresolved = false;

  if (trimmedProject) {
    const geo = await geocodeWithStreetAddressFallbacks(trimmedProject);
    if (!geo) {
      lat = DEFAULT_BRAMPTON_LAT;
      lon = DEFAULT_BRAMPTON_LON;
      resolvedLabel = DEFAULT_BRAMPTON_RESOLVED_LABEL;
      locationQuery = `${trimmedProject.slice(0, 240)} (forecast uses Brampton-area coordinates; street geocode unavailable)`;
      usedFallbackLocation = true;
      siteAddressUnresolved = true;
    } else {
      lat = geo.lat;
      lon = geo.lon;
      resolvedLabel = geo.label;
      locationQuery = trimmedProject.slice(0, 300);
    }
  } else {
    lat = DEFAULT_BRAMPTON_LAT;
    lon = DEFAULT_BRAMPTON_LON;
    resolvedLabel = DEFAULT_BRAMPTON_RESOLVED_LABEL;
    locationQuery = `${DEFAULT_BRAMPTON_RESOLVED_LABEL} (fixed coordinates)`;
  }

  return {
    lat,
    lon,
    resolvedLabel,
    locationQuery,
    usedFallbackLocation,
    siteAddressUnresolved,
  };
}

function summarizeWeeklyWeather(rows) {
  const days = Array.isArray(rows) ? rows.filter((row) => row && row.dateKey) : [];
  if (!days.length) {
    return {
      summaryItems: ["Weather data was not available for this report window."],
    };
  }

  const highs = days.map((row) => row.highC).filter((v) => v != null);
  const lows = days.map((row) => row.lowC).filter((v) => v != null);
  const precips = days.map((row) => Number(row.precipMm || 0)).filter((v) => Number.isFinite(v));
  const winds = days.map((row) => row.windMphMax).filter((v) => v != null);
  const rainyDays = days.filter((row) => Number(row.precipMm || 0) >= 1);
  const coldestLow = lows.length ? Math.min(...lows) : null;
  const warmestHigh = highs.length ? Math.max(...highs) : null;
  const coolestHigh = highs.length ? Math.min(...highs) : null;
  const totalPrecipMm = precips.reduce((sum, value) => sum + value, 0);
  const maxWindMph = winds.length ? Math.max(...winds) : null;

  const conditionCounts = new Map();
  for (const row of days) {
    const key = row.conditions || "Mixed conditions";
    conditionCounts.set(key, (conditionCounts.get(key) || 0) + 1);
  }
  const dominantCondition = Array.from(conditionCounts.entries()).sort((a, b) => b[1] - a[1])[0]?.[0];

  const summaryItems = [];
  if (dominantCondition && highs.length && lows.length) {
    summaryItems.push(
      `${dominantCondition} overall. Highs around ${coolestHigh} to ${warmestHigh}°C and lows around ${Math.min(...lows)} to ${Math.max(...lows)}°C.`
    );
  } else if (dominantCondition) {
    summaryItems.push(`${dominantCondition} overall through this report window.`);
  }

  if (totalPrecipMm < 1) {
    summaryItems.push("No meaningful rain is expected across the report window.");
  } else if (rainyDays.length === 1) {
    summaryItems.push(
      `One wetter day stands out on ${rainyDays[0].dateKey} with about ${mmToIn(totalPrecipMm)} in of precipitation for the week.`
    );
  } else {
    summaryItems.push(
      `${rainyDays.length} days show measurable precipitation, totaling about ${mmToIn(totalPrecipMm)} in for the week.`
    );
  }

  if (coldestLow != null && coldestLow <= 1) {
    summaryItems.push(`Cold snap risk is present, with overnight lows near ${coldestLow}°C.`);
  }

  if (maxWindMph != null && maxWindMph >= 20) {
    summaryItems.push(`Peak wind may reach about ${maxWindMph} mph, so exposed work should be coordinated accordingly.`);
  }

  return {
    summaryItems: summaryItems.slice(0, 4),
  };
}

function summarizeWeatherChunk(rows, prefix = "") {
  const days = Array.isArray(rows) ? rows.filter((row) => row && row.dateKey) : [];
  if (!days.length) return null;
  const highs = days.map((row) => row.highC).filter((v) => v != null);
  const lows = days.map((row) => row.lowC).filter((v) => v != null);
  const totalPrecipMm = days.reduce((sum, row) => sum + (Number(row.precipMm || 0) || 0), 0);
  const maxWindMph = days.reduce((max, row) => Math.max(max, Number(row.windMphMax || 0) || 0), 0);
  const rainyDays = days.filter((row) => Number(row.precipMm || 0) >= 1).length;
  const conditionCounts = new Map();
  for (const row of days) {
    const key = row.conditions || "Mixed conditions";
    conditionCounts.set(key, (conditionCounts.get(key) || 0) + 1);
  }
  const dominantCondition = Array.from(conditionCounts.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || "Mixed conditions";
  const parts = [];
  if (prefix) parts.push(prefix);
  parts.push(dominantCondition);
  if (highs.length && lows.length) {
    parts.push(`highs ${Math.min(...highs)} to ${Math.max(...highs)}°C, lows ${Math.min(...lows)} to ${Math.max(...lows)}°C`);
  }
  if (totalPrecipMm < 1) {
    parts.push("no meaningful rain");
  } else {
    parts.push(`${rainyDays} wet day${rainyDays === 1 ? "" : "s"}, ~${mmToIn(totalPrecipMm)} in total precip`);
  }
  if (maxWindMph >= 20) {
    parts.push(`wind up to ${maxWindMph} mph`);
  }
  return parts.join(" - ");
}

function buildWeeklySummaryItems(dailyRows, coverage) {
  const rows = Array.isArray(dailyRows) ? dailyRows : [];
  if (!rows.length) {
    return ["Weather data was not available for this report window."];
  }
  const items = [];
  for (let i = 0; i < rows.length; i += 7) {
    const chunk = rows.slice(i, i + 7);
    const first = chunk[0]?.dateKey;
    const last = chunk[chunk.length - 1]?.dateKey;
    const label = chunk.length > 1 ? `Week ${Math.floor(i / 7) + 1} (${first} to ${last})` : `Week ${Math.floor(i / 7) + 1} (${first})`;
    const line = summarizeWeatherChunk(chunk, label);
    if (line) items.push(line);
  }
  if (coverage && coverage.clippedEndDateKey && coverage.requestedEndDateKey > coverage.clippedEndDateKey) {
    items.push(
      `Forecast coverage ends at ${coverage.clippedEndDateKey}. Later dates through ${coverage.requestedEndDateKey} are beyond the current forecast horizon.`
    );
  }
  if (coverage && coverage.clippedStartDateKey && coverage.requestedStartDateKey < coverage.clippedStartDateKey) {
    items.unshift(
      `Forecast starts at ${coverage.clippedStartDateKey}; earlier dates from ${coverage.requestedStartDateKey} are already in the past for live forecast data.`
    );
  }
  return items.slice(0, 5);
}

/**
 * @param {object} opts
 * @param {string} [opts.addressLine] — project address / site location
 * @param {string} opts.dateKey — Eastern YYYY-MM-DD
 * @param {string} [opts.timeZone] — IANA, default America/New_York
 * @param {import('firebase-functions').logger} [opts.logger]
 * @param {string} [opts.runId]
 * @returns {Promise<object>} snapshot for PDF + Firestore
 */
async function fetchDailyWeatherSnapshot(opts) {
  const { addressLine, dateKey, timeZone = "America/New_York", logger, runId } = opts;

  if (!dateKey || !/^\d{4}-\d{2}-\d{2}$/.test(String(dateKey))) {
    return {
      ok: false,
      reason: "bad_date",
      message: "Weather lookup skipped (invalid report date).",
    };
  }

  const {
    lat,
    lon,
    resolvedLabel,
    locationQuery,
    usedFallbackLocation,
    siteAddressUnresolved,
  } = await resolveWeatherLocation(addressLine);

  const qs = new URLSearchParams({
    latitude: String(lat),
    longitude: String(lon),
    daily: "weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
    timezone: timeZone,
    start_date: dateKey,
    end_date: dateKey,
  });
  const url = `https://api.open-meteo.com/v1/forecast?${qs.toString()}`;

  try {
    const j = await fetchJson(url);
    const d = j.daily;
    if (!d || !Array.isArray(d.time) || d.time.length < 1) {
      return {
        ok: false,
        reason: "no_rows",
        usedFallbackLocation,
        message: "Weather service returned no daily row for this date.",
      };
    }

    const code = d.weathercode && d.weathercode[0];
    const tmax = d.temperature_2m_max && d.temperature_2m_max[0];
    const tmin = d.temperature_2m_min && d.temperature_2m_min[0];
    const precipMm = d.precipitation_sum && d.precipitation_sum[0];
    const windKmh = d.windspeed_10m_max && d.windspeed_10m_max[0];

    const hi = roundC(tmax);
    const lo = roundC(tmin);
    const precipIn = mmToIn(precipMm);
    const windMph = kmhToMph(windKmh);
    const conditions = weatherCodeSummary(code);

    const summaryLine = [
      conditions,
      hi != null && lo != null ? `High ${hi}°C / Low ${lo}°C` : null,
      precipIn != null ? `Precip ~${precipIn} in` : null,
      windMph != null ? `Wind (max) ~${windMph} mph` : null,
    ]
      .filter(Boolean)
      .join(" · ");

    return {
      ok: true,
      provider: "open-meteo",
      dateKey,
      timeZone,
      usedFallbackLocation,
      siteAddressUnresolved,
      locationQuery,
      resolvedLabel,
      latitude: lat,
      longitude: lon,
      conditions,
      highC: hi,
      lowC: lo,
      precipInches: precipIn,
      windMphMax: windMph,
      summaryLine,
      rawDaily: {
        weathercode: code,
        temperature_2m_max: tmax,
        temperature_2m_min: tmin,
        precipitation_sum: precipMm,
        windspeed_10m_max: windKmh,
      },
    };
  } catch (e) {
    if (logger) {
      logger.warn("dailyReportWeather: fetch failed", { runId, message: e.message });
    }
    return {
      ok: false,
      reason: "fetch_error",
      usedFallbackLocation,
      message: `Weather lookup failed (${String(e.message || e).slice(0, 160)}).`,
    };
  }
}

async function fetchWeatherRangeSummary(opts) {
  const {
    addressLine,
    startDateKey,
    endDateKey,
    timeZone = "America/New_York",
    logger,
    runId,
  } = opts;

  if (!safeDateKeyLike(startDateKey) || !safeDateKeyLike(endDateKey)) {
    return {
      ok: false,
      reason: "bad_date",
      message: "Weather lookup skipped (invalid report window).",
      summaryItems: ["Weather data was not available for this report window."],
    };
  }

  const todayDateKey = dateKeyUtcToday();
  const maxForecastEndDateKey = addDaysToDateKey(todayDateKey, 15);
  const clippedStartDateKey = maxDateKey(startDateKey, todayDateKey);
  const clippedEndDateKey = minDateKey(endDateKey, maxForecastEndDateKey);

  if (!clippedStartDateKey || !clippedEndDateKey || clippedStartDateKey > clippedEndDateKey) {
    return {
      ok: false,
      reason: "outside_forecast_window",
      message: "Requested report window is outside the live forecast horizon.",
      summaryItems: [
        `Live forecast coverage currently starts at ${todayDateKey} and ends at ${maxForecastEndDateKey}.`,
        `Requested report window (${startDateKey} to ${endDateKey}) falls outside that range.`,
      ],
    };
  }

  const {
    lat,
    lon,
    resolvedLabel,
    locationQuery,
    usedFallbackLocation,
    siteAddressUnresolved,
  } = await resolveWeatherLocation(addressLine);

  const qs = new URLSearchParams({
    latitude: String(lat),
    longitude: String(lon),
    daily: "weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
    timezone: timeZone,
    start_date: clippedStartDateKey,
    end_date: clippedEndDateKey,
  });
  const url = `https://api.open-meteo.com/v1/forecast?${qs.toString()}`;

  try {
    const j = await fetchJson(url);
    const d = j.daily;
    if (!d || !Array.isArray(d.time) || d.time.length < 1) {
      return {
        ok: false,
        reason: "no_rows",
        usedFallbackLocation,
        locationQuery,
        resolvedLabel,
        message: "Weather service returned no rows for this report window.",
        summaryItems: ["Weather data was not available for this report window."],
      };
    }

    const dailyRows = d.time.map((dateKey, index) => {
      const code = d.weathercode && d.weathercode[index];
      const tmax = d.temperature_2m_max && d.temperature_2m_max[index];
      const tmin = d.temperature_2m_min && d.temperature_2m_min[index];
      const precipMm = d.precipitation_sum && d.precipitation_sum[index];
      const windKmh = d.windspeed_10m_max && d.windspeed_10m_max[index];
      return {
        dateKey,
        conditions: weatherCodeSummary(code),
        highC: roundC(tmax),
        lowC: roundC(tmin),
        precipMm: Number.isFinite(Number(precipMm)) ? Number(precipMm) : 0,
        precipInches: mmToIn(precipMm),
        windMphMax: kmhToMph(windKmh),
      };
    });

    const result = {
      ok: true,
      provider: "open-meteo",
      startDateKey,
      endDateKey,
      timeZone,
      usedFallbackLocation,
      siteAddressUnresolved,
      locationQuery,
      resolvedLabel,
      latitude: lat,
      longitude: lon,
      dailyRows,
      clippedStartDateKey,
      clippedEndDateKey,
      summaryItems: buildWeeklySummaryItems(dailyRows, {
        requestedStartDateKey: startDateKey,
        requestedEndDateKey: endDateKey,
        clippedStartDateKey,
        clippedEndDateKey,
      }),
    };
    if (logger) {
      logger.info("dailyReportWeather: weekly fetch ok", {
        runId,
        startDateKey,
        endDateKey,
        resolvedLabel: result.resolvedLabel,
        usedFallbackLocation: result.usedFallbackLocation,
        summaryItems: result.summaryItems,
      });
    }
    return result;
  } catch (e) {
    if (logger) {
      logger.warn("dailyReportWeather: weekly fetch failed", { runId, message: e.message });
    }
    return {
      ok: false,
      reason: "fetch_error",
      usedFallbackLocation,
      message: `Weather lookup failed (${String(e.message || e).slice(0, 160)}).`,
      summaryItems: ["Weather data was not available for this report window."],
    };
  }
}

module.exports = {
  fetchDailyWeatherSnapshot,
  fetchWeatherRangeSummary,
  summarizeWeeklyWeather,
  buildWeeklySummaryItems,
  geocodeAddressLine,
  isDisplayOnlyDefaultLocation,
  DEFAULT_WEATHER_LOCATION_LINE,
  DEFAULT_BRAMPTON_LAT,
  DEFAULT_BRAMPTON_LON,
};
