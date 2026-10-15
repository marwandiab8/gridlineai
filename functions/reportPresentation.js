/**
 * How the daily site report presents its facts: short dates, equipment grouped by type, a status line per task, a
 * factual summary of the day, and people's names instead of their sign-ins. Pure functions (apart from
 * resolvePeopleNames, which reads gridlineai's labourer and member lists), used by dailyReportPdf.js and the PDF
 * builder.
 */

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const DAY_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

/** "2026-10-05" -> "Oct 5" (with ", 2026" when `withYear`); anything else unchanged. */
function shortDate(dateKey, withYear = false) {
  const m = DAY_RE.exec(String(dateKey || ""));
  if (!m) return String(dateKey || "");
  return `${MONTHS[Number(m[2]) - 1]} ${Number(m[3])}${withYear ? `, ${m[1]}` : ""}`;
}

/** "Oct 5", "Oct 5 - 26", "Sep 28 - Oct 2", "Dec 30, 2026 - Jan 4, 2027". */
function shortDateRange(start, end) {
  const a = DAY_RE.exec(String(start || ""));
  const b = DAY_RE.exec(String(end || ""));
  if (!a) return shortDate(end);
  if (!b || start === end) return shortDate(start);
  if (a[1] !== b[1]) return `${shortDate(start, true)} - ${shortDate(end, true)}`;
  if (a[2] === b[2]) return `${shortDate(start)} - ${Number(b[3])}`;
  return `${shortDate(start)} - ${shortDate(end)}`;
}

/**
 * Equipment on site as one row per kind of item, owner and supplier: { type, count, units, crew, supplier, offRent },
 * off-rent items (awaiting pickup) kept apart. `units` lists names and unit numbers that say more than the type.
 */
function groupEquipment(list) {
  const groups = new Map();
  for (const e of Array.isArray(list) ? list : []) {
    const type = String(e.type || "Equipment");
    const crew = String(e.crew || "");
    const supplier = String(e.rental || "");
    const offRent = e.onRent === false;
    const key = [type, crew, supplier, offRent].join("|");
    const g = groups.get(key) || { type, count: 0, units: [], crew, supplier, offRent };
    g.count += 1;
    const name = e.name && e.name !== type ? String(e.name).replace(new RegExp(`^${type.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*`, "i"), "").trim() : "";
    const unit = e.unit ? `#${e.unit}` : "";
    const label = name && e.unit && name === String(e.unit) ? unit : [name, unit && !name.includes(e.unit) ? unit : ""].filter(Boolean).join(" ");
    if (label && !g.units.includes(label)) g.units.push(label);
    groups.set(key, g);
  }
  return [...groups.values()].sort((a, b) => a.type.localeCompare(b.type) || Number(a.offRent) - Number(b.offRent) || b.count - a.count || a.crew.localeCompare(b.crew));
}

/** "Done 4:47 p.m. - Kevin Ashdown", "Open since Sep 26 - 3 days overdue", "Could not be done: gate locked". */
function taskStatusText(t) {
  if (t.status === "done") return ["Done", t.doneTime, t.doneBy ? `- ${t.doneBy}` : ""].filter(Boolean).join(" ");
  if (t.status === "blocked") return `Could not be done: ${t.note || "no reason given"}`;
  const since = t.since ? `Open since ${shortDate(t.since)}` : "Open";
  return t.overdueDays ? `${since} - ${t.overdueDays} day${t.overdueDays === 1 ? "" : "s"} overdue` : since;
}

const plural = (n, one, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

/**
 * The day in a few factual sentences, from the report's own numbers (field notes add their own summary on top):
 * crews and headcount, Site Logistics tasks, scheduled work, equipment, issues, inspections and pours.
 */
function siteFactsSummary({ workers = 0, trades = 0, tasks = null, activities = [], equipment = [], issues = 0, inspections = 0, pours = 0 } = {}) {
  const out = [];
  if (workers) out.push(`${plural(workers, "worker")}${trades ? ` from ${plural(trades, "trade")}` : ""} ${workers === 1 ? "was" : "were"} on site.`);
  if (tasks && tasks.total) {
    const bits = [`${tasks.done} of ${plural(tasks.total, "Site Logistics task")} completed`];
    if (tasks.blocked) bits.push(`${tasks.blocked} could not be done`);
    if (tasks.open) bits.push(`${tasks.open} still open`);
    out.push(`${bits.join(", ")}.`);
  }
  if (activities.length) {
    const crews = new Set(activities.map((a) => a.company || a.trade || a.crew || "").filter(Boolean)).size;
    out.push(`${plural(activities.length, "activity", "activities")}${crews ? ` across ${plural(crews, "crew")}` : ""} ${activities.length === 1 ? "was" : "were"} scheduled in the look-ahead.`);
  }
  if (equipment.length) {
    const off = equipment.filter((e) => e.onRent === false).length;
    out.push(`${plural(equipment.length, "piece")} of equipment on site${off ? `, ${off} off rent awaiting pickup` : ""}.`);
  }
  const logged = [issues && plural(issues, "issue"), inspections && plural(inspections, "inspection"), pours && plural(pours, "concrete pour")].filter(Boolean);
  if (logged.length) out.push(`Logged: ${logged.join(", ")}.`);
  return out.join(" ");
}

const WEATHER_WORDS = /\b(weather|overcast|cloud|sun|clear|rain|drizzle|snow|showers?|fog|wind|gust|temperature|high of|low of|°c|°f|precipitation|forecast|humid|mph|km\/h|degrees?)\b/i;

/** True when an executive summary only describes the weather (every sentence is about it). */
function isWeatherOnlySummary(text) {
  const sentences = String(text || "").split(/(?<=[.!?])\s+/).map((s) => s.trim()).filter(Boolean);
  return sentences.length > 0 && sentences.every((s) => WEATHER_WORDS.test(s));
}

/**
 * Names for Site Logistics sign-ins (emails, or +1 phone numbers): a labourer linked to the sign-in in gridlineai's
 * Labourer manager, else an app member with that email. Map(sign-in -> name); unknown sign-ins are left out.
 */
async function resolvePeopleNames(db, ids) {
  const wanted = [...new Set((ids || []).map((id) => String(id || "").trim().toLowerCase()).filter(Boolean))];
  const names = new Map();
  if (!wanted.length || !db) return names;
  const labourers = await db.collection("labourers").get();
  for (const doc of labourers.docs) {
    const d = doc.data() || {};
    const name = String(d.displayName || d.name || "").trim();
    if (!name || d.active === false) continue;
    for (const id of Array.isArray(d.siteLogisticsIds) ? d.siteLogisticsIds : []) {
      const key = String(id || "").trim().toLowerCase();
      if (wanted.includes(key) && !names.has(key)) names.set(key, name);
    }
  }
  for (const id of wanted.filter((w) => !names.has(w) && w.includes("@"))) {
    const member = await db.collection("appMembers").doc(id).get();
    const name = member.exists ? String((member.data() || {}).displayName || "").trim() : "";
    if (name) names.set(id, name);
  }
  return names;
}

module.exports = {
  groupEquipment,
  isWeatherOnlySummary,
  resolvePeopleNames,
  shortDate,
  shortDateRange,
  siteFactsSummary,
  taskStatusText,
};
