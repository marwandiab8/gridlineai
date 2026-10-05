// Activity codes for labour hours, so each hour can be billed (or not) correctly.
//
// A code says what the work was FOR, not which trade did it: rough carpentry for hoarding is Winter Heat
// (extra, charged to the owner), while rough carpentry for safety railing is General Conditions (in our
// contract). Codes are permanent ids stored on labour entries - never reuse a code for a different meaning;
// add a new one instead. Labels and descriptions may be reworded.
//
// `hint` is the plain-language "when to use it" line on the labourers' keyword guide (public/labour-guide.html).
// Each activity has a `keyword` labourers type at the start of a part of their text ("5h hoarding lift east
// windows L3"); the rest of the part is kept as their note (where / what). The report prints the activity's
// `description`, written for the owner's reviewers: it states only what the keyword guarantees was done, so
// where the method differs (from the slab vs from a scissor lift) there are separate keywords.
//
// Text without a keyword is matched to a code only when exactly one activity is clearly the best match.
// Anything unclear ("rough carpentry", "general labour and unloading rebar") stays uncoded for the supervisor.

const LABOUR_CATEGORIES = Object.freeze([
  { id: "winter-heat", label: "Winter Heat", chargeable: true, note: "Extra" },
  { id: "general-conditions", label: "General Conditions", chargeable: false, note: "In contract" },
  { id: "other", label: "Other Work", chargeable: false, note: "Not extra" },
]);

const CARPENTRY = /wood|lumber|carpent|framing|2x4|plywood|cut/;
const INSTALLING = /install|put up|erect|build|fix|secur|fasten/;

// `rules`: each rule is a list of patterns that must ALL appear in the text. A rule with more patterns is more
// specific and wins over a shorter one.
const LABOUR_ACTIVITIES = Object.freeze([
  {
    code: "WH-HOARD-PREP",
    category: "winter-heat",
    keyword: "hoarding prep",
    label: "Hoarding - prepare lumber and tarps",
    hint: "Cutting and getting lumber and tarps ready for hoarding. Not putting it up yet.",
    description: "Measure curtain wall and window openings; cut and assemble dimensional lumber for hoarding frames; cut tarps to size; stage material at the openings.",
    rules: [[CARPENTRY, /winter|hoard/], [/prep/, /hoard|winter\s*protection/]],
  },
  {
    code: "WH-HOARD-INSTALL",
    category: "winter-heat",
    keyword: "hoarding install",
    label: "Hoarding - install from floor",
    hint: "Putting hoarding up while standing on the floor or a ladder.",
    description: "Install hoarding frames at curtain wall and window openings working from the floor slab; fasten frames to the structure; fit and secure tarps and seal the edges to retain heat.",
    rules: [[INSTALLING, /hoard|winter\s*protection/], [/curtain\s*wall|window/, /tarp|plastic|poly|enclos|protect|cover/]],
  },
  {
    code: "WH-HOARD-LIFT",
    category: "winter-heat",
    keyword: "hoarding lift",
    label: "Hoarding - install from scissor lift",
    hint: "Putting hoarding up from the scissor lift.",
    description: "Install hoarding frames at curtain wall and window openings using a 47 ft scissor lift; fasten frames to the structure; fit and secure tarps and seal the edges to retain heat.",
    rules: [[/hoard|winter\s*protection/, /scissor|\blift\b|boom/]],
  },
  {
    code: "WH-HOARD-REPAIR",
    category: "winter-heat",
    keyword: "hoarding repair",
    label: "Hoarding - inspect and repair",
    hint: "Fixing hoarding that came loose, tore or blew open.",
    description: "Inspect hoarding after wind and weather; re-secure loose frames, replace torn tarps and re-seal gaps to maintain the heated enclosure.",
    rules: [[/hoard|winter\s*protection|tarp/, /\brepair|re-?secur|patch|\bfix|\bwind\b|torn|inspect/]],
  },
  {
    code: "WH-HOARD-REMOVE",
    category: "winter-heat",
    keyword: "hoarding remove",
    label: "Hoarding - remove",
    hint: "Taking hoarding down so windows or curtain wall can go in.",
    description: "Remove hoarding frames and tarps to release openings for curtain wall and window installation; salvage reusable lumber and tarps, stockpile them and clear the debris.",
    rules: [[/hoard|winter\s*protection/, /remov|dismantl|take\s*down|strip/]],
  },
  {
    code: "WH-TARP",
    category: "winter-heat",
    keyword: "tarps",
    label: "Tarps - install / remove for heat",
    hint: "Putting up or taking down tarps to hold heat in (not hoarding frames).",
    description: "Install and remove tarps at slab edges and openings to enclose heated work areas and retain heat during heating operations.",
    rules: [[/tarp/]],
  },
  {
    code: "WH-FUEL",
    category: "winter-heat",
    keyword: "heater fuel",
    label: "Heaters - refuel",
    hint: "Filling heaters or swapping fuel tanks.",
    description: "Refuel temporary heaters and exchange fuel tanks; check that each heater is running after refuelling.",
    rules: [[/refuel|propane|diesel/], [/heater/, /fill|fuel/]],
  },
  {
    code: "WH-HEATER",
    category: "winter-heat",
    keyword: "heater move",
    label: "Heaters - set up / relocate",
    hint: "Moving, setting up or checking heaters and heat hoses.",
    description: "Set up and relocate temporary heaters and heat ducting to keep heated work areas at the required temperature; check heater operation.",
    rules: [[/\bheaters?\b/], [/heat(?:er)?\s*(?:duct|hose)/]],
  },
  {
    code: "WH-BLANKET",
    category: "winter-heat",
    keyword: "blankets",
    label: "Insulated blankets - place / remove",
    hint: "Laying or picking up insulated blankets on concrete or ground.",
    description: "Place and remove insulated blankets to protect concrete, footings and ground from freezing.",
    rules: [[/blanket/]],
  },
  {
    code: "WH-SNOW",
    category: "winter-heat",
    keyword: "snow",
    label: "Snow removal",
    hint: "Shovelling or clearing snow.",
    description: "Shovel and clear snow from work areas, slab, access routes and stairs so work can proceed safely.",
    rules: [[/snow/]],
  },
  {
    code: "WH-ICE",
    category: "winter-heat",
    keyword: "ice",
    label: "Ice removal / salting",
    hint: "Chipping ice or spreading salt.",
    description: "Break up and remove ice; apply salt and ice melt on walkways, ramps, stairs and work areas.",
    rules: [[/\bice\b|de-?ic|\bsalt/]],
  },
  {
    code: "WH-PUMP",
    category: "winter-heat",
    keyword: "snow pump",
    label: "Pump melted snow / ice",
    hint: "Pumping out water from melted snow or ice.",
    description: "Set up pumps and discharge hoses to remove water from melted snow and ice; monitor pumps and clear blockages.",
    rules: [[/pump|water|drain/, /snow|\bice\b|melt|thaw/]],
  },
  {
    code: "WH-OTHER",
    category: "winter-heat",
    keyword: "winter other",
    label: "Other winter heat work",
    hint: "Other cold-weather work not on this list. Say what in the note.",
    description: "Other winter heat work, as noted.",
    rules: [],
  },
  {
    code: "GC-HOUSE",
    category: "general-conditions",
    keyword: "housekeeping",
    aliases: ["house keeping"],
    label: "Site housekeeping",
    hint: "Cleaning up, sweeping, garbage, keeping walkways clear.",
    description: "General site housekeeping: collect and remove debris, sweep floors and stairs, empty bins and keep access routes clear to maintain a safe work environment.",
    rules: [[/house\s*-?\s*keep/], [/clean\s*-?\s*up|cleaning|sweep/]],
  },
  {
    code: "GC-SIGN",
    category: "general-conditions",
    keyword: "signage",
    label: "Barricades / signage",
    hint: "Barricades, signs and caution tape.",
    description: "Install and maintain barricades, safety signage and caution tape at hazards and restricted areas.",
    rules: [[/barricad|signage|\bsigns?\b/]],
  },
  {
    code: "GC-RAIL-INSTALL",
    category: "general-conditions",
    keyword: "railing install",
    label: "Safety railing - install",
    hint: "Putting up new safety railing.",
    description: "Install perimeter and opening safety railing at slab edges, floor openings and stairs.",
    rules: [[/\brail/, INSTALLING]],
  },
  {
    code: "GC-RAIL-REINSTATE",
    category: "general-conditions",
    keyword: "railing reinstate",
    label: "Safety railing - remove / reinstate",
    hint: "Taking railing down for a trade and putting it back, or re-securing it.",
    description: "Remove safety railing for trade access and reinstate it after; check and re-secure railing.",
    rules: [[/\brail/, /reinstat|re\s*and\s*re|r\s*&\s*r|remov|replac|re-?secur/]],
  },
  {
    code: "GC-RAIL-CARP",
    category: "general-conditions",
    keyword: "railing carpentry",
    label: "Rough carpentry - safety railing",
    hint: "Cutting and building wood railing.",
    description: "Cut and prepare lumber for temporary wood safety railing and guards; build the railing sections.",
    rules: [[CARPENTRY, /\brail|guard|safety/]],
  },
  {
    code: "GC-PROTECT",
    category: "general-conditions",
    keyword: "protection",
    label: "Temporary protection - stairs / floors",
    hint: "Protecting stairs, floors or finished work.",
    description: "Install and maintain temporary protection on stairs, floors and finished work.",
    rules: [[/stair|floor|finish/, /protect/]],
  },
  {
    code: "GC-SAFETY",
    category: "general-conditions",
    keyword: "safety",
    label: "Site safety",
    hint: "Safety walk, or fixing hazards with the supervisor.",
    description: "Site safety walk with the supervisor; inspect and correct hazards at railings, openings and access routes.",
    rules: [[/site\s*safety|safety\s*(?:check|walk|inspection|meeting|talk)/]],
  },
  {
    code: "GC-OTHER",
    category: "general-conditions",
    keyword: "gc other",
    label: "Other general conditions",
    hint: "Other site work not on this list. Say what in the note.",
    description: "Other general conditions work, as noted.",
    rules: [],
  },
  {
    code: "OT-GEN",
    category: "other",
    keyword: "general",
    aliases: ["general labour", "general labor", "general labouring", "general laboring"],
    label: "General labour",
    hint: "Helping trades with anything else.",
    description: "General labour assisting trades as directed by the site supervisor.",
    rules: [[/general\s*lab/]],
  },
  {
    code: "OT-MAT",
    category: "other",
    keyword: "unloading",
    aliases: ["unload"],
    label: "Unload / move materials",
    hint: "Unloading trucks or moving material.",
    description: "Unload deliveries and move materials to the work areas.",
    rules: [[/unload|lift(?:ing)?\s+to|move\s+material|moving\s+material|carry/]],
  },
  {
    code: "OT-CARP",
    category: "other",
    keyword: "carpentry other",
    label: "Rough carpentry - other",
    hint: "Carpentry that is not for hoarding or railing. Say what in the note.",
    description: "Rough carpentry not for winter heat or safety, as noted.",
    rules: [],
  },
  {
    code: "OT-OTHER",
    category: "other",
    keyword: "other",
    label: "Other work",
    hint: "Anything else. Say what in the note.",
    description: "Other work, as noted.",
    rules: [],
  },
]);

const ACTIVITY_BY_CODE = new Map(LABOUR_ACTIVITIES.map((a) => [a.code, a]));
const CATEGORY_BY_ID = new Map(LABOUR_CATEGORIES.map((c) => [c.id, c]));

function getLabourActivity(code) {
  return ACTIVITY_BY_CODE.get(String(code || "").trim().toUpperCase()) || null;
}

function getLabourCategory(id) {
  return CATEGORY_BY_ID.get(String(id || "")) || null;
}

const KEYWORDS = LABOUR_ACTIVITIES
  .flatMap((activity) => [activity.keyword, ...(activity.aliases || [])].map((keyword) => ({ code: activity.code, keyword })))
  .sort((x, y) => y.keyword.length - x.keyword.length);

/**
 * The activity whose keyword starts the text ("Hoarding-lift: east windows L3"), and the rest as the note.
 * Longest keyword first, so "snow pump" wins over "snow". Null when the text doesn't start with a keyword.
 */
function matchLabourKeyword(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  const flat = raw.toLowerCase().replace(/-/g, " ");
  for (const { code, keyword } of KEYWORDS) {
    if (!flat.startsWith(keyword)) continue;
    const next = flat.charAt(keyword.length);
    if (next && /[a-z0-9]/.test(next)) continue;
    const note = raw.slice(keyword.length).replace(/^[\s:;,.\-]+/, "").trim();
    return { code, note };
  }
  return null;
}

/** The code for a piece of work text, or null when no single activity is clearly the best match. */
function suggestLabourActivityCode(text) {
  const raw = String(text || "").toLowerCase();
  if (!raw.trim()) return null;
  let best = 0;
  let codes = new Set();
  for (const activity of LABOUR_ACTIVITIES) {
    for (const rule of activity.rules) {
      if (!rule.every((pattern) => pattern.test(raw))) continue;
      if (rule.length > best) {
        best = rule.length;
        codes = new Set([activity.code]);
      } else if (rule.length === best) {
        codes.add(activity.code);
      }
    }
  }
  return codes.size === 1 ? [...codes][0] : null;
}

/** A keyword sets the code and leaves the rest as the note; otherwise the text is matched where clear. */
function codeWorkText(text) {
  const keyword = matchLabourKeyword(text);
  if (keyword) return { code: keyword.code, text: keyword.note, codedBy: "keyword" };
  const code = suggestLabourActivityCode(text);
  return { code, text, codedBy: code ? "auto" : null };
}

/**
 * Coded lines from the parts of an entry's work text (`parts`: [{ hours, task }], as labourRepository's
 * parseSegmentedBreakdown gives them), adding up to exactly `minutesWorked`. When the parts don't fit the
 * total, the whole text becomes one line; hours the parts don't account for go on an uncoded
 * "Not described" line, so totals never drift.
 */
function buildLabourLinesFromParts(parts, minutesWorked, wholeText) {
  const total = Math.max(0, Math.round(Number(minutesWorked) || 0));
  const lines = [];
  let used = 0;
  for (const part of parts || []) {
    const minutes = Math.round(Number(part && part.hours) * 60);
    const text = String((part && part.task) || "").replace(/\s+/g, " ").trim();
    if (!(minutes > 0) || !text || used + minutes > total) {
      lines.length = 0;
      used = 0;
      break;
    }
    used += minutes;
    lines.push({ ...codeWorkText(text), minutes });
  }
  if (!lines.length) {
    if (!(total > 0)) return [];
    const text = String(wholeText || "").replace(/\s+/g, " ").trim().slice(0, 300);
    return [{ ...codeWorkText(text), minutes: total }];
  }
  if (used < total) lines.push({ code: null, minutes: total - used, text: "Not described", codedBy: null });
  return lines;
}

/** Validates lines from the review screen or the web form; throws with a plain message. */
function normalizeLabourLines(lines, { minutesWorked = null, requireCodes = false, codedBy = "supervisor" } = {}) {
  if (!Array.isArray(lines) || !lines.length) throw new Error("Add at least one line.");
  if (lines.length > 20) throw new Error("Use 20 lines or fewer.");
  const out = lines.map((line, i) => {
    const hours = Number(line && line.hours);
    const minutes = line && Number.isFinite(Number(line.minutes)) ? Math.round(Number(line.minutes)) : Math.round(hours * 60);
    if (!(minutes > 0) || minutes > 24 * 60) throw new Error(`Line ${i + 1}: enter hours between 0.25 and 24.`);
    const rawCode = String((line && line.code) || "").trim();
    const activity = rawCode ? getLabourActivity(rawCode) : null;
    // An unknown code almost always means a page opened before the activity list changed.
    if (rawCode && !activity) throw new Error(`Line ${i + 1}: "${rawCode}" is no longer an activity. The activity list has changed since this page was opened - reload the page and choose again.`);
    if (requireCodes && !activity) throw new Error(`Line ${i + 1}: choose an activity.`);
    const text = String((line && line.text) || "").replace(/\s+/g, " ").trim().slice(0, 300);
    if (activity && /-OTHER$/.test(activity.code) && !text) throw new Error(`Line ${i + 1}: say what the "${activity.label}" work was.`);
    // Where the work was, e.g. a Site Logistics work area ("Z1 East Wing - L2"). Kept only when given.
    const location = String((line && line.location) || "").replace(/\s+/g, " ").trim().slice(0, 120);
    return { code: activity ? activity.code : null, minutes, text, codedBy: activity ? codedBy : null, ...(location ? { location } : {}) };
  });
  const sum = out.reduce((total, line) => total + line.minutes, 0);
  if (minutesWorked != null && sum !== Math.round(Number(minutesWorked))) {
    throw new Error(`The lines add up to ${formatMinutesAsHours(sum)}h but the entry is ${formatMinutesAsHours(minutesWorked)}h.`);
  }
  return out;
}

function formatMinutesAsHours(minutes) {
  const hours = Math.round((Number(minutes) || 0) / 60 * 100) / 100;
  return Number.isInteger(hours) ? String(hours) : hours.toFixed(2).replace(/0+$/, "").replace(/\.$/, "");
}

/** Hours per category for an entry's lines, plus uncoded hours. */
function summarizeLabourLines(lines) {
  const byCategory = new Map(LABOUR_CATEGORIES.map((c) => [c.id, 0]));
  let uncodedMinutes = 0;
  for (const line of lines || []) {
    const activity = getLabourActivity(line && line.code);
    const minutes = Math.round(Number(line && line.minutes) || 0);
    if (activity) byCategory.set(activity.category, byCategory.get(activity.category) + minutes);
    else uncodedMinutes += minutes;
  }
  return { byCategory, uncodedMinutes };
}

/** Short text for an SMS reply, e.g. "Winter Heat 6h, Other Work 3h, 1h for the supervisor to code". */
function describeLabourLinesShort(lines) {
  const { byCategory, uncodedMinutes } = summarizeLabourLines(lines);
  const parts = LABOUR_CATEGORIES
    .filter((c) => byCategory.get(c.id) > 0)
    .map((c) => `${c.label} ${formatMinutesAsHours(byCategory.get(c.id))}h`);
  if (uncodedMinutes > 0) parts.push(`${formatMinutesAsHours(uncodedMinutes)}h for the supervisor to code`);
  return parts.join(", ");
}

module.exports = {
  describeLabourLinesShort,
  LABOUR_ACTIVITIES,
  LABOUR_CATEGORIES,
  buildLabourLinesFromParts,
  formatMinutesAsHours,
  getLabourActivity,
  getLabourCategory,
  matchLabourKeyword,
  normalizeLabourLines,
  suggestLabourActivityCode,
  summarizeLabourLines,
};
