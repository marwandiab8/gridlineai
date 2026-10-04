// Activity codes for labour hours, so each hour can be billed (or not) correctly.
//
// A code says what the work was FOR, not which trade did it: rough carpentry for hoarding is Winter Heat
// (extra, charged to the owner), while rough carpentry for safety railing is General Conditions (in our
// contract). Codes are permanent ids stored on labour entries - never reuse a code for a different meaning;
// add a new one instead. Labels and descriptions may be reworded.
//
// Text from an SMS is matched to a code only when exactly one activity is the best match. Anything unclear
// ("rough carpentry", "general labour and unloading rebar") stays uncoded for the supervisor to code.

const LABOUR_CATEGORIES = Object.freeze([
  { id: "winter-heat", label: "Winter Heat", chargeable: true, note: "Extra - charged to the owner" },
  { id: "general-conditions", label: "General Conditions", chargeable: false, note: "In contract" },
  { id: "other", label: "Other Work", chargeable: false, note: "Not extra" },
]);

// `rules`: each rule is a list of patterns that must ALL appear in the text. A rule with more patterns is more
// specific and wins over a shorter one.
const LABOUR_ACTIVITIES = Object.freeze([
  {
    code: "WH-HOARD",
    category: "winter-heat",
    label: "Hoarding / winter protection",
    description: "Build and install hoarding (lumber and tarps) at curtain wall openings and windows, including the rough carpentry for it.",
    rules: [
      [/hoard/],
      [/winter\s*(?:protection|enclosure)/],
      [/curtain\s*wall|window/, /tarp|lumber|plastic|poly|enclos|protect|cover/],
      [/wood|lumber|carpent|framing|2x4|plywood/, /winter|hoard/],
    ],
  },
  {
    code: "WH-TARP",
    category: "winter-heat",
    label: "Install / remove tarps (heat)",
    description: "Install tarps to retain heat.",
    rules: [[/tarp/]],
  },
  {
    code: "WH-FUEL",
    category: "winter-heat",
    label: "Refuel heaters",
    description: "Fuel handling and refueling.",
    rules: [[/refuel|propane|diesel/], [/heater/, /fill|fuel/]],
  },
  {
    code: "WH-HEATER",
    category: "winter-heat",
    label: "Set up / move heaters",
    description: "Set up, move and check heaters and heat ducting.",
    rules: [[/\bheaters?\b/], [/heat(?:er)?\s*(?:duct|hose)/]],
  },
  {
    code: "WH-SNOW",
    category: "winter-heat",
    label: "Snow / ice removal",
    description: "Shovel and remove snow and ice.",
    rules: [[/snow/], [/\bice\b|de-?ic|\bsalt/]],
  },
  {
    code: "WH-PUMP",
    category: "winter-heat",
    label: "Pump melted snow / ice",
    description: "Set up pumps to remove melted snow and ice.",
    rules: [[/pump|water|drain/, /snow|\bice\b|melt|thaw/]],
  },
  {
    code: "WH-OTHER",
    category: "winter-heat",
    label: "Other winter heat work",
    description: "Other winter heat work (see note).",
    rules: [],
  },
  {
    code: "GC-HOUSE",
    category: "general-conditions",
    label: "Site housekeeping",
    description: "Maintain safe work environment.",
    rules: [[/house\s*-?\s*keep/], [/clean\s*-?\s*up|cleaning|sweep/]],
  },
  {
    code: "GC-SIGN",
    category: "general-conditions",
    label: "Barricades / signage",
    description: "Maintain barricades and signage.",
    rules: [[/barricad|signage|\bsigns?\b/]],
  },
  {
    code: "GC-RAIL",
    category: "general-conditions",
    label: "Safety railing - install / reinstate",
    description: "Install, remove and reinstate safety railing.",
    rules: [[/\brail/]],
  },
  {
    code: "GC-CARP",
    category: "general-conditions",
    label: "Rough carpentry - railing / safety",
    description: "Rough carpentry for safety railing and other safety work.",
    rules: [[/wood|lumber|carpent|framing|2x4|plywood/, /\brail|guard|safety/]],
  },
  {
    code: "GC-SAFETY",
    category: "general-conditions",
    label: "Site safety",
    description: "Site safety checks and upkeep.",
    rules: [[/site\s*safety|safety\s*(?:check|walk|inspection|meeting|talk)/]],
  },
  {
    code: "GC-OTHER",
    category: "general-conditions",
    label: "Other general conditions",
    description: "Other general conditions work (see note).",
    rules: [],
  },
  {
    code: "OT-GEN",
    category: "other",
    label: "General labour",
    description: "General labour.",
    rules: [[/general\s*lab/]],
  },
  {
    code: "OT-MAT",
    category: "other",
    label: "Unload / move materials",
    description: "Unload, move and lift materials.",
    rules: [[/unload|lift(?:ing)?\s+to|move\s+material|moving\s+material|carry/]],
  },
  {
    code: "OT-CARP",
    category: "other",
    label: "Rough carpentry - other",
    description: "Rough carpentry not for winter heat or safety.",
    rules: [],
  },
  {
    code: "OT-OTHER",
    category: "other",
    label: "Other work",
    description: "Other work (see note).",
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
    const code = suggestLabourActivityCode(text);
    lines.push({ code, minutes, text, codedBy: code ? "auto" : null });
  }
  if (!lines.length) {
    if (!(total > 0)) return [];
    const text = String(wholeText || "").replace(/\s+/g, " ").trim().slice(0, 300);
    const code = suggestLabourActivityCode(text);
    return [{ code, minutes: total, text, codedBy: code ? "auto" : null }];
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
    if (rawCode && !activity) throw new Error(`Line ${i + 1}: unknown activity code ${rawCode}.`);
    if (requireCodes && !activity) throw new Error(`Line ${i + 1}: choose an activity.`);
    const text = String((line && line.text) || "").replace(/\s+/g, " ").trim().slice(0, 300);
    if (activity && /-OTHER$/.test(activity.code) && !text) throw new Error(`Line ${i + 1}: say what the "${activity.label}" work was.`);
    return { code: activity ? activity.code : null, minutes, text, codedBy: activity ? codedBy : null };
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
  normalizeLabourLines,
  suggestLabourActivityCode,
  summarizeLabourLines,
};
