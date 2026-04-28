/**
 * Maps a field log line to one or more daily-report sections (construction-aware).
 * Every saved entry should participate in the unified day log; sections drive PDF/layout.
 */

const { textContainsManpowerRollcall } = require("./manpowerRollcall");

const DEFAULT_SECTIONS = ["dayLog"];

/**
 * @param {{ category?: string, tags?: string[], rawText?: string, normalizedText?: string }} input
 * @returns {string[]} unique section keys
 */
function computeDailySummarySections(input) {
  const category = String(input.category || "journal").toLowerCase();
  const tags = Array.isArray(input.tags) ? input.tags.map((t) => String(t).toLowerCase()) : [];
  const text = String(
    input.normalizedText || input.rawText || ""
  ).toLowerCase();

  const sections = new Set(DEFAULT_SECTIONS);

  const byCategory = {
    safety: ["safety", "openItems"],
    delay: ["delays"],
    deficiency: ["deficiencies", "openItems"],
    issue: ["issues", "openItems"],
    note: ["notes"],
    progress: ["workInProgress", "workCompleted"],
    delivery: ["deliveries"],
    inspection: ["inspections", "openItems"],
    journal: ["journal"],
  };

  for (const s of byCategory[category] || ["journal"]) {
    sections.add(s);
  }

  const add = (k) => sections.add(k);

  if (/\b(pour|concrete|slab|pump|ready\s*mix|mud\s*mat|curing|mixer|placement)\b/.test(text)) {
    add("concrete");
  }
  if (/\b(rain|snow|wind|forecast|weather|humid|hot\s*day|cold|freezing|icy)\b/.test(text)) {
    add("weather");
  }
  if (/\b(crew|manpower|labou?r|workers|short\s*staff|headcount|sub\s*crew)\b/.test(text)) {
    add("manpower");
  }
  if (textContainsManpowerRollcall(input.normalizedText || input.rawText || "")) {
    add("manpower");
  }
  if (/\b(inspect|inspection|consultant|englobe|geotech|third\s*party)\b/.test(text)) {
    add("inspections");
  }
  if (/\b(delay|late|waiting|cancelled|canceled|held\s*up|backorder|no\s*show)\b/.test(text)) {
    add("delays");
  }
  if (/\b(defect|deficiency|punch|missed|wrong|broken|hazard|unsafe)\b/.test(text)) {
    add("deficiencies");
    add("openItems");
  }
  if (/\b(deliver|delivery|truck\s*load|material\s*arrived|drop\s*off)\b/.test(text)) {
    add("deliveries");
  }
  if (/\b(complete|completed|done|wrapped|finished)\b/.test(text) && category === "progress") {
    add("workCompleted");
  }
  if (/\b(ongoing|in\s*progress|underway|continuing|tomorrow)\b/.test(text)) {
    add("workInProgress");
  }
  if (tags.includes("mms") || tags.includes("photo")) {
    add("photos");
  }

  if (category === "delay" && /\b(pour|concrete|slab)\b/.test(text)) {
    add("concrete");
  }
  if (category === "inspection" && /\b(rebar|duct|opening)\b/.test(text)) {
    add("issues");
    add("deficiencies");
  }

  return Array.from(sections);
}

/**
 * Heuristic: should this line surface under open items / action required.
 */
function isLikelyOpenItem(input) {
  const text = String(
    input.normalizedText || input.rawText || ""
  ).toLowerCase();
  const category = String(input.category || "").toLowerCase();
  if (["deficiency", "issue", "safety", "delay", "inspection"].includes(category))
    return true;
  if (/\b(pending|open|follow\s*up|unresolved|waiting|tomorrow|must\s*fix)\b/.test(text))
    return true;
  return false;
}

module.exports = {
  computeDailySummarySections,
  isLikelyOpenItem,
};
