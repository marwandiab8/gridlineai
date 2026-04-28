/**
 * Strict JSON daily report from OpenAI - primary path for superintendent-style PDF content.
 */

const OpenAI = require("openai");
const { getModels, sanitizeChatCompletionParams } = require("./aiConfig");
const { completionText } = require("./openaiHelpers");
const { isValidTradeHeading } = require("./dailyReportIntegrity");

function stripJsonFences(raw) {
  let s = String(raw || "").trim();
  if (s.startsWith("```")) {
    s = s.replace(/^```(?:json)?\s*/i, "").replace(/\s*```\s*$/i, "");
  }
  return s.trim();
}

/**
 * @returns {object|null}
 */
function parseStructuredDailyReportJson(raw) {
  const s = stripJsonFences(raw);
  if (!s) return null;
  try {
    return JSON.parse(s);
  } catch (_) {
    return null;
  }
}

function isGenericTradeLabel(value) {
  return /^(site\s*\/\s*general|general|general\s+conditions|misc(?:ellaneous)?|journal|notes?)$/i.test(
    String(value || "").trim()
  );
}

function sanitizeWorkSectionTrade(trade) {
  const t = String(trade || "").trim();
  if (!t || !isValidTradeHeading(t) || isGenericTradeLabel(t)) return "";
  return t.slice(0, 48);
}

function sanitizeJsonLine(text, maxLen = 260) {
  const t = String(text || "").replace(/\s+/g, " ").trim();
  if (!t) return "";
  if (
    /requested\s+item\s+to\s+be\s+included\s+in\s+the\s+daily\s+summary/i.test(t) ||
    /request(?:ing)?\s+(?:a\s+)?plan\s+for\s+tomorrow/i.test(t) ||
    /\basks?\s+for\s+tomorrow'?s?\s+weather\s+forecast\b/i.test(t) ||
    /unable\s+to\s+retrieve\s+live\s+weather\s+data/i.test(t) ||
    /\b(fix|correct|update|revise|clean\s*up|move|adjust|change)\b[\s\S]{0,60}\b(report|pdf|daily\s+report|summary|header|title|layout|spacing)\b/i.test(
      t
    ) ||
    /\b(add|include|put|show|attach|leave|keep|use)\b[\s\S]{0,70}\b(photo|photos|picture|pictures|pic|pics|image|images|this)\b[\s\S]{0,50}\b(report|pdf|daily\s+report|summary)\b/i.test(
      t
    ) ||
    /\bmake\s+sure\b[\s\S]{0,70}\b(photo|photos|picture|pictures|pic|pics|image|images|this|that)\b[\s\S]{0,50}\b(in|on)\s+(the\s+)?(report|pdf|daily\s+report|summary)\b/i.test(
      t
    ) ||
    /\b(add\s+the\s+below\s+updates|project\s+\S+\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b)/i.test(
      t
    )
  ) {
    return "";
  }
  if (!Number.isFinite(maxLen) || maxLen < 1) return t;
  return t.slice(0, maxLen);
}

function sanitizeExecutiveSummary(text, maxLen = 1600) {
  const raw = sanitizeJsonLine(text, Number.POSITIVE_INFINITY);
  if (!raw || raw.length <= maxLen) return raw;

  const clipped = raw.slice(0, maxLen + 1);
  const sentenceStops = [". ", "! ", "? "];
  let cut = -1;
  for (const stop of sentenceStops) {
    cut = Math.max(cut, clipped.lastIndexOf(stop));
  }
  if (cut >= Math.floor(maxLen * 0.55)) {
    return clipped.slice(0, cut + 1).trim();
  }

  const punctuationCut = Math.max(
    clipped.lastIndexOf("."),
    clipped.lastIndexOf("!"),
    clipped.lastIndexOf("?"),
    clipped.lastIndexOf(";")
  );
  if (punctuationCut >= Math.floor(maxLen * 0.55)) {
    return clipped.slice(0, punctuationCut + 1).trim();
  }

  const wordCut = clipped.lastIndexOf(" ");
  if (wordCut >= Math.floor(maxLen * 0.55)) {
    return clipped.slice(0, wordCut).trim();
  }

  return clipped.slice(0, maxLen).trim();
}

/**
 * Normalize and fix junk trade names in AI JSON.
 */
function sanitizeStructuredDailyReportJson(parsed) {
  if (!parsed || typeof parsed !== "object") return null;
  const out = { ...parsed };

  const weather = out.weather && typeof out.weather === "object" ? { ...out.weather } : {};
  weather.todaySummary = sanitizeJsonLine(weather.todaySummary, 320);
  weather.weeklyForecastRows = [];
  out.weather = weather;

  const manpower = out.manpower && typeof out.manpower === "object" ? { ...out.manpower } : {};
  manpower.rows = Array.isArray(manpower.rows)
    ? manpower.rows
        .map((r) => ({
          trade: sanitizeWorkSectionTrade(r.trade) || "\u2014",
          foreman: sanitizeJsonLine(r.foreman, 44) || "\u2014",
          workers: sanitizeJsonLine(r.workers, 16) || "\u2014",
          notes: sanitizeJsonLine(r.notes, 220) || "\u2014",
        }))
        .filter((r) => r.trade !== "\u2014")
    : [];
  manpower.summaryNote = sanitizeJsonLine(manpower.summaryNote, 320);
  out.manpower = manpower;

  const work = out.workCompletedInProgress && typeof out.workCompletedInProgress === "object"
    ? { ...out.workCompletedInProgress }
    : {};
  work.sections = Array.isArray(work.sections)
    ? work.sections
        .map((sec) => {
          const trade = sanitizeWorkSectionTrade(sec.trade);
          const items = Array.isArray(sec.items)
            ? sec.items.map((x) => sanitizeJsonLine(x, 320)).filter(Boolean)
            : [];
          return { trade, items };
        })
        .filter((sec) => sec.trade && sec.items.length > 0)
    : [];
  out.workCompletedInProgress = work;

  const issues = out.issuesDeficienciesDelays && typeof out.issuesDeficienciesDelays === "object"
    ? { ...out.issuesDeficienciesDelays }
    : {};
  issues.items = Array.isArray(issues.items)
    ? issues.items.map((x) => sanitizeJsonLine(x, 320)).filter(Boolean)
    : [];
  out.issuesDeficienciesDelays = issues;

  const inspections = out.inspections && typeof out.inspections === "object" ? { ...out.inspections } : {};
  inspections.items = Array.isArray(inspections.items)
    ? inspections.items.map((x) => sanitizeJsonLine(x, 320)).filter(Boolean)
    : [];
  out.inspections = inspections;

  const concrete = out.concreteSummary && typeof out.concreteSummary === "object"
    ? { ...out.concreteSummary }
    : {};
  concrete.rows = Array.isArray(concrete.rows)
    ? concrete.rows
        .map((r) => ({
          location: sanitizeJsonLine(r.location, 120),
          volume: sanitizeJsonLine(r.volume, 40),
          status: sanitizeJsonLine(r.status, 28),
        }))
        .filter((r) => r.location || r.volume || r.status)
    : [];
  concrete.narrativeNote = sanitizeJsonLine(concrete.narrativeNote, 320);
  out.concreteSummary = concrete;

  const openItems = out.openItems && typeof out.openItems === "object" ? { ...out.openItems } : {};
  openItems.rows = Array.isArray(openItems.rows)
    ? openItems.rows
        .map((r) => ({
          actionItem: sanitizeJsonLine(r.actionItem, 240),
          responsible: sanitizeJsonLine(r.responsible, 40) || "\u2014",
          status: sanitizeJsonLine(r.status, 28) || "Open",
        }))
        .filter((r) => r.actionItem)
    : [];
  openItems.narrativeNote = sanitizeJsonLine(openItems.narrativeNote, 320);
  out.openItems = openItems;

  out.executiveSummary = sanitizeExecutiveSummary(out.executiveSummary, 1600);

  return out;
}

const JSON_SCHEMA_HINT = `Return a single JSON object with this shape (all keys required; use empty strings or empty arrays when unknown):
{
  "executiveSummary": "string - 3-6 sentences, superintendent/site coordinator voice, summarize major trades, key progress, constraints, inspections, and critical next actions from INPUT only",
  "weather": { "todaySummary": "string - report day only", "weeklyForecastRows": [] },
  "manpower": {
    "rows": [ { "trade": "", "foreman": "", "workers": "", "notes": "" } ],
    "summaryNote": "string - short superintendent note that adds value beyond the table"
  },
  "workCompletedInProgress": {
    "sections": [ { "trade": "Real contractor or trade name (e.g. Coreydale, Electrical, Formwork, Waterproofing)", "items": ["specific field bullets with area, progress achieved, constraints, and coordination notes when present"] } ]
  },
  "issuesDeficienciesDelays": { "items": ["specific issue/deficiency/delay with impact and follow-up when stated"] },
  "inspections": { "items": ["consultant/inspector, scope inspected, finding/result, follow-up if required"] },
  "concreteSummary": {
    "rows": [ { "location": "", "volume": "", "status": "" } ],
    "narrativeNote": "short technical note only when it adds detail not already in the table"
  },
  "openItems": {
    "rows": [ { "actionItem": "", "responsible": "", "status": "" } ],
    "narrativeNote": "short action-required note only when useful"
  }
}

Trade names in workCompletedInProgress.sections must be real subs/trades. Prefer named contractors from INPUT when present (e.g. ALC, Road-Ex, Coreydale, O'Connor, SteelCon) over generic scopes like "Concrete", "Earthworks", or "Roadwork" unless INPUT only names the scope. Never use verbs or sentence starters as trade: not "Add", "Clarifies", "Received", "Give", "Request", "Internal", "Two", "The". Do not use generic buckets like "Site / General", "Journal", "Misc", or "Notes" unless there is truly no inferable trade or contractor.
Manpower rows must be clean and professional: infer foreman and workers from natural language when possible; never output rows for "Journal", "Site / General", "General", "Notes", or admin chatter.
Open items must be concrete and actionable; never include report-generation requests, weather requests, summary instructions, or app/debug chatter.
Never echo ingestion lines: no "Add the below updates", no "Project [name] Monday April ..." headers, no pasted command wrappers. Strip those from all JSON strings.`;

const JOURNAL_JSON_SCHEMA_HINT = `Return a single JSON object with this shape (all keys required; use empty strings or empty arrays when unknown):
{
  "overview": "string - 2-5 sentences, personal or shared daily journal voice, summarize what the day felt like and what stood out",
  "keyMoments": ["specific moments, events, or actions from the day in chronological or meaningful order"],
  "reflections": ["personal reflections, realizations, worries, gratitude, stressors, or emotional notes when supported by INPUT"],
  "closingNote": "string - short reflective closing note for the day"
}

Do not write like a superintendent report.
Do not output construction-only headings, manpower tables, inspections tables, concrete summaries, or open-items language unless the INPUT genuinely reads that way and it naturally belongs inside a personal journal sentence.
Do not echo commands, report/PDF requests, weather requests, project-switch commands, or app/debug chatter.
Do not invent events, feelings, or facts that are not supported by INPUT.`;

function sanitizeStructuredJournalReportJson(parsed) {
  if (!parsed || typeof parsed !== "object") return null;
  return {
    overview: sanitizeJsonLine(parsed.overview, 900),
    keyMoments: Array.isArray(parsed.keyMoments)
      ? parsed.keyMoments.map((x) => sanitizeJsonLine(x, 320)).filter(Boolean).slice(0, 12)
      : [],
    reflections: Array.isArray(parsed.reflections)
      ? parsed.reflections.map((x) => sanitizeJsonLine(x, 320)).filter(Boolean).slice(0, 10)
      : [],
    closingNote: sanitizeJsonLine(parsed.closingNote, 360),
  };
}

/**
 * @returns {Promise<object|null>}
 */
async function generateStructuredDailyReportJson({
  openaiApiKey,
  projectName,
  dateKey,
  timeZoneLabel,
  reportBundle,
  logger,
  runId,
  modelsOverride,
}) {
  if (!openaiApiKey || !String(reportBundle || "").trim()) return null;

  const client = new OpenAI({ apiKey: openaiApiKey });
  const models = getModels(modelsOverride);
  const userContent = `Project: ${projectName}. Report day: ${dateKey} (${timeZoneLabel}).

INPUT - curated field updates only (chronological). These are the ONLY facts you may use:
${String(reportBundle).slice(0, 12_000)}

${JSON_SCHEMA_HINT}

Rules:
- Write like a superintendent daily site log, not a chatbot or SMS recap.
- If the message begins with AUTHORITATIVE WEATHER FOR REPORT DAY, use ONLY those numbers and conditions for weather.todaySummary and any weather mentions in executiveSummary - never invent different temperatures, wind, or precipitation. If the message begins with WEATHER LOOKUP FAILED, say weather was unavailable; do not fabricate values.
- Do not repeat or echo: commands, report/PDF requests, "continue", clarification about reports, ChatGPT/keyword requests, workflow lines, or placeholders like "No message content provided".
- Do not echo bulk-ingestion text: never output "Add the below updates to the ... project", "Project Docksteader Monday ...", or similar wrappers; only the field facts belong in JSON.
- Report day is ${dateKey}. Do not paste contradictory embedded calendar dates from INPUT into narrative strings; use facts without wrong dates.
- Do not invent quantities, locations, trades, or events not supported by INPUT (except weather, which must match AUTHORITATIVE WEATHER when present).
- Prefer benchmark-grade detail and structure: trade-by-trade field activity, where work occurred, what progress was achieved, what constrained the work, and what coordination or inspection follow-up matters for the record.
- Avoid vague bullets like "work continued" or "site work ongoing" when INPUT supports a more specific location, component, quantity, inspection, or coordination note.
- If a manpower table is present in INPUT, structure it cleanly and do not repeat the same row-by-row content in summaryNote.
- If inspection or consultant information exists, surface consultant/inspector name, inspected scope, result, and follow-up.
- If concrete activity exists, surface location/scope, quantity when stated, and whether the pour was completed, delayed, or scheduled.
- If INPUT has no fact for a subsection, use empty array or "Not stated in field messages." for that string field.
- weather.weeklyForecastRows must remain an empty array [].
- manpower.summaryNote: omit or use a short superintendent note; do not paste the raw INPUT block. If INPUT is already tabular, summaryNote may be empty.`;

  const params = {
    model: models.primary,
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content:
          "You output only valid JSON for a construction daily field report. No markdown. No prose outside the JSON.",
      },
      { role: "user", content: userContent },
    ],
    max_completion_tokens: 8000,
    temperature: 0.2,
  };
  sanitizeChatCompletionParams(params, models.primary);

  try {
    const completion = await client.chat.completions.create(params);
    const raw = completionText(completion) || "";
    const parsed = parseStructuredDailyReportJson(raw);
    if (!parsed) {
      if (logger) logger.warn("dailyReportAiJson: parse failed", { runId });
      return null;
    }
    return sanitizeStructuredDailyReportJson(parsed);
  } catch (e) {
    if (logger) logger.warn("dailyReportAiJson: request failed", { runId, message: e.message });
    return null;
  }
}

async function generateStructuredJournalReportJson({
  openaiApiKey,
  dateKey,
  timeZoneLabel,
  reportBundle,
  logger,
  runId,
  modelsOverride,
}) {
  if (!openaiApiKey || !String(reportBundle || "").trim()) return null;

  const client = new OpenAI({ apiKey: openaiApiKey });
  const models = getModels(modelsOverride);
  const userContent = `Report day: ${dateKey} (${timeZoneLabel}).

INPUT - personal notes and journal-worthy events from this day only:
${String(reportBundle).slice(0, 12_000)}

${JOURNAL_JSON_SCHEMA_HINT}

Rules:
- Write like a thoughtful personal day journal, not a chatbot, not a superintendent, and not a construction daily site log.
- Keep the tone reflective, specific, and human.
- Use actual moments from INPUT. Prefer concrete detail over vague filler.
- Each input line includes an author. Treat authorship as factual context.
- If more than one contributor appears, write it as a shared/co-authored journal day. Do not make it sound like one person did everything.
- In a multi-contributor journal, write a creative third-person narrative using the contributor names from INPUT. Avoid unqualified first-person singular ("I", "me", "my") in the overview, reflections, and closing note.
- Never assign one contributor's action, feeling, errand, meal, or purchase to another contributor.
- Work or site events may appear if they were part of the person's day, but describe them as lived experience, not as a formal contractor report.
- Do not output section labels inside the text arrays.
- If INPUT is sparse, keep the journal honest and concise rather than inventing detail.`;

  const params = {
    model: models.primary,
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content:
          "You output only valid JSON for a personal daily journal report. No markdown. No prose outside the JSON.",
      },
      { role: "user", content: userContent },
    ],
    max_completion_tokens: 5000,
    temperature: 0.35,
  };
  sanitizeChatCompletionParams(params, models.primary);

  try {
    const completion = await client.chat.completions.create(params);
    const raw = completionText(completion) || "";
    const parsed = parseStructuredDailyReportJson(raw);
    if (!parsed) {
      if (logger) logger.warn("dailyReportAiJson: journal parse failed", { runId });
      return null;
    }
    return sanitizeStructuredJournalReportJson(parsed);
  } catch (e) {
    if (logger) logger.warn("dailyReportAiJson: journal request failed", { runId, message: e.message });
    return null;
  }
}

module.exports = {
  generateStructuredDailyReportJson,
  generateStructuredJournalReportJson,
  parseStructuredDailyReportJson,
  sanitizeExecutiveSummary,
  sanitizeStructuredDailyReportJson,
  sanitizeStructuredJournalReportJson,
};
