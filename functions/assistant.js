/**
 * Construction SMS assistant: OpenAI, commands, Firestore context, issues, summaries.
 */

const { FieldValue } = require("firebase-admin/firestore");
const OpenAI = require("openai");

const COL_USERS = "smsUsers";
const COL_PROJECTS = "projects";
const COL_ADMIN = "adminSettings";
const COL_ISSUES = "issueLogs";
const COL_SUMMARIES = "summaries";
const COL_PROJECT_TODOS = "projectTodos";

const { createSmsIssue, makeTitleFromBody, updateSmsIssueBody } = require("./issueRepository");
const { getModels } = require("./aiConfig");
const { completionText, chatCompletionWithFallback } = require("./openaiHelpers");
const {
  parseStructuredLog,
  parseDeficiencyDetails,
  parseDeficiencyIntakeRequest,
  extractExplicitReportDate,
  extractProjectScopeHint,
  parseDailyReportRequest,
  parseDayRollupRequest,
  isDailyReportPdfRequest,
  isAnyDayRollupRequest,
  isSummaryStyleRequest,
  startOfEasternDay,
  dateKeyEastern,
  addCalendarDaysToDateKey,
} = require("./logClassifier");
const {
  writeLogEntry,
  loadLogEntriesForDayForProject,
  loadTodayLogEntriesForProject,
  formatGroupedDayLog,
  lineText,
  formatRollupByReportSections,
  maybeEnhanceLogEntry,
  appendLinkedMediaIds,
} = require("./logEntryRepository");
const {
  buildUserProjectPatch,
  getAccessibleProjectForUser,
  getUserProjectSlugs,
  normalizeProjectSlug,
} = require("./projectAccess");
const {
  attachExistingMediaToIssueBySourceMessages,
} = require("./mediaRepository");
const {
  escapeRegExp,
  parseBareManpowerPair,
  parseManpowerCorrectionCommand,
  replaceManpowerTradeCount,
} = require("./manpowerRollcall");
const {
  findActiveAppMemberByApprovedPhone,
  findActiveLabourerByPhone,
  canAccessProject,
  canApproveProjectNoteRequests,
  roleAtLeast,
} = require("./authz");
const {
  parseLabourHoursCommand,
  parseLabourHoursBalanceQuery,
  parseManagementLabourBreakdownQuery,
  parseManagementLabourPdfRequest,
  parseManagementLabourTotalsQuery,
  getDateKeyRangeForBalanceQuery,
  formatLabourBalanceReply,
  writeLabourEntry,
  loadLabourEntries,
  buildLabourRollup,
  dayMultiplierFromDateKey,
  labourMinutesFromHours,
  validateLabourReportDateKey,
  startOfWeekFromDateKey,
} = require("./labourRepository");
const { loadLatestLookaheadSnapshot } = require("./lookaheadScheduleRepository");

const ADMIN_DOC_ID = "company";
const MAX_SMS_CHARS = 480;
const HISTORY_LIMIT = 18;
const RATE_WINDOW_MS = 60_000;
const RATE_MAX = 40;
const COL_PROJECT_NOTE_EDIT_REQUESTS = "projectNoteEditRequests";
const HOME_TODO_PROJECT_SLUG = "home";
const CORRECTION_LOOKBACK_DAYS = 7;

const rateBuckets = new Map();

function checkRateLimit(phoneE164, logger, runId) {
  const now = Date.now();
  let b = rateBuckets.get(phoneE164);
  if (!b || now - b.start > RATE_WINDOW_MS) {
    b = { start: now, count: 0 };
    rateBuckets.set(phoneE164, b);
  }
  b.count += 1;
  if (b.count > RATE_MAX) {
    logger.warn("assistant: rate limited", { runId, phoneE164, count: b.count });
    return false;
  }
  return true;
}

const BASE_CONSTRUCTION_SYSTEM = `You are a practical construction field assistant accessed by SMS, MMS, and voice. You help supers, PMs, foremen, and crew with real jobsite work.

Voice: direct, calm, field-friendly. Short sentences. No corporate filler. Sound like someone who has been on site.

You understand: scheduling and lookahead, rebar placement and embeds, concrete pours and curing, excavation and shoring, waterproofing and drainage, punch lists and deficiencies, inspections (structural, MEP, geotech), safety and trade coordination, manpower and crews, logistics and deliveries, weather impacts, sequencing, subs, RFIs, site issues, and daily field reports.

Rules:
- Never say you cannot log deficiencies, issues, safety items, or deliveries. This app saves those when the user texts the commands listed in "Commands:" below (e.g. log deficiency: …). If someone asks how to record a deficiency, give the exact text format—do not claim you lack access to logging.
- Never say you cannot receive, view, save, or attach pictures/photos/MMS. This Twilio number accepts MMS; the backend stores images and links them to the message and field logs. You do not process image bytes in chat, but the system does—so tell users their photos are received and saved, and suggest a caption or "log deficiency: …" / "log issue: …" with the next text if they want it classified.
- Never say you cannot receive voice messages. This Twilio number can take recorded voice updates by phone; the backend can transcribe and save them into the same reporting workflow.
- Keep replies concise for SMS (aim under 320 characters when possible; never exceed ${MAX_SMS_CHARS} characters).
- If you need more detail, offer to break into a second message or ask one sharp clarifying question.
- Use plain language; OK to use common site shorthand when it fits.
- If something is unsafe or could stop work, say so clearly and suggest immediate escalation to the site super/GC safety contact.
- If you lack project-specific info, say what you're assuming or ask one targeted question.
- Never invent permit numbers, inspection results, or contract obligations—say you don't have that record here.
- Never invent or paste https links for daily PDF reports, downloads, or file hosting (corporate portals, cloud drives, etc.). You are not given real PDF URLs in this chat. The system sends the actual download link in a separate automated SMS when the user triggers PDF generation (e.g. text: daily report or daily report please). If they want the PDF, tell them to send a daily PDF request like that — or wait for that system message — and do not make up URLs.`;

const LOG_ROUTING_SYSTEM = `You process inbound SMS/MMS messages into project reporting entries.

Assume every message is meant to update a log or issue. Do not ask the user what they mean unless the message is unusable.

Classification rules:
- If the user explicitly says "safety" or "safety issue", or the message clearly describes a hazard, unsafe condition, incident, near miss, missing protection, or injury risk, classify as "safety".
- If the user says "deficiency", "deficiciency", "deficiency log", "punch", or "punch item", or the message clearly describes defective, incomplete, missing, damaged, or incorrect work requiring correction, classify as "deficiency".
- If the message clearly refers to home, house, personal home, or journal, classify as "journal".
- Otherwise classify as "construction".

Defaulting rules:
- Default to "construction" unless "safety", "deficiency", or "journal" is clearly indicated.
- Do not default to "safety" unless the user explicitly says it or the content is unmistakably safety-related.
- Do not default to "deficiency" unless the user explicitly says it or the content is unmistakably a deficiency or punch item.
- Only use "journal" when the message clearly refers to home or personal journal content.

Photo rules:
- Every received photo must be included.
- Never omit, filter, rank, or choose among photos.
- If multiple photos are received, attach all of them.
- If the message contains only photos or minimal text, still create or update the appropriate entry and attach every photo.

Behavior rules:
- Preserve the user's meaning.
- Clean up spelling and grammar only to make the report readable.
- Keep project details whenever present, including location, unit, room, area, trade, crew, material, status, blocker, action, and follow-up.
- If the message reasonably appears to be an update to the current same-day or same-project context, treat it as an update instead of a brand new unrelated entry.

Return JSON only:
{
  "logType": "construction | journal | safety | deficiency",
  "title": "short report-ready title",
  "description": "clean report-ready description",
  "photos": ["include every received photo"],
  "tags": ["relevant tags if obvious"],
  "requiresFollowUp": true
}`;

const INTENT_ROUTING_SYSTEM = `You classify one inbound SMS/MMS message in context.

Decide whether the latest user message is primarily:
- a request for help, recall, continuation, lookup, explanation, rewrite, or other conversational assistance
- or a new journal/log entry that should be saved

Use the recent conversation context. Short follow-ups like "continue", "go on", "rewrite that", "show me the journal input", "what did I send", or "show me the activities for 2026-04-18" are requests, not new journal entries.

Return JSON only:
{
  "intent": "request | journal_entry | construction_entry | safety_entry | deficiency_entry",
  "confidence": 0.0,
  "reason": "short explanation"
}`;

const ACTION_ROUTING_SYSTEM = `You decide whether an inbound SMS should trigger one backend action immediately.

Choose exactly one action from this list:
- none
- project_set
- daily_pdf_request
- day_rollup
- lookahead_trade_query
- lookahead_activities_report
- lookahead_closeout_report
- labour_balance
- start_timer
- stop_timer
- deficiency_intake
- todo_create
- notify_request
- project_notes_update

Pick an action only when the user is clearly asking the assistant to do it now. If the user is asking for advice, explanation, brainstorming, or anything ambiguous, return action "none".

Return JSON only:
{
  "action": "none | project_set | daily_pdf_request | day_rollup | lookahead_trade_query | lookahead_activities_report | lookahead_closeout_report | labour_balance | start_timer | stop_timer | deficiency_intake | todo_create | notify_request | project_notes_update",
  "confidence": 0.0,
  "reason": "short explanation",
  "projectSlug": "only for project_set",
  "reportDateKey": "YYYY-MM-DD when explicitly requested, else empty",
  "reportType": "dailySiteLog | journal | empty",
  "preferAiNarrative": true,
  "tradeQuery": "trade / contractor name for lookahead queries, else empty",
  "range": "today | week | pay | month | this_week | next_week | empty",
  "timerLabel": "only for start_timer",
  "todoText": "task text for todo_create",
  "todoDueWindow": "next_week | next_month | empty",
  "notifyAudience": "management | project_users | empty",
  "notifyMessage": "message body for notify_request",
  "proposedNotes": "replacement project notes for project_notes_update",
  "deficiency": {
    "title": "",
    "description": "",
    "location": "",
    "area": "",
    "trade": "",
    "reference": "",
    "requestedAction": ""
  }
}

Rules:
- Use action "none" for general questions, strategy questions, and any request that is not clearly one of the supported actions.
- For daily summaries or daily log lookups, use action "day_rollup".
- For PDF requests, use action "daily_pdf_request".
- For lookahead trade requests, use action "lookahead_trade_query" and set range to this_week or next_week.
- For "create / generate the activities report" from lookahead, use action "lookahead_activities_report".
- For "create / generate the closeout report" from lookahead, use action "lookahead_closeout_report".
- For labour totals, use action "labour_balance" and set range to today, week, pay, or month.
- For deficiencies described in natural language, use action "deficiency_intake" and fill as many deficiency fields as the message clearly provides.
- For todo creation requests, use action "todo_create" with concise task text and optional due window.
- For notifications or broadcasts, use action "notify_request" with audience and message body.
- For project notes edits, use action "project_notes_update" with full replacement notes text.
- Never invent a project slug or date.`;

const SAFETY_LOG_RE =
  /\b(safety|safety issue|unsafe|hazard|incident|near\s*miss|missing protection|unguarded|injury risk|fall hazard|no ppe|without ppe|electrocution)\b/i;
const DEFICIENCY_LOG_RE =
  /\b(deficiency|deficiciency|deficiency log|punch|punch item|defect|defective|incomplete|missing|damaged|incorrect|broken)\b/i;
const JOURNAL_HOME_RE =
  /\b(personal home|home journal|journal|house|home)\b/i;

function truncateSms(text) {
  const t = (text || "").trim();
  if (t.length <= MAX_SMS_CHARS) return t;
  return t.slice(0, MAX_SMS_CHARS - 3) + "...";
}

function parseDateKeyUtc(value) {
  const raw = String(value || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  return Number.isNaN(date.getTime()) ? null : date;
}

function shiftDateKeyUtc(dateKey, deltaDays) {
  const date = parseDateKeyUtc(dateKey);
  if (!date || !Number.isFinite(deltaDays)) return "";
  date.setUTCDate(date.getUTCDate() + Number(deltaDays));
  return date.toISOString().slice(0, 10);
}

function formatShortDateKey(dateKey) {
  const date = parseDateKeyUtc(dateKey);
  if (!date) return String(dateKey || "").trim();
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(date);
}

function formatShortDateRange(startKey, endKey) {
  const start = String(startKey || "").trim();
  const end = String(endKey || startKey || "").trim();
  if (!start) return "date TBD";
  if (!end || end === start) return formatShortDateKey(start);
  const startDate = parseDateKeyUtc(start);
  const endDate = parseDateKeyUtc(end);
  if (!startDate || !endDate) return `${start || "?"} to ${end || "?"}`;
  const sameMonth = start.slice(0, 7) === end.slice(0, 7);
  if (sameMonth) {
    return `${formatShortDateKey(start)}-${new Intl.DateTimeFormat("en-US", {
      day: "numeric",
      timeZone: "UTC",
    }).format(endDate)}`;
  }
  return `${formatShortDateKey(start)}-${formatShortDateKey(end)}`;
}

function normalizeLooseToken(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeLookaheadActivityLabel(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\s*[-:]\s*$/, "")
    .slice(0, 160);
}

function parseLookaheadActivitiesQuery(text) {
  const raw = String(text || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!raw) return null;
  if (!/\b(activities?|lookahead|schedule)\b/i.test(raw)) return null;

  const rangeMatch = raw.match(/\b(this|next)\s+week\b/i);
  if (!rangeMatch) return null;
  const range = String(rangeMatch[1] || "").toLowerCase() === "next" ? "next_week" : "this_week";
  const beforeRange = raw.slice(0, rangeMatch.index).trim().replace(/[,:-]+$/, "").trim();
  let tradeQuery = "";

  const patterns = [
    /\b(?:show|tell|give|list)\s+me\s+(?:the\s+)?(?:activities?|lookahead|schedule)\s+for\s+(.+)$/i,
    /\b(?:what(?:'s|s| is)\s+)?(?:the\s+)?(?:activities?|lookahead|schedule)\s+for\s+(.+)$/i,
    /\b(?:activities?|lookahead|schedule)\s+for\s+(.+)$/i,
    /^(.+?)\s+(?:activities?|lookahead|schedule)$/i,
  ];
  for (const pattern of patterns) {
    const match = beforeRange.match(pattern);
    if (!match) continue;
    tradeQuery = String(match[1] || "").trim();
    break;
  }

  tradeQuery = tradeQuery
    .replace(/^(?:for|on)\s+/i, "")
    .replace(/^(?:the\s+)?trade\s+/i, "")
    .replace(/\s+for$/i, "")
    .replace(/^["']|["']$/g, "")
    .trim();

  if (/^(?:all|everything|everyone|this project|the project)$/i.test(tradeQuery)) {
    tradeQuery = "";
  }

  return {
    tradeQuery,
    range,
  };
}

function getDateKeyWindowForLookaheadRange(range, now = new Date()) {
  const todayKey = dateKeyEastern(now);
  const thisWeekStart = startOfWeekFromDateKey(todayKey) || todayKey;
  if (range === "next_week") {
    const startKey = shiftDateKeyUtc(thisWeekStart, 7) || thisWeekStart;
    const endKey = shiftDateKeyUtc(startKey, 6) || startKey;
    return { startKey, endKey, label: "next week" };
  }
  const endKey = shiftDateKeyUtc(thisWeekStart, 6) || thisWeekStart;
  return { startKey: thisWeekStart, endKey, label: "this week" };
}

function taskIntersectsLookaheadWindow(task, startKey, endKey) {
  const scheduled = Array.isArray(task && task.scheduledDateKeys)
    ? task.scheduledDateKeys.filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(String(value || "").trim()))
    : [];
  if (scheduled.some((dateKey) => dateKey >= startKey && dateKey <= endKey)) return true;

  const startDate = String(task && task.startDate || "").trim();
  const finishDate = String(task && task.finishDate || task && task.startDate || "").trim();
  if (!startDate && !finishDate) return false;
  const effectiveStart = startDate || finishDate;
  const effectiveFinish = finishDate || startDate;
  return effectiveStart <= endKey && effectiveFinish >= startKey;
}

function taskMatchesTradeQuery(task, tradeQuery) {
  const queryNorm = normalizeLooseToken(tradeQuery);
  if (!queryNorm) return true;
  const tradeNorm = normalizeLooseToken(task && task.actionBy);
  if (!tradeNorm) return false;
  return tradeNorm === queryNorm || tradeNorm.includes(queryNorm) || queryNorm.includes(tradeNorm);
}

function taskRelevantDateRange(task, startKey, endKey) {
  const scheduled = Array.isArray(task && task.scheduledDateKeys)
    ? [...new Set(task.scheduledDateKeys.filter((dateKey) => dateKey >= startKey && dateKey <= endKey))].sort()
    : [];
  if (scheduled.length) {
    return {
      startKey: scheduled[0],
      endKey: scheduled[scheduled.length - 1],
    };
  }
  const taskStart = String(task && task.startDate || "").trim();
  const taskFinish = String(task && task.finishDate || task && task.startDate || "").trim();
  if (!taskStart && !taskFinish) return { startKey, endKey };
  const effectiveStart = taskStart && taskStart > startKey ? taskStart : startKey;
  const effectiveEnd = taskFinish && taskFinish < endKey ? taskFinish : endKey;
  return {
    startKey: effectiveStart || startKey,
    endKey: effectiveEnd || effectiveStart || endKey,
  };
}

function formatLookaheadTaskLine(task, startKey, endKey) {
  const label = normalizeLookaheadActivityLabel(task && task.activity) || "Untitled activity";
  const relevantRange = taskRelevantDateRange(task, startKey, endKey);
  return `${formatShortDateRange(relevantRange.startKey, relevantRange.endKey)}: ${label}`;
}

function formatLookaheadActivitiesReply({
  projectName,
  tradeQuery,
  rangeLabel,
  startKey,
  endKey,
  tasks,
}) {
  const scopeLabel = tradeQuery ? `${tradeQuery} activities` : "Activities";
  const prefix = `${projectName || "Project"} — ${scopeLabel} for ${rangeLabel} (${startKey} to ${endKey})`;
  if (!Array.isArray(tasks) || !tasks.length) {
    return `${prefix}: none found in the latest 3-week lookahead.`;
  }
  const lines = tasks.map((task) => formatLookaheadTaskLine(task, startKey, endKey));
  const maxItems = 6;
  const shown = lines.slice(0, maxItems);
  const more = lines.length - shown.length;
  return `${prefix}: ${shown.join("; ")}${more > 0 ? `; +${more} more.` : "."}`;
}

function formatManagementLabourTotalsReply({
  scopeLabel = "All labourers",
  rangeLabel,
  startKey,
  endKey,
  totalHours,
  totalEntries,
  labourerCount,
  projectCount,
  projectTotals,
}) {
  const hours = Math.round((Number(totalHours) || 0) * 100) / 100;
  const rangeBits =
    startKey && endKey
      ? startKey === endKey
        ? startKey
        : `${startKey} to ${endKey}`
      : "";
  if (!Number(totalEntries) || totalEntries < 1) {
    return `${scopeLabel} — ${rangeLabel} (${rangeBits}): no hours logged yet.`;
  }
  const topProjects = Array.isArray(projectTotals) ? projectTotals.slice(0, 3) : [];
  const topSummary = topProjects.length
    ? ` Top: ${topProjects
        .map((item) => {
          const slug = String(item && item.projectSlug ? item.projectSlug : "unassigned").trim() || "unassigned";
          const itemHours = Math.round((Number(item && item.totalHours) || 0) * 100) / 100;
          return `${slug} ${itemHours}h`;
        })
        .join(" · ")}.`
    : "";
  return `${scopeLabel} — ${rangeLabel} (${rangeBits}): ${hours}h across ${totalEntries} entries, ${labourerCount} labourers, ${projectCount} projects.${topSummary}`;
}

function formatManagementLabourBreakdownReply({ groupBy, rangeLabel, startKey, endKey, items }) {
  const rangeBits =
    startKey && endKey
      ? startKey === endKey
        ? startKey
        : `${startKey} to ${endKey}`
      : "";
  const label = groupBy === "project" ? "Hours by project" : "Hours by labourer";
  const rows = Array.isArray(items) ? items.filter(Boolean) : [];
  if (!rows.length) {
    return `${label} — ${rangeLabel} (${rangeBits}): no hours logged yet.`;
  }
  const shown = rows.slice(0, 6).map((item) => `${item.label} ${item.totalHours}h`);
  const more = rows.length - shown.length;
  return `${label} — ${rangeLabel} (${rangeBits}): ${shown.join("; ")}${more > 0 ? `; +${more} more.` : "."}`;
}

function inferInboundLogType(text) {
  const raw = String(text || "").trim();
  if (!raw) return "construction";
  if (JOURNAL_HOME_RE.test(raw)) return "journal";
  if (SAFETY_LOG_RE.test(raw)) return "safety";
  if (DEFICIENCY_LOG_RE.test(raw)) return "deficiency";
  return "construction";
}

function sanitizeRouteTags(tags, fallback = []) {
  const out = [];
  for (const value of Array.isArray(tags) ? tags : []) {
    const clean = String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 40);
    if (clean && !out.includes(clean)) out.push(clean);
    if (out.length >= 10) break;
  }
  for (const value of fallback) {
    if (value && !out.includes(value)) out.push(value);
  }
  return out.slice(0, 10);
}

function containsDecorativeUnicode(text) {
  return /[\p{Extended_Pictographic}\p{Emoji_Presentation}\u2600-\u27BF]/u.test(
    String(text || "")
  );
}

function preserveUserUnicodeText(originalText, cleanedText) {
  const original = String(originalText || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 4000);
  const cleaned = String(cleanedText || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 4000);
  if (!original) return cleaned;
  if (!cleaned) return original;
  return containsDecorativeUnicode(original) ? original : cleaned;
}

function sanitizeRoutePayload(raw, fallbackText, numMedia) {
  const fallbackType = inferInboundLogType(fallbackText);
  const fallbackDescription = String(fallbackText || "").trim();
  const candidateType = String(raw && raw.logType ? raw.logType : "")
    .trim()
    .toLowerCase();
  const logType = ["construction", "journal", "safety", "deficiency"].includes(candidateType)
    ? candidateType
    : fallbackType;
  const aiDescription = String(raw && raw.description ? raw.description : "");
  const description = preserveUserUnicodeText(fallbackDescription, aiDescription) || "Field update";
  const aiTitle = String(raw && raw.title ? raw.title : "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 160);
  const title =
    containsDecorativeUnicode(description) && !containsDecorativeUnicode(aiTitle)
      ? makeTitleFromBody(description, 120)
      : aiTitle || makeTitleFromBody(description, 120);
  const tags = sanitizeRouteTags(raw && raw.tags, [
    logType === "construction" ? "construction" : logType,
  ]);
  const requiresFollowUp = raw && typeof raw.requiresFollowUp === "boolean"
    ? raw.requiresFollowUp
    : ["safety", "deficiency"].includes(logType);
  const photoCount = Math.max(0, parseInt(String(numMedia || 0), 10) || 0);
  const photos = photoCount
    ? Array.from({ length: photoCount }, (_, index) => `photo_${index + 1}`)
    : [];

  return {
    logType,
    title,
    description,
    photos,
    tags,
    requiresFollowUp,
  };
}

function fallbackInboundIntent(text) {
  const raw = String(text || "").trim();
  if (!raw) return "request";
  if (
    /^(continue|go on|keep going|more|expand|elaborate|rewrite|reword|try again)$/i.test(raw) ||
    /\b(show|read|give)\s+me\s+.*\b(journal|input|note|notes|activities|activity|log)\b/i.test(raw) ||
    /\bwhat\s+(did\s+i\s+(say|send|log)|was\s+my\s+(journal|note|input))\b/i.test(raw)
  ) {
    return "request";
  }
  const inferred = inferInboundLogType(raw);
  if (inferred === "journal") return "journal_entry";
  if (inferred === "construction") return "construction_entry";
  if (inferred === "safety") return "safety_entry";
  if (inferred === "deficiency") return "deficiency_entry";
  return "request";
}

function sanitizeIntentPayload(raw, fallbackText) {
  const fallbackIntent = fallbackInboundIntent(fallbackText);
  const candidateIntent = String(raw && raw.intent ? raw.intent : "")
    .trim()
    .toLowerCase();
  const intent = [
    "request",
    "journal_entry",
    "construction_entry",
    "safety_entry",
    "deficiency_entry",
  ].includes(candidateIntent)
    ? candidateIntent
    : fallbackIntent;
  const confidenceRaw = Number(raw && raw.confidence);
  const confidence = Number.isFinite(confidenceRaw)
    ? Math.max(0, Math.min(1, confidenceRaw))
    : intent === fallbackIntent
      ? 0.55
      : 0.75;
  const reason = String(raw && raw.reason ? raw.reason : "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 240);
  const source = raw && typeof raw === "object" && Object.keys(raw).length ? "ai" : "fallback";
  return { intent, confidence, reason, source };
}

function sanitizeAssistantActionPlan(raw) {
  const allowedActions = new Set([
    "none",
    "project_set",
    "daily_pdf_request",
    "day_rollup",
    "lookahead_trade_query",
    "lookahead_activities_report",
    "lookahead_closeout_report",
    "labour_balance",
    "start_timer",
    "stop_timer",
    "deficiency_intake",
    "todo_create",
    "notify_request",
    "project_notes_update",
  ]);
  const allowedReportTypes = new Set(["", "dailySiteLog", "journal"]);
  const allowedRanges = new Set(["", "today", "week", "pay", "month", "this_week", "next_week"]);
  const allowedTodoDueWindows = new Set(["", "next_week", "next_month"]);
  const allowedNotifyAudiences = new Set(["", "management", "project_users"]);
  const action = String(raw && raw.action ? raw.action : "")
    .trim()
    .toLowerCase();
  const normalizedAction = allowedActions.has(action) ? action : "none";
  const confidence = Number(raw && raw.confidence);
  return {
    action: normalizedAction,
    confidence: Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : 0,
    reason: String(raw && raw.reason ? raw.reason : "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 240),
    projectSlug: normalizeProjectSlug(raw && raw.projectSlug ? raw.projectSlug : "") || null,
    reportDateKey: /^\d{4}-\d{2}-\d{2}$/.test(String(raw && raw.reportDateKey ? raw.reportDateKey : "").trim())
      ? String(raw.reportDateKey).trim()
      : null,
    reportType: allowedReportTypes.has(String(raw && raw.reportType ? raw.reportType : "").trim())
      ? String(raw && raw.reportType ? raw.reportType : "").trim()
      : "",
    preferAiNarrative: raw && typeof raw.preferAiNarrative === "boolean" ? raw.preferAiNarrative : false,
    tradeQuery: String(raw && raw.tradeQuery ? raw.tradeQuery : "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 80),
    range: allowedRanges.has(String(raw && raw.range ? raw.range : "").trim())
      ? String(raw && raw.range ? raw.range : "").trim()
      : "",
    timerLabel: String(raw && raw.timerLabel ? raw.timerLabel : "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 120),
    todoText: String(raw && raw.todoText ? raw.todoText : "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 500),
    todoDueWindow: allowedTodoDueWindows.has(String(raw && raw.todoDueWindow ? raw.todoDueWindow : "").trim())
      ? String(raw && raw.todoDueWindow ? raw.todoDueWindow : "").trim()
      : "",
    notifyAudience: allowedNotifyAudiences.has(String(raw && raw.notifyAudience ? raw.notifyAudience : "").trim())
      ? String(raw && raw.notifyAudience ? raw.notifyAudience : "").trim()
      : "",
    notifyMessage: String(raw && raw.notifyMessage ? raw.notifyMessage : "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 480),
    proposedNotes: String(raw && raw.proposedNotes ? raw.proposedNotes : "")
      .replace(/\r\n/g, "\n")
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim()
      .slice(0, 8000),
    deficiency: {
      title: String(raw && raw.deficiency && raw.deficiency.title ? raw.deficiency.title : "").replace(/\s+/g, " ").trim().slice(0, 160),
      description: String(raw && raw.deficiency && raw.deficiency.description ? raw.deficiency.description : "").replace(/\s+/g, " ").trim().slice(0, 4000),
      location: String(raw && raw.deficiency && raw.deficiency.location ? raw.deficiency.location : "").replace(/\s+/g, " ").trim().slice(0, 240),
      area: String(raw && raw.deficiency && raw.deficiency.area ? raw.deficiency.area : "").replace(/\s+/g, " ").trim().slice(0, 240),
      trade: String(raw && raw.deficiency && raw.deficiency.trade ? raw.deficiency.trade : "").replace(/\s+/g, " ").trim().slice(0, 120),
      reference: String(raw && raw.deficiency && raw.deficiency.reference ? raw.deficiency.reference : "").replace(/\s+/g, " ").trim().slice(0, 160),
      requestedAction: String(raw && raw.deficiency && raw.deficiency.requestedAction ? raw.deficiency.requestedAction : "").replace(/\s+/g, " ").trim().slice(0, 500),
    },
  };
}

function looksLikeExplicitAiChatRequest(text) {
  const raw = String(text || "").trim();
  if (!raw) return false;
  const lower = raw.toLowerCase();
  if (raw.endsWith("?")) return true;
  if (/^what\s+i\s+(?:ate|had|did|bought|cooked|made|worked|saw|sent|took|wore|used|spent)\b/i.test(raw)) {
    return false;
  }
  if (
    /^(help|commands|ai check|openai check|status|contact|contacts|reset|reset context)$/i.test(raw)
  ) {
    return true;
  }
  if (
    /^(what|why|how|when|where|who|can you|could you|would you|should i|do i|is it|are we|tell me|explain)\b/i.test(raw)
  ) {
    return true;
  }
  if (["schedule", "today", "safety", "report", "issue"].includes(lower)) {
    return true;
  }
  if (/\b(advice|suggestion|recommend|brainstorm|analyze|analysis)\b/i.test(raw)) {
    return true;
  }
  if (lower === "photo attachment" || lower === "voice attachment" || lower === "video attachment" || lower === "media attachment") {
    return true;
  }
  return false;
}

function buildRoutingDecision(patch = {}) {
  return {
    stage: String(patch.stage || "").trim() || "unknown",
    action: String(patch.action || "").trim() || "unknown",
    confidence: Number.isFinite(Number(patch.confidence))
      ? Math.max(0, Math.min(1, Number(patch.confidence)))
      : 0,
    reason: String(patch.reason || "").replace(/\s+/g, " ").trim().slice(0, 280) || "",
    source: String(patch.source || "").trim() || "unknown",
    matchedBy: String(patch.matchedBy || "").trim() || "unknown",
    safeFallbackUsed: patch.safeFallbackUsed === true,
  };
}

function withRoutingDecision(outboundMeta, patch) {
  return {
    ...outboundMeta,
    routingDecision: buildRoutingDecision(patch),
  };
}

function logRoutingTelemetry(logger, runId, phoneE164, routingDecision, extra = {}) {
  if (!logger || !routingDecision) return;
  logger.info("assistant: routing decision", {
    runId,
    phoneE164,
    stage: routingDecision.stage,
    action: routingDecision.action,
    confidence: routingDecision.confidence,
    reason: routingDecision.reason || null,
    source: routingDecision.source,
    matchedBy: routingDecision.matchedBy,
    safeFallbackUsed: routingDecision.safeFallbackUsed === true,
    ...extra,
  });
}

function looksLikeNarrativeSaveCandidate(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw || raw.length < 20) return false;
  if (raw.endsWith("?")) return false;
  if (/^(?:please\s+)?(?:show|what|when|where|why|how|who|can\s+you|could\s+you|would\s+you|do|did|is|are|should|tell|read|give|list|summarize|summary|daily\s+report|daily\s+log|report|help|status|project|reset)\b/i.test(raw)) {
    return false;
  }
  if (/^(?:labour|labor|hours?|time)\b/i.test(raw)) return false;
  if (/\b(?:i|we|ashley|my|our|today|this\s+morning|after|before|went|did|bought|woke|felt|planning|worked)\b/i.test(raw)) {
    return true;
  }
  return /[.!]\s+[A-Z]/.test(raw) || raw.split(/\s+/).length >= 8;
}

function decideFallbackRouting(intentPayload, text, explicitAiRequest) {
  const intent = intentPayload && intentPayload.intent ? intentPayload.intent : "request";
  const confidence = Number(intentPayload && intentPayload.confidence) || 0;
  const reason = String(intentPayload && intentPayload.reason || "").trim();
  const narrative = looksLikeNarrativeSaveCandidate(text);

  if (explicitAiRequest) {
    return buildRoutingDecision({
      stage: "non_command",
      action: "ai_reply",
      confidence: 0.98,
      reason: "Explicit conversational request matched question/help heuristics.",
      source: "deterministic",
      matchedBy: "explicit_ai_request",
    });
  }

  if (intent !== "request") {
    return buildRoutingDecision({
      stage: "non_command",
      action: "save_log",
      confidence: confidence || 0.75,
      reason: reason || `Intent classifier chose ${intent}.`,
      source: intentPayload && intentPayload.source ? intentPayload.source : "unknown",
      matchedBy: `intent:${intent}`,
    });
  }

  if (narrative && confidence < 0.8) {
    return buildRoutingDecision({
      stage: "non_command",
      action: "save_log",
      confidence: 0.7,
      reason: "Low-confidence request classification on narrative prose; using safe fallback to save the note.",
      source: "safe_fallback",
      matchedBy: "narrative_save_candidate",
      safeFallbackUsed: true,
    });
  }

  return buildRoutingDecision({
    stage: "non_command",
    action: "ai_reply",
    confidence: confidence || 0.7,
    reason: reason || "Intent classifier kept the message on the conversational path.",
    source: intentPayload && intentPayload.source ? intentPayload.source : "unknown",
    matchedBy: `intent:${intent}`,
  });
}

function isExplicitLabourEntryText(text) {
  const raw = String(text || "").trim();
  if (!raw) return false;
  if (/^(?:labour|labor|hours?|time)\s*[:\-–—]?\s*\d+(?:\.\d+)?\b/i.test(raw)) return true;
  if (/^\d+(?:\.\d+)?\s*(?:hours?|hrs?|h)\b/i.test(raw)) return true;
  return false;
}

function isExplicitLabourBalanceText(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return false;
  if (/\b(?:labour|labor)\b/i.test(raw)) return true;
  if (/\bpay\s*period\b|\bpayroll\b|\bpay\s*report\b|\bmy\s+hours?\b|\bhours?\s+for\b/i.test(raw)) return true;
  if (/\bhow\s+many\s+hours?\b|\bwhat(?:'s|s| is)\s+my\s+(?:total\s+)?hours?\b/i.test(raw)) return true;
  return false;
}

function normalizePendingAssistantFollowUp(raw) {
  if (!raw || typeof raw !== "object") return null;
  const prompt = String(raw.prompt || "").replace(/\s+/g, " ").trim().slice(0, 500);
  if (!prompt) return null;
  return {
    prompt,
    projectSlug: normalizeProjectSlug(raw.projectSlug) || null,
    createdAtMs: Number(raw.createdAtMs || 0) || 0,
  };
}

function shouldTrackAssistantFollowUp(replyText) {
  const raw = String(replyText || "").trim();
  if (!raw) return false;
  if (/\?\s*$/.test(raw)) return true;
  if (/\breply\b/i.test(raw) && /\b(which|what|when|where|who|how|yes|no)\b/i.test(raw)) return true;
  return false;
}

function looksLikeAssistantFollowUpAnswer(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return false;
  if (raw.length > 120) return false;
  if (/^(?:yes|y|yeah|yep|no|nope|ok|okay|sure|do that|go ahead|continue|more|skip|none|n\/a|na|that one|this one)$/i.test(raw)) {
    return true;
  }
  if (/^(?:it'?s|its|the|for|on|in|at|use|make it|set it to)\b/i.test(raw)) return true;
  return false;
}

function isAffirmativeCorrectionFollowUp(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return false;
  return /^(?:yes|y|yeah|yep|sure|ok|okay)\b[\s,.-]*(?:correct|fix|change|update)\b/i.test(raw);
}

function looksLikeCorrectionPrompt(prompt) {
  const raw = String(prompt || "").replace(/\s+/g, " ").trim();
  if (!raw) return false;
  return /\b(correct|correction|fix|change|update)\b/i.test(raw);
}

function buildRecentCorrectionDateKeys(reportDateKey = null, todayKey = dateKeyEastern(new Date()), lookbackDays = CORRECTION_LOOKBACK_DAYS) {
  if (reportDateKey) return [reportDateKey];
  const days = Math.max(1, Math.min(31, Number(lookbackDays) || CORRECTION_LOOKBACK_DAYS));
  const out = [];
  for (let offset = 0; offset < days; offset += 1) {
    out.push(addCalendarDaysToDateKey(todayKey, -offset));
  }
  return out;
}

function escapeReplacementText(value) {
  return String(value || "").replace(/\$/g, "$$$$");
}

function parseNarrativeCorrectionCommand(text, options = {}) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return null;
  const allowShortNotForm = options.allowShortNotForm === true;

  const patterns = [
    {
      re: /^(?:correct|change|fix|update)\s+(.+?)\s+not\s+(.+)$/i,
      map: (m) => ({ replacement: m[1], target: m[2] }),
    },
    {
      re: /^(?:replace)\s+(.+?)\s+(?:with|to)\s+(.+)$/i,
      map: (m) => ({ target: m[1], replacement: m[2] }),
    },
    {
      re: /^(?:change|correct|update|fix)\s+(.+?)\s+(?:with|to)\s+(.+)$/i,
      map: (m) => ({ target: m[1], replacement: m[2] }),
    },
  ];

  for (const candidate of patterns) {
    const match = raw.match(candidate.re);
    if (!match) continue;
    const mapped = candidate.map(match);
    const replacement = String(mapped.replacement || "").trim().replace(/^["']|["']$/g, "");
    const target = String(mapped.target || "").trim().replace(/^["']|["']$/g, "");
    if (!replacement || !target) continue;
    if (replacement.toLowerCase() === target.toLowerCase()) continue;
    return {
      target,
      replacement,
      rawText: raw.slice(0, 500),
    };
  }

  if (allowShortNotForm) {
    const shortNotMatch = raw.match(/^(.+?)\s+not\s+(.+)$/i);
    if (shortNotMatch) {
      const replacement = String(shortNotMatch[1] || "").trim().replace(/^["']|["']$/g, "");
      const target = String(shortNotMatch[2] || "").trim().replace(/^["']|["']$/g, "");
      if (
        replacement &&
        target &&
        replacement.toLowerCase() !== target.toLowerCase() &&
        replacement.split(/\s+/).length <= 4 &&
        target.split(/\s+/).length <= 4
      ) {
        return {
          target,
          replacement,
          rawText: raw.slice(0, 500),
        };
      }
    }
  }
  return null;
}

function applyNarrativeCorrectionToEntry(entry, correction) {
  if (!entry || !correction) return null;
  const target = String(correction.target || "").trim();
  const replacement = String(correction.replacement || "").trim();
  if (!target || !replacement) return null;
  const targetRe = new RegExp(`\\b${escapeRegExp(target)}\\b`, "i");
  const replaceRe = new RegExp(`\\b${escapeRegExp(target)}\\b`, "gi");
  const rawText = String(entry.rawText || "");
  const normalizedText = String(entry.normalizedText || rawText || "");
  if (!targetRe.test(rawText) && !targetRe.test(normalizedText)) return null;

  const nextRaw = rawText.replace(replaceRe, escapeReplacementText(replacement));
  const nextNormalized = normalizedText.replace(replaceRe, escapeReplacementText(replacement));
  if (nextRaw === rawText && nextNormalized === normalizedText) return null;

  return {
    rawText: nextRaw,
    normalizedText: nextNormalized,
    target,
    replacement,
  };
}

function applyManpowerCorrectionToEntry(entry, correction) {
  if (!entry || !correction) return null;
  const nextRaw = replaceManpowerTradeCount(entry.rawText || "", correction.trade, correction.workers);
  const nextNormalized =
    replaceManpowerTradeCount(entry.normalizedText || "", correction.trade, correction.workers) ||
    (nextRaw ? replaceManpowerTradeCount(nextRaw, correction.trade, correction.workers) : null);
  if (!nextRaw && !nextNormalized) return null;

  const rawText = nextRaw || entry.rawText || "";
  const normalizedText = nextNormalized || nextRaw || entry.normalizedText || rawText;
  const existingTags = Array.isArray(entry.tags) ? entry.tags : [];
  const existingSections = Array.isArray(entry.dailySummarySections) ? entry.dailySummarySections : [];
  return {
    rawText,
    normalizedText,
    tags: [...new Set([...existingTags, "manpower"])],
    dailySummarySections: [...new Set([...existingSections, "manpower", "dayLog"])],
  };
}

async function applyLatestManpowerCorrectionForSenderProject({
  db,
  FieldValue,
  phoneE164,
  projectSlug,
  reportDateKey,
  correction,
}) {
  const tradeRe = new RegExp(`\\b${escapeRegExp(String(correction.trade || "").trim())}\\b\\s+\\d{1,3}\\b`, "i");
  const dateKeys = buildRecentCorrectionDateKeys(reportDateKey);
  for (const dateKey of dateKeys) {
    const rows = await loadLogEntriesForDayForProject(db, phoneE164, dateKey, projectSlug);
    const candidates = rows.filter((entry) => {
      const tags = Array.isArray(entry.tags) ? entry.tags : [];
      const text = `${entry.rawText || ""}\n${entry.normalizedText || ""}`;
      return (
        tags.includes("manpower") ||
        entry.logCategory === "manpower" ||
        /\bmanpower\b/i.test(text)
      ) && tradeRe.test(text);
    });
    const target = candidates.length ? candidates[candidates.length - 1] : null;
    if (!target || !target.id) continue;

    const updated = applyManpowerCorrectionToEntry(target, correction);
    if (!updated) continue;

    await db.collection("logEntries").doc(target.id).set(
      {
        rawText: updated.rawText,
        normalizedText: updated.normalizedText,
        tags: updated.tags,
        dailySummarySections: updated.dailySummarySections,
        aiEnhanced: false,
        aiError: null,
        aiReportExtract: null,
        summaryText: null,
        updatedAt: FieldValue.serverTimestamp(),
        editedAt: FieldValue.serverTimestamp(),
        editedByPhone: phoneE164,
      },
      { merge: true }
    );

    if (target.issueCollection && target.canonicalIssueId) {
      await updateSmsIssueBody(db, FieldValue, {
        issueCollection: target.issueCollection,
        issueId: target.canonicalIssueId,
        changedBy: phoneE164,
        title: makeTitleFromBody(updated.normalizedText),
        description: updated.normalizedText,
        tags: updated.tags,
      }).catch(() => null);
    }

    return {
      logEntryId: target.id,
      reportDateKey: String(target.reportDateKey || dateKey || reportDateKey || "").trim() || null,
      updatedText: updated.normalizedText,
    };
  }
  return null;
}

async function applyLatestNarrativeCorrectionForSenderProject({
  db,
  FieldValue,
  phoneE164,
  projectSlug,
  reportDateKey,
  correction,
}) {
  const dateKeys = buildRecentCorrectionDateKeys(reportDateKey);
  for (const dateKey of dateKeys) {
    const rows = await loadLogEntriesForDayForProject(db, phoneE164, dateKey, projectSlug);
    const candidates = rows.filter((entry) => {
      if (!entry || !entry.id) return false;
      const tags = Array.isArray(entry.tags) ? entry.tags : [];
      if (tags.includes("manpower")) return false;
      if (String(entry.subtype || "").trim() === "timer") return false;
      const text = `${entry.rawText || ""}\n${entry.normalizedText || ""}`;
      return new RegExp(`\\b${escapeRegExp(String(correction.target || "").trim())}\\b`, "i").test(text);
    });
    const target = candidates.length ? candidates[candidates.length - 1] : null;
    if (!target || !target.id) continue;

    const updated = applyNarrativeCorrectionToEntry(target, correction);
    if (!updated) continue;

    await db.collection("logEntries").doc(target.id).set(
      {
        rawText: updated.rawText,
        normalizedText: updated.normalizedText,
        aiEnhanced: false,
        aiError: null,
        aiReportExtract: null,
        summaryText: null,
        updatedAt: FieldValue.serverTimestamp(),
        editedAt: FieldValue.serverTimestamp(),
        editedByPhone: phoneE164,
        lastCorrection: {
          target: updated.target,
          replacement: updated.replacement,
          correctedByPhone: phoneE164,
          correctedAt: FieldValue.serverTimestamp(),
        },
      },
      { merge: true }
    );

    if (target.issueCollection && target.canonicalIssueId) {
      await updateSmsIssueBody(db, FieldValue, {
        issueCollection: target.issueCollection,
        issueId: target.canonicalIssueId,
        changedBy: phoneE164,
        title: makeTitleFromBody(updated.normalizedText),
        description: updated.normalizedText,
        tags: Array.isArray(target.tags) ? target.tags : [],
      }).catch(() => null);
    }

    return {
      logEntryId: target.id,
      reportDateKey: String(target.reportDateKey || dateKey || reportDateKey || "").trim() || null,
      updatedText: updated.normalizedText,
      target: updated.target,
      replacement: updated.replacement,
    };
  }
  return null;
}

async function savePendingAssistantFollowUp(db, phoneE164, prompt, projectSlug) {
  const text = String(prompt || "").trim();
  if (!text) return;
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingAssistantFollowUp: {
        prompt: text.slice(0, 500),
        projectSlug: normalizeProjectSlug(projectSlug) || null,
        createdAtMs: Date.now(),
      },
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

async function clearPendingAssistantFollowUp(db, phoneE164) {
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingAssistantFollowUp: FieldValue.delete(),
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

function inferJournalTags(text) {
  const raw = String(text || "").toLowerCase();
  const tags = ["journal", "personal_diary"];
  if (/\b(feel|feeling|mood|stressed|happy|tired|anxious|good|bad)\b/.test(raw)) tags.push("feeling");
  if (/\b(plan|today|going to|will|intend|focus)\b/.test(raw)) tags.push("plan");
  if (/\b(done|completed|finished|progress)\b/.test(raw) || /\bwork(?:ed)?\s+on\b/.test(raw)) {
    tags.push("activity");
  }
  return [...new Set(tags)];
}

function parseProjectNotesUpdateCommand(text) {
  const raw = String(text || "").trim();
  const match = raw.match(
    /^(?:update\s+project\s+notes|update\s+notes|project\s+notes|notes\s+update)\s*:\s*([\s\S]+)$/i
  );
  if (!match) return null;
  const proposedNotes = String(match[1] || "")
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim()
    .slice(0, 8000);
  return proposedNotes ? { proposedNotes } : null;
}

function parseHomeTodoCommand(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  const match = raw.match(/^(?:xxx|todo|add\s+todo|create\s+todo)\b[\s:,-]*([\s\S]*)$/i);
  if (!match) return null;
  const body = String(match[1] || "").replace(/\s+/g, " ").trim();
  if (/^report\b/i.test(body)) return null;
  if (!body) {
    return {
      error: 'After "xxx", add the task text. Example: xxx fix the garage door by next week',
    };
  }
  let dueWindow = null;
  let dueLabel = null;
  if (/\b(?:by|within|in)\s+next\s+week\b|\bin\s+the\s+next\s+week\b/i.test(body)) {
    dueWindow = "next_week";
    dueLabel = "next week";
  } else if (/\b(?:by|within|in)\s+next\s+month\b|\bin\s+the\s+next\s+month\b/i.test(body)) {
    dueWindow = "next_month";
    dueLabel = "next month";
  }
  let dueByIso = null;
  if (dueWindow) {
    const dueDate = new Date();
    if (dueWindow === "next_week") {
      dueDate.setDate(dueDate.getDate() + 7);
    } else if (dueWindow === "next_month") {
      dueDate.setMonth(dueDate.getMonth() + 1);
    }
    dueDate.setHours(17, 0, 0, 0);
    dueByIso = dueDate.toISOString();
  }
  const extractedTags = [];
  const tagStrippedBody = body.replace(/(^|\s)@([a-z0-9][a-z0-9._-]{0,39})\b/gi, (_, lead, tag) => {
    const clean = String(tag || "").trim().toLowerCase();
    if (clean && !extractedTags.includes(clean)) extractedTags.push(clean);
    return lead || " ";
  });
  const cleanedTask = tagStrippedBody
    .replace(/\b(?:by|within|in)\s+next\s+week\b/gi, "")
    .replace(/\bin\s+the\s+next\s+week\b/gi, "")
    .replace(/\b(?:by|within|in)\s+next\s+month\b/gi, "")
    .replace(/\bin\s+the\s+next\s+month\b/gi, "")
    .replace(/\s+/g, " ")
    .replace(/[,:;.\-]+$/g, "")
    .trim();
  const taskText = cleanedTask || body;
  return {
    projectSlug: HOME_TODO_PROJECT_SLUG,
    taskText: taskText.slice(0, 500),
    dueWindow,
    dueLabel,
    dueByIso,
    tags: extractedTags,
    rawText: body.slice(0, 1000),
  };
}

function parseTodoReportRequest(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return null;
  const match = raw.match(
    /^(?:(?:please|can\s+you|could\s+you|i\s+need|i\s+want|get\s+me|send\s+me|text\s+me)\s+)?todo\s+report\s+(pdf|excel|xlsx)\b/i
  );
  if (!match) return null;
  const format = /pdf/i.test(match[1]) ? "pdf" : "xlsx";
  return {
    projectSlug: HOME_TODO_PROJECT_SLUG,
    format,
  };
}

function parseTodoListRequest(text, fallbackProjectSlug = null) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return null;
  if (!/\btodo(?:'s|s)?\b/i.test(raw)) return null;
  if (/^(?:xxx|todo|add\s+todo|create\s+todo)\b/i.test(raw)) return null;
  if (/^(?:(?:please|can\s+you|could\s+you|i\s+need|i\s+want|get\s+me|send\s+me|text\s+me)\s+)?todo\s+report\b/i.test(raw)) {
    return null;
  }

  const looksLikeListRequest =
    /^(?:(?:please\s+)?(?:showm?|shwo|list|read|give|text|send)\s+me|what(?:'s|\s+are)?|which|my\b|all\b|open\b|completed\b|done\b|in[\s-]?progress\b)/i.test(
      raw
    );
  if (!looksLikeListRequest) return null;

  const tagMatches = [...raw.matchAll(/(?:^|\s|["'])@([a-z0-9][a-z0-9._-]{0,39})\b/gi)];
  const tags = normalizeTodoTagsValue(tagMatches.map((match) => match[1]));
  let status = "active";
  if (/\b(?:completed|done|finished|closed)\b/i.test(raw)) status = "completed";
  else if (/\bin[\s-]?progress\b/i.test(raw)) status = "inprogress";
  else if (/\bopen\b/i.test(raw)) status = "open";
  const priorityMatch = raw.match(/\b(p[1-4])\b/i);
  const priority = priorityMatch ? String(priorityMatch[1] || "").trim().toLowerCase() : "";
  let projectSlug = null;
  const projectMatch = raw.match(/\b(?:for|on)\s+(this project|[a-z0-9][a-z0-9-_]{1,79})\b/i);
  if (projectMatch) {
    const requestedProject = String(projectMatch[1] || "").trim().toLowerCase();
    projectSlug =
      requestedProject === "this project"
        ? normalizeProjectSlug(fallbackProjectSlug)
        : normalizeProjectSlug(requestedProject);
  }

  return {
    projectSlug: projectSlug || null,
    status,
    priority,
    tags,
    mineOnly: true,
  };
}

function parseTodoMutationRequest(text, fallbackProjectSlug = null) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return null;

  const closeMatch = raw.match(/^(?:close|complete|finish|done)\s+todo\s+(.+)$/i);
  if (closeMatch) {
    return {
      action: "completed",
      targetText: String(closeMatch[1] || "").trim(),
      nextTaskText: "",
      projectSlug: extractTodoMutationProjectSlug(raw, fallbackProjectSlug),
    };
  }

  const reopenMatch = raw.match(/^(?:reopen|open)\s+todo\s+(.+)$/i);
  if (reopenMatch) {
    return {
      action: "open",
      targetText: String(reopenMatch[1] || "").trim(),
      nextTaskText: "",
      projectSlug: extractTodoMutationProjectSlug(raw, fallbackProjectSlug),
    };
  }

  const progressMatch = raw.match(/^(?:start|resume)\s+todo\s+(.+)$/i);
  if (progressMatch) {
    return {
      action: "inprogress",
      targetText: String(progressMatch[1] || "").trim(),
      nextTaskText: "",
      projectSlug: extractTodoMutationProjectSlug(raw, fallbackProjectSlug),
    };
  }

  const markMatch = raw.match(
    /^mark\s+todo\s+(.+?)\s+(?:as\s+)?(open|reopen(?:ed)?|in[\s-]?progress|start(?:ed)?|resume(?:d)?|complete(?:d)?|closed?|done|finish(?:ed)?)$/i
  );
  if (markMatch) {
    const rawStatus = String(markMatch[2] || "").trim().toLowerCase();
    const action =
      /open|reopen/.test(rawStatus)
        ? "open"
        : /progress|start|resume/.test(rawStatus)
          ? "inprogress"
          : "completed";
    return {
      action,
      targetText: String(markMatch[1] || "").trim(),
      nextTaskText: "",
      projectSlug: extractTodoMutationProjectSlug(raw, fallbackProjectSlug),
    };
  }

  const editMatch = raw.match(/^(?:edit|rename|change|update)\s+todo\s+(.+?)\s+(?:to|as)\s+(.+)$/i);
  if (editMatch) {
    return {
      action: "edit",
      targetText: String(editMatch[1] || "").trim(),
      nextTaskText: String(editMatch[2] || "").trim(),
      projectSlug: extractTodoMutationProjectSlug(raw, fallbackProjectSlug),
    };
  }

  return null;
}

function extractTodoMutationProjectSlug(text, fallbackProjectSlug = null) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  const projectMatch = raw.match(/\b(?:for|on)\s+(this project|[a-z0-9][a-z0-9-_]{1,79})\s*$/i);
  if (!projectMatch) return null;
  const requestedProject = String(projectMatch[1] || "").trim().toLowerCase();
  return requestedProject === "this project"
    ? normalizeProjectSlug(fallbackProjectSlug)
    : normalizeProjectSlug(requestedProject);
}

function stripTrailingTodoProjectPhrase(text) {
  return String(text || "")
    .replace(/\s+\b(?:for|on)\s+(?:this project|[a-z0-9][a-z0-9-_]{1,79})\s*$/i, "")
    .trim();
}

const TODO_NONE_RE = /^(?:n\/a|na|none|no|skip|nope|nil)$/i;
const TODO_YES_RE = /^(?:y|yes|yeah|yep|sure|ok|okay)$/i;
const TODO_NO_RE = /^(?:n|no|nope|nah|skip|none)$/i;
const TODO_PRIORITY_RE = /^(?:p[1-4]|none)$/i;

function normalizeTodoTextValue(value, maxLength = 500) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function normalizeTodoPriorityValue(value) {
  if (value == null || value === "") return null;
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "none") return null;
  return /^p[1-4]$/.test(raw) ? raw : "";
}

function normalizeTodoTagsValue(value) {
  const source = Array.isArray(value)
    ? value
    : String(value || "")
        .split(/[,\s]+/);
  const out = [];
  for (const item of source) {
    const clean = String(item || "")
      .trim()
      .replace(/^@+/, "")
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 40);
    if (clean && !out.includes(clean)) out.push(clean);
    if (out.length >= 20) break;
  }
  return out;
}

function parseTodoDateTimeInput(value) {
  if (value == null || value === "") return "";
  const raw = String(value || "").trim();
  if (TODO_NONE_RE.test(raw)) return null;
  if (/^\d{4}-\d{2}-\d{2}t\d{2}:\d{2}:\d{2}(?:\.\d{3})?z$/i.test(raw)) {
    const iso = new Date(raw).toISOString();
    return Number.isFinite(new Date(iso).getTime()) ? iso : "";
  }
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{1,2})(?::(\d{2}))?\s*(am|pm)?)?$/i);
  if (!match) return "";
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  let hour = match[4] != null ? Number(match[4]) : 17;
  const minute = match[5] != null ? Number(match[5]) : 0;
  const meridiem = String(match[6] || "").trim().toLowerCase();
  if (meridiem === "pm" && hour < 12) hour += 12;
  if (meridiem === "am" && hour === 12) hour = 0;
  if (
    !Number.isInteger(year) ||
    !Number.isInteger(month) ||
    !Number.isInteger(day) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    !Number.isInteger(hour) ||
    hour < 0 ||
    hour > 23 ||
    !Number.isInteger(minute) ||
    minute < 0 ||
    minute > 59
  ) {
    return "";
  }
  const iso = new Date(year, month - 1, day, hour, minute, 0, 0).toISOString();
  return Number.isFinite(new Date(iso).getTime()) ? iso : "";
}

function coerceAssistantDate(value) {
  if (!value) return null;
  try {
    if (typeof value.toDate === "function") return value.toDate();
    if (value.seconds) return new Date(value.seconds * 1000);
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) return date;
  } catch (_) {}
  return null;
}

function normalizeTodoStatusForList(value) {
  const raw = String(value || "open").trim().toLowerCase();
  if (raw === "completed") return "completed";
  if (raw === "inprogress") return "inprogress";
  return "open";
}

function matchesTodoListStatus(todo, statusFilter) {
  const status = normalizeTodoStatusForList(todo.status);
  if (statusFilter === "completed") return status === "completed";
  if (statusFilter === "inprogress") return status === "inprogress";
  if (statusFilter === "open") return status === "open";
  return status !== "completed";
}

function compareTodoListPriority(a, b) {
  const rank = { p1: 1, p2: 2, p3: 3, p4: 4 };
  const left = rank[String(a || "").trim().toLowerCase()] || 99;
  const right = rank[String(b || "").trim().toLowerCase()] || 99;
  return left - right;
}

function compareTodoListItems(a, b) {
  const priorityOrder = compareTodoListPriority(a.priority, b.priority);
  if (priorityOrder !== 0) return priorityOrder;
  const dueA = coerceAssistantDate(a.dueBy);
  const dueB = coerceAssistantDate(b.dueBy);
  if (dueA && dueB && dueA.getTime() !== dueB.getTime()) return dueA.getTime() - dueB.getTime();
  if (dueA && !dueB) return -1;
  if (!dueA && dueB) return 1;
  const updatedA = coerceAssistantDate(a.updatedAt) || coerceAssistantDate(a.createdAt);
  const updatedB = coerceAssistantDate(b.updatedAt) || coerceAssistantDate(b.createdAt);
  if (updatedA && updatedB && updatedA.getTime() !== updatedB.getTime()) return updatedB.getTime() - updatedA.getTime();
  return String(a.taskText || "").localeCompare(String(b.taskText || ""));
}

function formatTodoListDate(value) {
  const date = coerceAssistantDate(value);
  if (!date) return "";
  return date.toISOString().slice(0, 10);
}

function formatTodoListReply({ todos, request }) {
  const safeTodos = Array.isArray(todos) ? todos : [];
  const tagText = Array.isArray(request?.tags) && request.tags.length
    ? ` with ${request.tags.map((tag) => `@${tag}`).join(" ")}`
    : "";
  const projectText = request?.projectSlug ? ` for ${request.projectSlug}` : "";
  const priorityText = request?.priority ? ` at ${String(request.priority).toUpperCase()}` : "";
  const scopeLabel =
    request?.status === "completed"
      ? "completed"
      : request?.status === "inprogress"
        ? "in-progress"
        : request?.status === "open"
          ? "open"
          : "active";

  if (!safeTodos.length) {
    return `No ${scopeLabel} todos found for you${projectText}${tagText}${priorityText}.`;
  }

  const prefix = `Your ${scopeLabel} todos${projectText}${tagText}${priorityText} (${safeTodos.length}): `;
  let text = prefix;
  let shown = 0;
  for (let index = 0; index < safeTodos.length; index += 1) {
    const todo = safeTodos[index] || {};
    const parts = [`${shown + 1}) ${String(todo.taskText || "").trim()}`];
    const status = normalizeTodoStatusForList(todo.status);
    if (status === "inprogress") parts.push("[in progress]");
    if (todo.priority) parts.push(`[${String(todo.priority).toLowerCase()}]`);
    const tags = normalizeTodoTagsValue(todo.tags || []);
    if (tags.length) parts.push(tags.map((tag) => `@${tag}`).join(" "));
    if (!request?.projectSlug && todo.projectSlug) parts.push(`(${todo.projectSlug})`);
    const dueText = formatTodoListDate(todo.dueBy);
    if (dueText) parts.push(`due ${dueText}`);
    const itemText = parts.join(" ");
    const candidate = shown === 0 ? `${text}${itemText}` : `${text}; ${itemText}`;
    const remaining = safeTodos.length - (shown + 1);
    const suffix = remaining > 0 ? `; +${remaining} more` : "";
    if (candidate.length + suffix.length > MAX_SMS_CHARS) break;
    text = candidate;
    shown += 1;
  }

  if (shown < safeTodos.length) {
    text += `; +${safeTodos.length - shown} more`;
  }
  return text;
}

function normalizeTodoMatchText(value) {
  return String(value || "")
    .trim()
    .replace(/^["']|["']$/g, "")
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function scoreTodoMatch(todo, targetText) {
  const task = normalizeTodoMatchText(todo?.taskText);
  const target = normalizeTodoMatchText(stripTrailingTodoProjectPhrase(targetText));
  if (!task || !target) return 0;
  if (task === target) return 100;
  if (task.startsWith(target)) return 80;
  if (task.includes(target)) return 60;
  if (target.includes(task) && task.length >= 8) return 50;
  return 0;
}

function findBestTodoMatches(todos, targetText, maxResults = 3) {
  const scored = (Array.isArray(todos) ? todos : [])
    .map((todo) => ({ todo, score: scoreTodoMatch(todo, targetText) }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return compareTodoListItems(a.todo, b.todo);
    });
  const best = scored[0] || null;
  const topScore = best ? best.score : 0;
  const tiedBest = scored.filter((entry) => entry.score === topScore).map((entry) => entry.todo);
  return {
    best: tiedBest.length === 1 ? tiedBest[0] : null,
    ambiguous: tiedBest.length > 1 ? tiedBest.slice(0, maxResults) : [],
    suggestions: scored.slice(0, maxResults).map((entry) => entry.todo),
  };
}

function formatTodoMutationAmbiguousReply(targetText, suggestions) {
  const safe = (Array.isArray(suggestions) ? suggestions : []).slice(0, 3);
  if (!safe.length) {
    return `Could not find a todo matching "${stripTrailingTodoProjectPhrase(targetText)}".`;
  }
  return truncateSms(
    `More than one todo matches "${stripTrailingTodoProjectPhrase(targetText)}". Try again with more detail: ${safe
      .map((todo) => `"${String(todo.taskText || "").trim()}"`)
      .join("; ")}`
  );
}

function normalizePendingTodoDraft(raw) {
  if (!raw || typeof raw !== "object") return null;
  const normalizedDueBy =
    raw.dueBy === null && raw.dueDateCaptured === true
      ? null
      : parseTodoDateTimeInput(raw.dueBy);
  const reminders = Array.isArray(raw.reminders)
    ? raw.reminders.map((value) => parseTodoDateTimeInput(value)).filter((value) => typeof value === "string" && value)
    : [];
  return {
    projectSlug: normalizeProjectSlug(raw.projectSlug) || HOME_TODO_PROJECT_SLUG,
    projectName: normalizeTodoTextValue(raw.projectName, 120),
    taskText: normalizeTodoTextValue(raw.taskText, 500),
    sourceText: normalizeTodoTextValue(raw.sourceText, 1000),
    dueBy: normalizedDueBy,
    dueDateCaptured: raw.dueDateCaptured === true,
    reminderRequested: raw.reminderRequested === true ? true : raw.reminderRequested === false ? false : null,
    secondReminderWanted: raw.secondReminderWanted === true ? true : raw.secondReminderWanted === false ? false : null,
    reminders,
    priority: normalizeTodoPriorityValue(raw.priority),
    priorityCaptured: raw.priorityCaptured === true,
    tags: normalizeTodoTagsValue(raw.tags),
    tagsCaptured: raw.tagsCaptured === true,
    sourceMessageId: String(raw.sourceMessageId || "").trim() || null,
  };
}

function looksLikeTodoDateAnswer(text) {
  const raw = String(text || "").trim();
  if (!raw) return false;
  if (parseTodoDateTimeInput(raw) !== "") return true;
  const wordCount = raw.split(/\s+/).filter(Boolean).length;
  if (wordCount > 4) return false;
  return /\b(today|tomorrow|tmrw|next|monday|tuesday|wednesday|thursday|friday|saturday|sunday|am|pm)\b/i.test(raw);
}

function looksLikePendingTodoAnswer(draft, trimmedBody) {
  const body = String(trimmedBody || "").trim();
  if (!body) return false;
  if (body.toLowerCase() === "cancel" || body.toLowerCase() === "cancel todo") return true;
  if (body.toLowerCase() === "status" || body.toLowerCase() === "todo status") return true;
  const nextMissing = getNextMissingTodoField(draft);
  if (!nextMissing) return false;
  if (nextMissing === "dueBy" || nextMissing === "firstReminder" || nextMissing === "secondReminder") {
    return looksLikeTodoDateAnswer(body);
  }
  if (nextMissing === "reminderRequested" || nextMissing === "secondReminderWanted") {
    return TODO_YES_RE.test(body) || TODO_NO_RE.test(body);
  }
  if (nextMissing === "priority") {
    return TODO_PRIORITY_RE.test(body);
  }
  if (nextMissing === "tags") {
    return TODO_NONE_RE.test(body) || /@/.test(body) || /^[a-z0-9._-]+(?:[,\s]+[a-z0-9._-]+){0,5}$/i.test(body);
  }
  return false;
}

function getNextMissingTodoField(draft) {
  if (!draft.taskText) return "taskText";
  if (!draft.dueDateCaptured || draft.dueBy === "") return "dueBy";
  if (typeof draft.reminderRequested !== "boolean") return "reminderRequested";
  if (draft.reminderRequested && draft.reminders.length < 1) return "firstReminder";
  if (draft.reminderRequested && draft.secondReminderWanted === true && draft.reminders.length < 2) {
    return "secondReminder";
  }
  if (draft.reminderRequested && draft.reminders.length === 1 && draft.secondReminderWanted !== false) {
    return "secondReminderWanted";
  }
  if (!draft.priorityCaptured || draft.priority === "") return "priority";
  if (!draft.tagsCaptured || !Array.isArray(draft.tags)) return "tags";
  return null;
}

function todoFieldPrompt(field, draft) {
  if (field === "dueBy") {
    return `Todo noted${draft.taskText ? `: ${draft.taskText}.` : "."} What is the due date? Reply YYYY-MM-DD or YYYY-MM-DD HH:MM. Reply none if no due date.`;
  }
  if (field === "reminderRequested") {
    return "Do you need a reminder? Reply yes or no.";
  }
  if (field === "firstReminder") {
    return "When should I send the first reminder? Reply YYYY-MM-DD HH:MM.";
  }
  if (field === "secondReminderWanted") {
    return "Do you want a second reminder? Reply yes or no.";
  }
  if (field === "secondReminder") {
    return "When should I send the second reminder? Reply YYYY-MM-DD HH:MM.";
  }
  if (field === "priority") {
    return "What priority should I use? Reply p1, p2, p3, p4, or none.";
  }
  if (field === "tags") {
    return 'Any tags? Reply like "@home @calls" or reply none.';
  }
  return "Send the next todo detail.";
}

function parseLabourEntryCommand(text) {
  return parseLabourHoursCommand(text);
}

function formatLabourHoursShort(value) {
  return Math.round((Number(value) || 0) * 100) / 100;
}

function formatInvalidLabourDateReply(labourerName, validation) {
  const who = String(labourerName || "Your hours").trim() || "Your hours";
  const badDate = validation && validation.reportDateKey ? validation.reportDateKey : "that date";
  const suggestion = validation && validation.suggestedDateKey
    ? ` Did you mean ${validation.suggestedDateKey}? Reply with the corrected date and hours, for example: ${validation.suggestedDateKey} 8 hours forming.`
    : " Reply with the corrected date as YYYY-MM-DD and your hours.";
  return `${who}: I did not save the hours because ${badDate} does not look like a valid work date.${suggestion}`;
}

function resolveLabourCorrectionDateKey(text, now = new Date()) {
  const resolved = extractExplicitReportDate(text, now);
  return resolved && resolved.reportDateKey ? resolved.reportDateKey : null;
}

function sanitizeLabourCorrectionText(text) {
  return String(text || "")
    .replace(/\b(?:i\s+made\s+a\s+mistake|made\s+a\s+mistake)\b/gi, " ")
    .replace(/\b(?:please|pls|kindly)\b/gi, " ")
    .replace(/\b(?:can\s+you|could\s+you|would\s+you)\b/gi, " ")
    .replace(/\b(?:correct|correction|change|fix|update|wrong)\b/gi, " ")
    .replace(/\b(?:today|yesterday)\b/gi, " ")
    .replace(
      /\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s*\d{4})?\b/gi,
      " "
    )
    .replace(/\(\s*\d{4}-\d{2}(?:-?\d{2,3})\s*\)/gi, " ")
    .replace(/\b(?:for|on|dated|date)\s+\d{4}-\d{2}(?:-?\d{2,3})\b/gi, " ")
    .replace(/\b\d{4}-\d{2}(?:-?\d{2,3})\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseLabourCorrectionCommand(text, now = new Date()) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return null;
  const hasCorrectionCue =
    /\b(?:made\s+a\s+mistake|mistake|wrong|correct|correction|change|fix|update)\b/i.test(raw);
  if (!hasCorrectionCue) return null;

  const reportDateKey = resolveLabourCorrectionDateKey(raw, now) || dateKeyEastern(now);
  const cleaned = sanitizeLabourCorrectionText(raw);
  const replacementEntry = parseLabourHoursCommand(cleaned);

  const targetHoursPatterns = [
    /\b(?:should\s+be|should've\s+been|should\s+have\s+been|should\s+of\s+been|total\s+to|hours?\s+to|to|is|be|make\s+it)\s+(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)?\b/i,
    /\b(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)\b\s*$/i,
  ];
  let hours = replacementEntry && Number.isFinite(replacementEntry.hours) ? replacementEntry.hours : null;
  for (const re of targetHoursPatterns) {
    const match = raw.match(re);
    if (!match) continue;
    const parsed = Number(match[1]);
    if (Number.isFinite(parsed) && parsed > 0) {
      hours = parsed;
    }
  }

  if (!Number.isFinite(hours) || hours <= 0) return null;
  return {
    hours: Math.round(hours * 100) / 100,
    reportDateKey,
    workOn: replacementEntry && replacementEntry.workOn ? replacementEntry.workOn : null,
    rawText: raw,
  };
}

function parseStartTimerCommand(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  const match = raw.match(
    /^start\s+timer(?:\s+(?:for|on)\s+|\s*[:\-–—]\s*|\s+)?(.+)?$/i
  );
  if (!match) return null;
  const label = String(match[1] || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 160);
  return { label: label || "general task" };
}

function isStopTimerCommand(text) {
  return /^stop\s+timer(?:\b|$)/i.test(String(text || "").trim());
}

function formatDurationFromMs(durationMs) {
  const safeMs = Math.max(0, Number(durationMs) || 0);
  const totalMinutes = Math.round(safeMs / 60000);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours > 0 && minutes > 0) return `${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h`;
  return `${minutes}m`;
}

function parseNotificationRequest(text, fallbackProjectSlug) {
  const raw = String(text || "").trim();
  if (!raw) return null;

  const managementMatch = raw.match(
    /^(?:please\s+)?(?:can\s+you\s+)?(?:inform|notify|text|message)\s+management\b(?:\s+(?:that|about)\s+)?([\s\S]+)$/i
  );
  if (managementMatch) {
    const message = String(managementMatch[1] || "").replace(/\s+/g, " ").trim().slice(0, 480);
    if (!message) return null;
    return { audience: "management", messageBody: message, projectSlug: null };
  }

  const allUsersMatch = raw.match(
    /^(?:please\s+)?(?:can\s+you\s+)?(?:inform|notify|text|message)\s+all\s+users(?:\s+(?:on|for|in)\s+)(this project|[a-z0-9][a-z0-9-_]{1,79})(?:\s+(?:that|about)\s+)?([\s\S]+)$/i
  );
  if (allUsersMatch) {
    const requestedProject = String(allUsersMatch[1] || "").trim().toLowerCase();
    const projectSlug = requestedProject === "this project"
      ? normalizeProjectSlug(fallbackProjectSlug)
      : normalizeProjectSlug(requestedProject);
    const message = String(allUsersMatch[2] || "").replace(/\s+/g, " ").trim().slice(0, 480);
    if (!projectSlug || !message) return null;
    return { audience: "project_users", messageBody: message, projectSlug };
  }

  return null;
}

async function getAdminSettings(db) {
  const snap = await db.collection(COL_ADMIN).doc(ADMIN_DOC_ID).get();
  if (!snap.exists) {
    return {
      companyStandards: "",
      responseStyle: "",
      approvedTerminology: "",
      reportingPreferences: "",
      escalationRules: "",
    };
  }
  const d = snap.data() || {};
  return {
    companyStandards: d.companyStandards || "",
    responseStyle: d.responseStyle || "",
    approvedTerminology: d.approvedTerminology || "",
    reportingPreferences: d.reportingPreferences || "",
    escalationRules: d.escalationRules || "",
  };
}

async function getProject(db, slug) {
  if (!slug) return null;
  const snap = await db.collection(COL_PROJECTS).doc(slug).get();
  if (!snap.exists) return null;
  return { slug, id: snap.id, ...snap.data() };
}

async function getOrCreateUser(db, phoneE164) {
  const ref = db.collection(COL_USERS).doc(phoneE164);
  const snap = await ref.get();
  const now = FieldValue.serverTimestamp();
  if (!snap.exists) {
    await ref.set({
      phoneE164,
      role: null,
      displayName: null,
      activeProjectSlug: null,
      projectSlugs: [],
      contextResetAt: null,
      pendingTimer: null,
      createdAt: now,
      updatedAt: now,
      lastInboundAt: now,
    });
    return {
      phoneE164,
      role: null,
      displayName: null,
      activeProjectSlug: null,
      projectSlugs: [],
      contextResetAt: null,
      pendingDeficiencyIntake: null,
      pendingTodoIntake: null,
      pendingAssistantFollowUp: null,
      pendingTimer: null,
    };
  }
  const d = snap.data() || {};
  const patch = buildUserProjectPatch(d, null);
  await ref.set({
    ...patch,
    lastInboundAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  }, { merge: true });
  return {
    phoneE164,
    role: d.role || null,
    displayName: d.displayName || null,
    activeProjectSlug: normalizeProjectSlug(d.activeProjectSlug) || null,
    projectSlugs: getUserProjectSlugs(d),
    contextResetAt: d.contextResetAt || null,
    pendingDeficiencyIntake: d.pendingDeficiencyIntake || null,
    pendingTodoIntake: d.pendingTodoIntake || null,
    pendingAssistantFollowUp: normalizePendingAssistantFollowUp(d.pendingAssistantFollowUp),
    pendingTimer: d.pendingTimer || null,
  };
}

function matchesProjectScope(recordProjectSlug, projectSlug) {
  return (normalizeProjectSlug(recordProjectSlug) || null) ===
    (normalizeProjectSlug(projectSlug) || null);
}

async function loadThreadMessages(db, threadKey, contextResetAt, projectSlug) {
  let q = db
    .collection("messages")
    .where("threadKey", "==", threadKey)
    .orderBy("createdAt", "desc")
    .limit(HISTORY_LIMIT);
  const snap = await q.get();
  let rows = snap.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
  if (contextResetAt && contextResetAt.toMillis) {
    const cut = contextResetAt.toMillis();
    rows = rows.filter((r) => {
      const c = r.createdAt;
      if (!c || !c.toMillis) return true;
      return c.toMillis() > cut;
    });
  }
  if (projectSlug !== undefined) {
    rows = rows.filter((r) => matchesProjectScope(r.projectSlug, projectSlug));
  }
  rows.reverse();
  return rows;
}

function rowsToOpenAIMessages(rows) {
  const out = [];
  for (const r of rows) {
    if (r.direction === "inbound" && r.body) {
      out.push({ role: "user", content: String(r.body) });
    } else if (r.direction === "outbound" && r.body) {
      out.push({ role: "assistant", content: String(r.body) });
    }
  }
  return out.slice(-16);
}

function buildLayeredSystemPrompt(admin, project, user) {
  const parts = [BASE_CONSTRUCTION_SYSTEM];
  if (admin.companyStandards) parts.push("Company standards:\n" + admin.companyStandards);
  if (admin.responseStyle) parts.push("Response style:\n" + admin.responseStyle);
  if (admin.approvedTerminology) parts.push("Approved terminology:\n" + admin.approvedTerminology);
  if (admin.reportingPreferences) parts.push("Reporting preferences:\n" + admin.reportingPreferences);
  if (admin.escalationRules) parts.push("Escalation rules:\n" + admin.escalationRules);
  if (project) {
    const block = [];
    if (project.name) block.push("Project name: " + project.name);
    if (project.instructionText) block.push("Project instructions:\n" + project.instructionText);
    if (project.contactsText) block.push("Key contacts:\n" + project.contactsText);
    if (project.scheduleNotes) block.push("Schedule / lookahead notes:\n" + project.scheduleNotes);
    if (project.faqText) block.push("Project FAQs:\n" + project.faqText);
    if (project.notes) block.push("Additional notes:\n" + project.notes);
    if (block.length) parts.push("Active project context:\n" + block.join("\n\n"));
  } else {
    parts.push(
      "No active project is assigned for this user. Encourage them to text: project <slug> (e.g. project docksteader) if your org uses project codes."
    );
  }
  if (user.role) parts.push("User role on file: " + user.role + ". Tailor depth accordingly.");
  parts.push(
    "Commands: help, status, start timer [for task], stop timer, daily log / daily summary, daily report (PDF), project <slug>, reset, contact, update project notes:, labour hours:. Personal diary updates are auto-saved to journal unless you are explicitly asking a question. Log with: log safety:, log delay:, log deficiency:, log issue:, log delivery:, log note:, log progress:, log inspection:, or shorthand (e.g. safety icy stairs, punch broken tile). Labourers can text hours and the work they did, like: labour 8.0 framing cleanup. They can ask: how many hours today, this week, or this pay period. Users can text photos (MMS) to this number—every photo is stored and linked automatically. Users can also call this number and press 1 to leave a recorded voice message."
  );
  return parts.join("\n\n---\n\n");
}

async function callOpenAI(openaiApiKey, system, historyMessages, latestUserText, logger, runId, modelsOverride) {
  const client = new OpenAI({ apiKey: openaiApiKey });
  const messages = [
    { role: "system", content: system },
    ...historyMessages,
    { role: "user", content: latestUserText },
  ];
  const completion = await chatCompletionWithFallback(
    client,
    {
      messages,
      max_completion_tokens: 500,
      temperature: 0.35,
    },
    logger,
    runId,
    modelsOverride
  );
  const raw = completionText(completion);
  logger.info("assistant: openai ok", {
    runId,
    model: completion.model,
    usage: completion.usage,
  });
  return raw.trim();
}

const HELP_TEXT =
  "Commands: help — ai check — status — start timer [for task] — stop timer — project <name> — reset — contact — daily log / daily summary — daily report (PDF) — update project notes:. Personal diary entries auto-save to journal unless you send an explicit AI question. Labourers can text hours (labour 8.0 framing cleanup) and ask for totals (e.g. how many hours this week). Log: log safety:, log delay:, log deficiency:, log issue:, log delivery:, log inspection:, log note:, log progress:, daily log: … — or shorthand (safety …, delay …, punch …). Every MMS photo is saved and linked. You can also call this number and press 1 to leave a recorded voice message.";

function parseProjectCommand(text) {
  const m = text.trim().match(/^project\s+(\S+)/i);
  return m ? m[1].toLowerCase() : null;
}

function isExplicitProjectSetRequest(text, projectSlug = "") {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return false;
  const slug = normalizeProjectSlug(projectSlug);
  if (/^project\s+\S+$/i.test(raw)) return true;
  if (/^(?:switch|change|set|use|select)\s+(?:the\s+)?project\b/i.test(raw)) {
    return !slug || new RegExp(`\\b${escapeRegExp(slug)}\\b`, "i").test(raw);
  }
  if (slug) {
    if (new RegExp(`^(?:project\\s+${escapeRegExp(slug)}|${escapeRegExp(slug)}\\s+project)$`, "i").test(raw)) {
      return true;
    }
    if (
      new RegExp(
        `^(?:switch|change|set|use|select)(?:\\s+(?:the\\s+)?)?project(?:\\s+(?:to|as))?\\s+${escapeRegExp(slug)}$`,
        "i"
      ).test(raw)
    ) {
      return true;
    }
  }
  return false;
}

const DEFICIENCY_NONE_RE = /^(?:n\/a|na|none|no reference|unknown|not sure|skip)$/i;

function normalizeDeficiencyTextValue(value, fallback = "") {
  return String(value || fallback || "").trim();
}

function normalizePendingDeficiencyDraft(raw) {
  if (!raw || typeof raw !== "object") return null;
  return {
    projectSlug: normalizeProjectSlug(raw.projectSlug) || null,
    projectName: normalizeDeficiencyTextValue(raw.projectName),
    title: normalizeDeficiencyTextValue(raw.title),
    description: normalizeDeficiencyTextValue(raw.description),
    location: normalizeDeficiencyTextValue(raw.location),
    area: normalizeDeficiencyTextValue(raw.area),
    trade: normalizeDeficiencyTextValue(raw.trade),
    reference: normalizeDeficiencyTextValue(raw.reference),
    requestedAction: normalizeDeficiencyTextValue(raw.requestedAction),
    sourceMessageIds: [...new Set((raw.sourceMessageIds || []).map((v) => String(v || "").trim()).filter(Boolean))],
  };
}

function getNextMissingDeficiencyField(draft) {
  if (!draft.projectSlug) return "projectSlug";
  if (!draft.title) return "title";
  if (!draft.description) return "description";
  if (!draft.location && !draft.area) return "locationArea";
  if (!draft.trade) return "trade";
  if (!draft.reference) return "reference";
  if (!draft.requestedAction) return "requestedAction";
  return null;
}

function deficiencyFieldPrompt(field, draft) {
  if (field === "projectSlug") {
    return "Which project is this for? Reply with the project slug, for example: home";
  }
  if (field === "title") {
    return draft.projectName
      ? `Creating a deficiency for ${draft.projectName}. Send a short title.`
      : "Creating a deficiency. Send a short title.";
  }
  if (field === "description") {
    return "What is the deficiency? Send the main description.";
  }
  if (field === "locationArea") {
    return "What location or area is affected?";
  }
  if (field === "trade") {
    return "Which trade is responsible or affected?";
  }
  if (field === "reference") {
    return 'What reference should I record? Reply with drawing, unit, detail, or "none".';
  }
  if (field === "requestedAction") {
    return "What action is required to correct it?";
  }
  return "Send the next deficiency detail.";
}

function summarizeDeficiencyDraft(draft) {
  const present = [];
  if (draft.projectSlug) present.push(`project=${draft.projectSlug}`);
  if (draft.title) present.push("title");
  if (draft.description) present.push("description");
  if (draft.location || draft.area) present.push("location/area");
  if (draft.trade) present.push("trade");
  if (draft.reference) present.push("reference");
  if (draft.requestedAction) present.push("action");
  const next = getNextMissingDeficiencyField(draft);
  const nextLabel =
    next === "locationArea" ? "location or area" : next === "requestedAction" ? "action" : next;
  const presentText = present.length ? present.join(", ") : "nothing yet";
  return next
    ? `Deficiency draft: have ${presentText}. Next needed: ${nextLabel}.`
    : `Deficiency draft is complete for ${draft.projectName || draft.projectSlug}.`;
}

function buildDeficiencyDraftText(draft) {
  return [
    `Title: ${draft.title}`,
    `Description: ${draft.description}`,
    `Location: ${draft.location || "-"}`,
    `Area: ${draft.area || "-"}`,
    `Trade: ${draft.trade}`,
    `Reference: ${draft.reference}`,
    `Required action: ${draft.requestedAction}`,
  ].join(" | ");
}

function applyExplicitBlank(field, value) {
  if (!DEFICIENCY_NONE_RE.test(value)) return value;
  if (field === "reference") return "None provided";
  if (field === "trade") return "Unknown / not confirmed";
  if (field === "location") return "";
  if (field === "area") return "";
  return value;
}

function parseProjectReply(text) {
  const hinted = extractProjectScopeHint(text || "");
  if (hinted.projectSlug) return hinted.projectSlug;
  const direct = normalizeProjectSlug(String(text || "").replace(/^project\s+/i, ""));
  return direct || null;
}

function elevateProjectAccessWithApprovedMember(projectAccess, memberAccess) {
  if (!projectAccess || !projectAccess.exists || projectAccess.allowed) return projectAccess;
  if (!memberAccess) return projectAccess;
  const projectSlug = projectAccess.projectSlug || null;
  if (!projectSlug || !canAccessProject(memberAccess, projectSlug)) return projectAccess;
  return {
    ...projectAccess,
    allowed: true,
    reason: null,
    accessVia: "approved-phone-app-member",
    memberAccess,
  };
}

async function getAssistantProjectAccess(db, phoneE164, projectSlug, userData) {
  const baseAccess = await getAccessibleProjectForUser(db, phoneE164, projectSlug, { userData });
  if (!baseAccess.exists || baseAccess.allowed) return baseAccess;
  const memberAccess = await findActiveAppMemberByApprovedPhone(db, phoneE164);
  return elevateProjectAccessWithApprovedMember(baseAccess, memberAccess);
}

function buildLogAuthorFields(phoneE164, user, memberAccess) {
  const memberName = String((memberAccess && memberAccess.memberData && memberAccess.memberData.displayName) || "").trim();
  const memberEmail = String((memberAccess && memberAccess.email) || "").trim();
  const userDisplayName = String((user && user.displayName) || "").trim();
  const authorName = memberName || userDisplayName || null;
  const authorEmail = memberEmail || null;
  return {
    authorName,
    authorEmail,
    authorLabel: authorName || authorEmail || String(phoneE164 || "").trim() || null,
  };
}

function mergeDeficiencyFieldsIntoDraft(draft, parsed) {
  const next = { ...draft };
  if (parsed && parsed.fields) {
    for (const key of ["title", "description", "location", "area", "trade", "reference", "requestedAction"]) {
      if (parsed.fields[key]) {
        next[key] = applyExplicitBlank(
          key,
          normalizeDeficiencyTextValue(parsed.fields[key])
        );
      }
    }
  }
  return next;
}

async function savePendingDeficiencyDraft(db, phoneE164, draft) {
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingDeficiencyIntake: draft,
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

async function clearPendingDeficiencyDraft(db, phoneE164) {
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingDeficiencyIntake: FieldValue.delete(),
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

async function savePendingTodoDraft(db, phoneE164, draft) {
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingTodoIntake: draft,
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

async function clearPendingTodoDraft(db, phoneE164) {
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingTodoIntake: FieldValue.delete(),
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

function shouldBypassPendingDeficiency(trimmedBody, lower, pendingDraft) {
  if (!pendingDraft) return true;
  const nextMissing = getNextMissingDeficiencyField(pendingDraft);
  if (nextMissing === "projectSlug" && parseProjectCommand(trimmedBody)) {
    return false;
  }
  if (
    lower === "help" ||
    lower === "commands" ||
    lower === "?" ||
    lower === "ai check" ||
    lower === "openai check" ||
    lower === "contact" ||
    lower === "contacts" ||
    lower === "reset" ||
    lower === "reset conversation" ||
    lower === "reset context"
  ) {
    return true;
  }
  if (isDailyReportPdfRequest(trimmedBody) || isAnyDayRollupRequest(trimmedBody)) {
    return true;
  }
  return false;
}

function shouldBypassPendingTodo(trimmedBody, lower, channel = "") {
  const channelNorm = String(channel || "").trim().toLowerCase();
  if (channelNorm.startsWith("voice") || channelNorm === "sms_audio_note" || channelNorm === "sms_audio_note_reviewed") {
    return true;
  }
  if (
    lower === "help" ||
    lower === "commands" ||
    lower === "?" ||
    lower === "contact" ||
    lower === "contacts" ||
    lower === "reset" ||
    lower === "reset conversation" ||
    lower === "reset context"
  ) {
    return true;
  }
  if (parseProjectCommand(trimmedBody)) return true;
  if (parseManagementLabourTotalsQuery(trimmedBody)) return true;
  if (parseManagementLabourBreakdownQuery(trimmedBody)) return true;
  if (parseManagementLabourPdfRequest(trimmedBody)) return true;
  if (parseLabourHoursBalanceQuery(trimmedBody)) return true;
  if (parseTodoListRequest(trimmedBody)) return true;
  if (parseTodoReportRequest(trimmedBody)) return true;
  if (parseTodoMutationRequest(trimmedBody)) return true;
  if (isDailyReportPdfRequest(trimmedBody) || isAnyDayRollupRequest(trimmedBody)) return true;
  return false;
}

async function resolveDeficiencyProject({
  db,
  phoneE164,
  user,
  projectSlug,
}) {
  const slug = normalizeProjectSlug(projectSlug);
  if (!slug) {
    return { ok: false, replyText: "Which project is this for? Reply with the project slug." };
  }
  const projectAccess = await getAssistantProjectAccess(db, phoneE164, slug, user);
  if (!projectAccess.exists) {
    return {
      ok: false,
      replyText: `Project "${slug}" does not exist. Reply with one of your assigned project slugs.`,
    };
  }
  if (!projectAccess.allowed) {
    return {
      ok: false,
      replyText: `Project "${slug}" is not assigned to this phone number. Reply with one of your own projects.`,
    };
  }
  return {
    ok: true,
    projectSlug: projectAccess.projectSlug || slug,
    projectName: (projectAccess.projectData && projectAccess.projectData.name) || slug,
  };
}

async function createTodoFromDraft({
  db,
  phoneE164,
  currentMemberAccess,
  logAuthorFields,
  relatedMessageId,
  draft,
  source = "sms_todo_intake",
}) {
  const todoRef = db.collection(COL_PROJECT_TODOS).doc();
  await todoRef.set({
    projectSlug: draft.projectSlug || HOME_TODO_PROJECT_SLUG,
    scope: "project",
    visibility: "management",
    status: "open",
    taskText: draft.taskText,
    sourceText: draft.sourceText || draft.taskText,
    dueWindow: null,
    dueLabel: null,
    dueBy: draft.dueBy || null,
    startedAt: null,
    finishedAt: null,
    priority: draft.priority || null,
    recurrence: { mode: "none", customText: "" },
    labels: [],
    tags: Array.isArray(draft.tags) ? draft.tags : [],
    reminders: Array.isArray(draft.reminders) ? draft.reminders : [],
    dependencies: [],
    comments: [],
    subTodos: [],
    createdByPhone: phoneE164,
    createdByEmail: currentMemberAccess.email || logAuthorFields.authorEmail || null,
    createdByName:
      String(currentMemberAccess.memberData?.displayName || logAuthorFields.authorName || phoneE164).trim() ||
      phoneE164,
    source,
    sourceMessageId: relatedMessageId || draft.sourceMessageId || null,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });
  return todoRef.id;
}

async function loadTodoListForPhone(db, phoneE164, request) {
  const snap = await db
    .collection(COL_PROJECT_TODOS)
    .where("createdByPhone", "==", phoneE164)
    .limit(100)
    .get();

  const requestedTags = normalizeTodoTagsValue(request?.tags || []);
  const todos = snap.docs
    .map((doc) => ({ id: doc.id, ...(doc.data() || {}) }))
    .filter((todo) => {
      if (!request?.projectSlug) return true;
      return normalizeProjectSlug(todo.projectSlug) === normalizeProjectSlug(request.projectSlug);
    })
    .filter((todo) => matchesTodoListStatus(todo, request?.status || "active"))
    .filter((todo) => {
      if (!request?.priority) return true;
      return String(todo.priority || "").trim().toLowerCase() === String(request.priority || "").trim().toLowerCase();
    })
    .filter((todo) => {
      if (!requestedTags.length) return true;
      const todoTags = normalizeTodoTagsValue(todo.tags || []);
      return requestedTags.every((tag) => todoTags.includes(tag));
    })
    .sort(compareTodoListItems);

  return todos;
}

async function applyTodoMutationForPhone({
  db,
  FieldValue,
  phoneE164,
  currentMemberAccess,
  mutation,
}) {
  const statusFilter =
    mutation.action === "completed"
      ? "active"
      : mutation.action === "open"
        ? "completed"
        : mutation.action === "inprogress"
          ? "active"
          : "all";
  const todos = await loadTodoListForPhone(db, phoneE164, {
    projectSlug: mutation.projectSlug || null,
    status: statusFilter,
    priority: "",
    tags: [],
  });
  const pool =
    statusFilter === "all"
      ? todos
      : todos.filter((todo) =>
          mutation.action === "open"
            ? normalizeTodoStatusForList(todo.status) === "completed"
            : normalizeTodoStatusForList(todo.status) !== "completed"
        );
  const match = findBestTodoMatches(pool, mutation.targetText);
  if (match.ambiguous.length) {
    return { ok: false, reason: "ambiguous", suggestions: match.ambiguous };
  }
  if (!match.best) {
    return { ok: false, reason: "not_found", suggestions: match.suggestions };
  }

  const todo = match.best;
  const todoRef = db.collection(COL_PROJECT_TODOS).doc(todo.id);
  const updates = {
    updatedAt: FieldValue.serverTimestamp(),
    updatedByEmail: currentMemberAccess.email || null,
  };

  if (mutation.action === "edit") {
    updates.taskText = normalizeTodoTextValue(mutation.nextTaskText, 500);
  } else if (mutation.action === "completed") {
    updates.status = "completed";
    updates.finishedAt = todo.finishedAt || new Date().toISOString();
    updates.completedAt = FieldValue.serverTimestamp();
    updates.completedByEmail = currentMemberAccess.email || null;
  } else if (mutation.action === "open") {
    updates.status = "open";
    updates.finishedAt = null;
    updates.completedAt = FieldValue.delete();
    updates.completedByEmail = FieldValue.delete();
  } else if (mutation.action === "inprogress") {
    updates.status = "inprogress";
    updates.startedAt = todo.startedAt || new Date().toISOString();
    updates.finishedAt = null;
    updates.completedAt = FieldValue.delete();
    updates.completedByEmail = FieldValue.delete();
  }

  await todoRef.set(updates, { merge: true });
  return {
    ok: true,
    todoId: todo.id,
    projectSlug: normalizeProjectSlug(todo.projectSlug) || null,
    previousTaskText: String(todo.taskText || "").trim(),
    nextTaskText: mutation.action === "edit" ? updates.taskText : String(todo.taskText || "").trim(),
    status:
      mutation.action === "edit"
        ? normalizeTodoStatusForList(todo.status)
        : mutation.action === "completed"
          ? "completed"
          : mutation.action === "open"
            ? "open"
            : "inprogress",
  };
}

async function handlePendingTodoTurn({
  db,
  phoneE164,
  user,
  currentMemberAccess,
  trimmedBody,
  lower,
  relatedMessageId,
  logAuthorFields,
  outboundMeta,
}) {
  let draft = normalizePendingTodoDraft(user.pendingTodoIntake) || null;
  if (!draft || !draft.taskText) {
    await clearPendingTodoDraft(db, phoneE164);
    return {
      replyText: "That todo draft expired. Send xxx or todo: plus the task again.",
      outboundMeta: { ...outboundMeta, command: "todo_intake_missing" },
    };
  }

  if (lower === "cancel" || lower === "cancel todo") {
    await clearPendingTodoDraft(db, phoneE164);
    return {
      replyText: "Todo intake cancelled.",
      outboundMeta: { ...outboundMeta, command: "todo_intake_cancelled" },
    };
  }

  if (lower === "status" || lower === "todo status") {
    await savePendingTodoDraft(db, phoneE164, draft);
    return {
      replyText: truncateSms(todoFieldPrompt(getNextMissingTodoField(draft) || "tags", draft)),
      outboundMeta: { ...outboundMeta, command: "todo_intake_status", projectSlug: draft.projectSlug || null },
    };
  }

  const nextMissing = getNextMissingTodoField(draft);
  if (nextMissing === "dueBy") {
    const parsed = parseTodoDateTimeInput(trimmedBody);
    if (parsed === "") {
      await savePendingTodoDraft(db, phoneE164, draft);
      return {
        replyText: "Use YYYY-MM-DD or YYYY-MM-DD HH:MM for the due date. Reply none if there is no due date.",
        outboundMeta: { ...outboundMeta, command: "todo_intake_due_invalid", projectSlug: draft.projectSlug },
      };
    }
    draft.dueBy = parsed;
    draft.dueDateCaptured = true;
  } else if (nextMissing === "reminderRequested") {
    if (TODO_YES_RE.test(trimmedBody)) draft.reminderRequested = true;
    else if (TODO_NO_RE.test(trimmedBody)) {
      draft.reminderRequested = false;
      draft.secondReminderWanted = false;
      draft.reminders = [];
    } else {
      await savePendingTodoDraft(db, phoneE164, draft);
      return {
        replyText: "Reply yes or no. Do you need a reminder?",
        outboundMeta: { ...outboundMeta, command: "todo_intake_reminder_prompt", projectSlug: draft.projectSlug },
      };
    }
  } else if (nextMissing === "firstReminder") {
    const parsed = parseTodoDateTimeInput(trimmedBody);
    if (!parsed) {
      await savePendingTodoDraft(db, phoneE164, draft);
      return {
        replyText: "Use YYYY-MM-DD HH:MM for the first reminder.",
        outboundMeta: { ...outboundMeta, command: "todo_intake_first_reminder_invalid", projectSlug: draft.projectSlug },
      };
    }
    draft.reminders = [parsed];
  } else if (nextMissing === "secondReminderWanted") {
    if (TODO_YES_RE.test(trimmedBody)) draft.secondReminderWanted = true;
    else if (TODO_NO_RE.test(trimmedBody)) draft.secondReminderWanted = false;
    else {
      await savePendingTodoDraft(db, phoneE164, draft);
      return {
        replyText: "Reply yes or no. Do you want a second reminder?",
        outboundMeta: { ...outboundMeta, command: "todo_intake_second_reminder_prompt", projectSlug: draft.projectSlug },
      };
    }
  } else if (nextMissing === "secondReminder") {
    const parsed = parseTodoDateTimeInput(trimmedBody);
    if (!parsed) {
      await savePendingTodoDraft(db, phoneE164, draft);
      return {
        replyText: "Use YYYY-MM-DD HH:MM for the second reminder.",
        outboundMeta: { ...outboundMeta, command: "todo_intake_second_reminder_invalid", projectSlug: draft.projectSlug },
      };
    }
    draft.reminders = [...draft.reminders.slice(0, 1), parsed];
  } else if (nextMissing === "priority") {
    const parsed = normalizeTodoPriorityValue(trimmedBody);
    if (parsed === "") {
      await savePendingTodoDraft(db, phoneE164, draft);
      return {
        replyText: "Reply p1, p2, p3, p4, or none for priority.",
        outboundMeta: { ...outboundMeta, command: "todo_intake_priority_invalid", projectSlug: draft.projectSlug },
      };
    }
    draft.priority = parsed;
    draft.priorityCaptured = true;
  } else if (nextMissing === "tags") {
    draft.tags = TODO_NONE_RE.test(trimmedBody) ? [] : normalizeTodoTagsValue(trimmedBody);
    draft.tagsCaptured = true;
  }

  const remaining = getNextMissingTodoField(draft);
  if (remaining) {
    await savePendingTodoDraft(db, phoneE164, draft);
    return {
      replyText: truncateSms(todoFieldPrompt(remaining, draft)),
      outboundMeta: {
        ...outboundMeta,
        command: "todo_intake",
        projectSlug: draft.projectSlug || null,
      },
    };
  }

  const todoId = await createTodoFromDraft({
    db,
    phoneE164,
    currentMemberAccess,
    logAuthorFields,
    relatedMessageId,
    draft,
  });
  await clearPendingTodoDraft(db, phoneE164);
  return {
    replyText: truncateSms(
      `Saved todo: ${draft.taskText}.${draft.dueBy ? " Due date set." : ""}${draft.reminders.length ? ` ${draft.reminders.length} reminder${draft.reminders.length > 1 ? "s" : ""} set.` : ""}${draft.priority ? ` Priority ${draft.priority}.` : ""}${draft.tags.length ? ` Tags: ${draft.tags.map((tag) => `@${tag}`).join(" ")}.` : ""}`
    ),
    outboundMeta: {
      ...outboundMeta,
      command: "todo_created",
      projectSlug: draft.projectSlug || null,
      pendingTodoIntake: false,
      todoId,
    },
  };
}

async function loadTodayActivity(db, phoneE164, projectSlug) {
  const start = startOfEasternDay(new Date());
  const messagesSnap = await db
    .collection("messages")
    .where("threadKey", "==", phoneE164)
    .where("createdAt", ">=", start)
    .orderBy("createdAt", "asc")
    .limit(80)
    .get()
    .catch(() => null);

  let issuesSnap = await db
    .collection(COL_ISSUES)
    .where("phoneE164", "==", phoneE164)
    .where("createdAt", ">=", start)
    .orderBy("createdAt", "asc")
    .limit(50)
    .get()
    .catch(() => null);

  const messages = (messagesSnap
    ? messagesSnap.docs.map((d) => d.data())
    : []).filter((m) => matchesProjectScope(m.projectSlug, projectSlug));
  const issues = (issuesSnap ? issuesSnap.docs.map((d) => d.data()) : [])
    .filter((i) => matchesProjectScope(i.projectSlug, projectSlug));

  const lines = [];
  for (const m of messages) {
    if (m.body)
      lines.push(`${m.direction}: ${m.body}`);
  }
  for (const i of issues) {
    lines.push(`log [${i.type}]: ${i.message}`);
  }
  return { start, lines, projectSlug };
}

async function buildDailySummary(db, openaiApiKey, phoneE164, projectSlug, logger, runId, modelsOverride) {
  const { lines } = await loadTodayActivity(db, phoneE164, projectSlug);
  if (lines.length === 0) {
    return {
      text: "Nothing logged today yet for this number—no SMS and no issue/delivery notes. Text log issue: … or send updates and try again.",
      summaryMeta: { lineCount: 0 },
    };
  }
  const bundle = lines.join("\n").slice(0, 12_000);
  const system =
    "You write concise field daily summaries for construction supers. Output plain text only, SMS length (under 400 chars if possible). Bullet style OK with - . No XML.";
  const userPrompt = `Project context: ${projectSlug || "none assigned"}.\nToday's logged lines:\n${bundle}\n\nSummarize what mattered for the field team.`;
  try {
    const client = new OpenAI({ apiKey: openaiApiKey });
    const completion = await chatCompletionWithFallback(
      client,
      {
        messages: [
          { role: "system", content: system },
          { role: "user", content: userPrompt },
        ],
        max_completion_tokens: 400,
        temperature: 0.3,
      },
      logger,
      runId,
      modelsOverride
    );
    const raw = completionText(completion);
    const text = truncateSms(raw.trim());
    return {
      text,
      summaryMeta: { lineCount: lines.length, ai: true },
    };
  } catch (e) {
    logger.error("assistant: summary openai fail", { runId, message: e.message });
    const fallback = truncateSms(
      `Today (${lines.length} items): ` + lines.slice(-6).join(" | ")
    );
    return {
      text: fallback,
      summaryMeta: { lineCount: lines.length, ai: false, error: String(e.message) },
    };
  }
}

async function routeGenericInboundLog({
  db,
  openaiApiKey,
  logger,
  runId,
  phoneE164,
  user = null,
  trimmedBody,
  relatedMessageId,
  numMedia,
  effectiveProjectSlug,
  effectiveProjectName,
  logAuthorFields,
  modelsOverride,
  outboundMeta,
}) {
  const routingDecision =
    outboundMeta && outboundMeta.routingDecision
      ? outboundMeta.routingDecision
      : buildRoutingDecision({
          stage: "non_command",
          action: "save_log",
          confidence: 0.7,
          reason: "Generic inbound log routing selected the save-to-log path.",
          source: "generic_router",
          matchedBy: "routeGenericInboundLog",
        });
  logRoutingTelemetry(logger, runId, phoneE164, routingDecision, {
    projectSlug: effectiveProjectSlug || null,
    numMedia: Math.max(0, Number(numMedia) || 0),
  });
  const extracted = extractExplicitReportDate(trimmedBody);
  const cleanedText = (extracted.cleanedText || trimmedBody || "").trim() || "Field update";
  let routed = null;

  if (openaiApiKey) {
    try {
      const client = new OpenAI({ apiKey: openaiApiKey });
      const completion = await chatCompletionWithFallback(
        client,
        {
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: LOG_ROUTING_SYSTEM },
            {
              role: "user",
              content:
                `Text: ${cleanedText}\n` +
                `Received photos: ${Math.max(0, numMedia || 0)}\n` +
                `Current project: ${effectiveProjectName || effectiveProjectSlug || "none"}\n`,
            },
          ],
          max_completion_tokens: 500,
          temperature: 0.1,
        },
        logger,
        runId,
        modelsOverride
      );
      routed = JSON.parse(completionText(completion) || "{}");
    } catch (routeErr) {
      logger.warn("assistant: generic log route ai failed", {
        runId,
        message: routeErr.message,
      });
    }
  }

  const payload = sanitizeRoutePayload(routed, cleanedText, numMedia);
  const reportDateKey = extracted.reportDateKey || null;
  const tags = payload.tags.includes("photo") || numMedia <= 0
    ? payload.tags
    : [...payload.tags, "photo"];

  if (payload.logType === "safety" || payload.logType === "deficiency") {
    const created = await createSmsIssue(db, FieldValue, {
      phoneE164,
      projectSlug: effectiveProjectSlug,
      projectName: effectiveProjectName,
      bodyText: payload.description,
      rawSms: trimmedBody,
      source: routed ? "ai" : "sms",
      logParsedType: payload.logType,
      classifierType: payload.logType,
      tags,
      relatedMessageId: relatedMessageId || null,
      titleOverride: payload.title,
      descriptionOverride: payload.description,
    });

    const logEntry = await writeLogEntry(db, FieldValue, {
      phoneE164,
      ...logAuthorFields,
      projectSlug: effectiveProjectSlug,
      reportDateKey,
      rawText: trimmedBody,
      normalizedText: payload.description,
      category: payload.logType,
      subtype: "ai_routed",
      tags,
      sourceMessageId: relatedMessageId || null,
      canonicalIssueId: created.issueId,
      issueCollection: created.issueCollection,
    });

    return {
      replyText: truncateSms(
        `Saved as ${payload.logType}${payload.requiresFollowUp ? " for follow-up" : ""}: ${payload.title}`
      ),
      outboundMeta: {
        ...withRoutingDecision(outboundMeta, routingDecision),
        aiUsed: Boolean(routed),
        command: `log_${payload.logType}`,
        projectSlug: effectiveProjectSlug,
        issueLogId: created.issueId,
        issueCollection: created.issueCollection,
        logEntryId: logEntry.logEntryId,
        logCategory: payload.logType,
        reportDateKey,
        classification: `ai_routed:${payload.logType}`,
        enhanceLogEntry: false,
      },
    };
  }

  const isJournal = payload.logType === "journal";
  let saveProjectSlug = effectiveProjectSlug || null;
  if (isJournal && !saveProjectSlug && user) {
    const homeAccess = await getAssistantProjectAccess(db, phoneE164, "home", user);
    if (homeAccess.exists && homeAccess.allowed && homeAccess.projectSlug) {
      saveProjectSlug = normalizeProjectSlug(homeAccess.projectSlug) || "home";
    }
  }
  const logEntry = await writeLogEntry(db, FieldValue, {
    phoneE164,
    ...logAuthorFields,
    projectSlug: saveProjectSlug,
    reportDateKey,
    rawText: trimmedBody,
    normalizedText: payload.description,
    category: isJournal ? "journal" : "note",
    subtype: "ai_routed",
    tags,
    sourceMessageId: relatedMessageId || null,
  });

  return {
    replyText: truncateSms(
      isJournal
        ? `Saved to the home journal${payload.title ? `: ${payload.title}` : "."}`
        : `Saved to today's construction log${payload.title ? `: ${payload.title}` : "."}`
    ),
    outboundMeta: {
      ...withRoutingDecision(outboundMeta, routingDecision),
      aiUsed: Boolean(routed),
      command: isJournal ? "log_journal" : "log_construction",
      projectSlug: saveProjectSlug,
      logEntryId: logEntry.logEntryId,
      logCategory: isJournal ? "journal" : "note",
      reportDateKey,
      classification: `ai_routed:${payload.logType}`,
      enhanceLogEntry: false,
    },
  };
}

async function classifyGenericInboundIntent({
  openaiApiKey,
  logger,
  runId,
  historyMessages,
  trimmedBody,
  modelsOverride,
}) {
  let classified = null;
  if (openaiApiKey) {
    try {
      const client = new OpenAI({ apiKey: openaiApiKey });
      const completion = await chatCompletionWithFallback(
        client,
        {
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: INTENT_ROUTING_SYSTEM },
            ...historyMessages.slice(-8),
            { role: "user", content: trimmedBody },
          ],
          max_completion_tokens: 220,
          temperature: 0.1,
        },
        logger,
        runId,
        modelsOverride
      );
      classified = JSON.parse(completionText(completion) || "{}");
    } catch (intentErr) {
      logger.warn("assistant: inbound intent ai failed", {
        runId,
        message: intentErr.message,
      });
    }
  }
  return sanitizeIntentPayload(classified, trimmedBody);
}

async function planAssistantAction({
  openaiApiKey,
  logger,
  runId,
  historyMessages,
  trimmedBody,
  effectiveProjectSlug,
  effectiveProjectName,
  modelsOverride,
}) {
  if (!openaiApiKey) return sanitizeAssistantActionPlan(null);
  try {
    const client = new OpenAI({ apiKey: openaiApiKey });
    const completion = await chatCompletionWithFallback(
      client,
      {
        response_format: { type: "json_object" },
        messages: [
          { role: "system", content: ACTION_ROUTING_SYSTEM },
          ...historyMessages.slice(-6),
          {
            role: "user",
            content:
              `Current project: ${effectiveProjectName || effectiveProjectSlug || "none"}\n` +
              `Latest message: ${trimmedBody}`,
          },
        ],
        max_completion_tokens: 260,
        temperature: 0.1,
      },
      logger,
      runId,
      modelsOverride
    );
    return sanitizeAssistantActionPlan(JSON.parse(completionText(completion) || "{}"));
  } catch (err) {
    logger.warn("assistant: action planner ai failed", {
      runId,
      message: err.message,
    });
    return sanitizeAssistantActionPlan(null);
  }
}

async function handleDeficiencyIntakeTurn({
  db,
  logger,
  runId,
  phoneE164,
  user,
  trimmedBody,
  lower,
  relatedMessageId,
  numMedia,
  effectiveProjectSlug,
  effectiveProjectName,
  logAuthorFields,
  deficiencyRequest,
  outboundMeta,
}) {
  const replyBody = /^photo attachment$/i.test(trimmedBody) ? "" : trimmedBody;
  let draft = normalizePendingDeficiencyDraft(user.pendingDeficiencyIntake) || {
    projectSlug: null,
    projectName: "",
    title: "",
    description: "",
    location: "",
    area: "",
    trade: "",
    reference: "",
    requestedAction: "",
    sourceMessageIds: [],
  };

  if (lower === "cancel" || lower === "cancel deficiency") {
    await clearPendingDeficiencyDraft(db, phoneE164);
    return {
      replyText: "Deficiency draft cancelled.",
      outboundMeta: {
        ...outboundMeta,
        command: "deficiency_cancelled",
        pendingDeficiencyIntake: false,
      },
    };
  }

  if (relatedMessageId && numMedia > 0) {
    draft.sourceMessageIds = [
      ...new Set([...(draft.sourceMessageIds || []), relatedMessageId]),
    ];
  }

  const parsedDetails = deficiencyRequest
    ? {
        projectSlug: deficiencyRequest.projectSlug || null,
        fields: deficiencyRequest.fields || {},
      }
    : parseDeficiencyDetails(replyBody);
  const parsedFieldCount = Object.keys(parsedDetails.fields || {}).length;

  if (!draft.projectSlug && !parsedDetails.projectSlug && effectiveProjectSlug) {
    draft.projectSlug = effectiveProjectSlug;
    draft.projectName = effectiveProjectName || effectiveProjectSlug;
  }

  draft = mergeDeficiencyFieldsIntoDraft(draft, parsedDetails);

  let explicitProjectSlug = parsedDetails.projectSlug || null;
  const nextMissingBeforeFallback = getNextMissingDeficiencyField(draft);
  if (!explicitProjectSlug && !deficiencyRequest && nextMissingBeforeFallback === "projectSlug") {
    explicitProjectSlug = parseProjectReply(replyBody);
  }

  if (explicitProjectSlug) {
    const resolvedProject = await resolveDeficiencyProject({
      db,
      phoneE164,
      user,
      projectSlug: explicitProjectSlug,
    });
    if (!resolvedProject.ok) {
      await savePendingDeficiencyDraft(db, phoneE164, draft);
      return {
        replyText: resolvedProject.replyText,
        outboundMeta: {
          ...outboundMeta,
          command: "deficiency_project_invalid",
          projectSlug: draft.projectSlug || null,
          pendingDeficiencyIntake: true,
        },
      };
    }
    draft.projectSlug = resolvedProject.projectSlug;
    draft.projectName = resolvedProject.projectName;
    outboundMeta.projectSlug = resolvedProject.projectSlug;
  }

  const nextMissing = getNextMissingDeficiencyField(draft);
  if (!deficiencyRequest && parsedFieldCount === 0 && replyBody) {
    if (nextMissing === "title") {
      draft.title = replyBody;
    } else if (nextMissing === "description") {
      draft.description = replyBody;
    } else if (nextMissing === "locationArea") {
      draft.location = applyExplicitBlank("location", replyBody) || replyBody;
    } else if (nextMissing === "trade") {
      draft.trade = applyExplicitBlank("trade", replyBody);
    } else if (nextMissing === "reference") {
      draft.reference = applyExplicitBlank("reference", replyBody);
    } else if (nextMissing === "requestedAction") {
      draft.requestedAction = replyBody;
    }
  }

  const remaining = getNextMissingDeficiencyField(draft);
  if (lower === "status" || lower === "deficiency status") {
    await savePendingDeficiencyDraft(db, phoneE164, draft);
    return {
      replyText: truncateSms(
        `${summarizeDeficiencyDraft(draft)} ${remaining ? deficiencyFieldPrompt(remaining, draft) : ""}`.trim()
      ),
      outboundMeta: {
        ...outboundMeta,
        command: "deficiency_status",
        projectSlug: draft.projectSlug || outboundMeta.projectSlug || null,
        pendingDeficiencyIntake: true,
      },
    };
  }

  if (remaining) {
    await savePendingDeficiencyDraft(db, phoneE164, draft);
    return {
      replyText: truncateSms(deficiencyFieldPrompt(remaining, draft)),
      outboundMeta: {
        ...outboundMeta,
        command: "deficiency_intake",
        projectSlug: draft.projectSlug || outboundMeta.projectSlug || null,
        pendingDeficiencyIntake: true,
      },
    };
  }

  const deficiencyText = buildDeficiencyDraftText(draft);
  let created;
  try {
    created = await createSmsIssue(db, FieldValue, {
      phoneE164,
      projectSlug: draft.projectSlug,
      projectName: draft.projectName || draft.projectSlug,
      bodyText: deficiencyText,
      rawSms: replyBody || deficiencyText,
      source: "sms",
      logParsedType: "deficiency",
      classifierType: null,
      tags: ["deficiency", "sms_intake"],
      relatedMessageId: relatedMessageId || null,
      titleOverride: draft.title,
      descriptionOverride: draft.description,
      fieldOverrides: {
        location: draft.location,
        area: draft.area,
        trade: draft.trade,
        reference: draft.reference,
        requestedAction: draft.requestedAction,
      },
    });
  } catch (saveErr) {
    logger.error("assistant: deficiency intake save failed", {
      runId,
      message: saveErr.message,
      stack: saveErr.stack,
    });
    await savePendingDeficiencyDraft(db, phoneE164, draft);
    return {
      replyText: "Could not save that deficiency yet. Try again in a moment.",
      outboundMeta: {
        ...outboundMeta,
        command: "deficiency_save_failed",
        aiError: String(saveErr.message),
        projectSlug: draft.projectSlug || null,
        pendingDeficiencyIntake: true,
      },
    };
  }

  let relinked = { attached: 0, mediaIds: [], storagePaths: [], photos: [] };
  try {
    relinked = await attachExistingMediaToIssueBySourceMessages({
      db,
      FieldValue,
      issueCollection: created.issueCollection,
      issueId: created.issueId,
      sourceMessageIds: draft.sourceMessageIds || [],
      changedBy: phoneE164,
      projectSlug: draft.projectSlug,
    });
  } catch (mediaErr) {
    logger.warn("assistant: deficiency media relink failed", {
      runId,
      message: mediaErr.message,
    });
  }

  let le = null;
  try {
    le = await writeLogEntry(db, FieldValue, {
      phoneE164,
      ...logAuthorFields,
      projectSlug: draft.projectSlug,
      rawText: deficiencyText,
      normalizedText: deficiencyText,
      category: "deficiency",
      subtype: "sms_intake",
      tags: ["deficiency", "sms_intake"],
      sourceMessageId:
        relatedMessageId ||
        (draft.sourceMessageIds && draft.sourceMessageIds[draft.sourceMessageIds.length - 1]) ||
        null,
      canonicalIssueId: created.issueId,
      issueCollection: created.issueCollection,
      linkedMediaIds: relinked.storagePaths || [],
      status: "open",
    });
  } catch (leErr) {
    logger.warn("assistant: deficiency logEntry write failed", {
      runId,
      message: leErr.message,
    });
  }

  if (le && relinked.storagePaths && relinked.storagePaths.length) {
    try {
      await appendLinkedMediaIds(db, FieldValue, le.logEntryId, relinked.storagePaths);
    } catch (linkErr) {
      logger.warn("assistant: deficiency linked media append failed", {
        runId,
        message: linkErr.message,
      });
    }
  }

  await clearPendingDeficiencyDraft(db, phoneE164);

  const mediaNote = relinked.attached ? ` Photos linked: ${relinked.attached}.` : "";
  return {
    replyText: truncateSms(
      `Deficiency saved for ${draft.projectName || draft.projectSlug}: ${draft.title}.${mediaNote}`
    ),
    outboundMeta: {
      ...outboundMeta,
      command: "log_deficiency",
      projectSlug: draft.projectSlug || null,
      issueLogId: created.issueId,
      issueCollection: created.issueCollection,
      logEntryId: le ? le.logEntryId : null,
      logCategory: "deficiency",
      classification: "deterministic:sms_intake:deficiency",
      enhanceLogEntry: Boolean(le && le.logEntryId),
      pendingDeficiencyIntake: false,
    },
  };
}

function formatDeterministicRollup(entries, preferDetail, reportDateKey) {
  const grouped = formatGroupedDayLog(entries);
  const { counts, byCat } = grouped;
  const total = entries.length;
  const dayLabel = reportDateKey || dateKeyEastern(new Date());
  const head = `${dayLabel} ${total} entr${total === 1 ? "y" : "ies"} (Eastern): ${Object.keys(counts)
    .map((k) => `${k} ${counts[k]}`)
    .join(", ")}`;
  const order = [
    "safety",
    "delay",
    "deficiency",
    "issue",
    "delivery",
    "inspection",
    "note",
    "progress",
    "journal",
  ];
  const lines = [head];
  const perCat = preferDetail ? 6 : 2;
  for (const cat of order) {
    const arr = byCat[cat];
    if (!arr || !arr.length) continue;
    lines.push(`${cat}:`);
    for (const item of arr.slice(-perCat)) {
      const s = item.slice(0, 100);
      lines.push(`- ${s}${item.length > 100 ? "…" : ""}`);
    }
  }
  for (const cat of Object.keys(byCat)) {
    if (order.includes(cat)) continue;
    lines.push(`${cat}: ${byCat[cat].length}`);
  }
  return truncateSms(lines.join("\n"));
}

async function buildDayRollup(
  db,
  openaiApiKey,
  phoneE164,
  projectSlug,
  reportDateKey,
  logger,
  runId,
  modelsOverride,
  preferAiNarrative
) {
  const requestedDateKey = reportDateKey || dateKeyEastern(new Date());
  const isTodayRequest = requestedDateKey === dateKeyEastern(new Date());
  const entries = await loadLogEntriesForDayForProject(
    db,
    phoneE164,
    requestedDateKey,
    projectSlug
  );
  if (!entries.length) {
    if (!isTodayRequest) {
      return {
        text: `No log entries found for ${requestedDateKey}.`,
        summaryMeta: {
          lineCount: 0,
          ai: false,
          source: "logEntries",
          reportDateKey: requestedDateKey,
        },
      };
    }
    const legacy = await buildDailySummary(
      db,
      openaiApiKey,
      phoneE164,
      projectSlug,
      logger,
      runId,
      modelsOverride
    );
    return {
      text: legacy.text,
      summaryMeta: {
        ...legacy.summaryMeta,
        source: "legacy_messages_issues",
        reportDateKey: requestedDateKey,
      },
    };
  }

  const detFull = formatDeterministicRollup(entries, true, requestedDateKey);
  const detShort = formatDeterministicRollup(entries, false, requestedDateKey);

  if (!preferAiNarrative || !openaiApiKey) {
    logger.info("assistant: day rollup deterministic", {
      runId,
      count: entries.length,
      ai: false,
    });
    return {
      text: detFull,
      summaryMeta: {
        lineCount: entries.length,
        ai: false,
        source: "logEntries",
        reportDateKey: requestedDateKey,
      },
    };
  }

  try {
    const client = new OpenAI({ apiKey: openaiApiKey });
    const bundle = entries
      .map((e) => {
        const secs = (e.dailySummarySections || ["dayLog"]).join(",");
        const body = lineText(e);
        return `[category=${e.category || "journal"}; sections=${secs}] ${body}`;
      })
      .join("\n")
      .slice(0, 10000);
    const bySec = formatRollupByReportSections(entries);
    const sectionHint = Object.keys(bySec)
      .map((k) => `${k}: ${bySec[k].length} line(s)`)
      .join("; ");
    const completion = await chatCompletionWithFallback(
      client,
      {
        messages: [
          {
            role: "system",
            content:
              "You summarize ONE unified construction site day (Eastern Time calendar day). Every line is part of the same daily log—includes deficiencies, delays, safety, notes, journal, etc. Do not treat categories as separate worlds. Plain text only. Aim under 400 characters. No XML or markdown.",
          },
          {
            role: "user",
            content: `Project: ${projectSlug || "none assigned"}\nSection counts: ${sectionHint}\nAll entries:\n${bundle}\n\nSingle tight daily field summary weaving issues, work, and delays together.`,
          },
        ],
        max_completion_tokens: 420,
        temperature: 0.25,
      },
      logger,
      runId,
      modelsOverride
    );
    const raw = completionText(completion).trim();
    const text = truncateSms(raw || detShort);
    logger.info("assistant: day rollup ai ok", { runId, count: entries.length });
    return {
      text,
      summaryMeta: {
        lineCount: entries.length,
        ai: true,
        source: "logEntries",
        reportDateKey: requestedDateKey,
      },
    };
  } catch (e) {
    logger.error("assistant: day rollup ai fail", { runId, message: e.message });
    return {
      text: detShort,
      summaryMeta: {
        lineCount: entries.length,
        ai: false,
        error: String(e.message),
        source: "logEntries",
        reportDateKey: requestedDateKey,
      },
    };
  }
}

/**
 * Main entry: returns { replyText, outboundMeta }
 * outboundMeta: { aiUsed, aiError, command, projectSlug, issueLogId, summarySaved, dailyPdfRequested, reportDateKey, reportType, logEntryId }
 */
async function buildReply({
  db,
  openaiApiKey,
  logger,
  runId,
  from,
  body,
  relatedMessageId,
  numMedia = 0,
  channel = "sms",
  models: modelsOverride,
}) {
  const models = getModels(modelsOverride);
  const phoneE164 = from.trim();
  const trimmedBody = (body || "").trim();
  let userMessageForAI = trimmedBody;
  let lower = trimmedBody.toLowerCase();

  const user = await getOrCreateUser(db, phoneE164);
  const currentMemberAccess = await findActiveAppMemberByApprovedPhone(db, phoneE164);
  const logAuthorFields = buildLogAuthorFields(phoneE164, user, currentMemberAccess);
  const admin = await getAdminSettings(db);
  let project = null;

  const outboundMeta = {
    aiUsed: false,
    aiError: null,
    command: null,
    projectSlug: user.activeProjectSlug || null,
    issueLogId: null,
    issueCollection: null,
    summarySaved: false,
    dailyPdfRequested: false,
    reportDateKey: null,
    reportType: null,
    logEntryId: null,
    logCategory: null,
    classification: null,
    enhanceLogEntry: false,
    pendingDeficiencyIntake: false,
    notifyRequest: null,
    labourPdfRequested: false,
    labourReportStartKey: null,
    labourReportEndKey: null,
    labourReportAllLabourers: false,
    todoReportRequested: false,
    todoReportFormat: null,
    lookaheadReportRequested: false,
    lookaheadReportKind: null,
    routingDecision: null,
  };

  const isLabourReportRequest = (text) => {
    const raw = String(text || "").trim().toLowerCase();
    if (!raw) return false;
    if (raw === "report") return true;
    if (raw === "pay period report") return true;
    if (raw === "payperiod report") return true;
    if (raw === "pay report") return true;
    if (raw === "labour report") return true;
    if (raw === "labor report") return true;
    if (raw === "labour pay period report") return true;
    if (raw === "labor pay period report") return true;
    return false;
  };

  const buildScopedManagementLabourEntries = async (range, projectScope = "") => {
    const entries = await loadLabourEntries(db, {
      startKey: range.startKey,
      endKey: range.endKey,
    });
    const explicitProjectSlug =
      projectScope === "this_project"
        ? normalizeProjectSlug(effectiveProjectSlug)
        : normalizeProjectSlug(projectScope);
    return entries.filter((entry) => {
      const projectSlug = String(entry && entry.projectSlug ? entry.projectSlug : "").trim().toLowerCase();
      if (explicitProjectSlug && projectSlug !== explicitProjectSlug) return false;
      return currentMemberAccess.allProjects === true || canAccessProject(currentMemberAccess, projectSlug);
    });
  };

  const filterLabourEntriesByLabourerQuery = (entries, labourerQuery = "") => {
    const query = String(labourerQuery || "")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
    if (!query) return entries;
    const compactQuery = query.replace(/[^a-z0-9]+/g, "");
    const compactPossessiveQuery =
      compactQuery.length > 2 && compactQuery.endsWith("s") ? compactQuery.slice(0, -1) : "";
    return (entries || []).filter((entry) => {
      const label = String(entry && (entry.labourerName || entry.labourerPhone) ? entry.labourerName || entry.labourerPhone : "")
        .replace(/\s+/g, " ")
        .trim()
        .toLowerCase();
      const compactLabel = label.replace(/[^a-z0-9]+/g, "");
      return (
        label.includes(query) ||
        compactLabel.includes(compactQuery) ||
        (compactPossessiveQuery && compactLabel.includes(compactPossessiveQuery))
      );
    });
  };

  // ---- Commands (deterministic) ----
  if (!trimmedBody) {
    return {
      replyText: "Send a message or text help for commands.",
      outboundMeta: { ...outboundMeta, command: "empty" },
    };
  }

  if (lower === "help" || lower === "commands" || lower === "?") {
    return { replyText: HELP_TEXT, outboundMeta: { ...outboundMeta, command: "help" } };
  }

  if (lower === "ai check" || lower === "openai check") {
    try {
      const client = new OpenAI({ apiKey: openaiApiKey });
      const completion = await chatCompletionWithFallback(
        client,
        {
          messages: [
            {
              role: "user",
              content: "Reply with exactly OK (two letters) and nothing else.",
            },
          ],
          max_completion_tokens: 16,
          temperature: 0,
        },
        logger,
        runId,
        modelsOverride
      );
      const usedModel = completion.model || models.primary;
      logger.info("assistant: ai check ok", { runId, model: usedModel });
      return {
        replyText: `OpenAI OK (${usedModel}). Key works; you can use the assistant.`,
        outboundMeta: { ...outboundMeta, aiUsed: true, command: "ai_check" },
      };
    } catch (e) {
      logger.error("assistant: ai check failed", { runId, message: e.message });
      return {
        replyText: truncateSms(
          `OpenAI check failed: ${e.message}. Verify OPENAI_API_KEY secret and OpenAI billing.`
        ),
        outboundMeta: {
          ...outboundMeta,
          aiUsed: false,
          aiError: String(e.message),
          command: "ai_check_failed",
        },
      };
    }
  }

  // Labour pay period PDF report request (labourers).
  if (isLabourReportRequest(trimmedBody)) {
    const labourer = await findActiveLabourerByPhone(db, phoneE164).catch(() => null);
    if (labourer) {
      const now = new Date();
      const range = getDateKeyRangeForBalanceQuery("pay", now);
      if (!range || !range.startKey || !range.endKey) {
        return {
          replyText: "Could not determine your current pay period. Try again in a minute.",
          outboundMeta: { ...outboundMeta, command: "labour_report_range_failed" },
        };
      }
      return {
        replyText: "OK. Generating your pay period labour report now. You will get a download link shortly.",
        outboundMeta: {
          ...outboundMeta,
          command: "labour_report_pdf",
          labourPdfRequested: true,
          labourReportStartKey: range.startKey,
          labourReportEndKey: range.endKey,
        },
      };
    }
    // Fall through for non-labourers (e.g. daily report "report" command).
  }

  const managementLabourPdfRequest = parseManagementLabourPdfRequest(trimmedBody);
  if (managementLabourPdfRequest) {
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only admin or management phones can generate labour PDFs for all labourers. Ask admin to approve this phone in Team.",
        outboundMeta: { ...outboundMeta, command: "labour_report_forbidden" },
      };
    }
    const range = getDateKeyRangeForBalanceQuery(managementLabourPdfRequest.range);
    if (!range || !range.startKey || !range.endKey) {
      return {
        replyText: "Could not determine that labour report range. Try again in a minute.",
        outboundMeta: { ...outboundMeta, command: "labour_report_range_failed" },
      };
    }
    return {
      replyText: "OK. Generating your labour PDF for all labourers now. You will get a download link shortly.",
      outboundMeta: {
        ...outboundMeta,
        command: "labour_report_pdf_all_labourers",
        labourPdfRequested: true,
        labourReportStartKey: range.startKey,
        labourReportEndKey: range.endKey,
        labourReportAllLabourers: true,
      },
    };
  }

  const pendingDeficiencyDraft = normalizePendingDeficiencyDraft(user.pendingDeficiencyIntake);
  const pendingTodoDraft = normalizePendingTodoDraft(user.pendingTodoIntake);
  const deficiencyRequest = parseDeficiencyIntakeRequest(trimmedBody);
  if (
    pendingDeficiencyDraft &&
    (lower === "cancel" ||
      lower === "cancel deficiency" ||
      lower === "status" ||
      lower === "deficiency status")
  ) {
    return handleDeficiencyIntakeTurn({
      db,
      logger,
      runId,
      phoneE164,
      user,
      trimmedBody,
      lower,
      relatedMessageId,
      numMedia,
      effectiveProjectSlug: user.activeProjectSlug || null,
      effectiveProjectName: null,
      logAuthorFields,
      deficiencyRequest: null,
      outboundMeta,
    });
  }

  const projCmd = parseProjectCommand(trimmedBody);
  if (
    pendingDeficiencyDraft &&
    !deficiencyRequest &&
    !shouldBypassPendingDeficiency(trimmedBody, lower, pendingDeficiencyDraft)
  ) {
    return handleDeficiencyIntakeTurn({
      db,
      logger,
      runId,
      phoneE164,
      user,
      trimmedBody,
      lower,
      relatedMessageId,
      numMedia,
      effectiveProjectSlug: user.activeProjectSlug || null,
      effectiveProjectName: null,
      logAuthorFields,
      deficiencyRequest: null,
      outboundMeta,
    });
  }
  if (projCmd) {
    const projectAccess = await getAssistantProjectAccess(db, phoneE164, projCmd, user);
    if (!projectAccess.exists) {
      return {
        replyText: `No project "${projCmd}" in the system yet. Ask admin to add projects/${projCmd} in Firestore, then try again.`,
        outboundMeta: { ...outboundMeta, command: "project_missing", projectSlug: null },
      };
    }
    if (!projectAccess.allowed) {
      return {
        replyText: `Project "${projCmd}" is not assigned to this phone number. Use one of your own projects or switch it from the dashboard.`,
        outboundMeta: { ...outboundMeta, command: "project_forbidden", projectSlug: null },
      };
    }
    const patch = buildUserProjectPatch(user, projCmd, {
      activeProjectSlug: projCmd,
    });
    await db.collection(COL_USERS).doc(phoneE164).set({
      ...patch,
      updatedAt: FieldValue.serverTimestamp(),
    }, { merge: true });
    const name = (projectAccess.projectData && projectAccess.projectData.name) || projCmd;
    return {
      replyText: `Active project set to: ${name} (${projCmd}).`,
      outboundMeta: { ...outboundMeta, command: "project_set", projectSlug: projCmd },
    };
  }

  const projectHint = extractProjectScopeHint(trimmedBody);
  let effectiveProjectSlug = user.activeProjectSlug || null;
  let effectiveProjectName = null;
  let scopedBody = trimmedBody;
  if (projectHint.projectSlug) {
    const projectAccess = await getAssistantProjectAccess(
      db,
      phoneE164,
      projectHint.projectSlug,
      user
    );
    if (!projectAccess.exists) {
      return {
        replyText: `Project "${projectHint.projectSlug}" does not exist. Nothing was queued or logged. Use one of your assigned projects.`,
        outboundMeta: {
          ...outboundMeta,
          command: "project_missing",
          projectSlug: null,
        },
      };
    }
    if (!projectAccess.allowed) {
      return {
        replyText: `Project "${projectHint.projectSlug}" is not assigned to this phone number. Nothing was queued or logged.`,
        outboundMeta: {
          ...outboundMeta,
          command: "project_forbidden",
          projectSlug: null,
        },
      };
    }
    effectiveProjectSlug = projectAccess.projectSlug || null;
    effectiveProjectName =
      (projectAccess.projectData && projectAccess.projectData.name) ||
      effectiveProjectSlug;
    scopedBody = projectHint.cleanedText || "";
    if (projectHint.scopeOnly) {
      const patch = buildUserProjectPatch(user, effectiveProjectSlug, {
        activeProjectSlug: effectiveProjectSlug,
      });
      await db.collection(COL_USERS).doc(phoneE164).set({
        ...patch,
        updatedAt: FieldValue.serverTimestamp(),
      }, { merge: true });
      return {
        replyText: `Active project set to: ${effectiveProjectName} (${effectiveProjectSlug}).`,
        outboundMeta: {
          ...outboundMeta,
          command: "project_set",
          projectSlug: effectiveProjectSlug,
        },
      };
    }
  }

  if (effectiveProjectSlug) {
    project = await getProject(db, effectiveProjectSlug);
    if (!effectiveProjectName && project) {
      effectiveProjectName = project.name || effectiveProjectSlug;
    }
  }
  outboundMeta.projectSlug = effectiveProjectSlug;
  userMessageForAI = scopedBody || trimmedBody;
  lower = userMessageForAI.toLowerCase();
  const pendingAssistantFollowUp = normalizePendingAssistantFollowUp(user.pendingAssistantFollowUp);
  const shortAssistantFollowUp =
    Boolean(pendingAssistantFollowUp) &&
    looksLikeAssistantFollowUpAnswer(userMessageForAI) &&
    (
      !pendingAssistantFollowUp.projectSlug ||
      pendingAssistantFollowUp.projectSlug === normalizeProjectSlug(effectiveProjectSlug)
    );
  const correctionPromptActive =
    Boolean(pendingAssistantFollowUp) && looksLikeCorrectionPrompt(pendingAssistantFollowUp.prompt);

  if (shortAssistantFollowUp && correctionPromptActive && isAffirmativeCorrectionFollowUp(userMessageForAI)) {
    await savePendingAssistantFollowUp(
      db,
      phoneE164,
      "Send the corrected manpower like: ALC 17 or: correct manpower for ALC to 17.",
      effectiveProjectSlug
    );
    return {
      replyText: 'Send the corrected manpower like "ALC 17" or "correct manpower for ALC to 17".',
      outboundMeta: { ...outboundMeta, command: "manpower_correction_clarify" },
    };
  }

  if (
    pendingTodoDraft &&
    !shouldBypassPendingTodo(trimmedBody, lower, channel) &&
    looksLikePendingTodoAnswer(pendingTodoDraft, trimmedBody)
  ) {
    return handlePendingTodoTurn({
      db,
      phoneE164,
      user,
      currentMemberAccess,
      trimmedBody,
      lower,
      relatedMessageId,
      logAuthorFields,
      outboundMeta,
    });
  }

  const todoMutation = parseTodoMutationRequest(userMessageForAI, effectiveProjectSlug);
  if (todoMutation) {
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only admin or management phones can update todo items here. Ask admin to approve this phone in Team.",
        outboundMeta: {
          ...outboundMeta,
          command: "todo_update_forbidden",
          projectSlug: todoMutation.projectSlug || null,
        },
      };
    }
    if (todoMutation.projectSlug && !canAccessProject(currentMemberAccess, todoMutation.projectSlug)) {
      return {
        replyText: `This phone can’t update todo items for ${todoMutation.projectSlug}.`,
        outboundMeta: {
          ...outboundMeta,
          command: "todo_update_project_forbidden",
          projectSlug: todoMutation.projectSlug,
        },
      };
    }
    const updated = await applyTodoMutationForPhone({
      db,
      FieldValue,
      phoneE164,
      currentMemberAccess,
      mutation: todoMutation,
    });
    if (!updated.ok) {
      return {
        replyText:
          updated.reason === "ambiguous"
            ? formatTodoMutationAmbiguousReply(todoMutation.targetText, updated.suggestions)
            : truncateSms(
                `Could not find a todo matching "${stripTrailingTodoProjectPhrase(todoMutation.targetText)}".`
              ),
        outboundMeta: {
          ...outboundMeta,
          command: updated.reason === "ambiguous" ? "todo_update_ambiguous" : "todo_update_not_found",
          projectSlug: todoMutation.projectSlug || null,
        },
      };
    }
    const replyText =
      todoMutation.action === "edit"
        ? `Updated todo: ${updated.previousTaskText} -> ${updated.nextTaskText}`
        : todoMutation.action === "completed"
          ? `Closed todo: ${updated.nextTaskText}`
          : todoMutation.action === "open"
            ? `Reopened todo: ${updated.nextTaskText}`
            : `Marked todo in progress: ${updated.nextTaskText}`;
    return {
      replyText: truncateSms(replyText),
      outboundMeta: {
        ...outboundMeta,
        command:
          todoMutation.action === "edit"
            ? "todo_edited"
            : todoMutation.action === "completed"
              ? "todo_closed"
              : todoMutation.action === "open"
                ? "todo_reopened"
                : "todo_inprogress",
        projectSlug: updated.projectSlug || todoMutation.projectSlug || null,
        todoId: updated.todoId,
      },
    };
  }

  const manpowerCorrection =
    parseManpowerCorrectionCommand(userMessageForAI, { allowBarePair: correctionPromptActive }) ||
    (shortAssistantFollowUp && correctionPromptActive ? parseBareManpowerPair(userMessageForAI) : null);
  if (manpowerCorrection) {
    const explicitCorrectionDateKey = extractExplicitReportDate(userMessageForAI).reportDateKey || null;
    const corrected = await applyLatestManpowerCorrectionForSenderProject({
      db,
      FieldValue,
      phoneE164,
      projectSlug: effectiveProjectSlug,
      reportDateKey: explicitCorrectionDateKey,
      correction: manpowerCorrection,
    });
    if (corrected) {
      if (pendingAssistantFollowUp) {
        await clearPendingAssistantFollowUp(db, phoneE164);
      }
      return {
        replyText: truncateSms(
          `Corrected manpower for ${manpowerCorrection.trade} to ${manpowerCorrection.workers}${
            corrected.reportDateKey ? ` on ${corrected.reportDateKey}` : ""
          }.`
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, {
            stage: "deterministic",
            action: "edit_log",
            confidence: 0.99,
            reason: "Matched manpower correction command.",
            source: "deterministic",
            matchedBy: "parseManpowerCorrectionCommand",
          }),
          command: "manpower_corrected",
          logEntryId: corrected.logEntryId,
          logCategory: "note",
          reportDateKey: corrected.reportDateKey,
        },
      };
    }
    return {
      replyText: truncateSms(
        `Could not find a manpower entry for ${manpowerCorrection.trade} to correct${
          explicitCorrectionDateKey ? ` on ${explicitCorrectionDateKey}` : ""
        }. Send the full manpower line again if needed.`
      ),
      outboundMeta: {
        ...outboundMeta,
        command: "manpower_correction_not_found",
        reportDateKey: explicitCorrectionDateKey,
      },
    };
  }

  const narrativeCorrection = parseNarrativeCorrectionCommand(userMessageForAI, {
    allowShortNotForm: correctionPromptActive,
  });
  if (narrativeCorrection) {
    const explicitCorrectionDateKey = extractExplicitReportDate(userMessageForAI).reportDateKey || null;
    const corrected = await applyLatestNarrativeCorrectionForSenderProject({
      db,
      FieldValue,
      phoneE164,
      projectSlug: effectiveProjectSlug,
      reportDateKey: explicitCorrectionDateKey,
      correction: narrativeCorrection,
    });
    if (corrected) {
      if (pendingAssistantFollowUp) {
        await clearPendingAssistantFollowUp(db, phoneE164);
      }
      return {
        replyText: truncateSms(
          `Corrected saved log${corrected.reportDateKey ? ` for ${corrected.reportDateKey}` : ""}: ${corrected.updatedText}`
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, {
            stage: "deterministic",
            action: "edit_log",
            confidence: 0.99,
            reason: "Matched narrative correction command.",
            source: "deterministic",
            matchedBy: "parseNarrativeCorrectionCommand",
          }),
          command: "narrative_correction_applied",
          projectSlug: effectiveProjectSlug || null,
          logEntryId: corrected.logEntryId,
          logCategory: "note",
          reportDateKey: corrected.reportDateKey,
          correctionTarget: corrected.target,
          correctionReplacement: corrected.replacement,
        },
      };
    }
    return {
      replyText: truncateSms(
        `Could not find a saved log entry to correct from "${narrativeCorrection.target}" to "${narrativeCorrection.replacement}"${
          explicitCorrectionDateKey ? ` on ${explicitCorrectionDateKey}` : ""
        }. Re-send the full corrected log if needed.`
      ),
      outboundMeta: {
        ...outboundMeta,
        command: "narrative_correction_not_found",
        projectSlug: effectiveProjectSlug || null,
        reportDateKey: explicitCorrectionDateKey,
      },
    };
  }

  const dailyReportRequest = parseDailyReportRequest(userMessageForAI);
  if (dailyReportRequest && dailyReportRequest.invalidReason) {
    return {
      replyText: truncateSms(
        `${dailyReportRequest.invalidReason} Try: "daily report", "daily report yesterday", "daily report home", or "daily report journal home 2026-04-10".`
      ),
      outboundMeta: {
        ...outboundMeta,
        command: "daily_pdf_request_invalid",
      },
    };
  }
  if (
    dailyReportRequest &&
    dailyReportRequest.projectSlug &&
    dailyReportRequest.projectSlug !== effectiveProjectSlug
  ) {
    const projectAccess = await getAssistantProjectAccess(
      db,
      phoneE164,
      dailyReportRequest.projectSlug,
      user
    );
    if (!projectAccess.exists) {
      return {
        replyText: `Project "${dailyReportRequest.projectSlug}" does not exist. Nothing was queued or logged.`,
        outboundMeta: {
          ...outboundMeta,
          command: "project_missing",
          projectSlug: null,
        },
      };
    }
    if (!projectAccess.allowed) {
      return {
        replyText: `Project "${dailyReportRequest.projectSlug}" is not assigned to this phone number. Nothing was queued or logged.`,
        outboundMeta: {
          ...outboundMeta,
          command: "project_forbidden",
          projectSlug: null,
        },
      };
    }
    effectiveProjectSlug = projectAccess.projectSlug || null;
    effectiveProjectName =
      (projectAccess.projectData && projectAccess.projectData.name) ||
      effectiveProjectSlug;
    project = effectiveProjectSlug ? await getProject(db, effectiveProjectSlug) : null;
    outboundMeta.projectSlug = effectiveProjectSlug;
  }

  if (
    deficiencyRequest ||
    (pendingDeficiencyDraft &&
      !shouldBypassPendingDeficiency(trimmedBody, lower, pendingDeficiencyDraft))
  ) {
    return handleDeficiencyIntakeTurn({
      db,
      logger,
      runId,
      phoneE164,
      user,
      trimmedBody,
      lower,
      relatedMessageId,
      numMedia,
      effectiveProjectSlug,
      effectiveProjectName,
      logAuthorFields,
      deficiencyRequest,
      outboundMeta,
    });
  }

  if (lower === "reset" || lower === "reset conversation" || lower === "reset context") {
    await db.collection(COL_USERS).doc(phoneE164).update({
      contextResetAt: FieldValue.serverTimestamp(),
      pendingDeficiencyIntake: FieldValue.delete(),
      pendingTodoIntake: FieldValue.delete(),
      pendingAssistantFollowUp: FieldValue.delete(),
      updatedAt: FieldValue.serverTimestamp(),
    });
    return {
      replyText: "Conversation context cleared for this number. Older texts won't shape the next replies.",
      outboundMeta: { ...outboundMeta, command: "reset" },
    };
  }

  if (lower === "status") {
    const p = user.activeProjectSlug || "none";
    const projectCount = getUserProjectSlugs(user).length;
    const role = user.role || "not set";
    const activeTimer = user.pendingTimer && Number(user.pendingTimer.startedAtMs) > 0
      ? `timer=${String(user.pendingTimer.label || "general task")} (${formatDurationFromMs(Date.now() - Number(user.pendingTimer.startedAtMs))})`
      : "timer=none";
    return {
      replyText: `Status: project=${p}, projects=${projectCount}, role=${role}, ${activeTimer}. Text project <slug> to switch. Text help for more.`,
      outboundMeta: { ...outboundMeta, command: "status" },
    };
  }

  if (lower === "contact" || lower === "contacts") {
    if (project && project.contactsText) {
      return {
        replyText: truncateSms("Contacts:\n" + project.contactsText),
        outboundMeta: { ...outboundMeta, command: "contact" },
      };
    }
    return {
      replyText: "No contacts on file for this project. Admin can add contactsText on the project doc.",
      outboundMeta: { ...outboundMeta, command: "contact" },
    };
  }

  const notifyRequest = parseNotificationRequest(userMessageForAI, effectiveProjectSlug);
  if (notifyRequest) {
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only management can send broadcast notifications. Ask admin to approve your phone in Team.",
        outboundMeta: {
          ...outboundMeta,
          command: "notify_forbidden",
        },
      };
    }
    if (
      notifyRequest.audience === "project_users" &&
      !canAccessProject(currentMemberAccess, notifyRequest.projectSlug)
    ) {
      return {
        replyText: `You cannot notify project ${notifyRequest.projectSlug} because it is not assigned to your account.`,
        outboundMeta: {
          ...outboundMeta,
          command: "notify_project_forbidden",
          projectSlug: notifyRequest.projectSlug,
        },
      };
    }
    return {
      replyText: truncateSms(
        notifyRequest.audience === "management"
          ? `Sending your update to management: ${notifyRequest.messageBody}`
          : `Sending your update to all users on ${notifyRequest.projectSlug}: ${notifyRequest.messageBody}`
      ),
      outboundMeta: {
        ...outboundMeta,
        command: notifyRequest.audience === "management" ? "notify_management" : "notify_project_users",
        projectSlug: notifyRequest.projectSlug || outboundMeta.projectSlug || null,
        notifyRequest: {
          audience: notifyRequest.audience,
          projectSlug: notifyRequest.projectSlug || null,
          messageBody: notifyRequest.messageBody,
          requestedByPhone: phoneE164,
          requestedByName: logAuthorFields.authorName || null,
          requestedByEmail: logAuthorFields.authorEmail || null,
        },
      },
    };
  }

  const todoReportRequest = parseTodoReportRequest(userMessageForAI);
  if (todoReportRequest) {
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only admin or management phones can generate todo reports. Ask admin to approve this phone in Team.",
        outboundMeta: {
          ...outboundMeta,
          command: "todo_report_forbidden",
          projectSlug: HOME_TODO_PROJECT_SLUG,
        },
      };
    }
    if (!canAccessProject(currentMemberAccess, HOME_TODO_PROJECT_SLUG)) {
      return {
        replyText: `This phone can’t generate todo reports for ${HOME_TODO_PROJECT_SLUG}.`,
        outboundMeta: {
          ...outboundMeta,
          command: "todo_report_project_forbidden",
          projectSlug: HOME_TODO_PROJECT_SLUG,
        },
      };
    }
    return {
      replyText:
        todoReportRequest.format === "pdf"
          ? "OK. Generating your todo PDF report now. You will get a download link shortly."
          : "OK. Generating your todo Excel report now. You will get a download link shortly.",
      outboundMeta: {
        ...outboundMeta,
        command: todoReportRequest.format === "pdf" ? "todo_report_pdf" : "todo_report_excel",
        projectSlug: HOME_TODO_PROJECT_SLUG,
        todoReportRequested: true,
        todoReportFormat: todoReportRequest.format,
      },
    };
  }

  const todoListRequest = parseTodoListRequest(userMessageForAI, effectiveProjectSlug);
  if (todoListRequest) {
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only admin or management phones can view todo items here. Ask admin to approve this phone in Team.",
        outboundMeta: {
          ...outboundMeta,
          command: "todo_list_forbidden",
          projectSlug: todoListRequest.projectSlug || null,
        },
      };
    }
    if (
      todoListRequest.projectSlug &&
      !canAccessProject(currentMemberAccess, todoListRequest.projectSlug)
    ) {
      return {
        replyText: `This phone can’t view todo items for ${todoListRequest.projectSlug}.`,
        outboundMeta: {
          ...outboundMeta,
          command: "todo_list_project_forbidden",
          projectSlug: todoListRequest.projectSlug,
        },
      };
    }
    const todos = await loadTodoListForPhone(db, phoneE164, todoListRequest);
    return {
      replyText: truncateSms(formatTodoListReply({ todos, request: todoListRequest })),
      outboundMeta: {
        ...outboundMeta,
        command: "todo_list_view",
        projectSlug: todoListRequest.projectSlug || null,
        todoListStatus: todoListRequest.status,
        todoListPriority: todoListRequest.priority || null,
        todoListTags: todoListRequest.tags,
        todoListCount: todos.length,
      },
    };
  }

  const homeTodoCommand = parseHomeTodoCommand(userMessageForAI);
  if (homeTodoCommand) {
    if (homeTodoCommand.error) {
      return {
        replyText: homeTodoCommand.error,
        outboundMeta: { ...outboundMeta, command: "home_todo_invalid" },
      };
    }
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only admin or management phones can add home todo items. Ask admin to approve this phone in Team.",
        outboundMeta: {
          ...outboundMeta,
          command: "home_todo_forbidden",
          projectSlug: HOME_TODO_PROJECT_SLUG,
        },
      };
    }
    if (!canAccessProject(currentMemberAccess, HOME_TODO_PROJECT_SLUG)) {
      return {
        replyText: `This phone can’t add todo items for ${HOME_TODO_PROJECT_SLUG}.`,
        outboundMeta: {
          ...outboundMeta,
          command: "home_todo_project_forbidden",
          projectSlug: HOME_TODO_PROJECT_SLUG,
        },
      };
    }
    await savePendingTodoDraft(db, phoneE164, {
      projectSlug: HOME_TODO_PROJECT_SLUG,
      projectName: "home",
      taskText: homeTodoCommand.taskText,
      sourceText: homeTodoCommand.rawText,
      dueBy: null,
      dueDateCaptured: false,
      reminderRequested: null,
      secondReminderWanted: null,
      reminders: [],
      priority: null,
      priorityCaptured: false,
      tags: homeTodoCommand.tags || [],
      tagsCaptured: false,
      sourceMessageId: relatedMessageId || null,
    });
    return {
      replyText: truncateSms(todoFieldPrompt("dueBy", { taskText: homeTodoCommand.taskText })),
      outboundMeta: withRoutingDecision(
        {
          ...outboundMeta,
          command: "todo_intake_started",
          projectSlug: HOME_TODO_PROJECT_SLUG,
          pendingTodoIntake: true,
        },
        {
          stage: "deterministic",
          action: "create_todo",
          confidence: 0.99,
          reason: 'Matched "xxx" home todo command.',
          source: "deterministic",
          matchedBy: "parseHomeTodoCommand",
        }
      ),
    };
  }

  const notesUpdate = parseProjectNotesUpdateCommand(userMessageForAI);
  if (notesUpdate) {
    if (!effectiveProjectSlug) {
      return {
        replyText: "Set a project first, then text: update project notes: your updated notes here",
        outboundMeta: { ...outboundMeta, command: "project_notes_missing_project" },
      };
    }
    const memberAccess = await findActiveAppMemberByApprovedPhone(db, phoneE164);
    if (!memberAccess) {
      return {
        replyText: "This phone is not approved for SMS project note updates. Ask admin to approve this number on your app member.",
        outboundMeta: { ...outboundMeta, command: "project_notes_phone_unapproved" },
      };
    }
    if (!canAccessProject(memberAccess, effectiveProjectSlug)) {
      return {
        replyText: `This phone can’t update notes for ${effectiveProjectSlug}. Switch to one of your assigned projects first.`,
        outboundMeta: { ...outboundMeta, command: "project_notes_forbidden", projectSlug: effectiveProjectSlug },
      };
    }
    const projectRef = db.collection(COL_PROJECTS).doc(effectiveProjectSlug);
    const projectSnap = await projectRef.get();
    if (!projectSnap.exists) {
      return {
        replyText: `Project "${effectiveProjectSlug}" was not found.`,
        outboundMeta: { ...outboundMeta, command: "project_missing", projectSlug: null },
      };
    }
    const projectData = projectSnap.data() || {};
    const currentNotes = String(projectData.notes || "")
      .replace(/\r\n/g, "\n")
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .trim()
      .slice(0, 8000);
    if (canApproveProjectNoteRequests(memberAccess)) {
      await projectRef.set(
        {
          notes: notesUpdate.proposedNotes,
          updatedAt: FieldValue.serverTimestamp(),
          notesUpdatedAt: FieldValue.serverTimestamp(),
          notesUpdatedByEmail: memberAccess.email,
          notesUpdatedByPhone: phoneE164,
        },
        { merge: true }
      );
      return {
        replyText: `Project notes updated for ${projectData.name || effectiveProjectSlug}.`,
        outboundMeta: {
          ...outboundMeta,
          command: "project_notes_updated",
          projectSlug: effectiveProjectSlug,
        },
      };
    }
    const requestRef = db.collection(COL_PROJECT_NOTE_EDIT_REQUESTS).doc();
    await requestRef.set({
      type: "projectNotes",
      status: "pending",
      source: "sms",
      projectSlug: effectiveProjectSlug,
      projectName: projectData.name || effectiveProjectSlug,
      currentNotes,
      proposedNotes: notesUpdate.proposedNotes,
      requesterComment: "Submitted by SMS",
      requestedByEmail: memberAccess.email,
      requestedByName: String(memberAccess.memberData?.displayName || memberAccess.email || "").trim(),
      requestedByRole: memberAccess.role,
      requestedByPhone: phoneE164,
      reportId: null,
      reportTitle: null,
      reportDateKey: null,
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    });
    return {
      replyText: `Project note update submitted for ${projectData.name || effectiveProjectSlug}. Request ${requestRef.id} is pending approval.`,
      outboundMeta: {
        ...outboundMeta,
        command: "project_notes_request_submitted",
        projectSlug: effectiveProjectSlug,
      },
    };
  }

  const lookaheadActivitiesQuery = shortAssistantFollowUp
    ? null
    : parseLookaheadActivitiesQuery(userMessageForAI);
  if (lookaheadActivitiesQuery) {
    if (!effectiveProjectSlug) {
      return {
        replyText: "Set a project first, then ask for activities by trade for this week or next week.",
        outboundMeta: { ...outboundMeta, command: "lookahead_query_missing_project" },
      };
    }
    const snapshot = await loadLatestLookaheadSnapshot({
      db,
      projectSlug: effectiveProjectSlug,
    });
    if (!snapshot || !Array.isArray(snapshot.tasks) || !snapshot.tasks.length) {
      return {
        replyText: `No saved lookahead schedule was found for ${effectiveProjectName || effectiveProjectSlug} yet.`,
        outboundMeta: {
          ...outboundMeta,
          command: "lookahead_query_no_snapshot",
          projectSlug: effectiveProjectSlug,
        },
      };
    }

    const range = getDateKeyWindowForLookaheadRange(lookaheadActivitiesQuery.range, new Date());
    const matchingTasks = snapshot.tasks
      .filter((task) => taskIntersectsLookaheadWindow(task, range.startKey, range.endKey))
      .filter((task) => taskMatchesTradeQuery(task, lookaheadActivitiesQuery.tradeQuery))
      .sort((a, b) => {
        const aRange = taskRelevantDateRange(a, range.startKey, range.endKey);
        const bRange = taskRelevantDateRange(b, range.startKey, range.endKey);
        if (aRange.startKey !== bRange.startKey) return aRange.startKey.localeCompare(bRange.startKey);
        return normalizeLookaheadActivityLabel(a.activity).localeCompare(
          normalizeLookaheadActivityLabel(b.activity)
        );
      });

    return {
      replyText: truncateSms(
        formatLookaheadActivitiesReply({
          projectName: effectiveProjectName || effectiveProjectSlug,
          tradeQuery: lookaheadActivitiesQuery.tradeQuery,
          rangeLabel: range.label,
          startKey: range.startKey,
          endKey: range.endKey,
          tasks: matchingTasks,
        })
      ),
      outboundMeta: {
        ...withRoutingDecision(outboundMeta, {
          stage: "deterministic",
          action: "lookahead_trade_query",
          confidence: 0.99,
          reason: "Matched lookahead activities query.",
          source: "deterministic",
          matchedBy: "parseLookaheadActivitiesQuery",
        }),
        command: "lookahead_trade_query",
        projectSlug: effectiveProjectSlug,
        reportStartKey: range.startKey,
        reportEndKey: range.endKey,
        range: lookaheadActivitiesQuery.range,
        trade: lookaheadActivitiesQuery.tradeQuery || null,
        taskCount: matchingTasks.length,
        lookaheadSnapshotId: snapshot.id || null,
      },
    };
  }

  const managementLabourTotalsQuery = shortAssistantFollowUp
    ? null
    : parseManagementLabourTotalsQuery(userMessageForAI);
  const managementLabourBreakdownQuery = shortAssistantFollowUp
    ? null
    : parseManagementLabourBreakdownQuery(userMessageForAI);
  if (managementLabourBreakdownQuery) {
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only admin or management phones can view labour breakdowns here. Ask admin to approve this phone in Team.",
        outboundMeta: {
          ...outboundMeta,
          command: "labour_totals_forbidden",
        },
      };
    }
    const range = getDateKeyRangeForBalanceQuery(managementLabourBreakdownQuery.range);
    if (!range || !range.startKey || !range.endKey) {
      return {
        replyText: "Could not look up that hours range. Try: total hours by project this pay period.",
        outboundMeta: { ...outboundMeta, command: "labour_totals_error" },
      };
    }
    const requestedProjectSlug =
      managementLabourBreakdownQuery.projectScope === "this_project"
        ? normalizeProjectSlug(effectiveProjectSlug)
        : normalizeProjectSlug(managementLabourBreakdownQuery.projectScope);
    if (requestedProjectSlug && !canAccessProject(currentMemberAccess, requestedProjectSlug)) {
      return {
        replyText: `This phone can’t view labour totals for ${requestedProjectSlug}.`,
        outboundMeta: { ...outboundMeta, command: "labour_totals_project_forbidden", projectSlug: requestedProjectSlug },
      };
    }
    const scopedEntries = await buildScopedManagementLabourEntries(
      range,
      managementLabourBreakdownQuery.projectScope
    );
    const keyFn =
      managementLabourBreakdownQuery.groupBy === "project"
        ? (entry) => String(entry && entry.projectSlug ? entry.projectSlug : "").trim().toLowerCase() || "unassigned"
        : (entry) =>
            String(entry && (entry.labourerName || entry.labourerPhone) ? entry.labourerName || entry.labourerPhone : "")
              .trim() || "Unknown";
    const totals = new Map();
    for (const entry of scopedEntries) {
      const key = keyFn(entry);
      totals.set(key, (totals.get(key) || 0) + Number(entry.hours || 0));
    }
    const items = [...totals.entries()]
      .map(([label, totalHours]) => ({
        label,
        totalHours: Math.round(Number(totalHours || 0) * 100) / 100,
      }))
      .sort((a, b) => {
        if (b.totalHours !== a.totalHours) return b.totalHours - a.totalHours;
        return a.label.localeCompare(b.label);
      });
    return {
      replyText: truncateSms(
        formatManagementLabourBreakdownReply({
          groupBy: managementLabourBreakdownQuery.groupBy,
          rangeLabel: range.label,
          startKey: range.startKey,
          endKey: range.endKey,
          items,
        })
      ),
      outboundMeta: {
        ...outboundMeta,
        command:
          managementLabourBreakdownQuery.groupBy === "project"
            ? "management_labour_totals_by_project"
            : "management_labour_totals_by_labourer",
        reportStartKey: range.startKey,
        reportEndKey: range.endKey,
        range: managementLabourBreakdownQuery.range,
        groupBy: managementLabourBreakdownQuery.groupBy,
        projectSlug: requestedProjectSlug || null,
      },
    };
  }
  if (managementLabourTotalsQuery) {
    if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
      return {
        replyText:
          "Only admin or management phones can view labour totals for all labourers. Ask admin to approve this phone in Team.",
        outboundMeta: {
          ...outboundMeta,
          command: "labour_totals_forbidden",
        },
      };
    }
    const range = getDateKeyRangeForBalanceQuery(managementLabourTotalsQuery.range);
    if (!range || !range.startKey || !range.endKey) {
      return {
        replyText: "Could not look up that hours range. Try: total hours for all labourers today or this pay period.",
        outboundMeta: { ...outboundMeta, command: "labour_totals_error" },
      };
    }
    const requestedProjectSlug =
      managementLabourTotalsQuery.projectScope === "this_project"
        ? normalizeProjectSlug(effectiveProjectSlug)
        : normalizeProjectSlug(managementLabourTotalsQuery.projectScope);
    if (requestedProjectSlug && !canAccessProject(currentMemberAccess, requestedProjectSlug)) {
      return {
        replyText: `This phone can’t view labour totals for ${requestedProjectSlug}.`,
        outboundMeta: { ...outboundMeta, command: "labour_totals_project_forbidden", projectSlug: requestedProjectSlug },
      };
    }
    const scopedEntriesRaw = await buildScopedManagementLabourEntries(
      range,
      managementLabourTotalsQuery.projectScope
    );
    const scopedEntries = filterLabourEntriesByLabourerQuery(
      scopedEntriesRaw,
      managementLabourTotalsQuery.labourerQuery
    );
    const summary = buildLabourRollup(scopedEntries);
    const byProject = new Map();
    for (const entry of scopedEntries) {
      const projectSlug =
        String(entry && entry.projectSlug ? entry.projectSlug : "").trim().toLowerCase() || "unassigned";
      const entryHours = Number(entry && entry.hours);
      byProject.set(projectSlug, (byProject.get(projectSlug) || 0) + (Number.isFinite(entryHours) ? entryHours : 0));
    }
    const projectTotals = [...byProject.entries()]
      .map(([projectSlug, totalHours]) => ({
        projectSlug,
        totalHours: Math.round(Number(totalHours || 0) * 100) / 100,
      }))
      .sort((a, b) => {
        if (b.totalHours !== a.totalHours) return b.totalHours - a.totalHours;
        return a.projectSlug.localeCompare(b.projectSlug);
      });
    return {
      replyText: truncateSms(
        formatManagementLabourTotalsReply({
          scopeLabel: managementLabourTotalsQuery.labourerQuery
            ? `Labourer ${managementLabourTotalsQuery.labourerQuery}`
            : "All labourers",
          rangeLabel: range.label,
          startKey: range.startKey,
          endKey: range.endKey,
          totalHours: summary.totalHours,
          totalEntries: summary.totalEntries,
          labourerCount: summary.labourerTotals.length,
          projectCount: projectTotals.length,
          projectTotals,
        })
      ),
      outboundMeta: {
        ...outboundMeta,
        command: "management_labour_totals",
        reportStartKey: range.startKey,
        reportEndKey: range.endKey,
        range: managementLabourTotalsQuery.range,
        totalEntries: summary.totalEntries,
        totalHours: summary.totalHours,
        labourerCount: summary.labourerTotals.length,
        projectCount: projectTotals.length,
        projectSlug: requestedProjectSlug || null,
        labourerQuery: managementLabourTotalsQuery.labourerQuery || null,
      },
    };
  }

  const hoursBalanceQuery = shortAssistantFollowUp ? null : parseLabourHoursBalanceQuery(userMessageForAI);
  if (hoursBalanceQuery) {
    const labourer = await findActiveLabourerByPhone(db, phoneE164);
    if (!labourer) {
      if (isExplicitLabourBalanceText(userMessageForAI)) {
        return {
          replyText:
            "This phone is not registered as a labourer yet. Ask the office to add your name and phone on the Labour page.",
          outboundMeta: { ...outboundMeta, command: "labourer_phone_unregistered" },
        };
      }
    } else {
      const range = getDateKeyRangeForBalanceQuery(hoursBalanceQuery.range);
      if (!range || !range.startKey || !range.endKey) {
        return {
          replyText: "Could not look up that hours range. Try: how many hours today, this week, or this pay period.",
          outboundMeta: { ...outboundMeta, command: "labour_hours_balance_error" },
        };
      }
      const entries = await loadLabourEntries(db, {
        startKey: range.startKey,
        endKey: range.endKey,
        labourerPhone: phoneE164,
      });
      const summary = buildLabourRollup(entries);
      const labourerName =
        labourer.displayName ||
        String((labourer.labourerData && labourer.labourerData.name) || "").trim() ||
        phoneE164;
      return {
        replyText: truncateSms(
          formatLabourBalanceReply({
            labourerName,
            rangeLabel: range.label,
            startKey: range.startKey,
            endKey: range.endKey,
            totalHours: summary.totalHours,
            totalPaidHours: summary.totalPaidHours,
            totalEntries: summary.totalEntries,
          })
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, {
            stage: "deterministic",
            action: "labour_balance",
            confidence: 0.99,
            reason: "Matched labour balance query.",
            source: "deterministic",
            matchedBy: "parseLabourHoursBalanceQuery",
          }),
          command: "labour_hours_balance",
          reportStartKey: range.startKey,
          reportEndKey: range.endKey,
          range: hoursBalanceQuery.range,
          totalEntries: summary.totalEntries,
          totalHours: summary.totalHours,
          totalPaidHours: summary.totalPaidHours,
        },
      };
    }
  }

  const labourCorrectionCommand = shortAssistantFollowUp ? null : parseLabourCorrectionCommand(userMessageForAI);
  if (labourCorrectionCommand) {
    const labourer = await findActiveLabourerByPhone(db, phoneE164);
    if (!labourer) {
      return {
        replyText:
          "This phone is not registered as a labourer yet. Ask the office to add your name and phone on the Labour page.",
        outboundMeta: { ...outboundMeta, command: "labourer_phone_unregistered" },
      };
    }
    const labourerName =
      labourer.displayName ||
      String(labourer.labourerData && labourer.labourerData.name ? labourer.labourerData.name : "").trim() ||
      phoneE164;
    const correctionDateValidation = validateLabourReportDateKey(labourCorrectionCommand.reportDateKey, new Date());
    if (!correctionDateValidation.ok) {
      return {
        replyText: truncateSms(formatInvalidLabourDateReply(labourerName, correctionDateValidation)),
        outboundMeta: {
          ...outboundMeta,
          command: "labour_entry_invalid_date",
          labourerName,
          labourerPhone: phoneE164,
          reportDateKey: labourCorrectionCommand.reportDateKey,
          suggestedDateKey: correctionDateValidation.suggestedDateKey || null,
          reason: correctionDateValidation.reason || null,
        },
      };
    }
    const existingForDate = await loadLabourEntries(db, {
      startKey: labourCorrectionCommand.reportDateKey,
      endKey: labourCorrectionCommand.reportDateKey,
      labourerPhone: phoneE164,
    });
    if (existingForDate.length < 1) {
      return {
        replyText: truncateSms(
          `${labourerName}, I could not find your hours entry for ${labourCorrectionCommand.reportDateKey} to correct.`
        ),
        outboundMeta: {
          ...outboundMeta,
          command: "labour_correction_missing_entry",
          labourerName,
          labourerPhone: phoneE164,
          reportDateKey: labourCorrectionCommand.reportDateKey,
        },
      };
    }

    const targetEntry = existingForDate[existingForDate.length - 1];
    const nextWorkOn = labourCorrectionCommand.workOn || String(targetEntry.workOn || "").trim();
    const nextNotes = labourCorrectionCommand.workOn
      ? labourCorrectionCommand.rawText
      : String(targetEntry.notes || "").trim();
    await db.collection("labourEntries").doc(String(targetEntry.id)).update({
      minutesWorked: labourMinutesFromHours(labourCorrectionCommand.hours),
      hours: FieldValue.delete(),
      workOn: nextWorkOn,
      notes: nextNotes,
      updatedAt: FieldValue.serverTimestamp(),
    });

    const correctedHours = formatLabourHoursShort(labourCorrectionCommand.hours);
    const isTodayCorrection = labourCorrectionCommand.reportDateKey === dateKeyEastern(new Date());
    return {
      replyText: truncateSms(
        `OK. Corrected your hours for ${isTodayCorrection ? "today" : labourCorrectionCommand.reportDateKey} to ${correctedHours}h. Your total for ${
          isTodayCorrection ? "today" : labourCorrectionCommand.reportDateKey
        } is now ${correctedHours}h.`
      ),
      outboundMeta: {
        ...withRoutingDecision(outboundMeta, {
          stage: "deterministic",
          action: "labour_entry_corrected",
          confidence: 0.99,
          reason: "Matched labour correction command.",
          source: "deterministic",
          matchedBy: "parseLabourCorrectionCommand",
        }),
        command: "labour_entry_corrected",
        labourerName,
        labourerPhone: phoneE164,
        reportDateKey: labourCorrectionCommand.reportDateKey,
        correctedHours: labourCorrectionCommand.hours,
        labourEntryId: targetEntry.id || null,
      },
    };
  }

  const labourEntryCommand = shortAssistantFollowUp ? null : parseLabourEntryCommand(userMessageForAI);
  if (labourEntryCommand) {
    const labourer = await findActiveLabourerByPhone(db, phoneE164);
    if (!labourer) {
      if (isExplicitLabourEntryText(userMessageForAI)) {
        return {
          replyText:
            "This phone is not registered as a labourer yet. Ask the office to add your name and phone on the Labour page.",
          outboundMeta: { ...outboundMeta, command: "labourer_phone_unregistered" },
        };
      }
    } else {
      const labourProject =
        effectiveProjectSlug ||
        normalizeProjectSlug(
          labourer.labourerData && labourer.labourerData.activeProjectSlug
            ? labourer.labourerData.activeProjectSlug
            : (Array.isArray(labourer.projectSlugs) && labourer.projectSlugs[0]) || ""
        ) ||
        null;
      const labourerName =
        labourer.displayName ||
        String(labourer.labourerData && labourer.labourerData.name ? labourer.labourerData.name : "").trim() ||
        phoneE164;
      const reportDateKey = labourEntryCommand.reportDateKey || dateKeyEastern(new Date());
      const dateValidation = validateLabourReportDateKey(reportDateKey, new Date());
      if (!dateValidation.ok) {
        return {
          replyText: truncateSms(formatInvalidLabourDateReply(labourerName, dateValidation)),
          outboundMeta: {
            ...outboundMeta,
            command: "labour_entry_invalid_date",
            labourerName,
            labourerPhone: phoneE164,
            reportDateKey,
            suggestedDateKey: dateValidation.suggestedDateKey || null,
            reason: dateValidation.reason || null,
          },
        };
      }
      const existingForDate = await loadLabourEntries(db, {
        startKey: reportDateKey,
        endKey: reportDateKey,
        labourerPhone: phoneE164,
      });
      if (existingForDate.length > 0) {
        return {
          replyText: truncateSms(
            `${labourerName}, you already entered your hours for ${reportDateKey}. One labour entry is allowed per day.`
          ),
          outboundMeta: {
            ...outboundMeta,
            command: "labour_entry_duplicate",
            labourerName,
            labourerPhone: phoneE164,
            reportDateKey,
            existingLabourEntryId: existingForDate[0]?.id || null,
          },
        };
      }
      const entry = await writeLabourEntry(db, FieldValue, {
        labourerName,
        labourerPhone: phoneE164,
        projectSlug: labourProject,
        reportDateKey,
        hours: labourEntryCommand.hours,
        workOn: labourEntryCommand.workOn,
        notes: labourEntryCommand.rawText,
        source: "sms",
        enteredByPhone: phoneE164,
      });
      const payMult = dayMultiplierFromDateKey(String(entry.reportDateKey || "").trim());
      const payHours = Math.round(labourEntryCommand.hours * payMult * 100) / 100;
      const payNote =
        payMult !== 1
          ? ` → ${payHours}h paid (${payMult === 2 ? "Sun 2x" : "Sat/holiday 1.5x"})`
          : "";
      return {
        replyText: truncateSms(
          `Saved ${labourEntryCommand.hours}h${payNote} for ${labourerName}${
            labourProject ? ` on ${labourProject}` : ""
          }: ${labourEntryCommand.workOn}`
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, {
            stage: "deterministic",
            action: "labour_entry",
            confidence: 0.99,
            reason: "Matched labour entry command.",
            source: "deterministic",
            matchedBy: "parseLabourHoursCommand",
          }),
          command: "labour_entry_saved",
          labourEntryId: entry.labourEntryId,
          labourerName,
          labourerPhone: phoneE164,
          projectSlug: labourProject,
          reportDateKey: entry.reportDateKey || null,
        },
      };
    }
  }

  const startTimerCommand = parseStartTimerCommand(userMessageForAI);
  if (startTimerCommand) {
    const startedAtMs = Date.now();
    const timerPayload = {
      label: startTimerCommand.label,
      startedAtMs,
      startedAtIso: new Date(startedAtMs).toISOString(),
      projectSlug: effectiveProjectSlug || null,
      projectName: effectiveProjectName || null,
    };
    await db.collection(COL_USERS).doc(phoneE164).set(
      {
        pendingTimer: timerPayload,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    return {
      replyText: truncateSms(
        `Timer started for ${startTimerCommand.label}.${effectiveProjectSlug ? ` Project ${effectiveProjectSlug}.` : ""} Text "stop timer" when done.`
      ),
      outboundMeta: {
        ...outboundMeta,
        command: "timer_started",
        projectSlug: effectiveProjectSlug || null,
      },
    };
  }

  if (isStopTimerCommand(userMessageForAI)) {
    const activeTimer = user.pendingTimer && Number(user.pendingTimer.startedAtMs) > 0
      ? user.pendingTimer
      : null;
    if (!activeTimer) {
      return {
        replyText: 'No active timer. Text "start timer for <task>" first.',
        outboundMeta: {
          ...outboundMeta,
          command: "timer_stop_without_active",
          projectSlug: effectiveProjectSlug || null,
        },
      };
    }
    const stopAtMs = Date.now();
    const durationMs = Math.max(0, stopAtMs - Number(activeTimer.startedAtMs || 0));
    const durationMinutes = Math.round(durationMs / 60000);
    const timerProjectSlug = normalizeProjectSlug(activeTimer.projectSlug) || effectiveProjectSlug || null;
    const timerLabel = String(activeTimer.label || "general task").trim() || "general task";

    const timerLogText = `Timer: ${timerLabel} · Start ${String(activeTimer.startedAtIso || "-")} · Stop ${new Date(stopAtMs).toISOString()} · Duration ${formatDurationFromMs(durationMs)} (${durationMinutes}m).`;
    const timerLog = await writeLogEntry(db, FieldValue, {
      phoneE164,
      ...logAuthorFields,
      projectSlug: timerProjectSlug,
      reportDateKey: dateKeyEastern(new Date(stopAtMs)),
      rawText: timerLogText,
      normalizedText: timerLogText,
      category: "note",
      subtype: "timer",
      tags: ["timer", "time_tracking"],
      sourceMessageId: relatedMessageId || null,
      status: "closed",
    });

    await db.collection(COL_USERS).doc(phoneE164).set(
      {
        pendingTimer: FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return {
      replyText: truncateSms(
        `Timer stopped for ${timerLabel}. Duration: ${formatDurationFromMs(durationMs)} (${durationMinutes}m). Logged to daily notes.`
      ),
      outboundMeta: {
        ...outboundMeta,
        command: "timer_stopped",
        projectSlug: timerProjectSlug,
        logEntryId: timerLog.logEntryId,
        logCategory: "note",
      },
    };
  }

  if (dailyReportRequest) {
    const requestDateKey = dailyReportRequest.reportDateKey || null;
    const requestType = dailyReportRequest.reportType || "dailySiteLog";
    const scopeBits = [];
    if (effectiveProjectSlug) scopeBits.push(effectiveProjectName || effectiveProjectSlug);
    if (requestDateKey) scopeBits.push(requestDateKey);
    if (requestType === "journal") scopeBits.push("journal");
    const scopeText = scopeBits.length ? ` (${scopeBits.join(" · ")})` : "";
    return {
      replyText:
        `Building your daily PDF report${scopeText}. You'll get another text with the download link in a minute.`,
      outboundMeta: {
        ...outboundMeta,
        command: "daily_pdf_request",
        dailyPdfRequested: true,
        projectSlug: effectiveProjectSlug,
        reportDateKey: requestDateKey,
        reportType: requestType,
      },
    };
  }

  const structured = parseStructuredLog(userMessageForAI);
  if (structured) {
    const logBody =
      (structured.body || "").trim() ||
      "(no description — add detail in a follow-up text)";
    const structuredReportDateKey = structured.reportDateKey || null;
    const logTags = [...(structured.tags || [])];
    if (structured.logParsedType === "progress") logTags.push("progress");
    if (structured.logParsedType === "manpower") logTags.push("manpower");
    if (structured.logParsedType === "daily_log") logTags.push("daily_log");
    if (structured.source === "shorthand") logTags.push("shorthand");

    let created;
    try {
      created = await createSmsIssue(db, FieldValue, {
        phoneE164,
        projectSlug: effectiveProjectSlug,
        projectName: effectiveProjectName,
        bodyText: logBody,
        rawSms: trimmedBody,
        source: "sms",
        logParsedType: structured.logParsedType,
        classifierType: null,
        tags: logTags,
        relatedMessageId: relatedMessageId || null,
      });
    } catch (saveErr) {
      logger.error("assistant: structured log Firestore save failed", {
        runId,
        message: saveErr.message,
        stack: saveErr.stack,
      });
      return {
        replyText:
          "Could not save that log to the database. Try again in a moment or contact your admin.",
        outboundMeta: {
          ...outboundMeta,
          command: "log_save_failed",
          aiError: String(saveErr.message),
        },
      };
    }

    let le;
    try {
      le = await writeLogEntry(db, FieldValue, {
        phoneE164,
        ...logAuthorFields,
        projectSlug: effectiveProjectSlug,
        rawText: trimmedBody,
        normalizedText: logBody,
        reportDateKey: structuredReportDateKey,
        category: structured.category,
        subtype: structured.source,
        tags: logTags,
        sourceMessageId: relatedMessageId || null,
        canonicalIssueId: created.issueId,
        issueCollection: created.issueCollection,
      });
    } catch (leErr) {
      logger.error("assistant: logEntry write failed after issue", {
        runId,
        message: leErr.message,
      });
    }

    outboundMeta.issueLogId = created.issueId;
    outboundMeta.issueCollection = created.issueCollection;
    outboundMeta.command = "log_" + structured.logParsedType;
    outboundMeta.logEntryId = le ? le.logEntryId : null;
    outboundMeta.logCategory = structured.category;
    outboundMeta.reportDateKey = structuredReportDateKey;
    outboundMeta.classification = `deterministic:${structured.source}:${structured.category}`;
    outboundMeta.enhanceLogEntry = Boolean(le && le.logEntryId);
    logger.info("assistant: structured log saved", {
      runId,
      category: structured.category,
      logParsedType: structured.logParsedType,
      issueId: created.issueId,
      logEntryId: outboundMeta.logEntryId,
    });
    const savedAsLabel =
      structured.logParsedType === "manpower" ? "manpower" : structured.category;
    const savedPrefix =
      structured.logParsedType === "manpower"
        ? "Saved as manpower log."
        : `Saved as ${savedAsLabel}.`;
    return {
      replyText: truncateSms(
        `${savedPrefix} ${
          structuredReportDateKey ? `(${structuredReportDateKey}) ` : ""
        }${logBody}`
      ),
      outboundMeta: withRoutingDecision(outboundMeta, {
        stage: "deterministic",
        action: "save_log",
        confidence: 0.99,
        reason: "Matched structured log command.",
        source: "deterministic",
        matchedBy: `parseStructuredLog:${structured.logParsedType}`,
      }),
    };
  }

  const dayRollupRequest = parseDayRollupRequest(userMessageForAI);
  if (dayRollupRequest) {
    const preferAi = dayRollupRequest.preferAiNarrative || isSummaryStyleRequest(userMessageForAI);
    const sum = await buildDayRollup(
      db,
      openaiApiKey,
      phoneE164,
      effectiveProjectSlug,
      dayRollupRequest.reportDateKey || null,
      logger,
      runId,
      modelsOverride,
      preferAi
    );
    await db.collection(COL_SUMMARIES).add({
      phoneE164,
      projectSlug: effectiveProjectSlug,
      summaryText: sum.text,
      period: "day",
      source: preferAi ? "sms_day_rollup_ai" : "sms_day_rollup",
      meta: sum.summaryMeta || {},
      createdAt: FieldValue.serverTimestamp(),
    });
    outboundMeta.summarySaved = true;
    outboundMeta.command = preferAi ? "daily_summary" : "daily_log_view";
    outboundMeta.aiUsed = Boolean(sum.summaryMeta && sum.summaryMeta.ai);
    outboundMeta.reportDateKey =
      (sum.summaryMeta && sum.summaryMeta.reportDateKey) ||
      dayRollupRequest.reportDateKey ||
      null;
    logger.info("assistant: day rollup sent", {
      runId,
      command: outboundMeta.command,
      aiUsed: outboundMeta.aiUsed,
      lineCount: sum.summaryMeta && sum.summaryMeta.lineCount,
      source: sum.summaryMeta && sum.summaryMeta.source,
    });
    return {
      replyText: sum.text,
      outboundMeta: withRoutingDecision(outboundMeta, {
        stage: "deterministic",
        action: "day_rollup",
        confidence: 0.99,
        reason: "Matched daily log/summary lookup request.",
        source: "deterministic",
        matchedBy: preferAi ? "parseDayRollupRequest:summary" : "parseDayRollupRequest:view",
      }),
    };
  }

  let historyRows = await loadThreadMessages(
    db,
    phoneE164,
    user.contextResetAt,
    effectiveProjectSlug
  );
  if (historyRows.length) {
    const last = historyRows[historyRows.length - 1];
    if (
      last.direction === "inbound" &&
      String(last.body || "").trim() === trimmedBody
    ) {
      historyRows = historyRows.slice(0, -1);
    }
  }
  const historyMessages = rowsToOpenAIMessages(historyRows);
  const aiActionPlan =
    shortAssistantFollowUp
      ? sanitizeAssistantActionPlan(null)
      : await planAssistantAction({
          openaiApiKey,
          logger,
          runId,
          historyMessages,
          trimmedBody: userMessageForAI,
          effectiveProjectSlug,
          effectiveProjectName,
          modelsOverride,
        });
  if (aiActionPlan.action !== "none" && aiActionPlan.confidence >= 0.84) {
    const plannerDecision = buildRoutingDecision({
      stage: "ai_action_router",
      action: aiActionPlan.action,
      confidence: aiActionPlan.confidence,
      reason: aiActionPlan.reason || "AI action router selected a supported backend action.",
      source: "ai",
      matchedBy: "planAssistantAction",
    });

    if (aiActionPlan.action === "project_set" && aiActionPlan.projectSlug) {
      if (!isExplicitProjectSetRequest(trimmedBody, aiActionPlan.projectSlug)) {
        logger.warn("assistant: rejected ai project_set without explicit switch wording", {
          runId,
          phoneE164,
          projectSlug: aiActionPlan.projectSlug,
          bodyPreview: String(trimmedBody || "").slice(0, 160),
        });
      } else {
      const projectAccess = await getAssistantProjectAccess(db, phoneE164, aiActionPlan.projectSlug, user);
      if (!projectAccess.exists) {
        return {
          replyText: `Project "${aiActionPlan.projectSlug}" does not exist. Use one of your assigned project slugs.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "project_missing",
            projectSlug: null,
          },
        };
      }
      if (!projectAccess.allowed) {
        return {
          replyText: `Project "${aiActionPlan.projectSlug}" is not assigned to this phone number.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "project_forbidden",
            projectSlug: null,
          },
        };
      }
      const slug = projectAccess.projectSlug || aiActionPlan.projectSlug;
      const patch = buildUserProjectPatch(user, slug, {
        activeProjectSlug: slug,
      });
      await db.collection(COL_USERS).doc(phoneE164).set(
        {
          ...patch,
          updatedAt: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      return {
        replyText: `Active project set to: ${(projectAccess.projectData && projectAccess.projectData.name) || slug} (${slug}).`,
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "project_set",
          projectSlug: slug,
        },
      };
      }
    }

    if (aiActionPlan.action === "daily_pdf_request") {
      const requestDateKey = aiActionPlan.reportDateKey || null;
      const requestType = aiActionPlan.reportType || "dailySiteLog";
      const scopeBits = [];
      if (effectiveProjectSlug) scopeBits.push(effectiveProjectName || effectiveProjectSlug);
      if (requestDateKey) scopeBits.push(requestDateKey);
      if (requestType === "journal") scopeBits.push("journal");
      const scopeText = scopeBits.length ? ` (${scopeBits.join(" · ")})` : "";
      return {
        replyText: `Building your daily PDF report${scopeText}. You'll get another text with the download link in a minute.`,
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "daily_pdf_request",
          dailyPdfRequested: true,
          projectSlug: effectiveProjectSlug,
          reportDateKey: requestDateKey,
          reportType: requestType,
        },
      };
    }

    if (aiActionPlan.action === "day_rollup") {
      const preferAi = aiActionPlan.preferAiNarrative === true;
      const sum = await buildDayRollup(
        db,
        openaiApiKey,
        phoneE164,
        effectiveProjectSlug,
        aiActionPlan.reportDateKey || null,
        logger,
        runId,
        modelsOverride,
        preferAi
      );
      await db.collection(COL_SUMMARIES).add({
        phoneE164,
        projectSlug: effectiveProjectSlug,
        summaryText: sum.text,
        period: "day",
        source: preferAi ? "sms_day_rollup_ai_action" : "sms_day_rollup_action",
        meta: sum.summaryMeta || {},
        createdAt: FieldValue.serverTimestamp(),
      });
      return {
        replyText: sum.text,
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          summarySaved: true,
          command: preferAi ? "daily_summary" : "daily_log_view",
          aiUsed: Boolean(sum.summaryMeta && sum.summaryMeta.ai),
          reportDateKey:
            (sum.summaryMeta && sum.summaryMeta.reportDateKey) || aiActionPlan.reportDateKey || null,
          projectSlug: effectiveProjectSlug,
        },
      };
    }

    if (aiActionPlan.action === "lookahead_trade_query") {
      if (!effectiveProjectSlug) {
        return {
          replyText: "Set a project first, then ask for activities by trade for this week or next week.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "lookahead_query_missing_project",
          },
        };
      }
      const snapshot = await loadLatestLookaheadSnapshot({
        db,
        projectSlug: effectiveProjectSlug,
      });
      if (!snapshot || !Array.isArray(snapshot.tasks) || !snapshot.tasks.length) {
        return {
          replyText: `No saved lookahead schedule was found for ${effectiveProjectName || effectiveProjectSlug} yet.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "lookahead_query_no_snapshot",
            projectSlug: effectiveProjectSlug,
          },
        };
      }
      const range = getDateKeyWindowForLookaheadRange(
        aiActionPlan.range === "next_week" ? "next_week" : "this_week",
        new Date()
      );
      const matchingTasks = snapshot.tasks
        .filter((task) => taskIntersectsLookaheadWindow(task, range.startKey, range.endKey))
        .filter((task) => taskMatchesTradeQuery(task, aiActionPlan.tradeQuery))
        .sort((a, b) => {
          const aRange = taskRelevantDateRange(a, range.startKey, range.endKey);
          const bRange = taskRelevantDateRange(b, range.startKey, range.endKey);
          if (aRange.startKey !== bRange.startKey) return aRange.startKey.localeCompare(bRange.startKey);
          return normalizeLookaheadActivityLabel(a.activity).localeCompare(
            normalizeLookaheadActivityLabel(b.activity)
          );
        });
      return {
        replyText: truncateSms(
          formatLookaheadActivitiesReply({
            projectName: effectiveProjectName || effectiveProjectSlug,
            tradeQuery: aiActionPlan.tradeQuery,
            rangeLabel: range.label,
            startKey: range.startKey,
            endKey: range.endKey,
            tasks: matchingTasks,
          })
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "lookahead_trade_query",
          projectSlug: effectiveProjectSlug,
          reportStartKey: range.startKey,
          reportEndKey: range.endKey,
          range: aiActionPlan.range || "this_week",
          trade: aiActionPlan.tradeQuery || null,
          taskCount: matchingTasks.length,
          lookaheadSnapshotId: snapshot.id || null,
        },
      };
    }

    if (aiActionPlan.action === "lookahead_activities_report") {
      if (!effectiveProjectSlug) {
        return {
          replyText: "Set a project first, then ask for the lookahead activities report.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "lookahead_report_missing_project",
          },
        };
      }
      return {
        replyText: `Generating the lookahead activities report for ${effectiveProjectName || effectiveProjectSlug}. You'll get a download link shortly.`,
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "lookahead_activities_report_request",
          projectSlug: effectiveProjectSlug,
          lookaheadReportRequested: true,
          lookaheadReportKind: "activities",
        },
      };
    }

    if (aiActionPlan.action === "lookahead_closeout_report") {
      if (!effectiveProjectSlug) {
        return {
          replyText: "Set a project first, then ask for the lookahead closeout report.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "lookahead_report_missing_project",
          },
        };
      }
      return {
        replyText: `Generating the lookahead closeout report for ${effectiveProjectName || effectiveProjectSlug}. You'll get a download link shortly.`,
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "lookahead_closeout_report_request",
          projectSlug: effectiveProjectSlug,
          lookaheadReportRequested: true,
          lookaheadReportKind: "closeout",
        },
      };
    }

    if (aiActionPlan.action === "labour_balance" && aiActionPlan.range) {
      const labourer = await findActiveLabourerByPhone(db, phoneE164);
      if (!labourer) {
        return {
          replyText:
            "This phone is not registered as a labourer yet. Ask the office to add your name and phone on the Labour page.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "labourer_phone_unregistered",
          },
        };
      }
      const range = getDateKeyRangeForBalanceQuery(aiActionPlan.range);
      if (!range || !range.startKey || !range.endKey) {
        return {
          replyText: "Could not look up that hours range. Try: how many hours today, this week, or this pay period.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "labour_hours_balance_error",
          },
        };
      }
      const entries = await loadLabourEntries(db, {
        startKey: range.startKey,
        endKey: range.endKey,
        labourerPhone: phoneE164,
      });
      const summary = buildLabourRollup(entries);
      const labourerName =
        labourer.displayName ||
        String((labourer.labourerData && labourer.labourerData.name) || "").trim() ||
        phoneE164;
      return {
        replyText: truncateSms(
          formatLabourBalanceReply({
            labourerName,
            rangeLabel: range.label,
            startKey: range.startKey,
            endKey: range.endKey,
            totalHours: summary.totalHours,
            totalPaidHours: summary.totalPaidHours,
            totalEntries: summary.totalEntries,
          })
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "labour_hours_balance",
          reportStartKey: range.startKey,
          reportEndKey: range.endKey,
          range: aiActionPlan.range,
          totalEntries: summary.totalEntries,
          totalHours: summary.totalHours,
          totalPaidHours: summary.totalPaidHours,
        },
      };
    }

    if (aiActionPlan.action === "deficiency_intake") {
      const deficiencyRequest = {
        projectSlug: aiActionPlan.projectSlug || effectiveProjectSlug || null,
        fields: {
          title: aiActionPlan.deficiency.title,
          description: aiActionPlan.deficiency.description,
          location: aiActionPlan.deficiency.location,
          area: aiActionPlan.deficiency.area,
          trade: aiActionPlan.deficiency.trade,
          reference: aiActionPlan.deficiency.reference,
          requestedAction: aiActionPlan.deficiency.requestedAction,
        },
        freeText: trimmedBody,
        normalizedText: trimmedBody,
      };
      return handleDeficiencyIntakeTurn({
        db,
        logger,
        runId,
        phoneE164,
        user,
        trimmedBody,
        lower,
        relatedMessageId,
        numMedia,
        effectiveProjectSlug,
        effectiveProjectName,
        logAuthorFields,
        deficiencyRequest,
        outboundMeta: withRoutingDecision(outboundMeta, plannerDecision),
      });
    }

    if (aiActionPlan.action === "todo_create" && aiActionPlan.todoText) {
      const todoProjectSlug = effectiveProjectSlug || HOME_TODO_PROJECT_SLUG;
      if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
        return {
          replyText:
            "Only admin or management phones can add todo items. Ask admin to approve this phone in Team.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "todo_create_forbidden",
            projectSlug: todoProjectSlug,
          },
        };
      }
      if (!canAccessProject(currentMemberAccess, todoProjectSlug)) {
        return {
          replyText: `This phone can’t add todo items for ${todoProjectSlug}.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "todo_create_project_forbidden",
            projectSlug: todoProjectSlug,
          },
        };
      }
      await savePendingTodoDraft(db, phoneE164, {
        projectSlug: todoProjectSlug,
        projectName: effectiveProjectName || todoProjectSlug,
        taskText: aiActionPlan.todoText,
        sourceText: trimmedBody,
        dueBy: null,
        dueDateCaptured: false,
        reminderRequested: null,
        secondReminderWanted: null,
        reminders: [],
        priority: null,
        priorityCaptured: false,
        tags: [],
        tagsCaptured: false,
        sourceMessageId: relatedMessageId || null,
      });
      return {
        replyText: truncateSms(todoFieldPrompt("dueBy", { taskText: aiActionPlan.todoText })),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "todo_intake_started",
          projectSlug: todoProjectSlug,
          pendingTodoIntake: true,
        },
      };
    }

    if (aiActionPlan.action === "notify_request" && aiActionPlan.notifyAudience && aiActionPlan.notifyMessage) {
      if (!currentMemberAccess || !roleAtLeast(currentMemberAccess.role, "management")) {
        return {
          replyText:
            "Only management can send broadcast notifications. Ask admin to approve your phone in Team.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "notify_forbidden",
          },
        };
      }
      const notifyProjectSlug =
        aiActionPlan.notifyAudience === "project_users"
          ? normalizeProjectSlug(aiActionPlan.projectSlug || effectiveProjectSlug || "")
          : null;
      if (aiActionPlan.notifyAudience === "project_users" && !notifyProjectSlug) {
        return {
          replyText: "Set a project first, or specify which project users should receive the notice.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "notify_missing_project",
          },
        };
      }
      if (
        aiActionPlan.notifyAudience === "project_users" &&
        !canAccessProject(currentMemberAccess, notifyProjectSlug)
      ) {
        return {
          replyText: `You cannot notify project ${notifyProjectSlug} because it is not assigned to your account.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "notify_project_forbidden",
            projectSlug: notifyProjectSlug,
          },
        };
      }
      return {
        replyText: truncateSms(
          aiActionPlan.notifyAudience === "management"
            ? `Sending your update to management: ${aiActionPlan.notifyMessage}`
            : `Sending your update to all users on ${notifyProjectSlug}: ${aiActionPlan.notifyMessage}`
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: aiActionPlan.notifyAudience === "management" ? "notify_management" : "notify_project_users",
          projectSlug: notifyProjectSlug || outboundMeta.projectSlug || null,
          notifyRequest: {
            audience: aiActionPlan.notifyAudience,
            projectSlug: notifyProjectSlug || null,
            messageBody: aiActionPlan.notifyMessage,
            requestedByPhone: phoneE164,
            requestedByName: logAuthorFields.authorName || null,
            requestedByEmail: logAuthorFields.authorEmail || null,
          },
        },
      };
    }

    if (aiActionPlan.action === "project_notes_update" && aiActionPlan.proposedNotes) {
      if (!effectiveProjectSlug) {
        return {
          replyText: "Set a project first, then ask me to update the project notes.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "project_notes_missing_project",
          },
        };
      }
      const notesUpdate = { proposedNotes: aiActionPlan.proposedNotes };
      const memberAccess = await findActiveAppMemberByApprovedPhone(db, phoneE164);
      if (!memberAccess) {
        return {
          replyText: "This phone is not approved for SMS project note updates. Ask admin to approve this number on your app member.",
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "project_notes_phone_unapproved",
          },
        };
      }
      if (!canAccessProject(memberAccess, effectiveProjectSlug)) {
        return {
          replyText: `This phone can’t update notes for ${effectiveProjectSlug}. Switch to one of your assigned projects first.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "project_notes_forbidden",
            projectSlug: effectiveProjectSlug,
          },
        };
      }
      const projectRef = db.collection(COL_PROJECTS).doc(effectiveProjectSlug);
      const projectSnap = await projectRef.get();
      if (!projectSnap.exists) {
        return {
          replyText: `Project "${effectiveProjectSlug}" was not found.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "project_missing",
            projectSlug: null,
          },
        };
      }
      const projectData = projectSnap.data() || {};
      const currentNotes = String(projectData.notes || "")
        .replace(/\r\n/g, "\n")
        .replace(/[ \t]+\n/g, "\n")
        .replace(/\n{3,}/g, "\n\n")
        .trim()
        .slice(0, 8000);
      if (canApproveProjectNoteRequests(memberAccess)) {
        await projectRef.set(
          {
            notes: notesUpdate.proposedNotes,
            updatedAt: FieldValue.serverTimestamp(),
            notesUpdatedAt: FieldValue.serverTimestamp(),
            notesUpdatedByEmail: memberAccess.email,
            notesUpdatedByPhone: phoneE164,
          },
          { merge: true }
        );
        return {
          replyText: `Project notes updated for ${projectData.name || effectiveProjectSlug}.`,
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "project_notes_updated",
            projectSlug: effectiveProjectSlug,
          },
        };
      }
      const requestRef = db.collection(COL_PROJECT_NOTE_EDIT_REQUESTS).doc();
      await requestRef.set({
        type: "projectNotes",
        status: "pending",
        source: "sms_ai_action",
        projectSlug: effectiveProjectSlug,
        projectName: projectData.name || effectiveProjectSlug,
        currentNotes,
        proposedNotes: notesUpdate.proposedNotes,
        requesterComment: "Submitted by SMS AI action",
        requestedByEmail: memberAccess.email,
        requestedByName: String(memberAccess.memberData?.displayName || memberAccess.email || "").trim(),
        requestedByRole: memberAccess.role,
        requestedByPhone: phoneE164,
        reportId: null,
        reportTitle: null,
        reportDateKey: null,
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
      });
      return {
        replyText: `Project note update submitted for ${projectData.name || effectiveProjectSlug}. Request ${requestRef.id} is pending approval.`,
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "project_notes_request_submitted",
          projectSlug: effectiveProjectSlug,
        },
      };
    }

    if (aiActionPlan.action === "start_timer") {
      const startedAtMs = Date.now();
      const label = aiActionPlan.timerLabel || "general task";
      const timerPayload = {
        label,
        startedAtMs,
        startedAtIso: new Date(startedAtMs).toISOString(),
        projectSlug: effectiveProjectSlug || null,
        projectName: effectiveProjectName || null,
      };
      await db.collection(COL_USERS).doc(phoneE164).set(
        {
          pendingTimer: timerPayload,
          updatedAt: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      return {
        replyText: truncateSms(
          `Timer started for ${label}.${effectiveProjectSlug ? ` Project ${effectiveProjectSlug}.` : ""} Text "stop timer" when done.`
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "timer_started",
          projectSlug: effectiveProjectSlug || null,
        },
      };
    }

    if (aiActionPlan.action === "stop_timer") {
      const activeTimer = user.pendingTimer && Number(user.pendingTimer.startedAtMs) > 0
        ? user.pendingTimer
        : null;
      if (!activeTimer) {
        return {
          replyText: 'No active timer. Text "start timer for <task>" first.',
          outboundMeta: {
            ...withRoutingDecision(outboundMeta, plannerDecision),
            command: "timer_stop_without_active",
            projectSlug: effectiveProjectSlug || null,
          },
        };
      }
      const stopAtMs = Date.now();
      const durationMs = Math.max(0, stopAtMs - Number(activeTimer.startedAtMs || 0));
      const durationMinutes = Math.round(durationMs / 60000);
      const timerProjectSlug = normalizeProjectSlug(activeTimer.projectSlug) || effectiveProjectSlug || null;
      const timerLabel = String(activeTimer.label || "general task").trim() || "general task";
      const timerLogText = `Timer: ${timerLabel} · Start ${String(activeTimer.startedAtIso || "-")} · Stop ${new Date(stopAtMs).toISOString()} · Duration ${formatDurationFromMs(durationMs)} (${durationMinutes}m).`;
      const timerLog = await writeLogEntry(db, FieldValue, {
        phoneE164,
        ...logAuthorFields,
        projectSlug: timerProjectSlug,
        reportDateKey: dateKeyEastern(new Date(stopAtMs)),
        rawText: timerLogText,
        normalizedText: timerLogText,
        category: "note",
        subtype: "timer",
        tags: ["timer", "time_tracking"],
        sourceMessageId: relatedMessageId || null,
        status: "closed",
      });
      await db.collection(COL_USERS).doc(phoneE164).set(
        {
          pendingTimer: FieldValue.delete(),
          updatedAt: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
      return {
        replyText: truncateSms(
          `Timer stopped for ${timerLabel}. Duration: ${formatDurationFromMs(durationMs)} (${durationMinutes}m). Logged to daily notes.`
        ),
        outboundMeta: {
          ...withRoutingDecision(outboundMeta, plannerDecision),
          command: "timer_stopped",
          projectSlug: timerProjectSlug,
          logEntryId: timerLog.logEntryId,
          logCategory: "note",
        },
      };
    }
  }
  const explicitAiRequest = shortAssistantFollowUp || looksLikeExplicitAiChatRequest(userMessageForAI);
  if (!explicitAiRequest) {
    const channelNorm = String(channel || "").trim().toLowerCase();
    if (channelNorm.startsWith("voice") || channelNorm === "sms_audio_note") {
      logger.info("assistant: voice fast-path log routing", {
        runId,
        channel,
      });
      return routeGenericInboundLog({
        db,
        openaiApiKey,
        logger,
        runId,
        phoneE164,
        user,
        trimmedBody,
        userMessageForAI,
        relatedMessageId,
        numMedia,
        effectiveProjectSlug,
        effectiveProjectName,
        logAuthorFields,
        modelsOverride,
        outboundMeta,
      });
    }
    const genericIntent = await classifyGenericInboundIntent({
      openaiApiKey,
      logger,
      runId,
      historyMessages,
      trimmedBody: userMessageForAI,
      modelsOverride,
    });
    const fallbackDecision = decideFallbackRouting(genericIntent, userMessageForAI, explicitAiRequest);
    if (fallbackDecision.action === "save_log") {
      logger.info("assistant: generic inbound routed", {
        runId,
        intent: genericIntent.intent,
        confidence: genericIntent.confidence,
        reason: genericIntent.reason || null,
      });
      return routeGenericInboundLog({
        db,
        openaiApiKey,
        logger,
        runId,
        phoneE164,
        user,
        trimmedBody,
        userMessageForAI,
        relatedMessageId,
        numMedia,
        effectiveProjectSlug,
        effectiveProjectName,
        logAuthorFields,
        modelsOverride,
        outboundMeta: withRoutingDecision(outboundMeta, fallbackDecision),
      });
    }
    logRoutingTelemetry(logger, runId, phoneE164, fallbackDecision, {
      projectSlug: effectiveProjectSlug || null,
      numMedia: Math.max(0, Number(numMedia) || 0),
    });
    outboundMeta.routingDecision = fallbackDecision;
  } else {
    const explicitDecision = decideFallbackRouting(null, userMessageForAI, true);
    logRoutingTelemetry(logger, runId, phoneE164, explicitDecision, {
      projectSlug: effectiveProjectSlug || null,
      numMedia: Math.max(0, Number(numMedia) || 0),
    });
    outboundMeta.routingDecision = explicitDecision;
  }

  if (
    lower === "schedule" ||
    lower === "today" ||
    lower === "safety" ||
    lower === "report" ||
    lower === "issue"
  ) {
    const hints = {
      schedule: "What should we focus on for schedule / lookahead?",
      today: "What should we prioritize on site today?",
      safety: "Top safety focus for today?",
      report: "Help me with a quick daily report style update.",
      issue: "How should I log or triage a site issue?",
    };
    userMessageForAI = hints[lower] || trimmedBody;
  }

  // ---- OpenAI conversational path ----
  const system = buildLayeredSystemPrompt(admin, project, user);

  let aiUserText = userMessageForAI;
  if (shortAssistantFollowUp && pendingAssistantFollowUp) {
    aiUserText =
      `Previous assistant question: ${pendingAssistantFollowUp.prompt}\n` +
      `User reply: ${userMessageForAI}`;
    await clearPendingAssistantFollowUp(db, phoneE164);
  }
  if (numMedia > 0) {
    const n = Math.min(10, Math.max(1, parseInt(String(numMedia), 10) || 1));
    const bodyLower = String(trimmedBody || "").trim().toLowerCase();
    const mediaKind = bodyLower === "voice attachment"
      ? "voice note"
      : bodyLower === "video attachment"
        ? "video"
        : bodyLower === "media attachment"
          ? "media"
          : "photo";
    const noCaption = [
      "photo attachment",
      "voice attachment",
      "video attachment",
      "media attachment",
    ].includes(bodyLower) || !String(trimmedBody || "").trim();
    aiUserText =
      `[Inbound included ${n} MMS ${mediaKind}(s)—the app saves attachments to storage and links them; do not say attachments cannot be received or saved.]\n\n` +
      (noCaption
        ? `User sent ${mediaKind}(s) with no text caption yet.`
        : userMessageForAI);
  }

  try {
    const rawReply = await callOpenAI(
      openaiApiKey,
      system,
      historyMessages,
      aiUserText,
      logger,
      runId,
      modelsOverride
    );
    const replyText = truncateSms(rawReply);
    if (shouldTrackAssistantFollowUp(replyText)) {
      await savePendingAssistantFollowUp(db, phoneE164, replyText, effectiveProjectSlug);
    } else if (pendingAssistantFollowUp) {
      await clearPendingAssistantFollowUp(db, phoneE164);
    }
    logger.info("assistant: openai chat ok", {
      runId,
      logEntryId: outboundMeta.logEntryId,
    });
    return {
      replyText,
      outboundMeta: {
        ...outboundMeta,
        aiUsed: true,
        command: "ai",
        logEntryId: outboundMeta.logEntryId || null,
        logCategory: outboundMeta.logCategory || null,
      },
    };
  } catch (e) {
    logger.error("assistant: openai error", { runId, message: e.message, stack: e.stack });
    const fallback = truncateSms(
      "AI unavailable—try again shortly. Tip: plain site updates save to the construction log, and home or journal texts save to the journal."
    );
    return {
      replyText: fallback || "AI unavailable. Try again or text help.",
      outboundMeta: {
        ...outboundMeta,
        aiUsed: false,
        aiError: String(e.message),
        command: "ai_error",
      },
    };
  }
}

/**
 * Project slug for voice/MMS audio when buildReply did not run (e.g. transcription failed)
 * or returned no project: active project, else `home` if the user may access it.
 */
async function resolveVoiceMediaProjectSlug(db, phoneE164) {
  const e164 = String(phoneE164 || "").trim();
  if (!e164) return null;
  const user = await getOrCreateUser(db, e164);
  const active = normalizeProjectSlug(user.activeProjectSlug);
  if (active) return active;
  const homeAccess = await getAssistantProjectAccess(db, e164, "home", user);
  if (homeAccess.exists && homeAccess.allowed && homeAccess.projectSlug) {
    return normalizeProjectSlug(homeAccess.projectSlug) || "home";
  }
  return null;
}

module.exports = {
  buildReply,
  checkRateLimit,
  resolveVoiceMediaProjectSlug,
  elevateProjectAccessWithApprovedMember,
  fallbackInboundIntent,
  inferInboundLogType,
  sanitizeIntentPayload,
  sanitizeRoutePayload,
  parseStartTimerCommand,
  parseHomeTodoCommand,
  parseTodoMutationRequest,
  parseTodoDateTimeInput,
  normalizePendingTodoDraft,
  getNextMissingTodoField,
  looksLikePendingTodoAnswer,
  shouldBypassPendingTodo,
  isStopTimerCommand,
  formatDurationFromMs,
  parseNotificationRequest,
  parseLookaheadActivitiesQuery,
  parseNarrativeCorrectionCommand,
  sanitizeAssistantActionPlan,
  looksLikeExplicitAiChatRequest,
  isExplicitLabourEntryText,
  isExplicitLabourBalanceText,
  parseTodoListRequest,
  parseTodoReportRequest,
  isAffirmativeCorrectionFollowUp,
  buildRecentCorrectionDateKeys,
  looksLikeAssistantFollowUpAnswer,
  looksLikeCorrectionPrompt,
  shouldTrackAssistantFollowUp,
  applyManpowerCorrectionToEntry,
  applyNarrativeCorrectionToEntry,
  looksLikeNarrativeSaveCandidate,
  taskMatchesTradeQuery,
  taskIntersectsLookaheadWindow,
  getDateKeyWindowForLookaheadRange,
  formatLookaheadActivitiesReply,
  decideFallbackRouting,
  inferJournalTags,
  isExplicitProjectSetRequest,
  RATE_MAX,
  RATE_WINDOW_MS,
  MAX_SMS_CHARS,
  COL_USERS,
  COL_PROJECTS,
  COL_ADMIN,
  COL_ISSUES,
  COL_SUMMARIES,
  COL_PROJECT_TODOS,
};
