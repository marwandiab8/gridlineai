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

const { createSmsIssue, makeTitleFromBody } = require("./issueRepository");
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
} = require("./logClassifier");
const {
  writeLogEntry,
  loadLogEntriesForDayForProject,
  loadTodayLogEntriesForProject,
  formatGroupedDayLog,
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
  findActiveAppMemberByApprovedPhone,
  findActiveLabourerByPhone,
  canAccessProject,
  canApproveProjectNoteRequests,
  roleAtLeast,
} = require("./authz");
const {
  parseLabourHoursCommand,
  parseLabourHoursBalanceQuery,
  getDateKeyRangeForBalanceQuery,
  formatLabourBalanceReply,
  writeLabourEntry,
  loadLabourEntries,
  buildLabourRollup,
  dayMultiplierFromDateKey,
} = require("./labourRepository");

const ADMIN_DOC_ID = "company";
const MAX_SMS_CHARS = 480;
const HISTORY_LIMIT = 18;
const RATE_WINDOW_MS = 60_000;
const RATE_MAX = 40;
const COL_PROJECT_NOTE_EDIT_REQUESTS = "projectNoteEditRequests";

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

function sanitizeRoutePayload(raw, fallbackText, numMedia) {
  const fallbackType = inferInboundLogType(fallbackText);
  const fallbackDescription = String(fallbackText || "").trim();
  const candidateType = String(raw && raw.logType ? raw.logType : "")
    .trim()
    .toLowerCase();
  const logType = ["construction", "journal", "safety", "deficiency"].includes(candidateType)
    ? candidateType
    : fallbackType;
  const description = String(raw && raw.description ? raw.description : fallbackDescription)
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 4000) || fallbackDescription || "Field update";
  const title = String(raw && raw.title ? raw.title : "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 160) || makeTitleFromBody(description, 120);
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
  return { intent, confidence, reason };
}

function looksLikeExplicitAiChatRequest(text) {
  const raw = String(text || "").trim();
  if (!raw) return false;
  const lower = raw.toLowerCase();
  if (raw.endsWith("?")) return true;
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

function parseLabourEntryCommand(text) {
  return parseLabourHoursCommand(text);
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
        ...outboundMeta,
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
      ...outboundMeta,
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
        const body = e.normalizedText || e.rawText || "";
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

  const pendingDeficiencyDraft = normalizePendingDeficiencyDraft(user.pendingDeficiencyIntake);
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

  const hoursBalanceQuery = parseLabourHoursBalanceQuery(userMessageForAI);
  if (hoursBalanceQuery) {
    const labourer = await findActiveLabourerByPhone(db, phoneE164);
    if (!labourer) {
      return {
        replyText:
          "This phone is not registered as a labourer yet. Ask the office to add your name and phone on the Labour page.",
        outboundMeta: { ...outboundMeta, command: "labourer_phone_unregistered" },
      };
    }
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
        ...outboundMeta,
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

  const labourEntryCommand = parseLabourEntryCommand(userMessageForAI);
  if (labourEntryCommand) {
    const labourer = await findActiveLabourerByPhone(db, phoneE164);
    if (!labourer) {
      return {
        replyText:
          "This phone is not registered as a labourer yet. Ask the office to add your name and phone on the Labour page.",
        outboundMeta: { ...outboundMeta, command: "labourer_phone_unregistered" },
      };
    }
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
        ...outboundMeta,
        command: "labour_entry_saved",
        labourEntryId: entry.labourEntryId,
        labourerName,
        labourerPhone: phoneE164,
        projectSlug: labourProject,
        reportDateKey: entry.reportDateKey || null,
      },
    };
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
    return {
      replyText: truncateSms(
        `Saved as ${savedAsLabel}. ${
          structuredReportDateKey ? `(${structuredReportDateKey}) ` : ""
        }${logBody}`
      ),
      outboundMeta,
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
      outboundMeta,
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
  const explicitAiRequest = looksLikeExplicitAiChatRequest(userMessageForAI);
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
    if (genericIntent.intent !== "request") {
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
        outboundMeta,
      });
    }
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

module.exports = {
  buildReply,
  checkRateLimit,
  elevateProjectAccessWithApprovedMember,
  fallbackInboundIntent,
  inferInboundLogType,
  sanitizeIntentPayload,
  sanitizeRoutePayload,
  parseStartTimerCommand,
  isStopTimerCommand,
  formatDurationFromMs,
  parseNotificationRequest,
  looksLikeExplicitAiChatRequest,
  inferJournalTags,
  RATE_MAX,
  RATE_WINDOW_MS,
  MAX_SMS_CHARS,
  COL_USERS,
  COL_PROJECTS,
  COL_ADMIN,
  COL_ISSUES,
  COL_SUMMARIES,
};
