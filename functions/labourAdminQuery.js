const { createHash } = require("crypto");
const { normalizeProjectSlug } = require("./projectAccess");
const { getReportRecordProjectOwnership } = require("./reportProjectOwnership");
const {
  LABOUR_PAY_PERIOD_CONFIG,
  labourMinutesFromHours,
  parseLabourHoursCommand,
} = require("./labourRepository");

const LABOUR_QUERY_TIME_ZONE = "America/Toronto";
const ADMIN_LABOUR_DENIAL_TEXT =
  "This labour query is available only to an authorized GridlineAI administrator.";
// Keep deterministic labour responses below assistant.js's 480-character SMS cap
// so the transport layer never truncates a worker or project total mid-value.
const LABOUR_QUERY_MAX_SMS_CHARS = 460;
const PERSONAL_PROJECT_SLUGS = new Set(["home", "personal"]);
const INVALID_PROJECT_SENTINELS = new Set([
  "_unassigned",
  "unassigned",
  "unknown",
  "none",
  "null",
]);

function normalizeWords(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’']/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhone(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (!/^[+\d\s().-]+$/.test(raw)) return "";
  const digits = raw.replace(/\D+/g, "");
  if (digits.length < 8 || digits.length > 15) return "";
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
  return raw.startsWith("+") ? `+${digits}` : "";
}

function privacySafeReference(value, prefix = "ref") {
  const raw = String(value || "").trim();
  if (!raw) return null;
  return `${prefix}_${createHash("sha256").update(raw).digest("hex").slice(0, 16)}`;
}

function isValidDateKey(value) {
  const raw = String(value || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return false;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  return !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === raw;
}

function shiftDateKey(dateKey, deltaDays) {
  if (!isValidDateKey(dateKey) || !Number.isFinite(Number(deltaDays))) return "";
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + Number(deltaDays));
  return date.toISOString().slice(0, 10);
}

function torontoDateKey(now = new Date()) {
  const date = now instanceof Date && !Number.isNaN(now.getTime()) ? now : new Date();
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: LABOUR_QUERY_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function startOfWeek(dateKey) {
  if (!isValidDateKey(dateKey)) return "";
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  const offset = date.getUTCDay() === 0 ? 6 : date.getUTCDay() - 1;
  return shiftDateKey(dateKey, -offset);
}

function endOfMonth(dateKey) {
  if (!isValidDateKey(dateKey)) return "";
  const [year, month] = dateKey.split("-").map(Number);
  return new Date(Date.UTC(year, month, 0)).toISOString().slice(0, 10);
}

function parsePeriodRequest(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  const range = raw.match(/\b(\d{4}-\d{2}-\d{2})\s*(?:to|through|thru|until|[–—])\s*(\d{4}-\d{2}-\d{2})\b/i);
  if (range) {
    return {
      kind: "explicit_range",
      startKey: range[1],
      endKey: range[2],
      explicit: true,
    };
  }
  const exact = raw.match(/\b(\d{4}-\d{2}-\d{2})\b/);
  if (exact) {
    return {
      kind: "exact_date",
      startKey: exact[1],
      endKey: exact[1],
      explicit: true,
    };
  }

  const normalized = normalizeWords(raw);
  const phrases = [
    ["previous pay period", "previous_pay_period"],
    ["last pay period", "previous_pay_period"],
    ["current pay period", "current_pay_period"],
    ["this pay period", "current_pay_period"],
    ["previous week", "last_week"],
    ["last week", "last_week"],
    ["this week", "this_week"],
    ["previous month", "last_month"],
    ["last month", "last_month"],
    ["this month", "this_month"],
    ["project to date", "project_to_date"],
    ["project date", "project_to_date"],
    ["all time", "all_time"],
    ["yesterday", "yesterday"],
    ["today", "today"],
  ];
  for (const [phrase, kind] of phrases) {
    if (normalized.includes(phrase)) return { kind, explicit: true };
  }
  return null;
}

function cleanEntityHint(value) {
  return String(value || "")
    .replace(/\b(?:job|project)\b/gi, " ")
    .replace(/\b(?:today|yesterday|this|last|previous|current|pay|period|week|month|all|time)\b.*$/i, " ")
    .replace(/\b\d{4}-\d{2}-\d{2}\b.*$/i, " ")
    .replace(/[?.!,;:]+$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractWorkerHint(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  const patterns = [
    /\bhow\s+many\s+hours\s+did\s+(.+?)\s+(?:log|work)(?:\b|\s)/i,
    /\bhow\s+many\s+hours\s+(?:has|have)\s+(.+?)\s+logged\b/i,
    /\b(?:hours?|time)\s+(?:for|by)\s+(?:worker|labou?rer|employee)\s+(.+?)(?=\s+(?:on|from|for|this|last|previous|current|today|yesterday|\d{4}-)|[?.!,;:]|$)/i,
    /^\s*([a-z][a-z.'-]*(?:\s+[a-z][a-z.'-]*){0,3})['’]?s?\s+(?:labou?r\s+)?hours?\b/i,
  ];
  for (const pattern of patterns) {
    const match = raw.match(pattern);
    const hint = cleanEntityHint(match && match[1]);
    if (hint && !/^(?:all|how|labou?r|total|what|who)\b/i.test(hint)) return hint;
  }
  return "";
}

function extractProjectHint(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  const patterns = [
    /\bfor\s+(?:the\s+)?(.+?)\s+(?:job|project)\b/i,
    /\b(?:job|project)\s+([a-z0-9][a-z0-9 .'_-]*?)(?=\s+(?:on|from|for|this|last|previous|current|today|yesterday|\d{4}-)|[?.!,;:]|$)/i,
    /\blabou?r\s+report\s+(?:for\s+)?([a-z0-9][a-z0-9 .'_-]*?)(?=\s+(?:on|from|for|this|last|previous|current|today|yesterday|\d{4}-)|[?.!,;:]|$)/i,
    /\bsend\s+me\s+(?:a\s+)?(?:pdf\s+)?labou?r\s+report\s+for\s+(.+?)(?=\s+(?:on|from|for|this|last|previous|current|today|yesterday|\d{4}-)|[?.!,;:]|$)/i,
  ];
  for (const pattern of patterns) {
    const match = raw.match(pattern);
    const hint = cleanEntityHint(match && match[1]);
    if (hint && !/^(?:all|all projects|all labourers)$/i.test(hint)) return hint;
  }
  return "";
}

function parseAdminLabourQuery(text) {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw || parseLabourHoursCommand(raw)) return null;
  const normalized = normalizeWords(raw);

  if (/^labou?r help$/.test(normalized)) {
    return {
      intent: "help",
      output: "text",
      grouping: "summary",
      projectHint: "",
      workerHint: "",
      period: null,
      confidence: 1,
      requiresAdministrator: true,
    };
  }

  const reportRequest =
    /\blabou?r\s+(?:hours?\s+)?report\b/i.test(raw) ||
    /\b(?:send|generate|create)\b[\s\S]*\blabou?r\b[\s\S]*\breport\b/i.test(raw);
  const whoRequest = /\bwho\s+(?:has|logged|entered)\s+(?:labou?r\s+)?hours?\b/i.test(raw);
  const totalRequest =
    /\bhow\s+many\b[\s\S]*\b(?:labou?r\s+)?hours?\b/i.test(raw) ||
    /\bhow\s+many\s+hours?\s+did\b/i.test(raw) ||
    /\btotal\s+labou?r\s+hours?\b/i.test(raw) ||
    /\b(?:labou?r\s+hours?|hours?\s+for\s+all\s+labou?rers?)\b/i.test(raw);
  if (!reportRequest && !whoRequest && !totalRequest) return null;

  const workerHint = whoRequest ? "" : extractWorkerHint(raw);
  const projectHint = extractProjectHint(raw);
  const period = parsePeriodRequest(raw);
  const allProjectsRequested =
    /\b(?:all|every)\s+(?:authorized\s+|work\s+)?projects?\b/i.test(raw) ||
    (reportRequest && !projectHint);
  const allLabourersRequested =
    /\ball\s+labou?rers?\b/i.test(raw) ||
    /\bevery(?:one|body)\b/i.test(raw) ||
    (!workerHint && !whoRequest);

  return {
    intent: reportRequest ? "report" : whoRequest ? "who" : "total",
    output: reportRequest ? "pdf" : "text",
    grouping: whoRequest ? "workers_by_project" : "summary",
    projectHint,
    workerHint,
    period,
    allProjectsRequested,
    allLabourersRequested,
    confidence: 1,
    requiresAdministrator: true,
  };
}

function isAdminLabourQueryCandidate(text) {
  return Boolean(parseAdminLabourQuery(text));
}

function isAdministratorLabourAccess(access) {
  return Boolean(
    access &&
    String(access.role || "").trim().toLowerCase() === "admin" &&
    String(access.memberDocId || access.email || "").trim()
  );
}

function getAdminLabourAccessDecision({ access, parsed, text }) {
  if (!parsed) return "not_a_query";
  if (isAdministratorLabourAccess(access)) return "administrator";
  const raw = String(text || "").trim().toLowerCase();
  const workerSelfServiceQuery =
    parsed.intent === "total" &&
    !parsed.workerHint &&
    !parsed.projectHint &&
    !/\b(?:all\s+labou?rers?|total\s+labou?r|who\s+has)\b/i.test(raw);
  const workerSelfServiceReport =
    parsed.intent === "report" &&
    !parsed.projectHint &&
    [
      "report",
      "pay period report",
      "payperiod report",
      "pay report",
      "labour report",
      "labor report",
      "labour pay period report",
      "labor pay period report",
    ].includes(raw);
  return workerSelfServiceQuery || workerSelfServiceReport
    ? "legacy_self_service"
    : "denied";
}

function normalizeProjectRegistry(rows) {
  const projects = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const idSlug = normalizeProjectSlug(row && (row.id || row.docId));
    const declaredSlug = normalizeProjectSlug(row && (row.projectSlug || row.slug));
    const contradictory = Boolean(idSlug && declaredSlug && idSlug !== declaredSlug);
    const projectSlug = contradictory ? "" : declaredSlug || idSlug;
    if (!projectSlug || INVALID_PROJECT_SENTINELS.has(projectSlug)) continue;
    const name = String(row && (row.name || row.displayName || row.title) || projectSlug)
      .replace(/\s+/g, " ")
      .trim() || projectSlug;
    const projectType = normalizeWords(row && (row.projectType || row.type));
    const personal = PERSONAL_PROJECT_SLUGS.has(projectSlug) || projectType === "personal" || projectType === "home";
    const explicitLabourEnabled = row && (row.labourEnabled ?? row.labourTrackingEnabled);
    const labourEnabled = personal
      ? explicitLabourEnabled === true
      : explicitLabourEnabled !== false;
    const aliases = new Set([name, projectSlug.replace(/-/g, " ")]);
    for (const alias of Array.isArray(row && row.aliases) ? row.aliases : []) {
      if (String(alias || "").trim()) aliases.add(String(alias).trim());
    }
    projects.push({
      projectSlug,
      name,
      active: row && row.active === false ? false : true,
      labourEnabled,
      personal,
      contradictory,
      aliases: [...aliases].map(normalizeWords).filter(Boolean),
    });
  }
  return projects.sort((a, b) => a.name.localeCompare(b.name));
}

function normalizeWorkerRegistry(rows) {
  const workers = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const docPhone = normalizePhone(row && (row.id || row.docId));
    const declaredPhone = normalizePhone(row && (row.phoneE164 || row.labourerPhone));
    const contradictory = Boolean(docPhone && declaredPhone && docPhone !== declaredPhone);
    const workerId = contradictory ? "" : declaredPhone || docPhone;
    const name = String(row && (row.name || row.displayName) || "")
      .replace(/\s+/g, " ")
      .trim();
    if (!workerId || !name) continue;
    const aliases = new Set([name]);
    for (const field of ["aliases", "previousNames", "nameAliases"]) {
      for (const alias of Array.isArray(row && row[field]) ? row[field] : []) {
        if (String(alias || "").trim()) aliases.add(String(alias).trim());
      }
    }
    workers.push({
      workerId,
      name,
      active: row && row.active === false ? false : true,
      contradictory,
      aliases: [...aliases].map(normalizeWords).filter(Boolean),
    });
  }
  return workers.sort((a, b) => a.name.localeCompare(b.name));
}

function resolveProjectScope(parsed, projects) {
  const active = (projects || []).filter((project) => project.active && !project.contradictory);
  const hint = normalizeWords(parsed && parsed.projectHint);
  const queryText = normalizeWords(parsed && parsed.queryText);
  if (parsed && parsed.allProjectsRequested === true && !hint) {
    return { status: "all_work_projects", project: null };
  }
  let matches = [];
  if (hint) {
    matches = active.filter((project) => project.aliases.includes(hint));
  } else if (queryText) {
    matches = active.filter((project) =>
      project.aliases.some((alias) => new RegExp(`(?:^| )${alias.replace(/ /g, "\\s+")}(?: |$)`).test(queryText))
    );
  }
  if (matches.length > 1) {
    return { status: "ambiguous", candidates: matches.map((project) => project.name).sort() };
  }
  if (matches.length === 1) {
    const project = matches[0];
    if (!project.labourEnabled) {
      return { status: "not_labour_enabled", project };
    }
    return { status: "resolved", project };
  }
  if (hint) return { status: "unknown", candidates: [] };
  return { status: "all_work_projects", project: null };
}

function resolveWorkerScope(parsed, workers) {
  const hint = normalizeWords(parsed && parsed.workerHint);
  if (!hint) return { status: "all", worker: null };
  const active = (workers || []).filter((worker) => worker.active && !worker.contradictory);
  const matches = active.filter((worker) => {
    if (worker.aliases.includes(hint)) return true;
    return worker.aliases.some((alias) => alias.split(" ")[0] === hint);
  });
  if (matches.length > 1) {
    return { status: "ambiguous", candidates: matches.map((worker) => worker.name).sort() };
  }
  if (matches.length === 1) return { status: "resolved", worker: matches[0] };
  return { status: "unknown", candidates: [] };
}

function validatePayPeriodConfig(config) {
  const source = config && typeof config === "object" ? config : null;
  const anchorStartKey = source && String(source.anchorStartKey || "").trim();
  const lengthDays = source && Number(source.lengthDays);
  if (!isValidDateKey(anchorStartKey) || !Number.isInteger(lengthDays) || lengthDays < 1 || lengthDays > 62) {
    return null;
  }
  return { anchorStartKey, lengthDays };
}

function payPeriodStart(dateKey, config) {
  const valid = validatePayPeriodConfig(config);
  if (!valid || !isValidDateKey(dateKey)) return "";
  const dateMs = new Date(`${dateKey}T00:00:00.000Z`).getTime();
  const anchorMs = new Date(`${valid.anchorStartKey}T00:00:00.000Z`).getTime();
  const deltaDays = Math.floor((dateMs - anchorMs) / 86400000);
  const periodIndex = Math.floor(deltaDays / valid.lengthDays);
  return shiftDateKey(valid.anchorStartKey, periodIndex * valid.lengthDays);
}

function resolveDateRange({ period, hasProject, hasWorker, output, now = new Date(), payPeriodConfig = LABOUR_PAY_PERIOD_CONFIG }) {
  const today = torontoDateKey(now);
  let requested = period && period.kind ? { ...period } : null;
  if (!requested) {
    if (output === "pdf") requested = { kind: "current_pay_period", explicit: false };
    else if (hasProject && !hasWorker) requested = { kind: "project_to_date", explicit: false };
    else if (hasWorker) requested = { kind: "current_pay_period", explicit: false };
    else requested = { kind: "current_pay_period", explicit: false };
  }

  if (requested.kind === "explicit_range" || requested.kind === "exact_date") {
    if (!isValidDateKey(requested.startKey) || !isValidDateKey(requested.endKey) || requested.startKey > requested.endKey) {
      return { status: "invalid", reason: "invalid_date_range" };
    }
    return {
      status: "resolved",
      kind: requested.kind,
      startKey: requested.startKey,
      endKey: requested.endKey,
      label: requested.kind === "exact_date" ? requested.startKey : `${requested.startKey} to ${requested.endKey}`,
      explicit: true,
    };
  }

  if (["current_pay_period", "previous_pay_period"].includes(requested.kind)) {
    const validConfig = validatePayPeriodConfig(payPeriodConfig);
    if (!validConfig) return { status: "configuration_required", reason: "pay_period_not_configured" };
    const currentStart = payPeriodStart(today, validConfig);
    const startKey = requested.kind === "previous_pay_period"
      ? shiftDateKey(currentStart, -validConfig.lengthDays)
      : currentStart;
    return {
      status: "resolved",
      kind: requested.kind,
      startKey,
      endKey: shiftDateKey(startKey, validConfig.lengthDays - 1),
      label: requested.kind === "previous_pay_period" ? "Previous pay period" : "Current pay period",
      explicit: requested.explicit === true,
    };
  }

  if (requested.kind === "today") {
    return { status: "resolved", kind: requested.kind, startKey: today, endKey: today, label: "Today", explicit: true };
  }
  if (requested.kind === "yesterday") {
    const key = shiftDateKey(today, -1);
    return { status: "resolved", kind: requested.kind, startKey: key, endKey: key, label: "Yesterday", explicit: true };
  }
  if (requested.kind === "this_week" || requested.kind === "last_week") {
    const thisStart = startOfWeek(today);
    const startKey = requested.kind === "last_week" ? shiftDateKey(thisStart, -7) : thisStart;
    return {
      status: "resolved",
      kind: requested.kind,
      startKey,
      endKey: shiftDateKey(startKey, 6),
      label: requested.kind === "last_week" ? "Last week" : "This week",
      explicit: true,
    };
  }
  if (requested.kind === "this_month" || requested.kind === "last_month") {
    const thisStart = `${today.slice(0, 7)}-01`;
    const startKey = requested.kind === "last_month" ? shiftDateKey(thisStart, -1).slice(0, 7) + "-01" : thisStart;
    return {
      status: "resolved",
      kind: requested.kind,
      startKey,
      endKey: endOfMonth(startKey),
      label: requested.kind === "last_month" ? "Last month" : "This month",
      explicit: true,
    };
  }
  if (requested.kind === "project_to_date" || requested.kind === "all_time") {
    return {
      status: "resolved",
      kind: requested.kind,
      startKey: null,
      endKey: today,
      label: requested.kind === "project_to_date" ? "Project-to-date" : "All time",
      explicit: requested.explicit === true,
    };
  }
  return { status: "invalid", reason: "unknown_period" };
}

function prepareAdminLabourQuery({ parsed, projectRows, workerRows, now = new Date(), payPeriodConfig = LABOUR_PAY_PERIOD_CONFIG }) {
  if (!parsed || parsed.requiresAdministrator !== true) {
    return { status: "not_a_query", reason: "not_a_query" };
  }
  if (parsed.intent === "help") {
    return { status: "help", parsed };
  }
  const enriched = { ...parsed, queryText: parsed.queryText || "" };
  const projects = normalizeProjectRegistry(projectRows);
  const workers = normalizeWorkerRegistry(workerRows);
  const projectResolution = resolveProjectScope(enriched, projects);
  if (projectResolution.status !== "resolved" && projectResolution.status !== "all_work_projects") {
    return { status: "clarification", reason: `project_${projectResolution.status}`, candidates: projectResolution.candidates || [] };
  }
  const workerResolution = resolveWorkerScope(enriched, workers);
  if (workerResolution.status !== "resolved" && workerResolution.status !== "all") {
    return { status: "clarification", reason: `worker_${workerResolution.status}`, candidates: workerResolution.candidates || [] };
  }
  const dateRange = resolveDateRange({
    period: parsed.period,
    hasProject: projectResolution.status === "resolved",
    hasWorker: workerResolution.status === "resolved",
    output: parsed.output,
    now,
    payPeriodConfig,
  });
  if (dateRange.status !== "resolved") {
    return { status: "clarification", reason: dateRange.reason || dateRange.status, candidates: [] };
  }
  if (parsed.intent === "who" && !parsed.period) {
    return { status: "clarification", reason: "date_required_for_who", candidates: [] };
  }
  return {
    status: "ready",
    parsed,
    projects,
    workers,
    request: {
      intent: parsed.intent,
      output: parsed.output,
      grouping: parsed.grouping,
      projectSlug: projectResolution.project ? projectResolution.project.projectSlug : null,
      projectName: projectResolution.project ? projectResolution.project.name : null,
      allWorkProjects: projectResolution.status === "all_work_projects",
      workerId: workerResolution.worker ? workerResolution.worker.workerId : null,
      workerName: workerResolution.worker ? workerResolution.worker.name : null,
      startKey: dateRange.startKey,
      endKey: dateRange.endKey,
      periodKind: dateRange.kind,
      periodLabel: dateRange.label,
    },
  };
}

function exclusionReasonForState(entry) {
  const status = normalizeWords(entry && entry.status);
  if (entry && (entry.deleted === true || entry.isDeleted === true || entry.deletedAt)) return "deleted";
  if (["deleted", "invalid", "void", "voided", "test"].includes(status)) return `status_${status}`;
  if (entry && (entry.test === true || entry.isTest === true)) return "test_record";
  const source = normalizeWords(entry && entry.source);
  if (["test", "fixture", "synthetic"].includes(source)) return "test_source";
  if (String(entry && (entry.duplicateOf || entry.duplicateOfId) || "").trim()) return "duplicate_record";
  return "";
}

function resolveEntryWorker(entry, workers) {
  const byId = new Map((workers || []).map((worker) => [worker.workerId, worker]));
  const phone = normalizePhone(entry && entry.labourerPhone);
  if (phone && byId.has(phone)) return { status: "resolved", worker: byId.get(phone) };
  const name = normalizeWords(entry && entry.labourerName);
  if (!name) return { status: "missing" };
  const matches = (workers || []).filter((worker) => worker.aliases.includes(name));
  if (matches.length === 1) return { status: "resolved", worker: matches[0], legacyNameMatch: true };
  return { status: matches.length > 1 ? "ambiguous" : "unknown" };
}

function minutesForCanonicalEntry(entry) {
  const hasMinutes = Object.prototype.hasOwnProperty.call(entry || {}, "minutesWorked");
  if (hasMinutes) {
    const minutes = Number(entry.minutesWorked);
    if (!Number.isSafeInteger(minutes) || minutes <= 0) {
      return { status: "invalid", reason: "invalid_minutes" };
    }
    const hasLegacyHours = Object.prototype.hasOwnProperty.call(entry || {}, "hours");
    const legacyMinutes = hasLegacyHours ? labourMinutesFromHours(entry.hours) : 0;
    return {
      status: "valid",
      minutes,
      source: "minutesWorked",
      auditFlags: hasLegacyHours
        ? [legacyMinutes === minutes ? "legacy_hours_ignored" : "contradictory_legacy_hours_ignored"]
        : [],
    };
  }
  const hours = Number(entry && entry.hours);
  const minutes = labourMinutesFromHours(hours);
  if (!Number.isFinite(hours) || hours <= 0 || !Number.isSafeInteger(minutes) || minutes <= 0) {
    return { status: "invalid", reason: "invalid_legacy_hours" };
  }
  return { status: "valid", minutes, source: "legacy_hours", auditFlags: ["legacy_hours_normalized"] };
}

function canonicalizeLabourEntries({ entries, projects, workers, request }) {
  const projectMap = new Map((projects || []).map((project) => [project.projectSlug, project]));
  const included = [];
  const excludedReasons = {};
  const auditFlags = {};
  const seenIds = new Set();
  const exclude = (reason) => {
    excludedReasons[reason] = (excludedReasons[reason] || 0) + 1;
  };

  for (const source of Array.isArray(entries) ? entries : []) {
    const entry = source && typeof source === "object" ? source : {};
    const id = String(entry.id || entry.docId || "").trim();
    if (!id || seenIds.has(id)) {
      exclude(id ? "duplicate_document_id" : "missing_document_id");
      continue;
    }
    seenIds.add(id);
    const reportDateKey = String(entry.reportDateKey || "").trim();
    if (!isValidDateKey(reportDateKey)) {
      exclude("invalid_report_date");
      continue;
    }
    if (request.startKey && reportDateKey < request.startKey) continue;
    if (request.endKey && reportDateKey > request.endKey) continue;

    const stateReason = exclusionReasonForState(entry);
    if (stateReason) {
      exclude(stateReason);
      continue;
    }

    const ownership = getReportRecordProjectOwnership(entry);
    if (!ownership.consistent || ownership.contradictory || ownership.malformed) {
      exclude(ownership.contradictory ? "contradictory_project_ownership" : "malformed_project_ownership");
      continue;
    }
    if (!ownership.assigned || !ownership.projectSlug) {
      exclude("missing_project_ownership");
      continue;
    }
    const project = projectMap.get(ownership.projectSlug);
    if (!project || !project.active || project.contradictory) {
      exclude("unrecognized_project");
      continue;
    }
    if (request.projectSlug && project.projectSlug !== request.projectSlug) continue;
    if (!request.projectSlug && (!project.labourEnabled || project.personal)) {
      exclude("project_not_labour_enabled");
      continue;
    }
    if (request.projectSlug && !project.labourEnabled) {
      exclude("project_not_labour_enabled");
      continue;
    }

    const workerResolution = resolveEntryWorker(entry, workers);
    if (workerResolution.status !== "resolved") {
      exclude(`worker_${workerResolution.status}`);
      continue;
    }
    const worker = workerResolution.worker;
    if (request.workerId && worker.workerId !== request.workerId) continue;

    const minutesResult = minutesForCanonicalEntry(entry);
    if (minutesResult.status !== "valid") {
      exclude(minutesResult.reason);
      continue;
    }
    for (const flag of minutesResult.auditFlags || []) {
      auditFlags[flag] = (auditFlags[flag] || 0) + 1;
    }
    included.push({
      id,
      reportDateKey,
      projectSlug: project.projectSlug,
      projectName: project.name,
      workerId: worker.workerId,
      workerName: worker.name,
      minutesWorked: minutesResult.minutes,
      valueSource: minutesResult.source,
    });
  }

  return {
    included,
    excludedCount: Object.values(excludedReasons).reduce((sum, count) => sum + count, 0),
    excludedReasons,
    auditFlags,
  };
}

function incrementMap(map, key, value) {
  map.set(key, (map.get(key) || 0) + value);
}

function aggregateCanonicalLabour({ canonical, request }) {
  const entries = [...(canonical.included || [])].sort((a, b) => {
    if (a.projectSlug !== b.projectSlug) return a.projectSlug.localeCompare(b.projectSlug);
    if (a.reportDateKey !== b.reportDateKey) return a.reportDateKey.localeCompare(b.reportDateKey);
    if (a.workerName !== b.workerName) return a.workerName.localeCompare(b.workerName);
    return a.id.localeCompare(b.id);
  });
  const projectMap = new Map();
  const workerIds = new Set();
  let totalMinutes = 0;

  for (const entry of entries) {
    totalMinutes += entry.minutesWorked;
    workerIds.add(entry.workerId);
    if (!projectMap.has(entry.projectSlug)) {
      projectMap.set(entry.projectSlug, {
        projectSlug: entry.projectSlug,
        projectName: entry.projectName,
        totalMinutes: 0,
        entryCount: 0,
        workerIds: new Set(),
        workerTotals: new Map(),
        dayTotals: new Map(),
        workerDayTotals: new Map(),
        documentIds: [],
      });
    }
    const project = projectMap.get(entry.projectSlug);
    project.totalMinutes += entry.minutesWorked;
    project.entryCount += 1;
    project.workerIds.add(entry.workerId);
    project.documentIds.push(entry.id);
    const workerKey = `${entry.workerId}\u0000${entry.workerName}`;
    incrementMap(project.workerTotals, workerKey, entry.minutesWorked);
    incrementMap(project.dayTotals, entry.reportDateKey, entry.minutesWorked);
    incrementMap(project.workerDayTotals, `${workerKey}\u0000${entry.reportDateKey}`, entry.minutesWorked);
  }

  const sections = [...projectMap.values()].map((project) => ({
    projectSlug: project.projectSlug,
    projectName: project.projectName,
    totalMinutes: project.totalMinutes,
    entryCount: project.entryCount,
    workerCount: project.workerIds.size,
    documentIds: project.documentIds,
    workerTotals: [...project.workerTotals.entries()]
      .map(([key, minutes]) => {
        const [workerId, workerName] = key.split("\u0000");
        return { workerId, workerName, totalMinutes: minutes };
      })
      .sort((a, b) => b.totalMinutes - a.totalMinutes || a.workerName.localeCompare(b.workerName)),
    dayTotals: [...project.dayTotals.entries()]
      .map(([reportDateKey, minutes]) => ({ reportDateKey, totalMinutes: minutes }))
      .sort((a, b) => a.reportDateKey.localeCompare(b.reportDateKey)),
    workerDayTotals: [...project.workerDayTotals.entries()]
      .map(([key, minutes]) => {
        const [workerId, workerName, reportDateKey] = key.split("\u0000");
        return { workerId, workerName, reportDateKey, totalMinutes: minutes };
      })
      .sort((a, b) =>
        a.workerName.localeCompare(b.workerName) || a.reportDateKey.localeCompare(b.reportDateKey)
      ),
  })).sort((a, b) => a.projectName.localeCompare(b.projectName));

  const earliest = entries.reduce(
    (minimum, entry) =>
      (!minimum || entry.reportDateKey < minimum ? entry.reportDateKey : minimum),
    null
  );
  const effectiveStartKey = request.startKey || earliest || request.endKey;
  return {
    request: { ...request, startKey: effectiveStartKey },
    totalMinutes,
    entryCount: entries.length,
    workerCount: workerIds.size,
    projectCount: sections.length,
    excludedCount: canonical.excludedCount,
    excludedReasons: canonical.excludedReasons,
    auditFlags: canonical.auditFlags,
    documentIds: entries.map((entry) => entry.id),
    sections,
    entries,
  };
}

function formatHours(minutes) {
  const value = Number(minutes);
  if (!Number.isSafeInteger(value) || value < 0) return "0";
  const whole = Math.floor(value / 60);
  const remainder = value % 60;
  if (!remainder) return String(whole);
  const decimal = (value / 60).toFixed(2).replace(/0+$/, "").replace(/\.$/, "");
  return decimal;
}

function formatHoursMinutes(minutes) {
  const value = Number(minutes);
  if (!Number.isSafeInteger(value) || value < 0) return "0h 0m";
  return `${Math.floor(value / 60)}h ${value % 60}m`;
}

function formatDateKeyForSms(dateKey) {
  if (!isValidDateKey(dateKey)) return String(dateKey || "");
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(`${dateKey}T12:00:00.000Z`));
}

function formatRangeForSms(startKey, endKey) {
  if (startKey === endKey) return formatDateKeyForSms(startKey);
  if (!isValidDateKey(startKey) || !isValidDateKey(endKey)) return `${startKey || ""} to ${endKey || ""}`.trim();
  const start = new Date(`${startKey}T12:00:00.000Z`);
  const end = new Date(`${endKey}T12:00:00.000Z`);
  const sameYear = start.getUTCFullYear() === end.getUTCFullYear();
  const startText = new Intl.DateTimeFormat("en-CA", {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
    ...(sameYear ? {} : { year: "numeric" }),
  }).format(start);
  const endText = new Intl.DateTimeFormat("en-CA", {
    timeZone: "UTC",
    month: start.getUTCMonth() === end.getUTCMonth() ? undefined : "short",
    day: "numeric",
    year: "numeric",
  }).format(end);
  return `${startText}–${endText}`;
}

function excludedWarning(result) {
  if (!result) return "";
  const warnings = [];
  if (result.excludedCount) {
    const count = result.excludedCount;
    warnings.push(
      `${count} invalid ${count === 1 ? "entry was" : "entries were"} excluded and ${count === 1 ? "requires" : "require"} review`
    );
  }
  const conflicts = Number(result.auditFlags?.contradictory_legacy_hours_ignored || 0);
  if (conflicts) {
    warnings.push(
      `${conflicts} conflicting legacy ${conflicts === 1 ? "hours value was" : "hours values were"} ignored and ${conflicts === 1 ? "requires" : "require"} review`
    );
  }
  return warnings.length ? ` Warning: ${warnings.join("; ")}.` : "";
}

function formatAdminLabourText(result) {
  const request = result.request || {};
  const range = formatRangeForSms(request.startKey, request.endKey);
  const rangeLabel = request.periodLabel && /^(Project-to-date|Current pay period|Previous pay period|All time)$/.test(request.periodLabel)
    ? `${request.periodLabel}${range ? ` (${range})` : ""}`
    : range || request.periodLabel || "Selected period";
  const warning = excludedWarning(result);

  if (!result.entryCount) {
    const scope = request.workerName || request.projectName || "the requested scope";
    const period = ["project_to_date", "all_time"].includes(request.periodKind)
      ? ` during ${request.periodLabel}`
      : range
        ? ` on ${range}`
        : "";
    return `No matching labour entries were found for ${scope}${period}.${warning}`;
  }

  if (request.intent === "who") {
    const sections = result.sections.map((section) => {
      const people = section.workerTotals.map((worker) => `${worker.workerName} ${formatHours(worker.totalMinutes)} h`).join("; ");
      return `${section.projectName}: ${people}; Total ${formatHours(section.totalMinutes)} h`;
    });
    const text = `${range} — ${sections.join(". ")}.${warning}`;
    if (text.length <= LABOUR_QUERY_MAX_SMS_CHARS) return text;
    return `${range}: ${formatHours(result.totalMinutes)} labour hours across ${result.workerCount} workers and ${result.entryCount} entries. Reply with a labour report request for the full breakdown.${warning}`;
  }

  if (request.workerName) {
    return `${request.workerName} — ${rangeLabel}: ${formatHours(result.totalMinutes)} hours across ${result.entryCount} ${result.entryCount === 1 ? "entry" : "entries"}.${warning}`;
  }
  if (request.projectName) {
    return `${request.projectName} — ${rangeLabel}: ${formatHours(result.totalMinutes)} labour hours across ${result.workerCount} workers and ${result.entryCount} entries.${warning}`;
  }
  const byProject = result.sections.map((section) => `${section.projectName}: ${formatHours(section.totalMinutes)} h`).join(". ");
  const text = `${rangeLabel}: ${formatHours(result.totalMinutes)} labour hours across ${result.workerCount} workers and ${result.entryCount} entries. ${byProject}.${warning}`;
  if (text.length <= LABOUR_QUERY_MAX_SMS_CHARS) return text;
  return `${rangeLabel}: ${formatHours(result.totalMinutes)} labour hours across ${result.workerCount} workers, ${result.projectCount} projects, and ${result.entryCount} entries. Reply with a labour report request for the project breakdown.${warning}`;
}

function formatAdminLabourHelp() {
  return [
    "Labour admin examples:",
    '"How many labour hours for Docksteader?"',
    '"How many hours did Ethan log on 2026-08-06?"',
    '"Who has hours on 2026-08-06?"',
    '"Labour report Docksteader 2026-08-01 to 2026-08-15"',
  ].join(" ");
}

function formatLabourClarification(prepared) {
  const reason = String(prepared && prepared.reason || "");
  const candidates = Array.isArray(prepared && prepared.candidates) ? prepared.candidates.slice(0, 4) : [];
  if (reason === "worker_ambiguous") return `Which worker did you mean: ${candidates.join(" or ")}?`;
  if (reason === "worker_unknown") return "Which registered worker did you mean? Reply with their exact current name.";
  if (reason === "project_ambiguous") return `Which project did you mean: ${candidates.join(" or ")}?`;
  if (reason === "project_unknown") return "Which project did you mean? Reply with its exact GridlineAI project name.";
  if (reason === "project_not_labour_enabled") return "That project is not configured for labour reporting.";
  if (reason === "pay_period_not_configured") return "The payroll-period calendar is not configured. Provide an explicit start and end date.";
  if (reason === "date_required_for_who") return "Which date should I check? Use YYYY-MM-DD.";
  if (reason === "invalid_date_range") return "Use a valid inclusive date or range, for example 2026-08-01 to 2026-08-15.";
  return "I could not resolve that labour scope safely. Provide an exact project, worker, and date range.";
}

async function loadLabourAdminRegistry(db) {
  const [projectSnap, workerSnap] = await Promise.all([
    db.collection("projects").get(),
    db.collection("labourers").get(),
  ]);
  return {
    projectRows: projectSnap.docs.map((doc) => ({ id: doc.id, ...doc.data() })),
    workerRows: workerSnap.docs.map((doc) => ({ id: doc.id, ...doc.data() })),
  };
}

async function loadCanonicalLabourEntryRows(db) {
  const snap = await db.collection("labourEntries").get();
  return snap.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
}

async function executePreparedAdminLabourQuery({ db, prepared }) {
  if (!prepared || prepared.status !== "ready") throw new Error("Prepared labour query is required.");
  const entries = await loadCanonicalLabourEntryRows(db);
  const canonical = canonicalizeLabourEntries({
    entries,
    projects: prepared.projects,
    workers: prepared.workers,
    request: prepared.request,
  });
  return aggregateCanonicalLabour({ canonical, request: prepared.request });
}

async function executeAdminLabourQuery({ db, parsed, now = new Date(), payPeriodConfig = LABOUR_PAY_PERIOD_CONFIG }) {
  const registry = await loadLabourAdminRegistry(db);
  const prepared = prepareAdminLabourQuery({
    parsed: { ...parsed, queryText: parsed.queryText || "" },
    ...registry,
    now,
    payPeriodConfig,
  });
  if (prepared.status !== "ready") return { prepared, result: null };
  const result = await executePreparedAdminLabourQuery({ db, prepared });
  return { prepared, result };
}

function buildLabourSmsRequestKey(messageSid) {
  const sid = String(messageSid || "").trim();
  if (!sid) return "";
  return createHash("sha256").update(`gridlineai-labour-query-v1:${sid}`).digest("hex").slice(0, 40);
}

function buildAdminLabourArtifactIdentity(requestKey) {
  const key = String(requestKey || "").trim().toLowerCase();
  if (!/^[a-f0-9]{40}$/.test(key)) return null;
  return {
    requestKey: key,
    requestDocId: `labour-query-${key}`,
    queueDocId: `admin-${key}`,
    reportId: `admin-${key}`,
    outboundMessageDocId: `labour-query-${key}`,
    storagePath: `adminLabourReports/${key}.pdf`,
  };
}

module.exports = {
  ADMIN_LABOUR_DENIAL_TEXT,
  LABOUR_QUERY_TIME_ZONE,
  aggregateCanonicalLabour,
  buildAdminLabourArtifactIdentity,
  buildLabourSmsRequestKey,
  canonicalizeLabourEntries,
  executeAdminLabourQuery,
  executePreparedAdminLabourQuery,
  formatAdminLabourHelp,
  formatAdminLabourText,
  formatHours,
  formatHoursMinutes,
  formatLabourClarification,
  getAdminLabourAccessDecision,
  isAdminLabourQueryCandidate,
  isAdministratorLabourAccess,
  isValidDateKey,
  loadCanonicalLabourEntryRows,
  loadLabourAdminRegistry,
  normalizeProjectRegistry,
  normalizeWorkerRegistry,
  parseAdminLabourQuery,
  parsePeriodRequest,
  prepareAdminLabourQuery,
  privacySafeReference,
  resolveDateRange,
  resolveProjectScope,
  resolveWorkerScope,
  torontoDateKey,
  validatePayPeriodConfig,
};
