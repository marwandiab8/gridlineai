/**
 * Deterministic SMS classification for field logs (no OpenAI required).
 */

function normalizeSmsForCommands(text) {
  return String(text || "")
    .replace(/[\u2018\u2019\u201A\u201B]/g, "'")
    .replace(/[\u201C\u201D\u201E\u201F]/g, '"')
    .replace(/\u00A0/g, " ")
    .trim();
}

function normalizeStructuredLogTypos(text) {
  return String(text || "")
    .replace(/^\s*load\s+(?=manp?o?w?e?r\b)/i, "log ")
    .replace(/\bdeficiciency\b/gi, "deficiency")
    .replace(/\bmanpwer\b/gi, "manpower")
    .replace(/\bmanpoewer\b/gi, "manpower")
    .replace(/\bmanpwower\b/gi, "manpower");
}

function retagStructuredNoteBody(parsed) {
  if (!parsed || parsed.logParsedType !== "note") return parsed;
  const body = String(parsed.body || "").trim();
  if (!body) return parsed;

  const nestedTypes = [
    {
      re: /^manpower\b[:\-–—]?\s*(.*)$/i,
      category: "note",
      logParsedType: "manpower",
      tags: ["manpower"],
    },
    {
      re: /^progress\b[:\-–—]?\s*(.*)$/i,
      category: "progress",
      logParsedType: "progress",
      tags: ["progress"],
    },
  ];

  for (const candidate of nestedTypes) {
    const match = body.match(candidate.re);
    if (!match) continue;
    const extracted = extractExplicitReportDate((match[1] || "").trim());
    return {
      ...parsed,
      category: candidate.category,
      logParsedType: candidate.logParsedType,
      tags: [...new Set([...(parsed.tags || []), ...candidate.tags])],
      body: extracted.cleanedText || body,
      reportDateKey: extracted.reportDateKey || parsed.reportDateKey || null,
    };
  }

  return parsed;
}

function normalizeProjectHintSlug(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function normalizeProjectScopedText(text) {
  return String(text || "")
    .replace(/\s+/g, " ")
    .replace(/\s+([:;,.!?])/g, "$1")
    .trim();
}

function extractProjectScopeHint(text) {
  const raw = normalizeSmsForCommands(text);
  if (!raw) {
    return {
      projectSlug: null,
      cleanedText: "",
      scopeOnly: false,
      matchedText: "",
    };
  }

  const scopeOnly = [
    /^(?:log\s+)?(?:project\s+(?<slugA>[a-z0-9][a-z0-9-]*)|(?<slugB>[a-z0-9][a-z0-9-]*)\s+project)$/i,
    /^(?:for|under|to|on|in)\s+(?:the\s+)?(?:project\s+(?<slugA>[a-z0-9][a-z0-9-]*)|(?<slugB>[a-z0-9][a-z0-9-]*)\s+project)$/i,
  ];
  for (const re of scopeOnly) {
    const m = raw.match(re);
    if (!m) continue;
    const slug = normalizeProjectHintSlug(m.groups?.slugA || m.groups?.slugB || "");
    if (!slug) continue;
    return {
      projectSlug: slug,
      cleanedText: "",
      scopeOnly: true,
      matchedText: m[0],
    };
  }

  const leadProject = raw.match(
    /^(?<lead>log|daily\s+log)\s+(?:project\s+(?<slugA>[a-z0-9][a-z0-9-]*)|(?<slugB>[a-z0-9][a-z0-9-]*)\s+project)(?<tail>(?:\s*[:\-–—]\s*.*|\s+.*)?)$/i
  );
  if (leadProject) {
    const slug = normalizeProjectHintSlug(
      leadProject.groups?.slugA || leadProject.groups?.slugB || ""
    );
    if (slug) {
      const cleaned = normalizeProjectScopedText(
        `${leadProject.groups?.lead || ""} ${leadProject.groups?.tail || ""}`
      );
      return {
        projectSlug: slug,
        cleanedText: cleaned,
        scopeOnly: /^(log|daily\s+log)$/i.test(cleaned),
        matchedText: leadProject[0],
      };
    }
  }

  const genericLead = raw.match(
    /^(?:project\s+(?<slugA>[a-z0-9][a-z0-9-]*)|(?<slugB>[a-z0-9][a-z0-9-]*)\s+project)\s+(?<rest>.+)$/i
  );
  if (genericLead) {
    const slug = normalizeProjectHintSlug(
      genericLead.groups?.slugA || genericLead.groups?.slugB || ""
    );
    if (slug) {
      const cleaned = normalizeProjectScopedText(genericLead.groups?.rest || "");
      return {
        projectSlug: slug,
        cleanedText: cleaned,
        scopeOnly: !cleaned,
        matchedText: genericLead[0],
      };
    }
  }

  const inline = raw.match(
    /\b(?:for|under|to|on|in)\s+(?:the\s+)?(?:project\s+(?<slugA>[a-z0-9][a-z0-9-]*)|(?<slugB>[a-z0-9][a-z0-9-]*)\s+project)\b/i
  );
  if (inline) {
    const slug = normalizeProjectHintSlug(inline.groups?.slugA || inline.groups?.slugB || "");
    if (slug) {
      const cleaned = normalizeProjectScopedText(raw.replace(inline[0], " "));
      return {
        projectSlug: slug,
        cleanedText: cleaned,
        scopeOnly: !cleaned,
        matchedText: inline[0],
      };
    }
  }

  return {
    projectSlug: null,
    cleanedText: raw,
    scopeOnly: false,
    matchedText: "",
  };
}

function dateKeyUtc(d = new Date()) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/** IANA zone for daily log boundaries, PDF titles, and `dateKey` on new writes. */
const DAILY_REPORT_TIME_ZONE = "America/New_York";

function dateKeyEastern(d = new Date()) {
  const dt = d instanceof Date ? d : new Date(d);
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(dt);
}

/**
 * Instant of local midnight in {@link DAILY_REPORT_TIME_ZONE} for calendar day `YYYY-MM-DD` in that zone.
 */
function startOfEasternDayForDateKey(key) {
  const parts = String(key || "").split("-").map(Number);
  if (parts.length !== 3 || parts.some((n) => !Number.isFinite(n))) {
    return new Date();
  }
  const [ys, ms, ds] = parts;
  let lo = Date.UTC(ys, ms - 1, ds) - 48 * 3600000;
  let hi = Date.UTC(ys, ms - 1, ds) + 48 * 3600000;
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    const midKey = dateKeyEastern(new Date(mid));
    if (midKey < key) lo = mid;
    else hi = mid;
  }
  return new Date(hi);
}

function startOfEasternDay(d = new Date()) {
  return startOfEasternDayForDateKey(dateKeyEastern(d));
}

/** Proleptic Gregorian step on the date string (for week rows, etc.). */
function addCalendarDaysToDateKey(dateKey, delta) {
  const [y, m, d] = String(dateKey || "").split("-").map(Number);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) {
    return dateKeyEastern(new Date());
  }
  const nd = new Date(Date.UTC(y, m - 1, d + Number(delta)));
  return `${nd.getUTCFullYear()}-${String(nd.getUTCMonth() + 1).padStart(2, "0")}-${String(
    nd.getUTCDate()
  ).padStart(2, "0")}`;
}

function easternNoonInstantForDateKey(key) {
  return startOfEasternDayForDateKey(key).getTime() + 12 * 3600000;
}

function fmtMonDayEasternDateKey(key) {
  const d = new Date(easternNoonInstantForDateKey(key));
  return new Intl.DateTimeFormat("en-US", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    month: "short",
    day: "numeric",
  }).format(d);
}

function weekdayShortEasternDateKey(key) {
  const d = new Date(easternNoonInstantForDateKey(key));
  return new Intl.DateTimeFormat("en-US", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    weekday: "short",
  }).format(d);
}

function formatDailySiteLogTitleEastern(dayStart) {
  const d0 = dayStart instanceof Date ? dayStart : new Date(dayStart);
  const d = new Date(d0.getTime() + 12 * 3600000);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  }).formatToParts(d);
  const get = (type) => parts.find((p) => p.type === type)?.value || "";
  return `Daily Site Log – ${get("weekday")} ${get("month")} ${get("day")}, ${get("year")}`;
}

function formatConcreteSummaryLabelEastern(dayStart) {
  const d0 = dayStart instanceof Date ? dayStart : new Date(dayStart);
  const d = new Date(d0.getTime() + 12 * 3600000);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    month: "short",
    day: "numeric",
  }).formatToParts(d);
  const get = (type) => parts.find((p) => p.type === type)?.value || "";
  return `${get("month")} ${get("day")}`;
}

/** Wall date + 24h time in the report zone, for captions. */
function formatWallDateTimeEt(d) {
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return "";
  const ymd = new Intl.DateTimeFormat("en-CA", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(dt);
  const hm = new Intl.DateTimeFormat("en-GB", {
    timeZone: DAILY_REPORT_TIME_ZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(dt);
  return `${ymd} ${hm} ET`;
}

function isIsoDateKey(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "").trim());
}

function normalizeLooseIsoDateKey(value) {
  const raw = String(value || "").trim();
  if (isIsoDateKey(raw)) return raw;
  const basic = raw.match(/^(\d{4})-(\d{2})-(\d{1,2})$/);
  if (basic) {
    return `${basic[1]}-${basic[2]}-${String(basic[3]).padStart(2, "0")}`;
  }
  const compact = raw.match(/^(\d{4})-(\d{2})(\d{2})$/);
  if (compact) {
    return `${compact[1]}-${compact[2]}-${compact[3]}`;
  }
  const fatFinger = raw.match(/^(\d{4})-(\d{2})0(\d{2})$/);
  if (fatFinger) {
    return `${fatFinger[1]}-${fatFinger[2]}-${fatFinger[3]}`;
  }
  return null;
}

function extractExplicitReportDate(text) {
  let cleanedText = normalizeSmsForCommands(text);
  let reportDateKey = null;

  const patterns = [
    /^\(\s*(\d{4}-\d{2}(?:-?\d{2,3}))\s*\)\s*[:\-–—]?\s*/i,
    /^(?:for|on|dated|date)\s+(\d{4}-\d{2}(?:-?\d{2,3}))\b\s*[:\-–—]?\s*/i,
  ];

  for (const re of patterns) {
    const match = cleanedText.match(re);
    if (!match) continue;
    reportDateKey = match[1];
    cleanedText = cleanedText.slice(match[0].length).trim();
    break;
  }

  if (!reportDateKey) {
    const embeddedPatterns = [
      /\(\s*(\d{4}-\d{2}(?:-?\d{2,3}))\s*\)/i,
      /\b(?:for|on|dated|date)\s+(\d{4}-\d{2}(?:-?\d{2,3}))\b/i,
    ];
    for (const re of embeddedPatterns) {
      const match = cleanedText.match(re);
      if (!match) continue;
      reportDateKey = match[1];
      break;
    }
  }

  return {
    reportDateKey: normalizeLooseIsoDateKey(reportDateKey),
    cleanedText,
  };
}

/**
 * @returns {{ category: string, body: string, logParsedType: string, tags: string[], source: string, reportDateKey: string|null } | null}
 */
function parseStructuredLog(text) {
  const t = normalizeStructuredLogTypos(normalizeSmsForCommands(text));
  const patterns = [
    // Longer / more specific first
    [/^log\s+(?:a\s+)?safety\s+issue\s*:\s*(.*)$/i, "safety", "safety", []],
    [/^log\s+(?:a\s+)?safety\s+issue\s*[-–—]\s*(.*)$/i, "safety", "safety", []],
    [/^log\s+safety\s+issue\s+(?!:)\s*(.+)$/i, "safety", "safety", []],
    [/^log\s+safety\s*:\s*(.*)$/i, "safety", "safety", []],
    [/^log\s+safety\s*[-–—]\s*(.*)$/i, "safety", "safety", []],
    [/^log\s+safety\s+(?!:)\s*(.+)$/i, "safety", "safety", []],

    [/^log\s+(?:a\s+)?deficiency\s*:\s*(.*)$/i, "deficiency", "deficiency", []],
    [/^log\s+(?:a\s+)?deficiency\s*[-–—]\s*(.*)$/i, "deficiency", "deficiency", []],
    [/^log\s+(?:a\s+)?deficiency\s+(?!:)\s*(.+)$/i, "deficiency", "deficiency", []],
    [/^log\s+(?:a\s+)?punch(?:\s+item)?\s*:\s*(.*)$/i, "deficiency", "deficiency", ["punch"]],
    [/^log\s+(?:a\s+)?punch(?:\s+item)?\s*[-–—]\s*(.*)$/i, "deficiency", "deficiency", ["punch"]],
    [/^log\s+(?:a\s+)?punch(?:\s+item)?\s+(?!:)\s*(.+)$/i, "deficiency", "deficiency", ["punch"]],

    [/^log\s+delay\s*:\s*(.*)$/i, "delay", "delay", []],
    [/^log\s+delay\s*[-–—]\s*(.*)$/i, "delay", "delay", []],
    [/^log\s+delay\s+(?!:)\s*(.+)$/i, "delay", "delay", []],

    [/^log\s+inspection\s*:\s*(.*)$/i, "inspection", "inspection", ["inspection"]],
    [/^log\s+inspection\s*[-–—]\s*(.*)$/i, "inspection", "inspection", ["inspection"]],
    [/^log\s+inspection\s+(?!:)\s*(.+)$/i, "inspection", "inspection", ["inspection"]],

    [/^log\s+(?:an?\s+)?issue\s*:\s*(.*)$/i, "issue", "issue", []],
    [/^log\s+(?:an?\s+)?issue\s*[-–—]\s*(.*)$/i, "issue", "issue", []],
    [/^log\s+issue\s+(?!:)\s*(.+)$/i, "issue", "issue", []],

    [/^log\s+(?:a\s+)?delivery\s*:\s*(.*)$/i, "delivery", "delivery", []],
    [/^log\s+(?:a\s+)?delivery\s*[-–—]\s*(.*)$/i, "delivery", "delivery", []],
    [/^log\s+delivery\s+(?!:)\s*(.+)$/i, "delivery", "delivery", []],

    [/^log\s+(?:a\s+)?note\s*:\s*(.*)$/i, "note", "note", []],
    [/^log\s+(?:a\s+)?note\s*[-–—]\s*(.*)$/i, "note", "note", []],
    [/^log\s+note\s+(?!:)\s*(.+)$/i, "note", "note", []],

    [/^log\s+manpower\s*:\s*(.*)$/i, "note", "manpower", ["manpower"]],
    [/^log\s+manpower\s*[-–—]\s*(.*)$/i, "note", "manpower", ["manpower"]],
    [/^log\s+manpower\s+(?!:)\s*(.+)$/i, "note", "manpower", ["manpower"]],

    [/^log\s+(?:a\s+)?progress\s*:\s*(.*)$/i, "progress", "progress", []],
    [/^log\s+(?:a\s+)?progress\s*[-–—]\s*(.*)$/i, "progress", "progress", []],
    [/^log\s+progress\s+(?!:)\s*(.+)$/i, "progress", "progress", []],

    [/^daily\s+log\s*:\s*(.*)$/i, "note", "daily_log", ["daily_log"]],
    [/^daily\s+log\s*[-–—]\s*(.*)$/i, "note", "daily_log", ["daily_log"]],
    [/^daily\s+log\s+(?!:)\s*(.+)$/i, "note", "daily_log", ["daily_log"]],

    [/^delivery\s*:\s*(.*)$/i, "delivery", "delivery", []],
    [/^delivery\s+(.+)$/i, "delivery", "delivery", []],

    [/^inspection\s*:\s*(.*)$/i, "inspection", "inspection", ["inspection"]],
    [/^inspection\s+(.+)$/i, "inspection", "inspection", ["inspection"]],
  ];

  for (const [re, category, logParsedType, tags] of patterns) {
    const m = t.match(re);
    if (m) {
      const extracted = extractExplicitReportDate((m[1] != null ? m[1] : "").trim());
      return retagStructuredNoteBody({
        category,
        body: extracted.cleanedText,
        logParsedType,
        tags,
        source: "command",
        reportDateKey: extracted.reportDateKey,
      });
    }
  }

  const MIN_SHORTHAND = 12;
  if (t.length < MIN_SHORTHAND) return null;

  const shorthand = [
    [/^safety\s*:\s*(.+)$/i, "safety", "safety", []],
    [/^safety\s+(.+)$/i, "safety", "safety", []],
    [/^delay\s*:\s*(.+)$/i, "delay", "delay", []],
    [/^delay\s+(.+)$/i, "delay", "delay", []],
    [/^deficiency\s*:\s*(.+)$/i, "deficiency", "deficiency", []],
    [/^deficiency\s+(.+)$/i, "deficiency", "deficiency", []],
    [/^punch(?:\s+item)?\s*:\s*(.+)$/i, "deficiency", "deficiency", ["punch"]],
    [/^punch(?:\s+item)?\s+(.+)$/i, "deficiency", "deficiency", ["punch"]],
    [/^issue\s*:\s*(.+)$/i, "issue", "issue", []],
    [/^issue\s+(.+)$/i, "issue", "issue", []],
    [/^note\s*:\s*(.+)$/i, "note", "note", []],
    [/^note\s+(.+)$/i, "note", "note", []],
    [/^manpower\s*:\s*(.+)$/i, "note", "manpower", ["manpower"]],
    [/^manpower\s+(.+)$/i, "note", "manpower", ["manpower"]],
    [/^progress\s*:\s*(.+)$/i, "progress", "progress", []],
    [/^progress\s+(.+)$/i, "progress", "progress", []],
  ];

  for (const [re, category, logParsedType, tags] of shorthand) {
    const m = t.match(re);
    if (m) {
      const extracted = extractExplicitReportDate((m[1] || "").trim());
      const body = extracted.cleanedText;
      if (!body) continue;
      return retagStructuredNoteBody({
        category,
        body,
        logParsedType,
        tags,
        source: "shorthand",
        reportDateKey: extracted.reportDateKey,
      });
    }
  }

  return null;
}

function mapDeficiencyLabel(label) {
  const key = String(label || "").toLowerCase().replace(/\s+/g, " ").trim();
  if (key === "project") return "projectSlug";
  if (key === "title") return "title";
  if (key === "description" || key === "desc" || key === "details") return "description";
  if (key === "location") return "location";
  if (key === "area") return "area";
  if (key === "trade") return "trade";
  if (key === "reference" || key === "ref") return "reference";
  if (key === "required action" || key === "action") return "requestedAction";
  return null;
}

function cleanDeficiencyValue(value) {
  return String(value || "")
    .replace(/^[\s:;,\-.]+/, "")
    .replace(/[\s,;]+$/g, "")
    .trim();
}

function parseDeficiencyDetails(text) {
  let raw = normalizeSmsForCommands(text);
  if (!raw) {
    return {
      projectSlug: null,
      fields: {},
      freeText: "",
    };
  }

  const hinted = extractProjectScopeHint(raw);
  let projectSlug = hinted.projectSlug || null;
  raw = projectSlug
    ? String(hinted.cleanedText || "").trim()
    : String(hinted.cleanedText || raw).trim();

  if (!projectSlug) {
    const leadProject = raw.match(
      /^(?:for\s+project\s+)?([a-z0-9][a-z0-9-]{0,79})\s*[:\-]\s*(.+)$/i
    );
    if (leadProject) {
      const maybe = normalizeProjectHintSlug(leadProject[1]);
      if (maybe && !mapDeficiencyLabel(maybe)) {
        projectSlug = maybe;
        raw = String(leadProject[2] || "").trim();
      }
    }
  }

  const labelRegex =
    /\b(required\s+action|description|reference|location|project|details|title|trade|action|area|desc|ref)\b\s*[:=-]?\s*/gi;
  const matches = [...raw.matchAll(labelRegex)];
  const fields = {};
  let freeText = raw;

  if (matches.length) {
    freeText = cleanDeficiencyValue(raw.slice(0, matches[0].index || 0));
    for (let i = 0; i < matches.length; i += 1) {
      const match = matches[i];
      const label = mapDeficiencyLabel(match[1]);
      if (!label) continue;
      const start = (match.index || 0) + match[0].length;
      const end = i + 1 < matches.length ? matches[i + 1].index || raw.length : raw.length;
      const value = cleanDeficiencyValue(raw.slice(start, end));
      if (!value) continue;
      if (label === "projectSlug") {
        const maybe = normalizeProjectHintSlug(value);
        if (maybe) projectSlug = maybe;
        continue;
      }
      fields[label] = value;
    }
  }

  return {
    projectSlug,
    fields,
    freeText,
  };
}

function parseDeficiencyIntakeRequest(text) {
  const raw = normalizeSmsForCommands(text);
  if (!raw) return null;

  const match = raw.match(
    /^(?:log|create|new)\s+(?:a\s+)?(?:deficiency|punch(?:\s+item)?)\b(?<rest>.*)$/i
  ) || raw.match(/^(?:deficiency|punch(?:\s+item)?)\b(?<rest>.*)$/i);
  if (!match) return null;

  let rest = String(match.groups?.rest || "").trim();
  rest = rest.replace(/^[:\-–—\s]+/, "");

  const parsed = parseDeficiencyDetails(rest);
  if (!parsed.fields.description && parsed.freeText) {
    parsed.fields.description = parsed.freeText;
  }
  return {
    projectSlug: parsed.projectSlug,
    fields: parsed.fields,
    freeText: parsed.freeText,
    normalizedText: raw,
  };
}

function isDailyLogViewRequest(text) {
  return /^(daily\s+log|show\s+today'?s?\s+log|what\s+did\s+i\s+log\s+today|full\s+log\s+today|today'?s?\s+log|show\s+my\s+log\s+today)$/i.test(
    (text || "").trim()
  );
}

function isSummaryStyleRequest(text) {
  return /^(daily\s+summary|summarize\s+today|summary\s+for\s+today|what\s+happened\s+today|site\s+summary|today'?s?\s+summary)$/i.test(
    (text || "").trim()
  );
}

function isAnyDayRollupRequest(text) {
  return Boolean(parseDayRollupRequest(text));
}

function parseDayRollupRequest(text) {
  const raw = normalizeSmsForCommands(text);
  if (!raw) return null;

  const dateMatch = raw.match(/\(\s*(\d{4}-\d{2}-\d{2})\s*\)|\b(\d{4}-\d{2}-\d{2})\b/i);
  const reportDateKey = dateMatch ? dateMatch[1] || dateMatch[2] : null;
  const wantsYesterday = /\byesterday\b/i.test(raw);
  const wantsToday = /\btoday\b/i.test(raw);

  const viewPatterns = [
    /^daily\s+log$/i,
    /^show\s+today'?s?\s+log$/i,
    /^what\s+did\s+i\s+log\s+today$/i,
    /^full\s+log\s+today$/i,
    /^today'?s?\s+log$/i,
    /^show\s+my\s+log\s+today$/i,
    /^show\s+me\s+what\s+is\s+logged\b/i,
    /^what\s+is\s+logged\b/i,
    /^show\s+what\s+is\s+logged\b/i,
    /^show\s+me\s+the\s+log\b/i,
    /^what\s+did\s+i\s+log\b/i,
    /^show\s+me\s+the\s+activities\b/i,
    /^show\s+me\s+activities\b/i,
    /^show\s+me\s+the\s+activity\b/i,
    /^show\s+me\s+activity\b/i,
    /^what\s+were\s+the\s+activities\b/i,
    /^what\s+was\s+the\s+activity\b/i,
  ];
  const summaryPatterns = [
    /^daily\s+summary$/i,
    /^summarize\s+today$/i,
    /^summary\s+for\s+today$/i,
    /^what\s+happened\s+today$/i,
    /^site\s+summary$/i,
    /^today'?s?\s+summary$/i,
    /^summarize\b/i,
    /^summary\b/i,
  ];

  const matchedView = viewPatterns.some((re) => re.test(raw));
  const matchedSummary = summaryPatterns.some((re) => re.test(raw));
  const mentionsLogged = /\b(log(?:ged)?|summary)\b/i.test(raw);
  const mentionsActivityLookup =
    /\bactivit(?:y|ies)\b/i.test(raw) &&
    /^(?:please\s+)?(?:show|what|read|give|tell|list)\b/i.test(raw);

  if (!matchedView && !matchedSummary) {
    if ((!mentionsLogged && !mentionsActivityLookup) || (!reportDateKey && !wantsYesterday && !wantsToday)) {
      return null;
    }
  }

  return {
    reportDateKey: isIsoDateKey(reportDateKey)
      ? reportDateKey
      : wantsYesterday
        ? addCalendarDaysToDateKey(dateKeyEastern(new Date()), -1)
        : wantsToday
          ? dateKeyEastern(new Date())
          : null,
    preferAiNarrative: Boolean(matchedSummary),
    normalizedText: raw,
  };
}

function finalizeDailyReportProjectCandidate(rest, existingProjectSlug) {
  const cleaned = String(rest || "")
    .replace(/\b(?:for|on|dated|date|project)\b/gi, " ")
    .replace(/[.!?,]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (existingProjectSlug) {
    const normalizedCleaned = normalizeProjectHintSlug(cleaned);
    return {
      projectSlug: existingProjectSlug,
      remainingText:
        !cleaned || normalizedCleaned === existingProjectSlug ? "" : cleaned,
    };
  }
  if (!cleaned) {
    return { projectSlug: null, remainingText: "" };
  }

  const projectOnly = cleaned.match(
    /^(?:project\s+)?([a-z0-9][a-z0-9-]{0,79})(?:\s+project)?$/i
  );
  if (projectOnly) {
    const maybe = normalizeProjectHintSlug(projectOnly[1]);
    if (maybe) {
      return { projectSlug: maybe, remainingText: "" };
    }
  }

  return {
    projectSlug: null,
    remainingText: cleaned,
  };
}

function parseDailyReportRequest(text) {
  const raw = normalizeSmsForCommands(text);
  if (!raw) return null;

  const hinted = extractProjectScopeHint(raw);
  let projectSlug = hinted.projectSlug || null;
  const t = String(hinted.cleanedText || raw).replace(/\s+/g, " ").trim();

  const direct = t.match(
    /^(?:(?:please|can\s+you|could\s+you|can\s+i|i\s+need|i\s+want|get\s+me|send\s+me|text\s+me|give\s+me)\s+)?(?<core>daily\s+report|daily\s+pdf|daily\s+pdf\s+report|daily\s+journal\s+pdf|journal\s+pdf|pdf\s+report(?:\s+today)?|journal\s+report|eod\s+report|end\s+of\s+day\s+report|generate\s+(?:daily\s+)?report)(?<rest>[\s\S]*)$/i
  );

  let rest = "";
  let matched = false;
  let reportType = "dailySiteLog";
  if (direct) {
    rest = String(direct.groups?.rest || "").trim();
    matched = true;
    if (/\b(?:daily\s+journal\s+pdf|journal\s+pdf|journal\s+report)\b/i.test(direct.groups?.core || "")) {
      reportType = "journal";
    }
  } else {
    const low = t.toLowerCase();
    if (
      /^(please|can\s+you|could\s+you|can\s+i|i\s+need|i\s+want|get\s+me|send\s+me|text\s+me|give\s+me)\b/i.test(
        low
      ) &&
      /\bdaily\s+report\b/i.test(t)
    ) {
      matched = true;
      rest = t
        .replace(
          /^(please|can\s+you|could\s+you|can\s+i|i\s+need|i\s+want|get\s+me|send\s+me|text\s+me|give\s+me)\s+/i,
          ""
        )
        .replace(/\bdaily\s+report\b/i, "")
        .trim();
    } else if (
      /\bdaily[\s-]+report\b/i.test(t) &&
      /\b(pdf|\.pdf|download|link|eod|end\s+of\s+day)\b/i.test(t)
    ) {
      matched = true;
      rest = t.replace(/\bdaily[\s-]+report\b/i, "").trim();
    } else if (
      /\b(send|text|give)\s+me\b/i.test(t) &&
      /\bdaily\b/i.test(t) &&
      /\b(report|pdf)\b/i.test(t)
    ) {
      matched = true;
      rest = t.trim();
    }
  }

  if (!matched) return null;
  if (rest && /^[:;â€”\-]/.test(rest)) return null;

  if (/\b(?:personal\s+)?journal(?:\s+style)?\b/i.test(rest)) {
    reportType = "journal";
    rest = rest.replace(/\b(?:personal\s+)?journal(?:\s+style)?\b/gi, " ");
  } else if (/\b(?:daily\s+)?site\s+log(?:\s+style)?\b/i.test(rest)) {
    reportType = "dailySiteLog";
    rest = rest.replace(/\b(?:daily\s+)?site\s+log(?:\s+style)?\b/gi, " ");
  }

  let reportDateKey = null;
  const iso = rest.match(/\b(\d{4}-\d{2}-\d{2})\b/);
  if (iso) {
    reportDateKey = iso[1];
    rest = rest.replace(iso[0], " ");
  } else if (/\byesterday\b/i.test(rest)) {
    reportDateKey = addCalendarDaysToDateKey(dateKeyEastern(new Date()), -1);
    rest = rest.replace(/\byesterday\b/gi, " ");
  } else if (/\btoday\b/i.test(rest)) {
    reportDateKey = dateKeyEastern(new Date());
    rest = rest.replace(/\btoday\b/gi, " ");
  }

  rest = rest
    .replace(/\b(?:please|thanks|thank\s+you|ok|okay|now|for|the|day|pdf|download|link|report)\b/gi, " ")
    .replace(/[.!?â€¦]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!projectSlug) {
    const trailingProject = rest.match(/^(?:project\s+)?([a-z0-9][a-z0-9-]{0,79})(?:\s+project)?$/i);
    if (trailingProject) {
      const maybe = normalizeProjectHintSlug(trailingProject[1]);
      if (maybe) {
        projectSlug = maybe;
      }
    }
  }

  return {
    reportType,
    reportDateKey,
    projectSlug,
    normalizedText: t,
  };
}

function parseDailyReportRequestV2(text) {
  const base = parseDailyReportRequest(text);
  if (!base) return null;

  const raw = normalizeSmsForCommands(text);
  const hinted = extractProjectScopeHint(raw);
  let rest = String(hinted.cleanedText || raw)
    .replace(/\s+/g, " ")
    .trim();

  const direct = rest.match(
    /^(?:(?:please|can\s+you|could\s+you|can\s+i|i\s+need|i\s+want|get\s+me|send\s+me|text\s+me|give\s+me)\s+)?(?:daily\s+report|daily\s+pdf|daily\s+pdf\s+report|daily\s+journal\s+pdf|journal\s+pdf|journal\s+report|pdf\s+report(?:\s+today)?|eod\s+report|end\s+of\s+day\s+report|generate\s+(?:daily\s+)?report)(?<tail>[\s\S]*)$/i
  );
  if (direct) {
    rest = String(direct.groups?.tail || "").trim();
  } else {
    rest = rest
      .replace(
        /^(please|can\s+you|could\s+you|can\s+i|i\s+need|i\s+want|get\s+me|send\s+me|text\s+me|give\s+me)\s+/i,
        ""
      )
      .replace(
        /\b(?:daily[\s-]+report|daily\s+pdf|daily\s+pdf\s+report|daily\s+journal\s+pdf|journal\s+pdf|journal\s+report)\b/i,
        ""
      )
      .trim();
  }

  const journalMatches = rest.match(/\b(?:personal\s+)?journal(?:\s+style)?\b/gi) || [];
  const siteLogMatches =
    rest.match(/\b(?:daily\s+)?site\s+log(?:\s+style)?\b/gi) || [];
  if (journalMatches.length && siteLogMatches.length) {
    return {
      ...base,
      invalidReason: "Choose either journal or daily site log, not both.",
    };
  }
  if (journalMatches.length > 1 || siteLogMatches.length > 1) {
    return {
      ...base,
      invalidReason: "Use only one report type in the SMS request.",
    };
  }
  rest = rest
    .replace(/\b(?:personal\s+)?journal(?:\s+style)?\b/gi, " ")
    .replace(/\b(?:daily\s+)?site\s+log(?:\s+style)?\b/gi, " ");

  const isoMatches = [...rest.matchAll(/\b(\d{4}-\d{2}-\d{2})\b/g)].map((m) => m[1]);
  const hasYesterday = /\byesterday\b/i.test(rest);
  const hasToday = /\btoday\b/i.test(rest);
  const dateHintCount =
    isoMatches.length + (hasYesterday ? 1 : 0) + (hasToday ? 1 : 0);
  if (dateHintCount > 1) {
    return {
      ...base,
      invalidReason: "Use only one report date in the SMS request.",
    };
  }
  rest = rest
    .replace(/\b\d{4}-\d{2}-\d{2}\b/g, " ")
    .replace(/\byesterday\b/gi, " ")
    .replace(/\btoday\b/gi, " ")
    .replace(
      /\b(?:please|thanks|thank\s+you|ok|okay|now|for|the|day|pdf|download|link|report|dated|date|on)\b/gi,
      " "
    )
    .replace(/[.!?]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const projectCandidate = finalizeDailyReportProjectCandidate(
    rest,
    base.projectSlug || null
  );
  if (projectCandidate.remainingText) {
    return {
      ...base,
      projectSlug: projectCandidate.projectSlug || base.projectSlug || null,
      invalidReason:
        `Couldn't understand "${projectCandidate.remainingText}" in that daily report request.`,
    };
  }

  return {
    ...base,
    projectSlug: projectCandidate.projectSlug || base.projectSlug || null,
    invalidReason: null,
  };
}

/** Asks for the end-of-day PDF (must hit backend, not OpenAI). */
function isDailyReportPdfRequest(text) {
  const raw = (text || "").trim();
  if (!raw) return false;
  const t = raw.replace(/\s+/g, " ").trim();

  const core =
    /^(daily\s+report|generate\s+report|end\s+of\s+day\s+report|pdf\s+report\s+today|eod\s+report)([\s\S]*)$/i;
  const m = t.match(core);
  if (m) {
    const rest = (m[2] || "").trim();
    if (!rest) return true;
    if (/^[:;—\-]/.test(rest)) return false;
    if (/^[.!?…]+$/u.test(rest)) return true;
    if (
      /^(please|thanks|thank\s+you|ok|okay|today|now|for\s+today|for\s+the\s+day)\b/i.test(rest)
    ) {
      return true;
    }
    if (/^(please|thanks|thank\s+you)\s*[.!?…]*$/iu.test(rest)) return true;
    return false;
  }

  const low = t.toLowerCase();
  if (
    /^(please|can\s+you|could\s+you|can\s+i|i\s+need|i\s+want|get\s+me|send\s+me|give\s+me)\b/i.test(
      low
    ) &&
    /\bdaily\s+report\b/i.test(t)
  ) {
    return true;
  }

  if (/\bdaily[\s-]+report\b/i.test(t) && /\b(pdf|\.pdf|download|link|eod|end\s+of\s+day)\b/i.test(t)) {
    return true;
  }

  if (
    /\b(send|text|give)\s+me\b/i.test(t) &&
    /\bdaily\b/i.test(t) &&
    /\b(report|pdf)\b/i.test(t)
  ) {
    return true;
  }

  return false;
}

function isDailyReportPdfRequestParsed(text) {
  return Boolean(parseDailyReportRequestV2(text));
}

/** Inbound messages that should not create a logEntry (commands / UI). */
function isMetaInbound(trimmedBody, lower) {
  if (!trimmedBody) return true;
  if (lower === "help" || lower === "commands" || lower === "?") return true;
  if (lower === "ai check" || lower === "openai check") return true;
  if (lower === "status") return true;
  if (lower === "contact" || lower === "contacts") return true;
  if (lower === "reset" || lower === "reset conversation" || lower === "reset context")
    return true;
  if (/^project\s+\S+/i.test(trimmedBody)) return true;
  if (isDailyReportPdfRequestParsed(trimmedBody)) return true;
  if (isAnyDayRollupRequest(trimmedBody)) return true;
  if (/^(continue|go on|keep going|more|expand|elaborate|rewrite|reword|try again)$/i.test(trimmedBody.trim()))
    return true;
  if (/\b(show|read|give)\s+me\s+.*\b(journal|input|note|notes)\b/i.test(trimmedBody))
    return true;
  if (/\bwhat\s+(did\s+i\s+(say|send)|was\s+my\s+(journal|note|input))\b/i.test(trimmedBody))
    return true;
  const oneWord = /^(schedule|today|safety|report|issue)$/i;
  if (oneWord.test(trimmedBody.trim())) return true;
  if (/^(thanks|thank you|ok|okay|yes|no|hi|hello)$/i.test(trimmedBody.trim()))
    return true;
  return false;
}

module.exports = {
  normalizeSmsForCommands,
  extractProjectScopeHint,
  dateKeyUtc,
  DAILY_REPORT_TIME_ZONE,
  dateKeyEastern,
  startOfEasternDay,
  startOfEasternDayForDateKey,
  addCalendarDaysToDateKey,
  fmtMonDayEasternDateKey,
  weekdayShortEasternDateKey,
  formatDailySiteLogTitleEastern,
  formatConcreteSummaryLabelEastern,
  formatWallDateTimeEt,
  parseStructuredLog,
  extractExplicitReportDate,
  parseDeficiencyDetails,
  parseDeficiencyIntakeRequest,
  parseDayRollupRequest,
  parseDailyReportRequest: parseDailyReportRequestV2,
  isMetaInbound,
  isDailyReportPdfRequest: isDailyReportPdfRequestParsed,
  isDailyLogViewRequest,
  isSummaryStyleRequest,
  isAnyDayRollupRequest,
};
