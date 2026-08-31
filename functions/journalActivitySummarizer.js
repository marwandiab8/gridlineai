const { dateKeyEastern } = require("./logClassifier");

const LOCATION_PAIRS = [
  ["arrive_work", "leave_work", "work"],
  ["arrive_home", "leave_home", "home"],
  ["arrive_gym", "leave_gym", "gym"],
];

function isTrackingEntry(entry) {
  return Boolean(
    entry &&
      (String(entry.source || "").trim() === "ios_shortcuts" ||
        String(entry.shortcutEventId || "").trim() ||
        String(entry.shortcutEventType || "").trim())
  );
}

function eventType(entry) {
  return String(entry && entry.shortcutEventType || "").trim().toLowerCase();
}

function eventTimeMs(entry) {
  const explicit = [
    entry && entry.shortcutEventAtIso,
    entry && entry.eventAtIso,
    entry && entry.shortcutEventAtMs,
    entry && entry.eventAtMs,
  ];
  for (const value of explicit) {
    const ms = typeof value === "number" ? value : value ? new Date(value).getTime() : 0;
    if (Number.isFinite(ms) && ms > 0) return ms;
  }
  try {
    const createdAt = entry && entry.createdAt;
    const ms = createdAt && createdAt.toDate
      ? createdAt.toDate().getTime()
      : createdAt && createdAt.seconds
        ? Number(createdAt.seconds) * 1000
        : new Date(createdAt).getTime();
    return Number.isFinite(ms) && ms > 0 ? ms : 0;
  } catch (_) {
    return 0;
  }
}

function displayName(entry, authorLabelsByIdentity) {
  const value = String(
    (entry && (entry.authorLabel || entry.authorName || entry.authorEmail)) || ""
  ).trim();
  if (!value || /^\+?[\d\s().-]+$/.test(value)) return "They";
  if (authorLabelsByIdentity && entry) {
    const email = String(entry.authorEmail || "").trim().toLowerCase();
    if (email && authorLabelsByIdentity.get(`email:${email}`)) {
      return authorLabelsByIdentity.get(`email:${email}`);
    }
  }
  return value;
}

function timeLabel(ms, timeZone) {
  try {
    return new Intl.DateTimeFormat("en-US", {
      timeZone: timeZone || "America/Toronto",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }).format(new Date(ms));
  } catch (_) {
    return "";
  }
}

function approximateTime(ms, timeZone) {
  const label = timeLabel(ms, timeZone);
  return label ? `approximately ${label.toLowerCase()}` : "approximately that time";
}

function durationLabel(startMs, endMs) {
  const minutes = Math.round((endMs - startMs) / 60000);
  if (!Number.isFinite(minutes) || minutes <= 0) return "";
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  if (hours === 1 && !remainder) return "1 hour";
  if (hours === 1) return `1 hour and ${remainder} minutes`;
  if (hours && remainder) return `${hours} hours and ${remainder} minutes`;
  if (hours) return `${hours} hours`;
  return `${minutes} minutes`;
}

function locationLabel(entry) {
  return String(
    entry && (entry.shortcutLocationLabel || entry.locationLabel || entry.location || "")
  ).trim();
}

function makeSummary(first, text, sourceEntries) {
  return {
    id: `journal-activity-${String(first.id || sourceEntries.length)}`,
    source: "ios_shortcuts_summary",
    category: "journal",
    rawText: text,
    normalizedText: text,
    summaryText: text,
    includeInDailySummary: true,
    dailySummarySections: ["journal", "dayLog"],
    reportDateKey: first.reportDateKey || first.dateKey || dateKeyEastern(new Date(eventTimeMs(first))),
    dateKey: first.dateKey || first.reportDateKey || null,
    shortcutEventAtIso: first.shortcutEventAtIso || first.eventAtIso || null,
    shortcutEventAtMs: eventTimeMs(first) || null,
    authorLabel: first.authorLabel || first.authorName || null,
    authorEmail: first.authorEmail || null,
    authorPhone: first.authorPhone || first.senderPhone || null,
    sourceEntryIds: sourceEntries.map((entry) => String(entry.id || "")).filter(Boolean),
    _journalActivitySummary: true,
  };
}

function summarizeJournalTrackingEntries(entries, options = {}) {
  const input = Array.isArray(entries) ? entries : [];
  const tracked = input
    .filter(isTrackingEntry)
    .map((entry) => ({ entry, type: eventType(entry), ms: eventTimeMs(entry) }))
    .filter((row) => row.type && row.ms > 0)
    .sort((a, b) => a.ms - b.ms || String(a.entry.id || "").localeCompare(String(b.entry.id || "")));
  const ordinary = input.filter((entry) => !isTrackingEntry(entry));
  const consumed = new Set();
  const summaries = [];
  const byType = new Map();
  for (const row of tracked) {
    const key = `${row.type}|${Math.round(row.ms / 120000)}`;
    if (byType.has(key)) {
      consumed.add(row.entry.id);
      continue;
    }
    byType.set(key, row);
  }
  const cleanTracked = tracked.filter((row) => !consumed.has(row.entry.id));

  const findPair = (startIndex, startType, endType, maxGapMs = 24 * 60 * 60 * 1000) => {
    const start = cleanTracked[startIndex];
    if (!start || start.type !== startType || consumed.has(start.entry.id)) return null;
    for (let i = startIndex + 1; i < cleanTracked.length; i += 1) {
      const candidate = cleanTracked[i];
      if (candidate.ms - start.ms > maxGapMs) break;
      if (consumed.has(candidate.entry.id)) continue;
      if (candidate.type === startType) return null;
      if (candidate.type === endType && candidate.ms > start.ms) return candidate;
    }
    return null;
  };

  const authorLabels = options.authorLabelsByIdentity || null;
  const summariesFor = (first, text, sourceRows) => {
    for (const row of sourceRows) consumed.add(row.entry.id);
    summaries.push(makeSummary(first.entry, text, sourceRows.map((row) => row.entry)));
  };

  for (let i = 0; i < cleanTracked.length; i += 1) {
    const row = cleanTracked[i];
    if (consumed.has(row.entry.id)) continue;
    const person = displayName(row.entry, authorLabels);
    const tz = row.entry.shortcutTimezone || row.entry.timezone || options.timeZone;
    const pairConfig = LOCATION_PAIRS.find(([start]) => start === row.type);
    if (pairConfig) {
      const end = findPair(i, pairConfig[0], pairConfig[1]);
      if (end) {
        const place = pairConfig[2];
        const duration = durationLabel(row.ms, end.ms);
        const durationText = duration ? ` (approximately ${duration})` : "";
        const workoutStart = place === "gym"
          ? cleanTracked.find((candidate) => candidate.type === "start_workout" && candidate.ms > row.ms && candidate.ms < end.ms && !consumed.has(candidate.entry.id))
          : null;
        const workoutFinish = workoutStart ? cleanTracked.find((candidate) => candidate.type === "finish_workout" && candidate.ms > workoutStart.ms && candidate.ms < end.ms && !consumed.has(candidate.entry.id)) : null;
        if (place === "gym" && workoutStart && workoutFinish) {
          const workoutDuration = durationLabel(workoutStart.ms, workoutFinish.ms);
          const workoutText = workoutDuration ? ` lasting approximately ${workoutDuration}` : "";
          summariesFor(
            row,
            `${person} went to the gym at ${timeLabel(row.ms, tz)}, completed a workout${workoutText}, and left at ${timeLabel(end.ms, tz)}${durationText}.`,
            [row, workoutStart, workoutFinish, end]
          );
          continue;
        }
        const text = place === "work"
          ? `${person} arrived at work at ${timeLabel(row.ms, tz)} and finished the workday at ${timeLabel(end.ms, tz)}${durationText}.`
          : place === "home"
            ? `${person} was at home from ${timeLabel(row.ms, tz)} to ${timeLabel(end.ms, tz)}${durationText}.`
            : `${person} went to the gym at ${timeLabel(row.ms, tz)} and left at ${timeLabel(end.ms, tz)}${durationText}.`;
        summariesFor(row, text, [row, end]);
        continue;
      }
    }
    const workoutEnd = row.type === "start_workout" ? findPair(i, "start_workout", "finish_workout", 12 * 60 * 60 * 1000) : null;
    if (workoutEnd) {
      const duration = durationLabel(row.ms, workoutEnd.ms);
      summariesFor(
        row,
        `${person} completed a workout${duration ? ` lasting approximately ${duration}` : ""}.`,
        [row, workoutEnd]
      );
      continue;
    }
    if (["start_spotify", "stop_spotify", "carplay_connected", "carplay_disconnected", "tracking_started", "tracking_stopped", "shortcut_triggered", "location_detected"].includes(row.type)) {
      consumed.add(row.entry.id);
      continue;
    }
    const place = row.type === "arrive_work" ? "work" : row.type === "leave_work" ? "work"
      : row.type === "arrive_home" ? "home" : row.type === "leave_home" ? "home"
        : row.type === "arrive_gym" ? "the gym" : row.type === "leave_gym" ? "the gym" : locationLabel(row.entry);
    if (row.type.startsWith("arrive_") && place) {
      summariesFor(row, `${person} arrived at ${place} at ${approximateTime(row.ms, tz)}.`, [row]);
    } else if (row.type.startsWith("leave_") && place) {
      summariesFor(row, `${person} left ${place} at ${approximateTime(row.ms, tz)}.`, [row]);
    } else if (row.type === "start_workout") {
      summariesFor(row, `${person} started a workout at ${approximateTime(row.ms, tz)}.`, [row]);
    } else if (row.type === "finish_workout") {
      summariesFor(row, `${person} completed a workout at ${approximateTime(row.ms, tz)}.`, [row]);
    } else {
      consumed.add(row.entry.id);
    }
  }

  const result = [...ordinary, ...summaries].sort((a, b) => {
    const at = eventTimeMs(a) || (a.createdAt && a.createdAt.toDate ? a.createdAt.toDate().getTime() : 0);
    const bt = eventTimeMs(b) || (b.createdAt && b.createdAt.toDate ? b.createdAt.toDate().getTime() : 0);
    return at - bt || String(a.id || "").localeCompare(String(b.id || ""));
  });
  return result;
}

module.exports = {
  isTrackingEntry,
  summarizeJournalTrackingEntries,
  durationLabel,
};
