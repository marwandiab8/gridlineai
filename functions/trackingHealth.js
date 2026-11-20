// Tracking health: twice a day, is the phone still reporting, and did what it reported reach Time Left?
// Quiet while all is well. When something needs a hand - OwnTracks silent for a day and a half, a drive
// stuck open, an arrival at work or the gym with no departure, events that failed to reach Time Left -
// the member gets one text, repeated at most once a day while the same problem lasts. The last
// assessment is kept in `trackingHealth/{memberEmail}`. See docs/tracking-reliability.md.
const COL_SHORTCUT_EVENTS = "iosShortcutEvents";
const COL_DRIVE_DETECTION = "driveDetection";
const COL_APP_MEMBERS = "appMembers";
const COL_HEALTH = "trackingHealth";
const COL_HEARTBEATS = "heartbeats";
const DEFAULT_TIMEZONE = "America/Toronto";

// A quiet day at home sends nothing in "significant changes" mode, so silence only counts after this.
const SILENT_AFTER_MS = 36 * 3600 * 1000;
const OPEN_BOUNDARY_AFTER_MS = 18 * 3600 * 1000;
const STUCK_DRIVE_AFTER_MS = 3 * 3600 * 1000;
const DELIVERY_LOOKBACK_MS = 24 * 3600 * 1000;
const DELIVERY_GRACE_MS = 30 * 60 * 1000;
const BOUNDARY_LOOKBACK_MS = 7 * 24 * 3600 * 1000;
const REPEAT_ALERT_AFTER_MS = 24 * 3600 * 1000;
// With the heartbeat Shortcut (docs/heartbeat-shortcut.md) the phone reports three times a day, so its silence
// means something within hours, and OwnTracks silence is fine while the heartbeats show the phone has not moved.
const HEARTBEAT_SILENT_MS = 14 * 3600 * 1000;
const STAYED_PUT_METERS = 300;
const MOVED_METERS = 500;
const OWNTRACKS_QUIET_MS = 3 * 3600 * 1000;
// Health Auto Export posts a few times a day with gaps of up to a night; its silence counts after a day.
const HEALTH_EXPORT_SILENT_MS = 24 * 3600 * 1000;
const STEPS_MOVED = 3000;
const STEPS_STAYED_PUT = 1500;
const HEALTH_OWNTRACKS_QUIET_MS = 6 * 3600 * 1000;
const SETTLED_DELIVERY = new Set(["delivered", "duplicate", "off", "unsupported_event_type"]);
const OPEN_BOUNDARIES = [
  { arrive: "arrive_work", leave: "leave_work", label: "work" },
  { arrive: "arrive_gym", leave: "leave_gym", label: "the gym" },
];
const HOME_BOUNDARIES = ["arrive_home", "leave_home"];

function haversineMeters(lat1, lon1, lat2, lon2) {
  const r = (d) => (d * Math.PI) / 180;
  const a = Math.sin(r(lat2 - lat1) / 2) ** 2 + Math.cos(r(lat1)) * Math.cos(r(lat2)) * Math.sin(r(lon2 - lon1) / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function formatWhen(ms, timezone) {
  try {
    return new Intl.DateTimeFormat("en-CA", { weekday: "short", hour: "numeric", minute: "2-digit", hour12: true, timeZone: timezone || DEFAULT_TIMEZONE })
      .format(new Date(ms))
      .replace(/\./g, "")
      .replace(/,/g, "")
      .toLowerCase()
      .replace(/^./, (c) => c.toUpperCase());
  } catch {
    return new Date(ms).toISOString().slice(0, 16);
  }
}

function formatSpan(ms) {
  const hours = Math.round(ms / 3600000);
  if (hours < 48) return `${hours} h`;
  return `${Math.round(hours / 24)} days`;
}

/** The member's latest event of one type since `sinceMs` (indexed: memberEmail + eventType + eventAtMs). */
async function latestEvent(db, memberEmail, eventType, sinceMs) {
  const snap = await db
    .collection(COL_SHORTCUT_EVENTS)
    .where("memberEmail", "==", memberEmail)
    .where("eventType", "==", eventType)
    .where("eventAtMs", ">=", sinceMs)
    .get()
    .catch(() => null);
  let best = null;
  if (snap) {
    snap.forEach((doc) => {
      const data = doc.data() || {};
      if (Number.isFinite(data.eventAtMs) && (!best || data.eventAtMs > best.eventAtMs)) best = { id: doc.id, ...data };
    });
  }
  return best;
}

/**
 * Looks at one member's tracking as of `nowMs`. Returns { ok, issues: [{ code, text }], lastLocationAtMs,
 * checkedAtMs }. Reads only; never throws.
 */
async function assessTrackingHealth({ db, memberEmail, nowMs = Date.now(), timezone = DEFAULT_TIMEZONE }) {
  const issues = [];
  const driveSnap = await db.collection(COL_DRIVE_DETECTION).doc(memberEmail).get().catch(() => null);
  const drive = driveSnap && driveSnap.exists ? driveSnap.data() || {} : {};

  let lastLocationAtMs = Number.isFinite(drive.lastPointAtMs) ? drive.lastPointAtMs : null;
  for (const eventType of HOME_BOUNDARIES) {
    const event = await latestEvent(db, memberEmail, eventType, nowMs - BOUNDARY_LOOKBACK_MS);
    if (event && (lastLocationAtMs == null || event.eventAtMs > lastLocationAtMs)) lastLocationAtMs = event.eventAtMs;
  }
  // The heartbeat, when the Shortcut is set up: is the phone alive, and has it moved since OwnTracks last reported?
  const hbSnap = await db.collection(COL_HEARTBEATS).doc(memberEmail).get().catch(() => null);
  const heartbeat = hbSnap && hbSnap.exists ? hbSnap.data() || {} : null;
  let stayedPut = false;
  if (heartbeat && Number.isFinite(heartbeat.lastAtMs)) {
    if (nowMs - heartbeat.lastAtMs > HEARTBEAT_SILENT_MS) {
      issues.push({ code: "heartbeat_silent", text: `No heartbeat from the phone since ${formatWhen(heartbeat.lastAtMs, timezone)}: the Shortcuts automation may be off, or the phone.` });
    } else if (Number.isFinite(heartbeat.latitude) && drive.state && drive.state.last && Number.isFinite(drive.state.last.latitude)) {
      const moved = haversineMeters(heartbeat.latitude, heartbeat.longitude, drive.state.last.latitude, drive.state.last.longitude);
      if (moved <= STAYED_PUT_METERS) stayedPut = true;
      else if (lastLocationAtMs != null && heartbeat.lastAtMs > lastLocationAtMs && nowMs - lastLocationAtMs > OWNTRACKS_QUIET_MS && moved > MOVED_METERS) {
        issues.push({ code: "owntracks_dead", text: `The phone has moved ${Math.round(moved / 100) / 10} km since OwnTracks last reported (${formatWhen(lastLocationAtMs, timezone)}), so OwnTracks seems to have stopped. Open it once; location access should be Always.` });
      }
    }
  }

  // Health Auto Export as the heartbeat: the phone is alive if it exports, and its steps say whether it moved.
  // The GPS heartbeat, when it is set up and fresh, has already answered.
  if (heartbeat && Number.isFinite(heartbeat.healthLastAtMs)) {
    const gpsFresh = Number.isFinite(heartbeat.lastAtMs) && nowMs - heartbeat.lastAtMs <= HEARTBEAT_SILENT_MS;
    if (nowMs - heartbeat.healthLastAtMs > HEALTH_EXPORT_SILENT_MS) {
      issues.push({ code: "health_export_silent", text: `No Health Auto Export since ${formatWhen(heartbeat.healthLastAtMs, timezone)}: its automation may be off, or the phone.` });
    } else if (!gpsFresh) {
      const since = Number(heartbeat.healthStepsSinceOwnTracks) || 0;
      if (since < STEPS_STAYED_PUT) stayedPut = true;
      else if (lastLocationAtMs != null && nowMs - lastLocationAtMs > HEALTH_OWNTRACKS_QUIET_MS && since >= STEPS_MOVED && !issues.some((issue) => issue.code === "owntracks_dead")) {
        issues.push({ code: "owntracks_dead", text: `You have taken ${since.toLocaleString("en-US")} steps since OwnTracks last reported (${formatWhen(lastLocationAtMs, timezone)}), so OwnTracks seems to have stopped. Open it once; location access should be Always.` });
      }
    }
  }

  if (lastLocationAtMs == null) {
    issues.push({ code: "never", text: "No location data has arrived from OwnTracks in the last week." });
  } else if (!stayedPut && nowMs - lastLocationAtMs > SILENT_AFTER_MS) {
    issues.push({
      code: "silent",
      text: `No location data since ${formatWhen(lastLocationAtMs, timezone)} (${formatSpan(nowMs - lastLocationAtMs)}). Open OwnTracks once: it should show your position, with location access set to Always.`,
    });
  }

  if (drive.status === "driving" && Number.isFinite(drive.lastPointAtMs) && nowMs - drive.lastPointAtMs > STUCK_DRIVE_AFTER_MS) {
    issues.push({ code: "stuck_drive", text: `A drive has been open since ${formatWhen(drive.lastPointAtMs, timezone)} without pings; the sweep that closes drives may be failing.` });
  }

  for (const pair of OPEN_BOUNDARIES) {
    const arrive = await latestEvent(db, memberEmail, pair.arrive, nowMs - BOUNDARY_LOOKBACK_MS);
    if (!arrive || nowMs - arrive.eventAtMs <= OPEN_BOUNDARY_AFTER_MS) continue;
    const leave = await latestEvent(db, memberEmail, pair.leave, arrive.eventAtMs);
    if (!leave) issues.push({ code: `open_${pair.arrive}`, text: `Arrived at ${pair.label} ${formatWhen(arrive.eventAtMs, timezone)} and no departure has been recorded since.` });
  }

  const recent = await db.collection(COL_SHORTCUT_EVENTS).where("eventAtMs", ">=", nowMs - DELIVERY_LOOKBACK_MS).get().catch(() => null);
  const failed = {};
  if (recent) {
    recent.forEach((doc) => {
      const data = doc.data() || {};
      if (data.memberEmail !== memberEmail || data.status === "voided") return;
      const delivery = data.timeLeftDelivery;
      if (!delivery) {
        if (Number.isFinite(data.eventAtMs) && nowMs - data.eventAtMs > DELIVERY_GRACE_MS) failed["not sent"] = (failed["not sent"] || 0) + 1;
        return;
      }
      const status = String(delivery.status || "").toLowerCase();
      if (SETTLED_DELIVERY.has(status)) return;
      failed[status || "unknown"] = (failed[status || "unknown"] || 0) + 1;
    });
  }
  const failedTotal = Object.values(failed).reduce((sum, n) => sum + n, 0);
  if (failedTotal) {
    const detail = Object.entries(failed).map(([status, n]) => `${status} ${n}`).join(", ");
    issues.push({ code: "delivery", text: `${failedTotal} event${failedTotal === 1 ? "" : "s"} from the last day did not reach Time Left (${detail}).` });
  }

  return { ok: !issues.length, issues, lastLocationAtMs, checkedAtMs: nowMs };
}

/**
 * Texts the member about one issue now, unless the same issue was texted in the last day, and records it in
 * trackingHealth/{member} so the scheduled check does not repeat it. Returns true when a text went out.
 */
async function alertIssueOnce({ db, FieldValue, member, issue, sendSms, nowMs = Date.now(), logger = null }) {
  const phone = member && member.data && member.data.approvedPhoneE164;
  if (!phone || typeof sendSms !== "function" || !issue) return false;
  const ref = db.collection(COL_HEALTH).doc(member.email);
  const snap = await ref.get().catch(() => null);
  const previous = snap && snap.exists ? snap.data() || {} : {};
  const codes = String(previous.lastAlertCodes || "").split(",").filter(Boolean);
  if (codes.includes(issue.code) && Number.isFinite(previous.lastAlertAtMs) && nowMs - previous.lastAlertAtMs < REPEAT_ALERT_AFTER_MS) return false;
  try {
    await sendSms(phone, `Tracking check: ${issue.text}`.slice(0, 600));
  } catch (err) {
    if (logger && typeof logger.warn === "function") logger.warn("trackingHealth: alert text failed", { memberEmail: member.email, message: err && err.message });
    return false;
  }
  await ref.set(
    {
      memberEmail: member.email,
      lastAlertAtMs: nowMs,
      lastAlertCodes: [...new Set([...codes, issue.code])].sort().join(","),
      updatedAt: FieldValue && typeof FieldValue.serverTimestamp === "function" ? FieldValue.serverTimestamp() : new Date(nowMs),
    },
    { merge: true }
  );
  return true;
}

/** The text message for an assessment with issues. */
function formatHealthAlert(assessment) {
  const lines = assessment.issues.map((issue) => issue.text);
  return `Tracking check: ${lines.join(" ")}`.slice(0, 600);
}

/**
 * Assesses every member with the Shortcuts/OwnTracks integration, keeps the result in trackingHealth/{email},
 * and texts the member's approved phone when there are issues (through `sendSms(to, body)`), at most once a
 * day for the same set of issues. Returns a summary.
 */
async function runTrackingHealthCheck({ db, FieldValue, nowMs = Date.now(), sendSms = null, logger = null }) {
  const members = await db.collection(COL_APP_MEMBERS).where("shortcutIntegration.enabled", "==", true).get();
  const summary = { checked: 0, withIssues: 0, alerted: 0 };
  for (const doc of members.docs) {
    const data = doc.data() || {};
    if (data.active === false) continue;
    const memberEmail = doc.id;
    const timezone = typeof data.timezone === "string" && data.timezone.trim() ? data.timezone.trim() : DEFAULT_TIMEZONE;
    let assessment;
    try {
      assessment = await assessTrackingHealth({ db, memberEmail, nowMs, timezone });
    } catch (err) {
      if (logger && typeof logger.warn === "function") logger.warn("trackingHealth: assessment failed", { memberEmail, message: err && err.message });
      continue;
    }
    summary.checked += 1;
    if (!assessment.ok) summary.withIssues += 1;

    const ref = db.collection(COL_HEALTH).doc(memberEmail);
    const previousSnap = await ref.get().catch(() => null);
    const previous = previousSnap && previousSnap.exists ? previousSnap.data() || {} : {};
    const codes = assessment.issues.map((issue) => issue.code).sort().join(",");
    const repeat = previous.lastAlertCodes === codes && Number.isFinite(previous.lastAlertAtMs) && nowMs - previous.lastAlertAtMs < REPEAT_ALERT_AFTER_MS;
    let alerted = false;
    if (!assessment.ok && typeof sendSms === "function" && data.approvedPhoneE164 && !repeat) {
      try {
        await sendSms(data.approvedPhoneE164, formatHealthAlert(assessment));
        alerted = true;
        summary.alerted += 1;
      } catch (err) {
        if (logger && typeof logger.warn === "function") logger.warn("trackingHealth: alert text failed", { memberEmail, message: err && err.message });
      }
    }
    await ref.set(
      {
        memberEmail,
        checkedAtMs: nowMs,
        checkedAt: new Date(nowMs),
        ok: assessment.ok,
        issues: assessment.issues,
        lastLocationAtMs: assessment.lastLocationAtMs,
        ...(alerted ? { lastAlertAtMs: nowMs, lastAlertCodes: codes } : {}),
        ...(assessment.ok ? { lastAlertCodes: "" } : {}),
        updatedAt: FieldValue && typeof FieldValue.serverTimestamp === "function" ? FieldValue.serverTimestamp() : new Date(nowMs),
      },
      { merge: true }
    );
  }
  return summary;
}

module.exports = {
  COL_HEALTH,
  HEARTBEAT_SILENT_MS,
  HEALTH_EXPORT_SILENT_MS,
  alertIssueOnce,
  SILENT_AFTER_MS,
  OPEN_BOUNDARY_AFTER_MS,
  STUCK_DRIVE_AFTER_MS,
  REPEAT_ALERT_AFTER_MS,
  assessTrackingHealth,
  formatHealthAlert,
  runTrackingHealthCheck,
};
