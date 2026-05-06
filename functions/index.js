const { onRequest, onCall, HttpsError } = require("firebase-functions/v2/https");
const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const { defineSecret, defineString } = require("firebase-functions/params");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const { FieldValue } = require("firebase-admin/firestore");
const twilio = require("twilio");
const OpenAI = require("openai");
const { buildReply, checkRateLimit, resolveVoiceMediaProjectSlug, COL_USERS } = require("./assistant");
const {
  addDashboardIssueNote,
  attachDashboardIssuePhoto,
  createDashboardIssue,
  createMmsPlaceholderIssue,
  deleteDashboardIssue,
  updateDashboardIssue,
  getIssueSnapshot,
} = require("./issueRepository");
const { COLLECTION_BY_TYPE } = require("./issueConstants");
const { attachTwilioMediaToIssue } = require("./mmsMedia");
const { countTwilioMediaParams, fetchTwilioMediaBuffer, guessExtension } = require("./twilioMediaFetch");
const { runIssueExport } = require("./issueExportHandler");
const { generateDailyReportPdf } = require("./dailyReportPdf");
const {
  COL_LABOURERS,
  COL_LABOUR_ENTRIES,
  normalizeLabourerName,
  normalizeLabourerPhone,
  parseLabourHoursCommand,
  writeLabourEntry,
  loadLabourEntries,
  buildLabourRollup,
  normalizeLabourRangeKeys,
} = require("./labourRepository");
const { generateLabourReportPdf } = require("./labourReportPdf");
const { maybeCaptionFirstMmsPhoto } = require("./mmsVisionCaption");
const { registerUploadedMedia, saveOneInboundMedia } = require("./mediaRepository");
const {
  maybeEnhanceLogEntry,
  appendLinkedMediaIds,
} = require("./logEntryRepository");
const {
  normalizeTwilioSecret,
  normalizeAccountSid,
  normalizeAuthToken,
  normalizePhoneE164,
  maskSidParts,
  logInboundTwilioSecrets,
} = require("./twilioSecrets");
const { isDailyReportPdfRequest } = require("./logClassifier");
const {
  buildUserProjectPatch,
  getAccessibleProjectForUser,
  getUserProjectAccess,
  getProjectRecord,
  normalizeProjectLocation,
  normalizeProjectName,
  normalizeProjectSlug,
} = require("./projectAccess");
const {
  canAccessProject,
  canApproveProjectNoteRequests,
  findActiveAppMemberByApprovedPhone,
  findActiveLabourerByPhone,
  getAppAccess,
  getOperatorAccess,
  normalizeEmail,
  normalizeRole,
  roleAtLeast,
} = require("./authz");
const {
  parseLookaheadWorkbookBuffer,
  formatCrewscopeStyleSummary,
} = require("./lookaheadSchedule");
const { createLookaheadActivitiesReportPdf } = require("./lookaheadActivitiesPdf");
const { createLookaheadCloseoutReportPdf } = require("./lookaheadCloseoutReport");
const {
  saveLookaheadSnapshot,
  loadPreviousLookaheadSnapshot,
} = require("./lookaheadScheduleRepository");

const COL_APP_MEMBERS = "appMembers";
const COL_PROJECT_NOTE_EDIT_REQUESTS = "projectNoteEditRequests";
const COL_PROJECT_TODOS = "projectTodos";
const TODO_STATUSES = new Set(["open", "inprogress", "completed"]);
const TODO_PRIORITIES = new Set(["p1", "p2", "p3", "p4"]);
const TODO_RECURRENCE_MODES = new Set([
  "none",
  "every_day",
  "every_week",
  "every_month",
  "every_year",
  "weekdays",
  "custom",
]);

const OPENAI_MODEL_PRIMARY = defineString("OPENAI_MODEL_PRIMARY", {
  default: "gpt-5.2-chat-latest",
});

/** Optional. If set, dashboard PDF button must send the same `token` (see public app). */
const DAILY_PDF_DASHBOARD_TOKEN = defineString("DAILY_PDF_DASHBOARD_TOKEN", {
  default: "",
});

/** Every message doc from this function includes this — if missing in Firestore, Twilio is not hitting this deployment. */
const MESSAGE_SCHEMA_VERSION = 3;
const AUDIO_REVIEW_CONFIRM_RE = /^(?:send|use(?:\s+it)?|confirm|yes|y|ok|okay|submit|save)$/i;
const AUDIO_REVIEW_CANCEL_RE = /^(?:cancel|discard|ignore|stop)$/i;

function normalizeTodoStatus(value) {
  const raw = String(value || "").trim().toLowerCase();
  return TODO_STATUSES.has(raw) ? raw : "";
}

function normalizeTodoText(value, maxLength = 500) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function normalizeTodoDateTime(value) {
  if (value == null || value === "") return null;
  const raw = String(value || "").trim();
  if (!raw) return null;
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toISOString() : "";
}

function normalizeTodoPriority(value) {
  if (value == null || value === "") return null;
  const raw = String(value || "").trim().toLowerCase();
  return TODO_PRIORITIES.has(raw) ? raw : "";
}

function normalizeTodoLabels(value) {
  const source = Array.isArray(value)
    ? value
    : String(value || "")
        .split(",");
  const out = [];
  for (const item of source) {
    const clean = String(item || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 40);
    if (clean && !out.includes(clean)) out.push(clean);
    if (out.length >= 20) break;
  }
  return out;
}

function normalizeTodoTags(value) {
  const source = Array.isArray(value)
    ? value
    : String(value || "")
        .split(",");
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

function normalizeTodoDependencies(value) {
  const source = Array.isArray(value)
    ? value
    : String(value || "")
        .split(/[,\n]/);
  const out = [];
  for (const item of source) {
    const clean = String(item || "").trim().slice(0, 120);
    if (clean && !out.includes(clean)) out.push(clean);
    if (out.length >= 30) break;
  }
  return out;
}

function normalizeTodoReminders(value) {
  const source = Array.isArray(value)
    ? value
    : String(value || "")
        .split(/[,\n]/);
  const out = [];
  for (const item of source) {
    const normalized = normalizeTodoDateTime(item);
    if (normalized === "") return "";
    if (normalized && !out.includes(normalized)) out.push(normalized);
    if (out.length >= 20) break;
  }
  return out;
}

function normalizeTodoRecurrence(value) {
  if (value == null || value === "") return { mode: "none", customText: "" };
  const raw =
    typeof value === "object" && !Array.isArray(value)
      ? value
      : { mode: value };
  const mode = String(raw.mode || "none").trim().toLowerCase();
  if (!TODO_RECURRENCE_MODES.has(mode)) return "";
  const customText = normalizeTodoText(raw.customText || "", 200);
  return {
    mode,
    customText: mode === "custom" ? customText : "",
  };
}

function normalizeTodoCommentText(value) {
  return normalizeTodoText(value || "", 1000);
}

/** Prefer runtime FIREBASE_CONFIG; fallback matches `public/app.js` storageBucket. */
let storageBucket = "gridlineai.firebasestorage.app";
try {
  if (process.env.FIREBASE_CONFIG) {
    const j = JSON.parse(process.env.FIREBASE_CONFIG);
    if (j && j.storageBucket) storageBucket = j.storageBucket;
  }
} catch (_) {
  /* keep fallback */
}
admin.initializeApp({ storageBucket });
const db = admin.firestore();

const TWILIO_ACCOUNT_SID = defineSecret("TWILIO_ACCOUNT_SID");
const TWILIO_AUTH_TOKEN = defineSecret("TWILIO_AUTH_TOKEN");
const TWILIO_PHONE_NUMBER = defineSecret("TWILIO_PHONE_NUMBER");
const OPENAI_API_KEY = defineSecret("OPENAI_API_KEY");
const COL_VOICE_MESSAGE_QUEUE = "voiceMessageProcessingQueue";
const COL_AUDIO_MESSAGE_QUEUE = "audioMessageProcessingQueue";

/** "auto" enforces signature validation outside emulator/dev. */
const ENFORCE_TWILIO_VALIDATE = defineString("ENFORCE_TWILIO_VALIDATE", {
  default: "auto",
});

function assertDashboardToken(request) {
  const configured = DAILY_PDF_DASHBOARD_TOKEN.value();
  if (configured && String(configured).trim() !== "") {
    if (request.data?.token !== configured) {
      throw new HttpsError(
        "permission-denied",
        "Invalid or missing dashboard token. Set DAILY_PDF_DASHBOARD_TOKEN or send the matching token."
      );
    }
  }
}

function assertDashboardTokenValue(token) {
  const configured = DAILY_PDF_DASHBOARD_TOKEN.value();
  if (configured && String(configured).trim() !== "") {
    if (String(token || "").trim() !== configured) {
      throw new HttpsError(
        "permission-denied",
        "Invalid or missing dashboard token. Set DAILY_PDF_DASHBOARD_TOKEN or send the matching token."
      );
    }
  }
}

async function assertIssueWriteProjectAccess(operator, projectSlugRaw) {
  if (roleAtLeast(operator.role, "management") || operator.allProjects === true) {
    return;
  }
  const slug = normalizeProjectSlug(String(projectSlugRaw || "").trim());
  if (!slug) {
    throw new HttpsError(
      "invalid-argument",
      "projectId is required when creating or editing issues for your account."
    );
  }
  if (!canAccessProject(operator, slug)) {
    throw new HttpsError(
      "permission-denied",
      `You are not assigned to project "${slug}".`
    );
  }
}

async function assertIssueWriteRecordAccess(operator, issueCollection, issueId) {
  if (roleAtLeast(operator.role, "management") || operator.allProjects === true) {
    return;
  }
  let rec;
  try {
    rec = await getIssueSnapshot(db, issueCollection, issueId);
  } catch (e) {
    throw new HttpsError("not-found", String(e.message || "Issue not found."));
  }
  const slug = normalizeProjectSlug(String(rec.issueData.projectId || "").trim());
  if (!slug || !canAccessProject(operator, slug)) {
    throw new HttpsError("permission-denied", "You cannot update this issue.");
  }
}

function setJsonCors(res) {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, X-Dashboard-Token");
}

function normalizeReportTypeInput(value) {
  return String(value || "").trim() === "journal" ? "journal" : "dailySiteLog";
}

function normalizeProjectSlugList(values) {
  const out = [];
  for (const value of Array.isArray(values) ? values : []) {
    const slug = normalizeProjectSlug(value);
    if (slug && !out.includes(slug)) out.push(slug);
  }
  return out;
}

function chunkArray(values, size) {
  const out = [];
  for (let i = 0; i < values.length; i += size) {
    out.push(values.slice(i, i + size));
  }
  return out;
}

const MAX_NOTIFY_RECIPIENTS = 80;

async function resolveNotificationRecipients(db, request = {}) {
  const audience = String(request.audience || "").trim();
  if (audience === "management") {
    const snap = await db
      .collection(COL_APP_MEMBERS)
      .where("active", "==", true)
      .limit(500)
      .get();
    const recipients = [];
    for (const docSnap of snap.docs) {
      const row = docSnap.data() || {};
      if (!roleAtLeast(row.role, "management")) continue;
      const phone = normalizePhoneE164(String(row.approvedPhoneE164 || "").trim());
      if (!phone) continue;
      recipients.push({
        phoneE164: phone,
        label: String(row.displayName || docSnap.id || phone).trim() || phone,
        recipientType: "management",
      });
    }
    return recipients;
  }

  if (audience === "project_users") {
    const projectSlug = normalizeProjectSlug(String(request.projectSlug || "").trim());
    if (!projectSlug) return [];
    const byPhone = new Map();
    const [activeSnap, listedSnap] = await Promise.all([
      db
        .collection("smsUsers")
        .where("activeProjectSlug", "==", projectSlug)
        .limit(500)
        .get(),
      db
        .collection("smsUsers")
        .where("projectSlugs", "array-contains", projectSlug)
        .limit(500)
        .get(),
    ]);
    for (const snap of [activeSnap, listedSnap]) {
      for (const docSnap of snap.docs) {
        const row = docSnap.data() || {};
        const phone = normalizePhoneE164(String(row.phoneE164 || docSnap.id || "").trim());
        if (!phone) continue;
        byPhone.set(phone, {
          phoneE164: phone,
          label: String(row.displayName || row.name || phone).trim() || phone,
          recipientType: "project_user",
        });
      }
    }
    return [...byPhone.values()];
  }

  return [];
}

async function sendSmsNotificationFanout({
  db,
  smsClient,
  accountSid,
  fromPhone,
  messagingServiceSid,
  requestedByPhone,
  requestedByName,
  requestedByEmail,
  projectSlug,
  messageBody,
  recipients,
  runId,
}) {
  const sent = [];
  const failed = [];
  const deduped = [];
  const seen = new Set();
  for (const recipient of recipients || []) {
    const phone = normalizePhoneE164(String(recipient && recipient.phoneE164 || "").trim());
    if (!phone) continue;
    if (seen.has(phone)) continue;
    seen.add(phone);
    deduped.push({ ...recipient, phoneE164: phone });
    if (deduped.length >= MAX_NOTIFY_RECIPIENTS) break;
  }

  const prefix = projectSlug ? `[${projectSlug}] ` : "";
  const senderLabel = String(requestedByName || requestedByEmail || requestedByPhone || "Site update")
    .trim()
    .slice(0, 60);
  const smsBody = `${prefix}Message from ${senderLabel}: ${String(messageBody || "").trim()}`.slice(0, 640);

  for (const recipient of deduped) {
    const payload = { to: recipient.phoneE164, body: smsBody };
    if (messagingServiceSid) payload.messagingServiceSid = messagingServiceSid;
    else payload.from = fromPhone;
    try {
      const tw = await smsClient.messages.create(payload);
      sent.push({
        phoneE164: recipient.phoneE164,
        label: recipient.label || recipient.phoneE164,
        sid: tw.sid || null,
        status: tw.status || null,
      });
      await db.collection("messages").add({
        direction: "outbound",
        from: fromPhone || null,
        to: recipient.phoneE164,
        body: smsBody,
        messageSid: tw.sid || null,
        delivery: "twilio_api_notification",
        threadKey: recipient.phoneE164,
        phoneE164: recipient.phoneE164,
        channel: "sms",
        schemaVersion: MESSAGE_SCHEMA_VERSION,
        command: "notify_fanout_delivery",
        projectSlug: projectSlug || null,
        notifyRequestedByPhone: requestedByPhone || null,
        notifyRequestedByName: requestedByName || null,
        notifyRequestedByEmail: requestedByEmail || null,
        twilioMessageStatus: tw.status || null,
        twilioAccountSid: accountSid || null,
        twilioSenderPhoneE164: fromPhone || null,
        twilioMessagingServiceSid: messagingServiceSid || null,
        twilioDestinationPhoneE164: recipient.phoneE164,
        createdAt: FieldValue.serverTimestamp(),
      });
    } catch (err) {
      failed.push({
        phoneE164: recipient.phoneE164,
        label: recipient.label || recipient.phoneE164,
        error: String(err && err.message || err || "send_failed").slice(0, 180),
      });
    }
  }

  return {
    attemptedCount: deduped.length,
    sentCount: sent.length,
    failedCount: failed.length,
    skippedSelfCount: 0,
    sent,
    failed,
    runId,
  };
}

function toClientTimestamp(value) {
  if (!value || typeof value.toMillis !== "function") return null;
  const millis = value.toMillis();
  return {
    seconds: Math.floor(millis / 1000),
    nanoseconds: (millis % 1000) * 1e6,
  };
}

function serializeIssueForClient(docSnap, issueCollection) {
  const data = docSnap.data() || {};
  return {
    id: docSnap.id,
    issueCollection,
    ...data,
    createdAt: toClientTimestamp(data.createdAt),
    updatedAt: toClientTimestamp(data.updatedAt),
    closedAt: toClientTimestamp(data.closedAt),
    dueDate: toClientTimestamp(data.dueDate),
    history: Array.isArray(data.history)
      ? data.history.map((entry) => ({
          ...entry,
          at: toClientTimestamp(entry && entry.at),
        }))
      : [],
    photos: Array.isArray(data.photos)
      ? data.photos.map((photo) => ({
          ...photo,
          uploadedAt: toClientTimestamp(photo && photo.uploadedAt),
        }))
      : [],
  };
}

function normalizeProjectNotesText(value) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim()
    .slice(0, 8000);
}

async function resolveApprovedPhoneSmsUser(phoneInput) {
  const normalized = normalizePhoneE164(phoneInput);
  if (!normalized) {
    throw new HttpsError("invalid-argument", "approvedPhoneE164 is required.");
  }
  const candidates = [];
  if (normalized) candidates.push(normalized);
  const digits = normalized.replace(/\D/g, "");
  if (digits.length === 10) candidates.push(`+1${digits}`);
  if (digits.length === 11 && digits.startsWith("1")) candidates.push(`+${digits}`);
  for (const candidate of candidates) {
    const snap = await db.collection("smsUsers").doc(candidate).get();
    if (snap.exists) {
      return {
        approvedPhoneE164: candidate,
        smsUserSnap: snap,
      };
    }
  }
  throw new HttpsError(
    "failed-precondition",
    "approvedPhoneE164 must match an existing smsUsers document. Use the same phone the user texted from, ideally in E.164 like +14165551234."
  );
}

async function buildSmsUserMemberSyncPatch({
  approvedPhoneE164,
  displayName,
  role,
  projectSlugs,
  allProjects,
}) {
  let syncedProjectSlugs = normalizeProjectSlugList(projectSlugs);
  if (allProjects === true) {
    const projectsSnap = await db.collection("projects").get();
    syncedProjectSlugs = projectsSnap.docs
      .map((docSnap) => normalizeProjectSlug(docSnap.id))
      .filter(Boolean);
  }

  const smsUserAccess = await getUserProjectAccess(db, approvedPhoneE164);
  const currentActive = smsUserAccess.activeProjectSlug || null;
  const nextActive = syncedProjectSlugs.includes(currentActive)
    ? currentActive
    : syncedProjectSlugs[0] || null;

  return {
    displayName: displayName || null,
    role: role || "user",
    projectSlugs: syncedProjectSlugs,
    activeProjectSlug: nextActive,
    updatedAt: FieldValue.serverTimestamp(),
  };
}

function assertManagementAccess(access) {
  if (!roleAtLeast(access && access.role, "management")) {
    throw new HttpsError(
      "permission-denied",
      "Management or admin access is required for this workflow."
    );
  }
}

async function assertAccessibleSmsUserForAccess(access, phoneE164) {
  const userAccess = await getUserProjectAccess(db, phoneE164);
  if (!userAccess.exists) {
    throw new HttpsError(
      "not-found",
      "No smsUsers document for this phone. Text the Twilio number once first."
    );
  }
  if (roleAtLeast(access && access.role, "admin") || access.allProjects === true) {
    return userAccess;
  }
  const scopedSlugs = normalizeProjectSlugList([
    userAccess.activeProjectSlug,
    ...(Array.isArray(userAccess.projectSlugs) ? userAccess.projectSlugs : []),
  ]);
  if (scopedSlugs.some((slug) => canAccessProject(access, slug))) {
    return userAccess;
  }
  throw new HttpsError(
    "permission-denied",
    "This phone is not assigned to a project you can access."
  );
}

const MAX_DAILY_PDF_SMS_ATTEMPTS = 3;
const MAX_LABOUR_PDF_SMS_ATTEMPTS = 3;

function buildDailyPdfSmsReplyBody({ pdfResult, projectName, projectSlug }) {
  const label = pdfResult.reportType === "journal" ? "Journal PDF" : "Daily PDF report";
  const scopeBits = [];
  if (projectName || projectSlug) scopeBits.push(projectName || projectSlug);
  if (pdfResult.reportDateKey) scopeBits.push(pdfResult.reportDateKey);
  const smsScopeText = scopeBits.length ? ` (${scopeBits.join(" - ")})` : "";
  return pdfResult.downloadURL
    ? `${label}${smsScopeText}: ${pdfResult.downloadURL}`
    : `${label}${smsScopeText} was generated, but no download link could be created. Stored at: ${pdfResult.storagePath}`;
}

function elevateProjectAccessForApprovedPhone(projectAccess, memberAccess) {
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

async function getProjectAccessForSmsDelivery(db, phoneE164, projectSlug, userAccess) {
  const baseAccess = await getAccessibleProjectForUser(db, phoneE164, projectSlug, userAccess);
  if (!baseAccess.exists || baseAccess.allowed) return baseAccess;
  const memberAccess = await findActiveAppMemberByApprovedPhone(db, phoneE164);
  return elevateProjectAccessForApprovedPhone(baseAccess, memberAccess);
}

async function processAssistantMessage({
  phoneE164,
  body,
  channel,
  replyFrom,
  openaiKey,
  runId,
  uploadedMedia = [],
  uploadedBy = null,
}) {
  const inboundRef = await db.collection("messages").add({
    direction: "inbound",
    from: phoneE164,
    to: replyFrom || null,
    body,
    messageSid: null,
    numMedia: 0,
    mediaCountEffective: 0,
    threadKey: phoneE164,
    phoneE164,
    channel,
    schemaVersion: MESSAGE_SCHEMA_VERSION,
    createdAt: FieldValue.serverTimestamp(),
  });

  let replyText;
  let outboundMeta = {};
  try {
    const out = await Promise.race([
      buildReply({
        db,
        openaiApiKey: openaiKey,
        logger,
        runId,
        from: phoneE164,
        body,
        relatedMessageId: inboundRef.id,
        numMedia: 0,
        channel,
        models: {
          primary: OPENAI_MODEL_PRIMARY.value(),
        },
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("__BUILD_REPLY_TIMEOUT__")), BUILD_REPLY_TIMEOUT_MS)
      ),
    ]);
    replyText = out.replyText;
    outboundMeta = out.outboundMeta || {};
  } catch (handlerErr) {
    if (handlerErr && handlerErr.message === "__BUILD_REPLY_TIMEOUT__") {
      replyText = "That took too long to process. Try a shorter message or text help.";
      outboundMeta = {
        aiUsed: false,
        aiError: "timeout",
        command: "timeout",
      };
    } else {
      logger.error("assistantCallable: buildReply threw", {
        runId,
        message: handlerErr.message,
        stack: handlerErr.stack,
      });
      replyText = "Something went wrong processing that. Try again or text help.";
      outboundMeta = {
        aiUsed: false,
        aiError: String(handlerErr.message || handlerErr),
        command: "handler_exception",
      };
    }
  }

  let safeReply = String(replyText || "").trim() || "OK.";
  let pdfResult = null;

  if (outboundMeta.dailyPdfRequested) {
    const userAccess = await getUserProjectAccess(db, phoneE164);
    const requestedSlug =
      outboundMeta.projectSlug != null && String(outboundMeta.projectSlug).trim() !== ""
        ? normalizeProjectSlug(outboundMeta.projectSlug)
        : userAccess.activeProjectSlug || null;
    const projectAccess = await getProjectAccessForSmsDelivery(
      db,
      phoneE164,
      requestedSlug,
      userAccess
    );
    const slug = projectAccess.projectSlug || null;
    const projectName = slug ? projectAccess.projectData.name || slug : null;
    const dashboardPdfType = normalizeReportTypeInput(outboundMeta.reportType || "");

    if (dashboardPdfType === "dailySiteLog" && !slug) {
      safeReply =
        "Daily site log PDF needs a project. Text: project docksteader (or your site slug), then request the daily PDF again.";
      outboundMeta = {
        ...outboundMeta,
        dailyPdfRequested: false,
        command: "daily_pdf_missing_project",
        aiUsed: false,
      };
    } else {
      pdfResult = await generateDailyReportPdf({
        db,
        bucket: admin.storage().bucket(),
        phoneE164,
        projectSlug: slug,
        projectName,
        reportDateKey: outboundMeta.reportDateKey || null,
        reportType: dashboardPdfType,
        openaiApiKey: openaiKey || null,
        logger,
        runId,
        modelsOverride: {
          primary: OPENAI_MODEL_PRIMARY.value(),
        },
      });
      safeReply = buildDailyPdfSmsReplyBody({
        pdfResult,
        projectName,
        projectSlug: slug,
      });
      outboundMeta = {
        ...outboundMeta,
        reportDateKey: pdfResult.reportDateKey || outboundMeta.reportDateKey || null,
        reportType: pdfResult.reportType || outboundMeta.reportType || null,
        projectSlug: slug,
        dailyPdfRequested: false,
        command: "daily_pdf_link",
      };
    }
  }

  await inboundRef.update({
    projectSlug: outboundMeta.projectSlug || null,
    command: outboundMeta.command || null,
    aiUsed: Boolean(outboundMeta.aiUsed),
    aiError: outboundMeta.aiError || null,
    logEntryId: outboundMeta.logEntryId || null,
    logCategory: outboundMeta.logCategory || null,
    classification: outboundMeta.classification || null,
    dailyPdfRequested: Boolean(outboundMeta.dailyPdfRequested),
    reportDateKey: outboundMeta.reportDateKey || null,
    reportType: outboundMeta.reportType || null,
    pendingDeficiencyIntake: Boolean(outboundMeta.pendingDeficiencyIntake),
  });

  const outboundRef = await db.collection("messages").add({
    direction: "outbound",
    from: replyFrom || "dashboard",
    to: phoneE164,
    body: safeReply,
    messageSid: null,
    delivery: "callable",
    replyToInboundDocId: inboundRef.id,
    threadKey: phoneE164,
    phoneE164,
    channel,
    schemaVersion: MESSAGE_SCHEMA_VERSION,
    projectSlug: outboundMeta.projectSlug || null,
    aiUsed: Boolean(outboundMeta.aiUsed),
    aiError: outboundMeta.aiError || null,
    command: outboundMeta.command || null,
    issueLogId: outboundMeta.issueLogId || null,
    issueCollection: outboundMeta.issueCollection || null,
    summarySaved: Boolean(outboundMeta.summarySaved),
    logEntryId: outboundMeta.logEntryId || null,
    logCategory: outboundMeta.logCategory || null,
    classification: outboundMeta.classification || null,
    dailyPdfRequested: Boolean(outboundMeta.dailyPdfRequested),
    reportDateKey: outboundMeta.reportDateKey || null,
    reportType: outboundMeta.reportType || null,
    pendingDeficiencyIntake: Boolean(outboundMeta.pendingDeficiencyIntake),
    createdAt: FieldValue.serverTimestamp(),
  });

  let uploadedMediaResult = [];
  if (Array.isArray(uploadedMedia) && uploadedMedia.length) {
    uploadedMediaResult = await registerUploadedMedia({
      db,
      FieldValue,
      phoneE164,
      projectSlug: outboundMeta.projectSlug || null,
      reportDateKey: outboundMeta.reportDateKey || null,
      sourceMessageId: inboundRef.id,
      linkedLogEntryId: outboundMeta.logEntryId || null,
      captionText: body,
      uploadedBy,
      files: uploadedMedia,
    });
    if (uploadedMediaResult.length && outboundMeta.logEntryId) {
      await appendLinkedMediaIds(
        db,
        FieldValue,
        outboundMeta.logEntryId,
        uploadedMediaResult.map((item) => item.storagePath).filter(Boolean)
      );
    }
  }

  if (outboundMeta.enhanceLogEntry && outboundMeta.logEntryId && openaiKey) {
    void maybeEnhanceLogEntry({
      db,
      openaiApiKey: openaiKey,
      logEntryId: outboundMeta.logEntryId,
      logger,
      runId,
      modelsOverride: {
        primary: OPENAI_MODEL_PRIMARY.value(),
      },
    });
  }

  return {
    inboundRef,
    outboundRef,
    replyText: safeReply,
    outboundMeta,
    pdfResult,
    uploadedMedia: uploadedMediaResult,
  };
}

function normalizePendingAudioReview(raw) {
  if (!raw || typeof raw !== "object") return null;
  const transcript = String(raw.transcript || "").trim();
  const sourceMessageId = String(raw.sourceMessageId || "").trim();
  const mediaUrl = String(raw.mediaUrl || "").trim();
  if (!transcript || !sourceMessageId || !mediaUrl) return null;
  return {
    transcript,
    sourceMessageId,
    mediaUrl,
    mediaContentType: String(raw.mediaContentType || "audio/mpeg").trim() || "audio/mpeg",
    mediaIndex: Math.max(0, Number(raw.mediaIndex || 0) || 0),
    messageSid: String(raw.messageSid || "").trim() || null,
    replyMessagingServiceSid: String(raw.replyMessagingServiceSid || "").trim() || null,
    sourceLabel: String(raw.sourceLabel || "voice").trim() || "voice",
    storageSource: String(raw.storageSource || "twilio_mms").trim() || "twilio_mms",
    fileStem: String(raw.fileStem || "voice-note").trim() || "voice-note",
    channel: String(raw.channel || "sms_audio_note_reviewed").trim() || "sms_audio_note_reviewed",
    recordingDuration: String(raw.recordingDuration || "").trim() || null,
    model: String(raw.model || "").trim() || null,
    createdAtMs: Number(raw.createdAtMs || 0) || 0,
  };
}

async function savePendingAudioReview(phoneE164, review) {
  const normalized = normalizePendingAudioReview(review);
  if (!phoneE164 || !normalized) return;
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingAudioReview: {
        ...normalized,
        createdAtMs: Date.now(),
      },
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

async function clearPendingAudioReview(phoneE164) {
  if (!phoneE164) return;
  await db.collection(COL_USERS).doc(phoneE164).set(
    {
      pendingAudioReview: FieldValue.delete(),
      updatedAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

function buildAudioReviewPrompt(transcript) {
  const text = String(transcript || "").replace(/\r\n/g, "\n").trim();
  return `Transcript draft:\n${text}\n\nReply SEND to use this in the report, or reply with the corrected text.`;
}

async function processPendingAudioReviewReply({
  phoneE164,
  inboundBody,
  inboundMessageSid,
  configuredFrom,
  openaiKey,
  runId,
  pendingReview,
}) {
  const rawBody = String(inboundBody || "").trim();
  if (!pendingReview) return null;
  if (!rawBody) {
    return {
      replyText: "Reply SEND to accept the transcript, or reply with the corrected text.",
      outboundMeta: {
        aiUsed: false,
        command: "audio_review_missing_reply",
      },
    };
  }
  if (AUDIO_REVIEW_CANCEL_RE.test(rawBody)) {
    await clearPendingAudioReview(phoneE164);
    await db.collection("messages").add({
      direction: "inbound",
      from: phoneE164,
      to: configuredFrom || null,
      body: rawBody,
      messageSid: inboundMessageSid || null,
      numMedia: 0,
      mediaCountEffective: 0,
      threadKey: phoneE164,
      phoneE164,
      channel: "sms",
      schemaVersion: MESSAGE_SCHEMA_VERSION,
      command: "audio_review_cancelled",
      createdAt: FieldValue.serverTimestamp(),
    });
    return {
      replyText: "Voice note review cancelled. Send a new voice note any time.",
      outboundMeta: {
        aiUsed: false,
        command: "audio_review_cancelled",
      },
    };
  }

  const finalTranscript = AUDIO_REVIEW_CONFIRM_RE.test(rawBody)
    ? pendingReview.transcript
    : rawBody;
  const reviewStatus = AUDIO_REVIEW_CONFIRM_RE.test(rawBody) ? "confirmed" : "edited";

  const inboundRef = await db.collection("messages").add({
    direction: "inbound",
    from: phoneE164,
    to: configuredFrom || null,
    body: finalTranscript,
    messageSid: inboundMessageSid || null,
    numMedia: 0,
    mediaCountEffective: 0,
    threadKey: phoneE164,
    phoneE164,
    channel: pendingReview.channel || "sms_audio_note_reviewed",
    schemaVersion: MESSAGE_SCHEMA_VERSION,
    sourceMessageId: pendingReview.sourceMessageId,
    transcriptReviewStatus: reviewStatus,
    originalTranscript: pendingReview.transcript,
    createdAt: FieldValue.serverTimestamp(),
  });

  let replyText;
  let outboundMeta = {};
  try {
    const out = await Promise.race([
      buildReply({
        db,
        openaiApiKey: openaiKey,
        logger,
        runId,
        from: phoneE164,
        body: finalTranscript,
        relatedMessageId: inboundRef.id,
        numMedia: 1,
        channel: pendingReview.channel || "sms_audio_note_reviewed",
        models: {
          primary: OPENAI_MODEL_PRIMARY.value(),
        },
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("__BUILD_REPLY_TIMEOUT__")), BUILD_REPLY_TIMEOUT_MS)
      ),
    ]);
    replyText = out.replyText;
    outboundMeta = out.outboundMeta || {};
  } catch (handlerErr) {
    if (handlerErr && handlerErr.message === "__BUILD_REPLY_TIMEOUT__") {
      replyText = "That took too long to process. Try again or send a shorter correction.";
      outboundMeta = {
        aiUsed: false,
        aiError: "timeout",
        command: "timeout",
      };
    } else {
      logger.error("pending audio review: buildReply threw", {
        runId,
        message: handlerErr.message,
        stack: handlerErr.stack,
      });
      replyText = "Something went wrong processing that voice note. Try again or resend it.";
      outboundMeta = {
        aiUsed: false,
        aiError: String(handlerErr.message || handlerErr),
        command: "handler_exception",
      };
    }
  }

  await inboundRef.update({
    projectSlug: outboundMeta.projectSlug || null,
    command: outboundMeta.command || null,
    aiUsed: Boolean(outboundMeta.aiUsed),
    aiError: outboundMeta.aiError || null,
    logEntryId: outboundMeta.logEntryId || null,
    logCategory: outboundMeta.logCategory || null,
    classification: outboundMeta.classification || null,
    reportDateKey: outboundMeta.reportDateKey || null,
    reportType: outboundMeta.reportType || null,
  });

  const mediaProjectSlug =
    outboundMeta.projectSlug || (await resolveVoiceMediaProjectSlug(db, phoneE164));

  let attachResult = null;
  try {
    attachResult = await saveOneInboundMedia({
      db,
      bucket: admin.storage().bucket(),
      FieldValue,
      accountSid: normalizeAccountSid(TWILIO_ACCOUNT_SID.value()),
      authToken: normalizeAuthToken(TWILIO_AUTH_TOKEN.value()),
      mediaUrl: pendingReview.mediaUrl,
      contentType: pendingReview.mediaContentType || "audio/mpeg",
      mediaIndex: pendingReview.mediaIndex || 0,
      messageSidTwilio: pendingReview.messageSid || "",
      sourceMessageId: pendingReview.sourceMessageId,
      senderPhone: phoneE164,
      projectSlug: mediaProjectSlug || null,
      reportDateKey: outboundMeta.reportDateKey || null,
      captionText: finalTranscript,
      linkedLogEntryId: outboundMeta.logEntryId || null,
      fileStem: pendingReview.fileStem || "voice-note",
      sourceLabel: pendingReview.sourceLabel || "voice",
      storageSource: pendingReview.storageSource || "twilio_mms",
      issueCollection: null,
      issueId: null,
      uploadedByPhone: phoneE164,
      logger,
      runId,
    });
  } catch (mediaErr) {
    logger.error("pending audio review: media save failed", {
      runId,
      message: mediaErr.message,
      stack: mediaErr.stack,
    });
  }

  await db.collection("messages").doc(pendingReview.sourceMessageId).set(
    {
      body: finalTranscript,
      projectSlug: mediaProjectSlug || null,
      command: outboundMeta.command || "audio_reviewed",
      aiUsed: Boolean(outboundMeta.aiUsed),
      aiError: outboundMeta.aiError || null,
      logEntryId: outboundMeta.logEntryId || null,
      logCategory: outboundMeta.logCategory || null,
      classification: outboundMeta.classification || null,
      reportDateKey: outboundMeta.reportDateKey || null,
      reportType: outboundMeta.reportType || null,
      mediaIds: attachResult && attachResult.mediaId ? [attachResult.mediaId] : [],
      mediaAttachedCount: attachResult ? 1 : 0,
      transcriptReviewStatus: reviewStatus,
      audioTranscription: {
        transcript: pendingReview.transcript,
        reviewedTranscript: finalTranscript,
        reviewStatus,
        mediaIndex: pendingReview.mediaIndex || 0,
        contentType: pendingReview.mediaContentType || null,
        model: pendingReview.model || null,
        status: "reviewed",
      },
      mediaProcessingAt: FieldValue.serverTimestamp(),
      reviewProcessedInboundMessageId: inboundRef.id,
    },
    { merge: true }
  );

  const outboundRef = await db.collection("messages").add({
    direction: "outbound",
    from: configuredFrom || null,
    to: phoneE164,
    body: String(replyText || "").trim() || "OK.",
    messageSid: null,
    delivery: "twiml_audio_review",
    replyToInboundDocId: inboundRef.id,
    threadKey: phoneE164,
    phoneE164,
    channel: "sms",
    schemaVersion: MESSAGE_SCHEMA_VERSION,
    projectSlug: outboundMeta.projectSlug || null,
    aiUsed: Boolean(outboundMeta.aiUsed),
    aiError: outboundMeta.aiError || null,
    command: outboundMeta.command || "audio_reviewed",
    logEntryId: outboundMeta.logEntryId || null,
    logCategory: outboundMeta.logCategory || null,
    classification: outboundMeta.classification || null,
    reportDateKey: outboundMeta.reportDateKey || null,
    reportType: outboundMeta.reportType || null,
    createdAt: FieldValue.serverTimestamp(),
  });

  await clearPendingAudioReview(phoneE164);

  return {
    replyText: String(replyText || "").trim() || "OK.",
    outboundMeta,
    inboundRef,
    outboundRef,
  };
}

/**
 * Twilio sends application/x-www-form-urlencoded.
 * Prefer rawBody when present — some Cloud Functions / Express setups leave req.body empty.
 */
function getTwilioParams(req) {
  if (req.rawBody && Buffer.isBuffer(req.rawBody)) {
    try {
      const params = new URLSearchParams(req.rawBody.toString());
      return Object.fromEntries(params.entries());
    } catch (e) {
      logger.warn("getTwilioParams: rawBody parse failed", { message: e.message });
    }
  }
  if (req.body && typeof req.body === "object" && !Buffer.isBuffer(req.body)) {
    return req.body;
  }
  return {};
}

function inferInboundAttachmentLabel(params, mediaCountEffective) {
  const count = Math.max(0, Number(mediaCountEffective) || 0);
  if (!count) return "";
  if (allInboundMediaAreAudio(params, mediaCountEffective)) return "Voice attachment";
  const types = [];
  for (let i = 0; i < count; i += 1) {
    const contentType = String(params[`MediaContentType${i}`] || "")
      .trim()
      .toLowerCase();
    if (contentType) types.push(contentType);
  }
  if (!types.length) return "Media attachment";
  const allAudio = types.every((type) => type.startsWith("audio/"));
  if (allAudio) return "Voice attachment";
  const allImage = types.every((type) => type.startsWith("image/"));
  if (allImage) return "Photo attachment";
  const allVideo = types.every((type) => type.startsWith("video/"));
  if (allVideo) return "Video attachment";
  return "Media attachment";
}

/**
 * Twilio often omits MediaContentType or sends application/octet-stream for MMS voice notes.
 * Treat those as audio when we have a media URL so queue/sync transcription can run.
 */
function inboundMediaSlotLooksLikeAudio(contentTypeRaw) {
  const ct = String(contentTypeRaw || "").trim().toLowerCase();
  if (!ct) return true;
  if (ct.startsWith("audio/")) return true;
  if (ct === "application/octet-stream") return true;
  return false;
}

function allInboundMediaAreAudio(params, mediaCountEffective) {
  const count = Math.max(0, Number(mediaCountEffective) || 0);
  if (!count) return false;
  for (let i = 0; i < count; i += 1) {
    const mediaUrl = String(params[`MediaUrl${i}`] || "").trim();
    if (!mediaUrl) return false;
    const contentType = params[`MediaContentType${i}`];
    if (!inboundMediaSlotLooksLikeAudio(contentType)) return false;
  }
  return true;
}

function firstAudioMediaFromParams(params, mediaCountEffective) {
  const count = Math.max(0, Number(mediaCountEffective) || 0);
  for (let i = 0; i < count; i += 1) {
    const mediaUrl = String(params[`MediaUrl${i}`] || "").trim();
    const contentTypeRaw = String(params[`MediaContentType${i}`] || "").trim();
    if (!mediaUrl) continue;
    if (!inboundMediaSlotLooksLikeAudio(contentTypeRaw)) continue;
    const contentType = contentTypeRaw.toLowerCase().startsWith("audio/")
      ? contentTypeRaw
      : "audio/mpeg";
    return { mediaIndex: i, mediaUrl, contentType };
  }
  return null;
}

function sniffAudioMimeFromBuffer(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 12) return null;
  const ascii4 = buffer.slice(0, 4).toString("ascii");
  if (ascii4 === "RIFF" && buffer.slice(8, 12).toString("ascii") === "WAVE") return "audio/wav";
  if (ascii4 === "OggS") return "audio/ogg";
  if (buffer[0] === 0xff && (buffer[1] & 0xe0) === 0xe0) return "audio/mpeg";
  if (buffer[0] === 0x1a && buffer[1] === 0x45 && buffer[2] === 0xdf && buffer[3] === 0xa3) return "audio/webm";
  if (buffer.slice(4, 8).toString("ascii") === "ftyp") {
    const brand = buffer.slice(8, 12).toString("ascii");
    if (/m4a|mp41|mp42|isom|iso2|M4A/i.test(brand)) return "audio/mp4";
    if (/3gp|3g2|3G2|3GP/i.test(brand)) return "audio/3gpp";
  }
  const headUtf8 = buffer.slice(0, 8).toString("utf8");
  if (headUtf8.startsWith("#!AMR") || headUtf8.startsWith("#!amr")) return "audio/amr";
  return null;
}

function resolveTranscriptionAudioMime(buffer, httpContentType, declaredType) {
  const sniffed = sniffAudioMimeFromBuffer(buffer);
  const http = String(httpContentType || "")
    .split(";")[0]
    .trim()
    .toLowerCase();
  const declared = String(declaredType || "")
    .split(";")[0]
    .trim()
    .toLowerCase();
  const candidates = [sniffed, http, declared];
  for (const c of candidates) {
    if (!c) continue;
    if (c.startsWith("text/")) continue;
    if (c.startsWith("audio/")) return c;
    if (c.startsWith("video/3gpp") || c.startsWith("video/3gpp2")) return c.replace(/^video\//, "audio/");
  }
  if (http === "application/octet-stream" && sniffed) return sniffed;
  if (declared === "application/octet-stream" && sniffed) return sniffed;
  return sniffed || (http && http !== "application/octet-stream" ? http : null) || declared || "audio/mpeg";
}

async function transcribeInboundTwilioAudio({
  params,
  mediaCountEffective,
  accountSid,
  authToken,
  openaiKey,
  logger,
  runId,
}) {
  if (!openaiKey) return null;
  const audioMedia = firstAudioMediaFromParams(params, mediaCountEffective);
  if (!audioMedia) return null;
  try {
    const { buffer, contentType: downloadedCt } = await fetchTwilioMediaBuffer(
      audioMedia.mediaUrl,
      accountSid,
      authToken,
      {
        logger,
        runId,
      }
    );
    if (!buffer || !buffer.length) return null;
    const resolvedMime = resolveTranscriptionAudioMime(buffer, downloadedCt, audioMedia.contentType);
    const ext = guessExtension(resolvedMime);
    const fileName = `voice-note-${audioMedia.mediaIndex}.${ext || "bin"}`;
    const tx = await transcribeAudioBufferWithFallback({
      buffer,
      fileName,
      contentType: resolvedMime,
      openaiKey,
      logger,
      runId,
      contextLabel: "inboundSms",
    });
    if (!tx || !tx.transcript) return null;
    return {
      transcript: tx.transcript,
      mediaIndex: audioMedia.mediaIndex,
      contentType: resolvedMime || null,
      model: tx.model || null,
    };
  } catch (err) {
    logger.warn("inboundSms: audio transcription failed", {
      runId,
      message: err.message,
    });
    return null;
  }
}

async function transcribeTwilioRecording({
  recordingUrl,
  contentType,
  accountSid,
  authToken,
  openaiKey,
  logger,
  runId,
}) {
  if (!openaiKey || !recordingUrl) return null;
  try {
    const { buffer, contentType: downloadedCt } = await fetchTwilioMediaBuffer(recordingUrl, accountSid, authToken, {
      logger,
      runId,
    });
    if (!buffer || !buffer.length) return null;
    const resolvedMime = resolveTranscriptionAudioMime(buffer, downloadedCt, contentType);
    const ext = guessExtension(resolvedMime);
    const fileName = `voice-message.${ext || "bin"}`;
    const tx = await transcribeAudioBufferWithFallback({
      buffer,
      fileName,
      contentType: resolvedMime,
      openaiKey,
      logger,
      runId,
      contextLabel: "inboundVoice",
    });
    if (!tx || !tx.transcript) return null;
    return {
      transcript: tx.transcript,
      contentType: resolvedMime,
      model: tx.model || null,
    };
  } catch (err) {
    logger.warn("inboundVoice: recording transcription failed", {
      runId,
      message: err.message,
    });
    return null;
  }
}

async function transcribeAudioBufferWithFallback({
  buffer,
  fileName,
  contentType,
  openaiKey,
  logger,
  runId,
  contextLabel,
}) {
  if (!buffer || !buffer.length) return null;
  const ctLow = String(contentType || "").toLowerCase();
  const fnLow = String(fileName || "").toLowerCase();
  const looksAmr = ctLow.includes("amr") || fnLow.endsWith(".amr");
  if (looksAmr) {
    try {
      const { transcribeAmrBuffer } = require("./googleSpeechTranscribe");
      const gText = await transcribeAmrBuffer(buffer, { logger, runId });
      const trimmedG = String(gText || "")
        .replace(/\s+/g, " ")
        .trim();
      if (trimmedG) {
        logger.info(`${contextLabel}: google speech amr ok`, {
          runId,
          chars: trimmedG.length,
        });
        return { transcript: trimmedG, model: "google-speech-amr" };
      }
    } catch (gErr) {
      logger.warn(`${contextLabel}: google speech amr failed`, {
        runId,
        message: gErr.message,
      });
    }
  }
  if (!openaiKey) return null;
  const FileCtor = globalThis.File || require("node:buffer").File;
  const client = new OpenAI({ apiKey: openaiKey });
  const models = ["whisper-1", "gpt-4o-mini-transcribe"];
  let lastError = null;
  for (const model of models) {
    try {
      const audioFile = new FileCtor([buffer], fileName, {
        type: contentType || "audio/mpeg",
      });
      const tx = await client.audio.transcriptions.create({
        file: audioFile,
        model,
        language: "en",
        prompt:
          "Transcribe exactly. This is often a construction site voice update. Preserve names, project slugs, trades, floor numbers, grid lines, unit numbers, dates, quantities, and material names. Do not summarize.",
      });
      const transcript = String((tx && tx.text) || "").replace(/\s+/g, " ").trim();
      if (transcript) {
        logger.info(`${contextLabel}: transcription ok`, {
          runId,
          model,
          chars: transcript.length,
        });
        return { transcript, model };
      }
      lastError = new Error(`empty transcript from ${model}`);
      logger.warn(`${contextLabel}: empty transcription`, {
        runId,
        model,
      });
    } catch (err) {
      lastError = err;
      logger.warn(`${contextLabel}: transcription model failed`, {
        runId,
        model,
        message: err.message,
      });
    }
  }
  if (lastError) {
    logger.warn(`${contextLabel}: all transcription attempts failed`, {
      runId,
      message: lastError.message,
    });
  }
  return null;
}

/** Twilio HTTP webhook must respond with TwiML in ~15s; keep AI path under this. */
const BUILD_REPLY_TIMEOUT_MS = 12_000;

/**
 * Twilio signs the exact webhook URL. Cloud Run may report path as "/" while the public URL
 * includes the function name (FUNCTION_TARGET) — try both. Also try with/without trailing slash.
 */
function buildWebhookUrls(req) {
  const proto = (req.get("x-forwarded-proto") || "https").split(",")[0].trim() || "https";
  const host = (req.get("x-forwarded-host") || req.get("host") || "")
    .split(",")[0]
    .trim();
  const fullPathWithQuery = String(req.originalUrl || req.url || "/").trim() || "/";
  const qIdx = fullPathWithQuery.indexOf("?");
  const pathOnly = qIdx >= 0 ? fullPathWithQuery.slice(0, qIdx) : fullPathWithQuery;
  const query = qIdx >= 0 ? fullPathWithQuery.slice(qIdx) : "";

  const urls = new Set();
  const addPathVariants = (pathname) => {
    const p = pathname.startsWith("/") ? pathname : `/${pathname}`;
    urls.add(`${proto}://${host}${p}${query}`);
    if (p !== "/" && !p.endsWith("/")) {
      urls.add(`${proto}://${host}${p}/${query}`);
    } else if (p.length > 1 && p.endsWith("/")) {
      const trimmed = p.replace(/\/+$/, "") || "/";
      urls.add(`${proto}://${host}${trimmed}${query}`);
    }
  };

  addPathVariants(pathOnly);

  const target = String(process.env.FUNCTION_TARGET || "").trim();
  if (target && (pathOnly === "/" || pathOnly === "")) {
    addPathVariants(`/${target}`);
  }

  return Array.from(urls);
}

/** Same host/query as current request but last path segment replaced (e.g. inboundSms → inboundVoice). */
function siblingCloudFunctionUrl(req, siblingExportName) {
  const primary = buildWebhookUrls(req)[0];
  try {
    const u = new URL(primary);
    const segs = u.pathname.split("/").filter(Boolean);
    if (segs.length > 0) {
      segs[segs.length - 1] = siblingExportName;
    } else {
      segs.push(siblingExportName);
    }
    u.pathname = `/${segs.join("/")}`;
    return u.toString();
  } catch (_) {
    return primary;
  }
}

function validateTwilioOrExplained(req, authToken, params) {
  const signature = req.get("X-Twilio-Signature");
  if (!signature) {
    return { ok: false, reason: "missing_signature" };
  }
  const tried = buildWebhookUrls(req);
  for (const url of tried) {
    if (twilio.validateRequest(authToken, signature, url, params)) {
      return { ok: true, url };
    }
  }
  return { ok: false, reason: "bad_signature", tried };
}

function shouldEnforceTwilioValidation() {
  const mode = String(ENFORCE_TWILIO_VALIDATE.value() || "auto")
    .trim()
    .toLowerCase();
  const isLocalDev =
    process.env.FUNCTIONS_EMULATOR === "true" ||
    process.env.FIREBASE_EMULATOR_HUB ||
    process.env.NODE_ENV === "development";

  if (isLocalDev) {
    return mode === "true";
  }
  return true;
}

function sendTwiml(res, text) {
  const twiml = new twilio.twiml.MessagingResponse();
  twiml.message(text);
  res.set("Content-Type", "text/xml; charset=utf-8");
  res.status(200).send(twiml.toString());
}

function sendVoiceTwiml(res, builder) {
  const vr = new twilio.twiml.VoiceResponse();
  builder(vr);
  res.set("Content-Type", "text/xml; charset=utf-8");
  res.status(200).send(vr.toString());
}

/** Twilio <Say> voice — neural TTS so callers hear a clear “AI” assistant, not silence. */
const VOICE_ASSISTANT_TWIML_VOICE = "Polly.Joanna-Neural";

function sayVoiceAssistant(verb, text) {
  const body = sanitizeVoiceText(text);
  if (!body) return;
  verb.say({ voice: VOICE_ASSISTANT_TWIML_VOICE }, body);
}

function sanitizeVoiceText(text) {
  return String(text || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 900);
}

function stripQueryFromUrl(url) {
  return String(url || "").split("?")[0];
}

/**
 * Generate daily PDF and send download link by SMS. Used from the Firestore queue
 * so delivery runs in a separate invocation (HTTP post-response work is unreliable on Cloud Run).
 */
async function deliverDailyPdfSmsViaTwilio({
  db,
  bucket,
  phoneE164,
  outboundProjectSlug,
  reportDateKey,
  reportType,
  replyToNumber,
  replyMessagingServiceSid,
  existingPdfResult,
  runId,
  accountSid,
  authToken,
  configuredFrom,
  openaiKey,
  logger,
  modelsPrimary,
}) {
  const userAccess = await getUserProjectAccess(db, phoneE164);
  const explicitSlug = normalizeProjectSlug(outboundProjectSlug) || null;
  let slug =
    explicitSlug ||
    userAccess.activeProjectSlug ||
    null;
  let projectName = null;
  if (slug) {
    const projectAccess = await getProjectAccessForSmsDelivery(
      db,
      phoneE164,
      slug,
      userAccess
    );
    if (!projectAccess.allowed) {
      logger.warn("deliverDailyPdfSms: rejected inaccessible project", {
        runId,
        phoneE164,
        projectSlug: slug,
        reason: projectAccess.reason,
      });
      throw new Error(
        explicitSlug
          ? `Project "${slug}" is not assigned to this phone number.`
          : `Active project "${slug}" is not accessible for this phone number.`
      );
    } else {
      projectName = projectAccess.projectData.name || slug;
    }
  }
  const normalizedReportType = normalizeReportTypeInput(reportType || "");
  if (normalizedReportType === "dailySiteLog" && !slug) {
    throw new Error(
      "Daily site log PDF needs a project. Text a project command first (e.g. project docksteader), then request the daily PDF again."
    );
  }
  const pdfResult = existingPdfResult || await generateDailyReportPdf({
    db,
    bucket,
    phoneE164,
    projectSlug: slug,
    projectName,
    reportDateKey: reportDateKey || null,
    reportType: normalizedReportType,
    openaiApiKey: openaiKey || null,
    logger,
    runId,
    modelsOverride: {
      primary: modelsPrimary,
    },
  });
  const smsClient = twilio(accountSid, authToken);
  const label = pdfResult.reportType === "journal" ? "Journal PDF" : "Daily PDF report";
  const scopeBits = [];
  if (projectName || slug) scopeBits.push(projectName || slug);
  if (pdfResult.reportDateKey) scopeBits.push(pdfResult.reportDateKey);
  const scopeText = scopeBits.length ? ` (${scopeBits.join(" · ")})` : "";
  const pdfBody = pdfResult.downloadURL
    ? `Daily PDF report (download): ${pdfResult.downloadURL}`
    : `Daily PDF report saved (${slug || "no project"}). Signed link unavailable—open Storage or the dashboard. Path: ${pdfResult.storagePath}`;
  const smsScopeText = scopeBits.length ? ` (${scopeBits.join(" - ")})` : "";
  const smsReplyBody = pdfResult.downloadURL
    ? `${label}${smsScopeText}: ${pdfResult.downloadURL}`
    : `${label}${smsScopeText} was generated, but no download link could be created. Stored at: ${pdfResult.storagePath}`;
  const renderedSmsReplyBody = buildDailyPdfSmsReplyBody({
    pdfResult,
    projectName,
    projectSlug: slug,
  });
  const senderPhoneE164 = normalizePhoneE164(replyToNumber || "") || configuredFrom || null;
  const messagingServiceSid = normalizeTwilioSecret(replyMessagingServiceSid || "") || null;
  const messagePayload = {
    to: phoneE164,
    body: renderedSmsReplyBody,
  };
  if (messagingServiceSid) {
    messagePayload.messagingServiceSid = messagingServiceSid;
  } else if (senderPhoneE164) {
    messagePayload.from = senderPhoneE164;
  } else {
    throw new Error("No Twilio sender identity is available for daily PDF SMS delivery.");
  }
  let sent;
  try {
    sent = await smsClient.messages.create(messagePayload);
  } catch (sendErr) {
    sendErr.pdfResult = pdfResult;
    sendErr.messageBody = renderedSmsReplyBody;
    sendErr.senderPhoneE164 = senderPhoneE164;
    sendErr.messagingServiceSid = messagingServiceSid;
    throw sendErr;
  }
  return {
    messageSid: sent && sent.sid ? sent.sid : null,
    messageStatus: sent && sent.status ? sent.status : null,
    senderPhoneE164,
    messagingServiceSid,
    messageBody: renderedSmsReplyBody,
    pdfResult,
  };
}

async function sendVoiceFollowupSms({
  phoneE164,
  body,
  replyToInboundDocId = null,
  projectSlug = null,
  command = "voice_message_sms_followup",
  maxLength = 640,
  accountSid,
  authToken,
  configuredFrom,
  replyMessagingServiceSid = null,
  logger,
  runId,
}) {
  const toPhone = normalizePhoneE164(String(phoneE164 || "").trim());
  const fromPhone = normalizePhoneE164(String(configuredFrom || "").trim());
  const messagingSid = normalizeTwilioSecret(replyMessagingServiceSid || "") || null;
  const smsBody = String(body || "").trim().slice(0, Math.max(160, Number(maxLength) || 640));
  if (!toPhone || !smsBody) return { sent: false, reason: "missing_fields" };
  if (!messagingSid && !fromPhone) return { sent: false, reason: "missing_sender" };
  try {
    const messagePayload = { to: toPhone, body: smsBody };
    if (messagingSid) {
      messagePayload.messagingServiceSid = messagingSid;
    } else {
      messagePayload.from = fromPhone;
    }
    const sent = await twilio(accountSid, authToken).messages.create(messagePayload);
    await db.collection("messages").add({
      direction: "outbound",
      from: fromPhone || null,
      to: toPhone,
      body: smsBody,
      messageSid: sent.sid || null,
      delivery: "twilio_api_voice_followup_sms",
      replyToInboundDocId: replyToInboundDocId || null,
      threadKey: toPhone,
      phoneE164: toPhone,
      channel: "sms",
      schemaVersion: MESSAGE_SCHEMA_VERSION,
      command,
      projectSlug: projectSlug || null,
      twilioMessageStatus: sent.status || null,
      twilioAccountSid: accountSid || null,
      twilioMessagingServiceSid: messagingSid || null,
      twilioSenderPhoneE164: fromPhone || null,
      twilioDestinationPhoneE164: toPhone,
      createdAt: FieldValue.serverTimestamp(),
    });
    return { sent: true, sid: sent.sid || null, status: sent.status || null };
  } catch (err) {
    logger.error("voice follow-up sms send failed", {
      runId,
      toPhone,
      message: err.message,
      stack: err.stack,
    });
    return { sent: false, reason: String(err.message || err) };
  }
}

async function processVoiceMessageQueueDoc(snap, eventData = null) {
  const d = snap.data() || {};
  const queueRef = snap.ref;
  const markQueue = async (patch) => {
    await queueRef.set(
      {
        ...patch,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  };
  if (d.status === "processed") return;

  const from = normalizePhoneE164(String(d.from || "").trim()) || String(d.from || "").trim();
  const to = normalizePhoneE164(String(d.to || "").trim()) || String(d.to || "").trim();
  const inboundMessageId = String(d.inboundMessageId || "").trim();
  const recordingUrl = String(d.recordingUrl || "").trim();
  const recordingContentType = String(d.recordingContentType || "audio/mpeg").trim();
  const recordingDuration = String(d.recordingDuration || "").trim();
  const callSid = String(d.callSid || "").trim();
  const runId = d.runId || `voiceq-${snap.id}`;

  if (!from || !recordingUrl || !inboundMessageId) {
    await markQueue({
      status: "failed",
      failedAt: FieldValue.serverTimestamp(),
      lastError: "missing required voice queue fields",
    }).catch(() => {});
    return;
  }

  const inboundRef = db.collection("messages").doc(inboundMessageId);
  const accountSid = normalizeAccountSid(TWILIO_ACCOUNT_SID.value());
  const authToken = normalizeAuthToken(TWILIO_AUTH_TOKEN.value());
  const configuredFrom = normalizePhoneE164(TWILIO_PHONE_NUMBER.value());
  const openaiKey = OPENAI_API_KEY.value();

  await markQueue({
    status: "processing",
    processingStartedAt: FieldValue.serverTimestamp(),
    attemptCount: Number(d.attemptCount || 0) + 1,
  }).catch(() => {});

  const voiceTranscript = await transcribeTwilioRecording({
    recordingUrl,
    contentType: recordingContentType,
    accountSid,
    authToken,
    openaiKey,
    logger,
    runId,
  });
  const voiceTranscriptText = String(voiceTranscript && voiceTranscript.transcript || "").trim();
  const transcriptionOk = Boolean(voiceTranscriptText);
  const voiceBody = transcriptionOk
    ? voiceTranscriptText
    : "Voice recording received but transcription failed";

  await inboundRef.set({
    body: voiceBody,
    audioTranscription: {
      transcript: voiceTranscriptText || null,
      mediaIndex: 0,
      contentType: (voiceTranscript && voiceTranscript.contentType) || recordingContentType || null,
      model: (voiceTranscript && voiceTranscript.model) || null,
      status: transcriptionOk ? "pending_review" : "failed",
    },
    recordingDuration: recordingDuration || null,
    mediaProcessingAt: FieldValue.serverTimestamp(),
  }, { merge: true });

  let replyText = "";
  let outboundMeta = {};
  if (!transcriptionOk) {
    replyText =
      "I saved your voice message, but I could not transcribe it clearly. Please call again and speak a bit slower after the tone, or text me the update.";
    outboundMeta = {
      aiUsed: false,
      aiError: "voice_transcription_failed",
      command: "voice_record_transcription_failed",
    };
  } else {
    await savePendingAudioReview(from, {
      transcript: voiceTranscriptText,
      sourceMessageId: inboundMessageId,
      mediaUrl: recordingUrl,
      mediaContentType: (voiceTranscript && voiceTranscript.contentType) || recordingContentType || "audio/mpeg",
      mediaIndex: 0,
      messageSid: callSid || null,
      replyMessagingServiceSid: d.replyMessagingServiceSid || null,
      sourceLabel: "voice",
      storageSource: "twilio_voice",
      fileStem: "voice-message",
      channel: "voice_recording_reviewed",
      recordingDuration,
      model: (voiceTranscript && voiceTranscript.model) || null,
    });
    replyText = buildAudioReviewPrompt(voiceTranscriptText);
    outboundMeta = {
      aiUsed: false,
      aiError: null,
      command: "voice_record_review_requested",
    };
  }

  await inboundRef.set({
    command: outboundMeta.command || "voice_message",
    aiUsed: Boolean(outboundMeta.aiUsed),
    aiError: outboundMeta.aiError || null,
    transcriptReviewStatus: transcriptionOk ? "pending" : null,
  }, { merge: true });

  const safeReply = sanitizeVoiceText(replyText || "I saved your voice message.");
  const smsFollowup = await sendVoiceFollowupSms({
    phoneE164: from,
    body: safeReply,
    replyToInboundDocId: inboundMessageId,
    projectSlug: null,
    command: outboundMeta.command || "voice_message_sms_followup",
    maxLength: transcriptionOk ? 1200 : 640,
    accountSid,
    authToken,
    configuredFrom,
    replyMessagingServiceSid: d.replyMessagingServiceSid || null,
    logger,
    runId,
  });

  await db.collection("messages").add({
    direction: "outbound",
    from: configuredFrom || to || null,
    to: from,
    body: safeReply,
    messageSid: null,
    delivery: "voice_queue_result",
    replyToInboundDocId: inboundMessageId,
    threadKey: from,
    phoneE164: from,
    channel: "voice",
    schemaVersion: MESSAGE_SCHEMA_VERSION,
    aiUsed: Boolean(outboundMeta.aiUsed),
    aiError: outboundMeta.aiError || null,
    command: outboundMeta.command || "voice_message_ai",
    smsFollowupSent: Boolean(smsFollowup.sent),
    createdAt: FieldValue.serverTimestamp(),
  });

  await markQueue({
    status: "processed",
    processedAt: FieldValue.serverTimestamp(),
    transcriptionStatus: transcriptionOk ? "ok" : "failed",
    reviewRequested: transcriptionOk,
    smsFollowupSent: Boolean(smsFollowup.sent),
    smsFollowupSid: smsFollowup.sid || null,
    lastError: outboundMeta.aiError || null,
  }).catch(() => {});
}

async function processAudioMessageQueueDoc(snap) {
  const d = snap.data() || {};
  const queueRef = snap.ref;
  const markQueue = async (patch) => {
    await queueRef.set(
      {
        ...patch,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  };
  if (d.status === "processed") return;

  const from = normalizePhoneE164(String(d.from || "").trim()) || String(d.from || "").trim();
  const to = normalizePhoneE164(String(d.to || "").trim()) || String(d.to || "").trim();
  const inboundMessageId = String(d.inboundMessageId || "").trim();
  const mediaUrl = String(d.mediaUrl || "").trim();
  const mediaContentType = String(d.mediaContentType || "audio/mpeg").trim();
  const mediaIndex = Math.max(0, Number(d.mediaIndex || 0) || 0);
  const messageSid = String(d.messageSid || "").trim();
  const rawCaption = String(d.rawCaption || "").trim();
  const runId = d.runId || `audioq-${snap.id}`;

  if (!from || !mediaUrl || !inboundMessageId) {
    await markQueue({
      status: "failed",
      failedAt: FieldValue.serverTimestamp(),
      lastError: "missing required audio queue fields",
    }).catch(() => {});
    return;
  }

  const inboundRef = db.collection("messages").doc(inboundMessageId);
  const accountSid = normalizeAccountSid(TWILIO_ACCOUNT_SID.value());
  const authToken = normalizeAuthToken(TWILIO_AUTH_TOKEN.value());
  const configuredFrom = normalizePhoneE164(TWILIO_PHONE_NUMBER.value());
  const openaiKey = OPENAI_API_KEY.value();

  await markQueue({
    status: "processing",
    processingStartedAt: FieldValue.serverTimestamp(),
    attemptCount: Number(d.attemptCount || 0) + 1,
  }).catch(() => {});

  let audioTranscript = null;
  let resolvedMimeForLog = null;
  try {
    const { buffer, contentType: downloadedCt } = await fetchTwilioMediaBuffer(mediaUrl, accountSid, authToken, {
      logger,
      runId,
    });
    if (buffer && buffer.length) {
      resolvedMimeForLog = resolveTranscriptionAudioMime(buffer, downloadedCt, mediaContentType);
      const ext = guessExtension(resolvedMimeForLog);
      audioTranscript = await transcribeAudioBufferWithFallback({
        buffer,
        fileName: `voice-note-${mediaIndex}.${ext || "bin"}`,
        contentType: resolvedMimeForLog,
        openaiKey,
        logger,
        runId,
        contextLabel: "audioQueue",
      });
    }
  } catch (err) {
    logger.warn("audio queue: transcription fetch/transcribe failed", {
      runId,
      message: err.message,
    });
  }

  const transcriptText = String(audioTranscript && audioTranscript.transcript || "").trim();
  const transcriptionOk = Boolean(transcriptText);
  const bodyText = transcriptionOk
    ? transcriptText
    : (rawCaption || "Audio message received but transcription failed");

  await inboundRef.set({
    body: bodyText,
    audioTranscription: {
      transcript: transcriptText || null,
      mediaIndex,
      contentType: resolvedMimeForLog || mediaContentType || null,
      model: (audioTranscript && audioTranscript.model) || null,
      status: transcriptionOk ? "pending_review" : "failed",
    },
    mediaProcessingAt: FieldValue.serverTimestamp(),
  }, { merge: true });

  let replyText = "";
  let outboundMeta = {};
  if (!transcriptionOk) {
    replyText =
      "I saved your audio note, but I could not transcribe it clearly. Please send a short text summary or try the voice note again with clearer audio.";
    outboundMeta = {
      aiUsed: false,
      aiError: "audio_transcription_failed",
      command: "audio_note_transcription_failed",
    };
  } else {
    await savePendingAudioReview(from, {
      transcript: transcriptText,
      sourceMessageId: inboundMessageId,
      mediaUrl,
      mediaContentType: resolvedMimeForLog || mediaContentType || "audio/mpeg",
      mediaIndex,
      messageSid: messageSid || null,
      replyMessagingServiceSid: d.replyMessagingServiceSid || null,
      sourceLabel: "voice",
      storageSource: "twilio_mms",
      fileStem: "voice-note",
      channel: "sms_audio_note_reviewed",
      model: (audioTranscript && audioTranscript.model) || null,
    });
    replyText = buildAudioReviewPrompt(transcriptText);
    outboundMeta = {
      aiUsed: false,
      aiError: null,
      command: "audio_note_review_requested",
    };
  }

  await inboundRef.set({
    command: outboundMeta.command || "audio_message",
    aiUsed: Boolean(outboundMeta.aiUsed),
    aiError: outboundMeta.aiError || null,
    transcriptReviewStatus: transcriptionOk ? "pending" : null,
  }, { merge: true });

  const safeReply = sanitizeVoiceText(replyText || "I saved your audio note.");
  const smsFollowup = await sendVoiceFollowupSms({
    phoneE164: from,
    body: safeReply,
    replyToInboundDocId: inboundMessageId,
    projectSlug: null,
    command: outboundMeta.command || "audio_message_sms_followup",
    maxLength: transcriptionOk ? 1200 : 640,
    accountSid,
    authToken,
    configuredFrom,
    replyMessagingServiceSid: d.replyMessagingServiceSid || null,
    logger,
    runId,
  });

  await markQueue({
    status: "processed",
    processedAt: FieldValue.serverTimestamp(),
    transcriptionStatus: transcriptionOk ? "ok" : "failed",
    reviewRequested: transcriptionOk,
    smsFollowupSent: Boolean(smsFollowup.sent),
    smsFollowupSid: smsFollowup.sid || null,
    lastError: outboundMeta.aiError || null,
  }).catch(() => {});
}

exports.deliverDailyPdfSms = onDocumentCreated(
  {
    document: "dailyPdfDeliveryQueue/{docId}",
    region: "northamerica-northeast1",
    timeoutSeconds: 300,
    memory: "512MiB",
    retry: true,
    secrets: [
      TWILIO_ACCOUNT_SID,
      TWILIO_AUTH_TOKEN,
      TWILIO_PHONE_NUMBER,
      OPENAI_API_KEY,
    ],
  },
  async (event) => {
    const docId = event.params && event.params.docId;
    logger.info("deliverDailyPdfSms: invoked", { docId: docId || null, hasEventData: Boolean(event.data) });

    let snap = null;
    if (docId) {
      try {
        snap = await db.collection("dailyPdfDeliveryQueue").doc(docId).get();
      } catch (e) {
        logger.error("deliverDailyPdfSms: queue read failed", { docId, message: e.message });
      }
    }
    if ((!snap || !snap.exists) && event.data && event.data.exists) {
      snap = event.data;
    }
    if (!snap || !snap.exists) {
      logger.error("deliverDailyPdfSms: missing or empty document snapshot", { docId: docId || null });
      return;
    }
    const d = snap.data() || {};
    const queueRef = snap.ref;
    const markQueue = async (patch) => {
      await queueRef.set(
        {
          ...patch,
          updatedAt: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    };
    if (d.status === "sent" && d.twilioMessageSid) {
      logger.info("deliverDailyPdfSms: queue already sent, skipping", {
        docId: snap.id,
        phoneE164: d.phoneE164 || null,
        twilioMessageSid: d.twilioMessageSid || null,
      });
      return;
    }
    const phoneE164 = (d.phoneE164 || "").trim();
    const runId = d.runId || `pdfq-${snap.id}`;
    const priorAttemptCount = Number(d.attemptCount || 0);
    const attemptNumber = priorAttemptCount + 1;
    if (!phoneE164) {
      logger.warn("deliverDailyPdfSms: missing phoneE164", {
        docId: snap.id,
      });
      await markQueue({
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        lastError: "missing phoneE164",
      }).catch(() => {});
      return;
    }

    const accountSid = normalizeAccountSid(TWILIO_ACCOUNT_SID.value());
    const authToken = normalizeAuthToken(TWILIO_AUTH_TOKEN.value());
    const configuredFrom = normalizePhoneE164(TWILIO_PHONE_NUMBER.value());
    const openaiKey = OPENAI_API_KEY.value();
    const replyToNumber = normalizePhoneE164(d.replyToNumber || "");
    const replyMessagingServiceSid = normalizeTwilioSecret(d.replyMessagingServiceSid || "");
    const replyAccountSid = normalizeAccountSid(d.replyAccountSid || "");
    const runtimeAccountSid = replyAccountSid || accountSid;

    if (
      !runtimeAccountSid ||
      !authToken ||
      (!configuredFrom && !replyToNumber && !replyMessagingServiceSid)
    ) {
      logger.error("deliverDailyPdfSms: missing Twilio secrets", { runId });
      await markQueue({
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        lastError: "missing Twilio account identity, sender identity, or credentials",
      }).catch(() => {});
      return;
    }

    await markQueue({
      status: "processing",
      startedAt: FieldValue.serverTimestamp(),
      attemptCount: FieldValue.increment(1),
      lastAttemptAt: FieldValue.serverTimestamp(),
      lastError: null,
    }).catch(() => {});

    const existingPdfResult =
      d.downloadURL || d.storagePath || d.reportDocId
        ? {
            reportId: d.reportDocId || null,
            reportDateKey: d.reportDateKey || null,
            reportType: normalizeReportTypeInput(d.reportType || ""),
            downloadURL: d.downloadURL || null,
            storagePath: d.storagePath || null,
          }
        : null;

    try {
      const delivery = await deliverDailyPdfSmsViaTwilio({
        db,
        bucket: admin.storage().bucket(),
        phoneE164,
        outboundProjectSlug: d.projectSlug || null,
        reportDateKey: d.reportDateKey || null,
        reportType: d.reportType || null,
        replyToNumber,
        replyMessagingServiceSid,
        existingPdfResult,
        runId,
        accountSid: runtimeAccountSid,
        authToken,
        configuredFrom,
        openaiKey,
        logger,
        modelsPrimary: OPENAI_MODEL_PRIMARY.value(),
      });
      await db.collection("messages").add({
        direction: "outbound",
        from: delivery.senderPhoneE164 || configuredFrom || null,
        to: phoneE164,
        body: delivery.messageBody || null,
        messageSid: delivery.messageSid || null,
        delivery: "twilio_api",
        replyToInboundDocId: d.replyToInboundDocId || null,
        threadKey: phoneE164,
        phoneE164,
        channel: "sms",
        schemaVersion: MESSAGE_SCHEMA_VERSION,
        projectSlug: d.projectSlug || null,
        command: "daily_pdf_link",
        reportDateKey: d.reportDateKey || null,
        reportType: d.reportType || null,
        twilioStatus: delivery.messageStatus || null,
        twilioMessagingServiceSid: delivery.messagingServiceSid || null,
        createdAt: FieldValue.serverTimestamp(),
      });
      logger.info("deliverDailyPdfSms: sent PDF link SMS", { runId, phoneE164 });
      await markQueue({
        status: "sent",
        sentAt: FieldValue.serverTimestamp(),
        twilioMessageSid: delivery.messageSid || null,
        twilioMessageStatus: delivery.messageStatus || null,
        twilioAccountSid: runtimeAccountSid || null,
        twilioSenderPhoneE164: delivery.senderPhoneE164 || null,
        twilioMessagingServiceSid: delivery.messagingServiceSid || null,
        twilioDestinationPhoneE164: phoneE164,
        storagePath: delivery.pdfResult && delivery.pdfResult.storagePath
          ? delivery.pdfResult.storagePath
          : null,
        downloadURL: delivery.pdfResult && delivery.pdfResult.downloadURL
          ? delivery.pdfResult.downloadURL
          : null,
        reportDocId: delivery.pdfResult && delivery.pdfResult.reportDocId
          ? delivery.pdfResult.reportDocId
          : null,
        lastError: null,
      }).catch(() => {});
    } catch (pdfErr) {
      logger.error("deliverDailyPdfSms: daily PDF failed", {
        runId,
        message: pdfErr.message,
        stack: pdfErr.stack,
      });
      let failureSmsSid = null;
      let failureSmsError = null;
      await markQueue({
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        lastError: String(pdfErr.message || pdfErr).slice(0, 1000),
        attemptCount: attemptNumber,
        reportDocId:
          pdfErr && pdfErr.pdfResult && pdfErr.pdfResult.reportId
            ? pdfErr.pdfResult.reportId
            : d.reportDocId || null,
        downloadURL:
          pdfErr && pdfErr.pdfResult && pdfErr.pdfResult.downloadURL
            ? pdfErr.pdfResult.downloadURL
            : d.downloadURL || null,
        storagePath:
          pdfErr && pdfErr.pdfResult && pdfErr.pdfResult.storagePath
            ? pdfErr.pdfResult.storagePath
            : d.storagePath || null,
      }).catch(() => {});
      if (attemptNumber < MAX_DAILY_PDF_SMS_ATTEMPTS) {
        logger.warn("deliverDailyPdfSms: retrying after failure", {
          runId,
          attemptNumber,
          maxAttempts: MAX_DAILY_PDF_SMS_ATTEMPTS,
          message: pdfErr.message,
        });
        throw pdfErr;
      }
      try {
        const smsClient = twilio(runtimeAccountSid, authToken);
        const failurePayload = {
          to: phoneE164,
          body: "Could not finish your daily PDF report. Try again later.",
        };
        if (replyMessagingServiceSid) {
          failurePayload.messagingServiceSid = replyMessagingServiceSid;
        } else {
          failurePayload.from = replyToNumber || configuredFrom;
        }
        const failureMessage = await smsClient.messages.create(failurePayload);
        failureSmsSid = failureMessage && failureMessage.sid ? failureMessage.sid : null;
      } catch (smsErr) {
        failureSmsError = String(smsErr.message || smsErr).slice(0, 500);
        logger.error("deliverDailyPdfSms: failed to send failure SMS", {
          runId,
          message: failureSmsError,
        });
      }
      await markQueue({
        failureSmsSid,
        failureSmsError,
        twilioAccountSid: runtimeAccountSid || null,
        twilioSenderPhoneE164: replyToNumber || configuredFrom || null,
        twilioMessagingServiceSid: replyMessagingServiceSid || null,
        twilioDestinationPhoneE164: phoneE164,
      }).catch(() => {});
      if (failureSmsSid || failureSmsError) {
        await db.collection("messages").add({
          direction: "outbound",
          from: replyToNumber || configuredFrom || null,
          to: phoneE164,
          body: "Could not finish your daily PDF report. Try again later.",
          messageSid: failureSmsSid || null,
          delivery: "twilio_api",
          replyToInboundDocId: d.replyToInboundDocId || null,
          threadKey: phoneE164,
          phoneE164,
          channel: "sms",
          schemaVersion: MESSAGE_SCHEMA_VERSION,
          projectSlug: d.projectSlug || null,
          command: "daily_pdf_link_failed",
          reportDateKey: d.reportDateKey || null,
          reportType: d.reportType || null,
          twilioStatus: failureSmsError ? "failed" : "queued",
          twilioMessagingServiceSid: replyMessagingServiceSid || null,
          aiError: failureSmsError || null,
          createdAt: FieldValue.serverTimestamp(),
        }).catch(() => {});
      }
    }
  }
);

exports.deliverLabourPdfSms = onDocumentCreated(
  {
    document: "labourPdfDeliveryQueue/{docId}",
    region: "northamerica-northeast1",
    timeoutSeconds: 300,
    memory: "512MiB",
    retry: true,
    secrets: [
      TWILIO_ACCOUNT_SID,
      TWILIO_AUTH_TOKEN,
      TWILIO_PHONE_NUMBER,
    ],
  },
  async (event) => {
    const docId = event.params && event.params.docId;
    logger.info("deliverLabourPdfSms: invoked", { docId: docId || null, hasEventData: Boolean(event.data) });

    let snap = null;
    if (docId) {
      try {
        snap = await db.collection("labourPdfDeliveryQueue").doc(docId).get();
      } catch (e) {
        logger.error("deliverLabourPdfSms: queue read failed", { docId, message: e.message });
      }
    }
    if ((!snap || !snap.exists) && event.data && event.data.exists) {
      snap = event.data;
    }
    if (!snap || !snap.exists) {
      logger.error("deliverLabourPdfSms: missing or empty document snapshot", { docId: docId || null });
      return;
    }

    const d = snap.data() || {};
    const queueRef = snap.ref;
    const markQueue = async (patch) => {
      await queueRef.set(
        {
          ...patch,
          updatedAt: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    };

    if (d.status === "sent" && d.twilioMessageSid) {
      logger.info("deliverLabourPdfSms: queue already sent, skipping", {
        docId: snap.id,
        phoneE164: d.phoneE164 || null,
        twilioMessageSid: d.twilioMessageSid || null,
      });
      return;
    }

    const phoneE164 = String(d.phoneE164 || "").trim();
    const runId = d.runId || `labourpdfq-${snap.id}`;
    const priorAttemptCount = Number(d.attemptCount || 0);
    const attemptNumber = priorAttemptCount + 1;
    const startKey = String(d.startKey || "").trim();
    const endKey = String(d.endKey || "").trim();
    const replyToNumber = String(d.replyToNumber || "").trim();
    const replyMessagingServiceSid = normalizeTwilioSecret(d.replyMessagingServiceSid || "") || null;
    const inboundDocId = d.replyToInboundDocId || null;

    if (!phoneE164) {
      await markQueue({
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        lastError: "missing phoneE164",
      }).catch(() => {});
      return;
    }
    const { startKey: normalizedStart, endKey: normalizedEnd } = normalizeLabourRangeKeys(startKey, endKey);
    if (!normalizedStart || !normalizedEnd) {
      await markQueue({
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        lastError: "missing or invalid startKey/endKey",
      }).catch(() => {});
      return;
    }

    const accountSid = normalizeAccountSid(TWILIO_ACCOUNT_SID.value());
    const authToken = normalizeAuthToken(TWILIO_AUTH_TOKEN.value());
    const configuredFrom = normalizePhoneE164(TWILIO_PHONE_NUMBER.value());
    const runtimeAccountSid = accountSid;
    if (!runtimeAccountSid || !authToken) {
      await markQueue({
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        lastError: "missing Twilio secrets",
        attemptCount: attemptNumber,
      }).catch(() => {});
      return;
    }

    await markQueue({
      status: "processing",
      processingAt: FieldValue.serverTimestamp(),
      attemptCount: attemptNumber,
      lastError: null,
    }).catch(() => {});

    try {
      const entries = await loadLabourEntries(db, {
        startKey: normalizedStart,
        endKey: normalizedEnd,
        labourerPhone: phoneE164,
      });
      const summary = buildLabourRollup(entries);
      const labourer = await findActiveLabourerByPhone(db, phoneE164).catch(() => null);

      const reportTitle = "Labour Hours Report";
      const scopeBits = [
        labourer ? labourer.displayName || labourer.phoneE164 : phoneE164,
        normalizedStart === normalizedEnd ? normalizedStart : `${normalizedStart} to ${normalizedEnd}`,
      ].filter(Boolean);
      const scopeLabel = scopeBits.join(" · ");
      const scopeSequenceKey = [
        "labourHours",
        normalizePhoneE164(phoneE164) || "",
        "",
        "",
      ].join("|");
      const sameScopeReportsSnap = await db
        .collection("labourReports")
        .where("type", "==", "labourHours")
        .where("startKey", "==", normalizedStart)
        .where("endKey", "==", normalizedEnd)
        .where("scopeSequenceKey", "==", scopeSequenceKey)
        .get();
      const sequence = String((sameScopeReportsSnap.size || 0) + 1).padStart(3, "0");
      const startStamp = normalizedStart.replace(/-/g, "_");
      const endStamp = normalizedEnd.replace(/-/g, "_");
      const fileName = `Labourers_Report_${startStamp}_to_${endStamp}_${sequence}.pdf`;
      const storagePath = fileName;
      const downloadToken = Math.random().toString(36).slice(2) + Date.now().toString(36);
      const bucket = admin.storage().bucket();

      const pdfResult = await generateLabourReportPdf({
        pdfTitle: reportTitle,
        subtitle: scopeLabel,
        summary: { ...summary, startKey: normalizedStart, endKey: normalizedEnd },
        entries,
        storageBucket: bucket,
        storagePath,
        downloadToken,
      });

      await db.collection("labourReports").add({
        type: "labourHours",
        reportTitle,
        labourerPhone: phoneE164 || null,
        labourerName: labourer ? labourer.displayName || null : null,
        projectSlug: null,
        startKey: normalizedStart,
        endKey: normalizedEnd,
        totalHours: summary.totalHours,
        totalPaidHours: summary.totalPaidHours,
        totalPayUnits: summary.totalPayUnits || null,
        totalEntries: summary.totalEntries,
        fileName,
        fileSequence: Number(sequence),
        scopeSequenceKey,
        storagePath: pdfResult.storagePath,
        downloadURL: pdfResult.downloadURL,
        createdAt: FieldValue.serverTimestamp(),
        createdByPhone: phoneE164,
        runId,
      }).catch(() => {});

      const smsBody = pdfResult.downloadURL
        ? `Labour report (${normalizedStart} to ${normalizedEnd}): ${pdfResult.downloadURL}`
        : `Labour report (${normalizedStart} to ${normalizedEnd}) generated. Stored at: ${pdfResult.storagePath}`;

      const smsClient = twilio(runtimeAccountSid, authToken);
      const payload = { to: phoneE164, body: smsBody };
      if (replyMessagingServiceSid) payload.messagingServiceSid = replyMessagingServiceSid;
      else payload.from = replyToNumber || configuredFrom;

      const sent = await smsClient.messages.create(payload);
      const messageSid = sent && sent.sid ? sent.sid : null;

      await markQueue({
        status: "sent",
        sentAt: FieldValue.serverTimestamp(),
        twilioMessageSid: messageSid,
        downloadURL: pdfResult.downloadURL || null,
        storagePath: pdfResult.storagePath || null,
        lastError: null,
      }).catch(() => {});

      await db.collection("messages").add({
        direction: "outbound",
        from: replyToNumber || configuredFrom || null,
        to: phoneE164,
        body: smsBody,
        messageSid: messageSid || null,
        delivery: "twilio_api",
        replyToInboundDocId: inboundDocId,
        threadKey: phoneE164,
        phoneE164,
        channel: "sms",
        schemaVersion: MESSAGE_SCHEMA_VERSION,
        command: "labour_report_link",
        twilioMessagingServiceSid: replyMessagingServiceSid || null,
        createdAt: FieldValue.serverTimestamp(),
      }).catch(() => {});
    } catch (err) {
      logger.error("deliverLabourPdfSms: labour PDF failed", {
        runId,
        message: err.message,
        stack: err.stack,
      });
      await markQueue({
        status: "failed",
        failedAt: FieldValue.serverTimestamp(),
        lastError: String(err.message || err).slice(0, 1000),
        attemptCount: attemptNumber,
      }).catch(() => {});
      if (attemptNumber < MAX_LABOUR_PDF_SMS_ATTEMPTS) throw err;
    }
  }
);

exports.processVoiceMessageQueue = onDocumentCreated(
  {
    document: `${COL_VOICE_MESSAGE_QUEUE}/{docId}`,
    region: "northamerica-northeast1",
    timeoutSeconds: 300,
    memory: "512MiB",
    retry: true,
    secrets: [
      TWILIO_ACCOUNT_SID,
      TWILIO_AUTH_TOKEN,
      TWILIO_PHONE_NUMBER,
      OPENAI_API_KEY,
    ],
  },
  async (event) => {
    const docId = event.params && event.params.docId;
    logger.info("processVoiceMessageQueue: invoked", {
      docId: docId || null,
      hasEventData: Boolean(event.data),
    });

    let snap = null;
    if (docId) {
      try {
        snap = await db.collection(COL_VOICE_MESSAGE_QUEUE).doc(docId).get();
      } catch (e) {
        logger.error("processVoiceMessageQueue: queue read failed", {
          docId,
          message: e.message,
        });
      }
    }
    if ((!snap || !snap.exists) && event.data && event.data.exists) {
      snap = event.data;
    }
    if (!snap || !snap.exists) {
      logger.error("processVoiceMessageQueue: missing or empty document snapshot", {
        docId: docId || null,
      });
      return;
    }
    await processVoiceMessageQueueDoc(snap, event.data || null);
  }
);

exports.processAudioMessageQueue = onDocumentCreated(
  {
    document: `${COL_AUDIO_MESSAGE_QUEUE}/{docId}`,
    region: "northamerica-northeast1",
    timeoutSeconds: 300,
    memory: "512MiB",
    retry: true,
    secrets: [
      TWILIO_ACCOUNT_SID,
      TWILIO_AUTH_TOKEN,
      TWILIO_PHONE_NUMBER,
      OPENAI_API_KEY,
    ],
  },
  async (event) => {
    const docId = event.params && event.params.docId;
    logger.info("processAudioMessageQueue: invoked", {
      docId: docId || null,
      hasEventData: Boolean(event.data),
    });

    let snap = null;
    if (docId) {
      try {
        snap = await db.collection(COL_AUDIO_MESSAGE_QUEUE).doc(docId).get();
      } catch (e) {
        logger.error("processAudioMessageQueue: queue read failed", {
          docId,
          message: e.message,
        });
      }
    }
    if ((!snap || !snap.exists) && event.data && event.data.exists) {
      snap = event.data;
    }
    if (!snap || !snap.exists) {
      logger.error("processAudioMessageQueue: missing or empty document snapshot", {
        docId: docId || null,
      });
      return;
    }
    await processAudioMessageQueueDoc(snap);
  }
);

exports.inboundSms = onRequest(
  {
    region: "northamerica-northeast1",
    invoker: "public",
    timeoutSeconds: 120,
    memory: "512MiB",
    secrets: [
      TWILIO_ACCOUNT_SID,
      TWILIO_AUTH_TOKEN,
      TWILIO_PHONE_NUMBER,
      OPENAI_API_KEY,
    ],
  },
  async (req, res) => {
    const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    logger.info("inboundSms: request start", {
      runId,
      method: req.method,
      contentType: req.get("content-type") || "",
    });

    try {
      if (req.method === "GET") {
        res
          .status(200)
          .type("text/plain")
          .send(
            "OK: inboundSms is live. In Twilio Console, set this URL as the incoming message webhook (POST). " +
              "Phone number: Phone Numbers > Active number > Messaging configuration. " +
              "Or Messaging Service > Integration > Incoming messages. " +
              "After upgrading Twilio, update Firebase secrets if Account SID / Auth Token / phone number changed, then redeploy functions."
          );
        return;
      }
      if (req.method !== "POST") {
        logger.warn("inboundSms: rejected method", { runId, method: req.method });
        res.status(405).set("Allow", "GET, POST").send("Method Not Allowed");
        return;
      }

      const accountSid = normalizeAccountSid(TWILIO_ACCOUNT_SID.value());
      const authToken = normalizeAuthToken(TWILIO_AUTH_TOKEN.value());
      const configuredFrom = normalizePhoneE164(TWILIO_PHONE_NUMBER.value());
      const openaiKey = OPENAI_API_KEY.value();

      if (!accountSid || !authToken || !configuredFrom) {
        logger.error("inboundSms: missing Twilio secrets", { runId });
        res.status(500).send("Server misconfiguration");
        return;
      }

      const params = getTwilioParams(req);

      logInboundTwilioSecrets(logger, runId, {
        accountSid,
        authToken,
        configuredFrom,
        sampleMediaUrl: params.MediaUrl0 || "",
      });

      const sidProbe = maskSidParts(accountSid);
      if (!sidProbe.sidLooksValid) {
        logger.warn(
          "inboundSms: TWILIO_ACCOUNT_SID not valid AC+32hex after normalization — MMS download will use Account SID from media URL when present",
          { runId, sidLength: sidProbe.sidLength, sidStart: sidProbe.sidStart }
        );
      }

      if (shouldEnforceTwilioValidation()) {
        const v = validateTwilioOrExplained(req, authToken, params);
        if (!v.ok) {
          logger.error("inboundSms: Twilio signature failed", {
            runId,
            reason: v.reason,
            tried: v.tried,
          });
          res.status(403).send("Forbidden");
          return;
        }
        logger.info("inboundSms: Twilio signature ok", { runId, url: v.url });
      }

      // Incoming calls post CallSid; SMS/MMS post MessageSid. If the Voice webhook URL was set to this
      // SMS endpoint, Twilio receives <Message> TwiML and plays "an application error has occurred".
      if (
        String(params.CallSid || "").trim() &&
        !String(params.MessageSid || "").trim() &&
        !String(params.SmsStatus || "").trim()
      ) {
        const voiceUrl = siblingCloudFunctionUrl(req, "inboundVoice");
        logger.warn("inboundSms: voice webhook misrouted to SMS URL — redirecting to inboundVoice", {
          runId,
          voiceUrl,
        });
        const vr = new twilio.twiml.VoiceResponse();
        vr.redirect({ method: "POST" }, voiceUrl);
        res.set("Content-Type", "text/xml; charset=utf-8");
        res.status(200).send(vr.toString());
        return;
      }

      const from = normalizePhoneE164(String(params.From || "").trim()) || String(params.From || "").trim();
      const to = normalizePhoneE164(String(params.To || "").trim()) || String(params.To || "").trim();
      const messagingServiceSid = normalizeTwilioSecret(params.MessagingServiceSid || "");
      const inboundAccountSid = normalizeAccountSid(params.AccountSid || "");
      if (
        inboundAccountSid &&
        accountSid &&
        inboundAccountSid !== accountSid
      ) {
        logger.warn("inboundSms: webhook AccountSid does not match TWILIO_ACCOUNT_SID secret", {
          runId,
          inboundAccountSidStart: inboundAccountSid.slice(0, 6),
          inboundAccountSidEnd: inboundAccountSid.slice(-4),
          configuredAccountSidStart: accountSid.slice(0, 6),
          configuredAccountSidEnd: accountSid.slice(-4),
        });
      }
      const numMedia = parseInt(String(params.NumMedia || "0"), 10) || 0;
      const mediaCountEffective = countTwilioMediaParams(params);
      if (mediaCountEffective !== numMedia) {
        logger.warn("inboundSms: NumMedia vs MediaUrl* mismatch — using effective count", {
          runId,
          numMedia,
          mediaCountEffective,
        });
      }
      let body = params.Body || "";
      if (!body.trim() && mediaCountEffective > 0) {
        body = inferInboundAttachmentLabel(params, mediaCountEffective);
      }
      const allAudioMedia = allInboundMediaAreAudio(params, mediaCountEffective);
      let audioTranscript = null;
      if (mediaCountEffective > 0 && !allAudioMedia) {
        audioTranscript = await transcribeInboundTwilioAudio({
          params,
          mediaCountEffective,
          accountSid,
          authToken,
          openaiKey,
          logger,
          runId,
        });
      }
      if (audioTranscript && audioTranscript.transcript) {
        body = audioTranscript.transcript;
        logger.info("inboundSms: audio note transcribed", {
          runId,
          mediaIndex: audioTranscript.mediaIndex,
          transcriptLength: audioTranscript.transcript.length,
        });
      }
      const messageSid = params.MessageSid || "";

      if (!from) {
        logger.warn("inboundSms: missing From (check webhook URL and POST body)", {
          runId,
          paramKeys: Object.keys(params),
          hasRawBody: Boolean(req.rawBody && req.rawBody.length),
          contentType: req.get("content-type") || "",
        });
        sendTwiml(res, "Could not read sender. Reply from your mobile number.");
        return;
      }

      if (to && configuredFrom && to.trim() !== configuredFrom.trim()) {
        logger.warn("inboundSms: To does not match TWILIO_PHONE_NUMBER secret", {
          runId,
          to,
          configuredFrom,
        });
      }

      if (!checkRateLimit(from, logger, runId)) {
        sendTwiml(
          res,
          "You're sending messages too fast—wait a minute and try again."
        );
        return;
      }

      const bodyTrimmedForCommands = (params.Body || body || "").trim();
      if (
        !openaiKey &&
        !isDailyReportPdfRequest(bodyTrimmedForCommands)
      ) {
        logger.error("inboundSms: missing OPENAI_API_KEY", { runId });
        const errText =
          "Assistant is offline (missing AI config). Admin must set OPENAI_API_KEY secret.";
        const inboundRef = await db.collection("messages").add({
          direction: "inbound",
          from,
          to,
          body,
          messageSid,
          numMedia,
          mediaCountEffective,
          threadKey: from,
          phoneE164: from,
          channel: "sms",
          schemaVersion: MESSAGE_SCHEMA_VERSION,
          createdAt: FieldValue.serverTimestamp(),
        });
        await db.collection("messages").add({
          direction: "outbound",
          from: configuredFrom,
          to: from,
          body: errText,
          messageSid: null,
          delivery: "twiml",
          replyToInboundDocId: inboundRef.id,
          threadKey: from,
          phoneE164: from,
          channel: "sms",
          schemaVersion: MESSAGE_SCHEMA_VERSION,
          aiUsed: false,
          aiError: "OPENAI_API_KEY missing",
          command: "config_error",
          createdAt: FieldValue.serverTimestamp(),
        });

        if (mediaCountEffective > 0) {
          let issueIdCfg = null;
          let issueCollectionCfg = null;
          try {
            const ph = await createMmsPlaceholderIssue(db, FieldValue, {
              phoneE164: from,
              projectSlug: null,
              bodyText: body,
              rawSms: params.Body || "",
              relatedMessageId: inboundRef.id,
            });
            issueIdCfg = ph.issueId;
            issueCollectionCfg = ph.issueCollection;
          } catch (phErr) {
            logger.error(
              "inboundSms: placeholder issue failed (no openai path) — ingesting media without issue",
              { runId, message: phErr.message }
            );
          }
          try {
            logger.info("inboundSms: MMS target bucket", {
              runId,
              bucket: admin.storage().bucket().name,
            });
            const attachNoAi = await attachTwilioMediaToIssue({
              db,
              storage: admin.storage(),
              FieldValue,
              accountSid,
              authToken,
              params,
              issueCollection: issueCollectionCfg,
              issueId: issueIdCfg,
              uploadedByPhone: from,
              logger,
              runId,
              messageSidTwilio: messageSid,
              sourceMessageId: inboundRef.id,
              projectSlug: null,
              reportDateKey: null,
              captionText: params.Body || body || "",
              linkedLogEntryId: null,
            });
            await inboundRef.update({
              mediaIds: attachNoAi.mediaIds || [],
              mediaAttachedCount: attachNoAi.attached || 0,
              mediaSkippedUrls: attachNoAi.skippedUrls || 0,
              mediaProcessingAt: FieldValue.serverTimestamp(),
              photoPreviewUrls: (attachNoAi.photos || [])
                .map((p) => p.downloadURL)
                .filter(Boolean),
              photoStoragePaths: (attachNoAi.photos || [])
                .map((p) => p.storagePath)
                .filter(Boolean),
            });
          } catch (mmsErr) {
            logger.error("inboundSms: MMS attach failed (no openai path)", {
              runId,
              message: mmsErr.message,
              stack: mmsErr.stack,
            });
          }
        }

        sendTwiml(res, errText);
        return;
      }

      if (!openaiKey && isDailyReportPdfRequest(bodyTrimmedForCommands)) {
        logger.warn(
          "inboundSms: OPENAI_API_KEY missing — still handling daily PDF request (deterministic)",
          { runId }
        );
      }

      const pendingAudioReviewSnap = await db.collection(COL_USERS).doc(from).get();
      const pendingAudioReview = normalizePendingAudioReview(
        pendingAudioReviewSnap.exists ? pendingAudioReviewSnap.get("pendingAudioReview") : null
      );
      if (openaiKey && pendingAudioReview && mediaCountEffective === 0) {
        const reviewed = await processPendingAudioReviewReply({
          phoneE164: from,
          inboundBody: bodyTrimmedForCommands,
          inboundMessageSid: messageSid || null,
          configuredFrom,
          openaiKey,
          runId,
          pendingReview: pendingAudioReview,
        });
        if (reviewed) {
          sendTwiml(res, reviewed.replyText || "OK.");
          return;
        }
      }

      if (openaiKey && mediaCountEffective > 0 && allAudioMedia) {
        logger.info("inboundSms: audio MMS queued for background processing", {
          runId,
          from,
          to,
          messageSid,
          mediaCountEffective,
        });

        const inboundRef = await db.collection("messages").add({
          direction: "inbound",
          from,
          to,
          body: String(params.Body || "").trim() || "Audio note received. Processing now.",
          messageSid,
          numMedia,
          mediaCountEffective,
          threadKey: from,
          phoneE164: from,
          channel: "sms",
          schemaVersion: MESSAGE_SCHEMA_VERSION,
          audioTranscription: {
            transcript: null,
            mediaIndex: 0,
            contentType: String(params.MediaContentType0 || "").trim() || null,
            model: null,
            status: "queued",
          },
          createdAt: FieldValue.serverTimestamp(),
        });

        const firstAudio = firstAudioMediaFromParams(params, mediaCountEffective);
        await db.collection(COL_AUDIO_MESSAGE_QUEUE).add({
          status: "queued",
          from,
          to,
          messageSid: messageSid || null,
          mediaUrl: firstAudio && firstAudio.mediaUrl ? firstAudio.mediaUrl : null,
          mediaContentType: firstAudio && firstAudio.contentType ? firstAudio.contentType : null,
          mediaIndex: firstAudio && Number.isFinite(firstAudio.mediaIndex) ? firstAudio.mediaIndex : 0,
          inboundMessageId: inboundRef.id,
          rawCaption: String(params.Body || "").trim() || "",
          replyMessagingServiceSid: messagingServiceSid || null,
          runId,
          createdAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        });

        const ackText = "Received your audio note. Processing now and I'll text you back with the result.";
        await db.collection("messages").add({
          direction: "outbound",
          from: configuredFrom,
          to: from,
          body: ackText,
          messageSid: null,
          delivery: "twiml_audio_queue_ack",
          replyToInboundDocId: inboundRef.id,
          threadKey: from,
          phoneE164: from,
          channel: "sms",
          schemaVersion: MESSAGE_SCHEMA_VERSION,
          aiUsed: false,
          aiError: null,
          command: "audio_message_queued",
          createdAt: FieldValue.serverTimestamp(),
        });

        sendTwiml(res, ackText);
        return;
      }

      logger.info("inboundSms: parsed webhook", {
        runId,
        from,
        to,
        bodyPreview: (body || "").slice(0, 160),
        messageSid,
        numMedia,
        mediaCountEffective,
      });

      const inboundRef = await db.collection("messages").add({
        direction: "inbound",
        from,
        to,
        body,
        messageSid,
        numMedia,
        mediaCountEffective,
        threadKey: from,
        phoneE164: from,
        channel: "sms",
        schemaVersion: MESSAGE_SCHEMA_VERSION,
        audioTranscription: audioTranscript
          ? {
              transcript: audioTranscript.transcript,
              mediaIndex: audioTranscript.mediaIndex,
              contentType: audioTranscript.contentType,
            }
          : null,
        createdAt: FieldValue.serverTimestamp(),
      });

      let replyText;
      let outboundMeta = {};

      try {
        const out = await Promise.race([
          buildReply({
            db,
            openaiApiKey: openaiKey,
            logger,
            runId,
            from,
            body,
            relatedMessageId: inboundRef.id,
            numMedia: mediaCountEffective,
            channel: "sms",
            models: {
              primary: OPENAI_MODEL_PRIMARY.value(),
            },
          }),
          new Promise((_, reject) =>
            setTimeout(
              () => reject(new Error("__BUILD_REPLY_TIMEOUT__")),
              BUILD_REPLY_TIMEOUT_MS
            )
          ),
        ]);
        replyText = out.replyText;
        outboundMeta = out.outboundMeta || {};
        if (outboundMeta.routingDecision) {
          logger.info("inboundSms: routing telemetry", {
            runId,
            stage: outboundMeta.routingDecision.stage || null,
            action: outboundMeta.routingDecision.action || null,
            confidence: outboundMeta.routingDecision.confidence ?? null,
            reason: outboundMeta.routingDecision.reason || null,
            source: outboundMeta.routingDecision.source || null,
            matchedBy: outboundMeta.routingDecision.matchedBy || null,
            safeFallbackUsed: outboundMeta.routingDecision.safeFallbackUsed === true,
          });
        }
      } catch (handlerErr) {
        if (handlerErr && handlerErr.message === "__BUILD_REPLY_TIMEOUT__") {
          logger.warn("inboundSms: buildReply timed out", { runId });
          replyText =
            "That took too long to process. Try a shorter message or text help.";
          outboundMeta = {
            aiUsed: false,
            aiError: "timeout",
            command: "timeout",
          };
        } else {
          logger.error("inboundSms: buildReply threw", {
            runId,
            message: handlerErr.message,
            stack: handlerErr.stack,
          });
          replyText =
            "Something went wrong processing that. Try again or text help.";
          outboundMeta = {
            aiUsed: false,
            aiError: String(handlerErr.message),
            command: "handler_exception",
          };
        }
      }

      let safeReply = String(replyText || "").trim() || "OK.";
      let dailyPdfQueueRef = null;
      let labourPdfQueueRef = null;

      if (outboundMeta.dailyPdfRequested) {
        try {
          dailyPdfQueueRef = await db.collection("dailyPdfDeliveryQueue").add({
            phoneE164: from,
            projectSlug: outboundMeta.projectSlug || null,
            reportDateKey: outboundMeta.reportDateKey || null,
            reportType: outboundMeta.reportType || null,
            replyToNumber: to || null,
            replyMessagingServiceSid: messagingServiceSid || null,
            replyAccountSid: inboundAccountSid || null,
            runId,
            replyToInboundDocId: inboundRef.id,
            status: "queued",
            attemptCount: 0,
            lastError: null,
            createdAt: FieldValue.serverTimestamp(),
          });
          logger.info("inboundSms: daily PDF queued for SMS delivery", {
            runId,
            queueDocId: dailyPdfQueueRef.id,
          });
        } catch (queueErr) {
          logger.error("inboundSms: daily PDF queue failed", {
            runId,
            message: queueErr.message,
            stack: queueErr.stack,
          });
          safeReply = "Could not queue your daily PDF report. Try again in a minute.";
          outboundMeta = {
            ...outboundMeta,
            dailyPdfRequested: false,
            aiError: String(queueErr.message || queueErr),
            command: "daily_pdf_queue_failed",
          };
        }
      }

      if (outboundMeta.labourPdfRequested) {
        try {
          labourPdfQueueRef = await db.collection("labourPdfDeliveryQueue").add({
            phoneE164: from,
            startKey: outboundMeta.labourReportStartKey || null,
            endKey: outboundMeta.labourReportEndKey || null,
            replyToNumber: to || null,
            replyMessagingServiceSid: messagingServiceSid || null,
            replyAccountSid: inboundAccountSid || null,
            runId,
            replyToInboundDocId: inboundRef.id,
            status: "queued",
            attemptCount: 0,
            lastError: null,
            createdAt: FieldValue.serverTimestamp(),
          });
          logger.info("inboundSms: labour PDF queued for SMS delivery", {
            runId,
            queueDocId: labourPdfQueueRef.id,
          });
        } catch (queueErr) {
          logger.error("inboundSms: labour PDF queue failed", {
            runId,
            message: queueErr.message,
            stack: queueErr.stack,
          });
          safeReply = "Could not queue your labour report. Try again in a minute.";
          outboundMeta = {
            ...outboundMeta,
            labourPdfRequested: false,
            aiError: String(queueErr.message || queueErr),
            command: "labour_pdf_queue_failed",
          };
        }
      }

      // Twilio waits ~15s for this HTTP response — reply must be sent before that.
      sendTwiml(res, safeReply);

      try {
        // Queue PDF SMS first. Later steps (MMS attach, etc.) can throw — must not skip delivery.
        await inboundRef.update({
          projectSlug: outboundMeta.projectSlug || null,
          command: outboundMeta.command || null,
          aiUsed: Boolean(outboundMeta.aiUsed),
          aiError: outboundMeta.aiError || null,
          logEntryId: outboundMeta.logEntryId || null,
          logCategory: outboundMeta.logCategory || null,
          classification: outboundMeta.classification || null,
          dailyPdfRequested: Boolean(outboundMeta.dailyPdfRequested),
          dailyPdfQueueDocId: dailyPdfQueueRef ? dailyPdfQueueRef.id : null,
          labourPdfRequested: Boolean(outboundMeta.labourPdfRequested),
          labourPdfQueueDocId: labourPdfQueueRef ? labourPdfQueueRef.id : null,
          reportDateKey: outboundMeta.reportDateKey || null,
          reportType: outboundMeta.reportType || null,
          pendingDeficiencyIntake: Boolean(outboundMeta.pendingDeficiencyIntake),
          notifyAudience:
            outboundMeta.notifyRequest && outboundMeta.notifyRequest.audience
              ? String(outboundMeta.notifyRequest.audience)
              : null,
          notifyProjectSlug:
            outboundMeta.notifyRequest && outboundMeta.notifyRequest.projectSlug
              ? normalizeProjectSlug(outboundMeta.notifyRequest.projectSlug)
              : null,
          routingStage: outboundMeta.routingDecision ? outboundMeta.routingDecision.stage || null : null,
          routingAction: outboundMeta.routingDecision ? outboundMeta.routingDecision.action || null : null,
          routingConfidence:
            outboundMeta.routingDecision && Number.isFinite(Number(outboundMeta.routingDecision.confidence))
              ? Number(outboundMeta.routingDecision.confidence)
              : null,
          routingReason: outboundMeta.routingDecision ? outboundMeta.routingDecision.reason || null : null,
          routingSource: outboundMeta.routingDecision ? outboundMeta.routingDecision.source || null : null,
          routingMatchedBy: outboundMeta.routingDecision ? outboundMeta.routingDecision.matchedBy || null : null,
          routingSafeFallbackUsed:
            outboundMeta.routingDecision ? outboundMeta.routingDecision.safeFallbackUsed === true : false,
        });

        const outboundRef = await db.collection("messages").add({
          direction: "outbound",
          from: configuredFrom,
          to: from,
          body: safeReply,
          messageSid: null,
          delivery: "twiml",
          replyToInboundDocId: inboundRef.id,
          threadKey: from,
          phoneE164: from,
          channel: "sms",
          schemaVersion: MESSAGE_SCHEMA_VERSION,
          projectSlug: outboundMeta.projectSlug || null,
          aiUsed: Boolean(outboundMeta.aiUsed),
          aiError: outboundMeta.aiError || null,
          command: outboundMeta.command || null,
          issueLogId: outboundMeta.issueLogId || null,
          issueCollection: outboundMeta.issueCollection || null,
          summarySaved: Boolean(outboundMeta.summarySaved),
          logEntryId: outboundMeta.logEntryId || null,
          logCategory: outboundMeta.logCategory || null,
          classification: outboundMeta.classification || null,
          dailyPdfRequested: Boolean(outboundMeta.dailyPdfRequested),
          dailyPdfQueueDocId: dailyPdfQueueRef ? dailyPdfQueueRef.id : null,
          reportDateKey: outboundMeta.reportDateKey || null,
          reportType: outboundMeta.reportType || null,
          pendingDeficiencyIntake: Boolean(outboundMeta.pendingDeficiencyIntake),
          routingStage: outboundMeta.routingDecision ? outboundMeta.routingDecision.stage || null : null,
          routingAction: outboundMeta.routingDecision ? outboundMeta.routingDecision.action || null : null,
          routingConfidence:
            outboundMeta.routingDecision && Number.isFinite(Number(outboundMeta.routingDecision.confidence))
              ? Number(outboundMeta.routingDecision.confidence)
              : null,
          routingReason: outboundMeta.routingDecision ? outboundMeta.routingDecision.reason || null : null,
          routingSource: outboundMeta.routingDecision ? outboundMeta.routingDecision.source || null : null,
          routingMatchedBy: outboundMeta.routingDecision ? outboundMeta.routingDecision.matchedBy || null : null,
          routingSafeFallbackUsed:
            outboundMeta.routingDecision ? outboundMeta.routingDecision.safeFallbackUsed === true : false,
          notifyAudience:
            outboundMeta.notifyRequest && outboundMeta.notifyRequest.audience
              ? String(outboundMeta.notifyRequest.audience)
              : null,
          notifyProjectSlug:
            outboundMeta.notifyRequest && outboundMeta.notifyRequest.projectSlug
              ? normalizeProjectSlug(outboundMeta.notifyRequest.projectSlug)
              : null,
          createdAt: FieldValue.serverTimestamp(),
        });

        if (outboundMeta.notifyRequest && outboundMeta.notifyRequest.messageBody) {
          try {
            const notifyProjectSlug = normalizeProjectSlug(outboundMeta.notifyRequest.projectSlug || "");
            const recipients = await resolveNotificationRecipients(db, {
              audience: outboundMeta.notifyRequest.audience,
              projectSlug: notifyProjectSlug,
            });
            const smsClient = twilio(accountSid, authToken);
            const fanout = await sendSmsNotificationFanout({
              db,
              smsClient,
              accountSid,
              fromPhone: configuredFrom,
              messagingServiceSid: messagingServiceSid || null,
              requestedByPhone: from,
              requestedByName: outboundMeta.notifyRequest.requestedByName || null,
              requestedByEmail: outboundMeta.notifyRequest.requestedByEmail || null,
              projectSlug: notifyProjectSlug || null,
              messageBody: outboundMeta.notifyRequest.messageBody,
              recipients,
              runId,
            });
            await outboundRef.update({
              notifyFanout: {
                audience: String(outboundMeta.notifyRequest.audience || ""),
                projectSlug: notifyProjectSlug || null,
                attemptedCount: fanout.attemptedCount,
                sentCount: fanout.sentCount,
                failedCount: fanout.failedCount,
                skippedSelfCount: fanout.skippedSelfCount || 0,
              },
            });
            const fanoutSummaryText =
              fanout.attemptedCount === 0
                ? "Notification result: no recipients matched for this audience/project."
                : fanout.failedCount > 0
                  ? `Notification result: sent ${fanout.sentCount}/${fanout.attemptedCount} (${fanout.failedCount} failed).`
                  : `Notification result: sent ${fanout.sentCount}/${fanout.attemptedCount}.`;
            try {
              const requesterPayload = { to: from, body: fanoutSummaryText };
              if (messagingServiceSid) requesterPayload.messagingServiceSid = messagingServiceSid;
              else requesterPayload.from = configuredFrom;
              const requesterMsg = await twilio(accountSid, authToken).messages.create(requesterPayload);
              await db.collection("messages").add({
                direction: "outbound",
                from: configuredFrom,
                to: from,
                body: fanoutSummaryText,
                messageSid: requesterMsg.sid || null,
                delivery: "twilio_api_notification_status",
                threadKey: from,
                phoneE164: from,
                channel: "sms",
                schemaVersion: MESSAGE_SCHEMA_VERSION,
                command: "notify_fanout_status",
                projectSlug: notifyProjectSlug || null,
                createdAt: FieldValue.serverTimestamp(),
              });
            } catch (statusErr) {
              logger.warn("inboundSms: failed to send notification status SMS", {
                runId,
                message: statusErr.message,
              });
            }
            logger.info("inboundSms: notification fanout complete", {
              runId,
              audience: outboundMeta.notifyRequest.audience,
              projectSlug: notifyProjectSlug || null,
              attemptedCount: fanout.attemptedCount,
              sentCount: fanout.sentCount,
              failedCount: fanout.failedCount,
              skippedSelfCount: fanout.skippedSelfCount || 0,
              failedSample: (fanout.failed || []).slice(0, 3),
            });
          } catch (notifyErr) {
            logger.error("inboundSms: notification fanout failed", {
              runId,
              message: notifyErr.message,
              stack: notifyErr.stack,
            });
            await outboundRef.update({
              notifyFanout: {
                audience: String(outboundMeta.notifyRequest.audience || ""),
                projectSlug: normalizeProjectSlug(outboundMeta.notifyRequest.projectSlug || "") || null,
                attemptedCount: 0,
                sentCount: 0,
                failedCount: 1,
                error: String(notifyErr.message || notifyErr).slice(0, 180),
              },
            });
            try {
              const failureText = "Notification failed to send. Please retry or check recipient setup.";
              const requesterPayload = { to: from, body: failureText };
              if (messagingServiceSid) requesterPayload.messagingServiceSid = messagingServiceSid;
              else requesterPayload.from = configuredFrom;
              await twilio(accountSid, authToken).messages.create(requesterPayload);
            } catch (_) {}
          }
        }

        let issueId = outboundMeta.issueLogId || null;
        let issueCollection = outboundMeta.issueCollection || null;
        let attachResult = null;

        if (
          mediaCountEffective > 0 &&
          !issueId &&
          !outboundMeta.pendingDeficiencyIntake &&
          !outboundMeta.logEntryId
        ) {
          try {
            const ph = await createMmsPlaceholderIssue(db, FieldValue, {
              phoneE164: from,
              projectSlug: outboundMeta.projectSlug || null,
              bodyText: body,
              rawSms: params.Body || "",
              relatedMessageId: inboundRef.id,
            });
            issueId = ph.issueId;
            issueCollection = ph.issueCollection;
            await outboundRef.update({
              issueLogId: issueId,
              issueCollection,
              command: outboundMeta.command || "mms_placeholder",
            });
          } catch (phErr) {
            logger.error("inboundSms: createMmsPlaceholderIssue failed — media will still be ingested without issue", {
              runId,
              message: phErr.message,
              stack: phErr.stack,
            });
          }
        } else if (mediaCountEffective > 0 && !issueId && outboundMeta.pendingDeficiencyIntake) {
          logger.info("inboundSms: skipping MMS placeholder while deficiency intake is active", {
            runId,
            replyToInboundDocId: inboundRef.id,
          });
        }

        if (mediaCountEffective > 0) {
          logger.info("inboundSms: MMS target bucket", {
            runId,
            bucket: admin.storage().bucket().name,
          });
          attachResult = await attachTwilioMediaToIssue({
            db,
            storage: admin.storage(),
            FieldValue,
            accountSid,
            authToken,
            params,
            issueCollection,
            issueId,
            uploadedByPhone: from,
            logger,
            runId,
            messageSidTwilio: messageSid,
            sourceMessageId: inboundRef.id,
            projectSlug: outboundMeta.projectSlug || null,
            reportDateKey: outboundMeta.reportDateKey || null,
            captionText: params.Body || body || "",
            linkedLogEntryId: outboundMeta.logEntryId || null,
          });
          try {
            await inboundRef.update({
              mediaIds: attachResult.mediaIds || [],
              mediaAttachedCount: attachResult.attached || 0,
              mediaSkippedUrls: attachResult.skippedUrls || 0,
              mediaProcessingAt: FieldValue.serverTimestamp(),
              photoPreviewUrls: (attachResult.photos || [])
                .map((p) => p.downloadURL)
                .filter(Boolean),
              photoStoragePaths: (attachResult.photos || [])
                .map((p) => p.storagePath)
                .filter(Boolean),
            });
          } catch (upErr) {
            logger.warn("inboundSms: inbound mediaIds update failed", {
              runId,
              message: upErr.message,
            });
          }
          if (openaiKey && issueId && issueCollection) {
            await maybeCaptionFirstMmsPhoto({
              db,
              issueCollection,
              issueId,
              openaiApiKey: openaiKey,
              logger,
              runId,
              modelsOverride: {
                primary: OPENAI_MODEL_PRIMARY.value(),
              },
            });
          }
        }

        if (mediaCountEffective > 0 && outboundMeta.logEntryId && attachResult) {
          try {
            const pathsFromAttach = (attachResult.photos || [])
              .map((p) => p.storagePath)
              .filter(Boolean);
            let pathsFromIssue = [];
            if (issueId && issueCollection) {
              const issueRef = db.collection(issueCollection).doc(issueId);
              const isnap = await issueRef.get();
              const photos = (isnap.data() || {}).photos || [];
              pathsFromIssue = photos.map((p) => p.storagePath).filter(Boolean);
            }
            const paths = [...new Set([...pathsFromIssue, ...pathsFromAttach])];
            if (paths.length) {
              await appendLinkedMediaIds(
                db,
                FieldValue,
                outboundMeta.logEntryId,
                paths
              );
            }
          } catch (linkErr) {
            logger.warn("inboundSms: logEntry photo link failed", {
              runId,
              message: linkErr.message,
            });
          }
        }

        if (
          outboundMeta.enhanceLogEntry &&
          outboundMeta.logEntryId &&
          openaiKey
        ) {
          void maybeEnhanceLogEntry({
            db,
            openaiApiKey: openaiKey,
            logEntryId: outboundMeta.logEntryId,
            logger,
            runId,
            modelsOverride: {
              primary: OPENAI_MODEL_PRIMARY.value(),
            },
          });
        }

        logger.info("inboundSms: complete", {
          runId,
          outboundDocId: outboundRef.id,
          command: outboundMeta.command,
          aiUsed: outboundMeta.aiUsed,
          numMedia,
          mediaCountEffective,
        });
      } catch (postTwiErr) {
        logger.error("inboundSms: post-TwiML work failed (reply was already sent)", {
          runId,
          message: postTwiErr.message,
          stack: postTwiErr.stack,
        });
      }
    } catch (error) {
      logger.error("inboundSms: error", {
        runId,
        message: error && error.message,
        stack: error && error.stack,
      });
      if (!res.headersSent) {
        try {
          sendTwiml(
            res,
            "Temporary error on our side. Try again in a minute."
          );
        } catch (_) {
          res.status(500).send("Internal Server Error");
        }
      }
    }
  }
);

/** Twilio Voice webhook: speech-in, spoken AI reply out. */
exports.inboundVoice = onRequest(
  {
    region: "northamerica-northeast1",
    invoker: "public",
    timeoutSeconds: 120,
    memory: "512MiB",
    secrets: [
      TWILIO_ACCOUNT_SID,
      TWILIO_AUTH_TOKEN,
      TWILIO_PHONE_NUMBER,
      OPENAI_API_KEY,
    ],
  },
  async (req, res) => {
    const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    try {
      if (req.method === "OPTIONS") {
        res.set("Access-Control-Allow-Origin", "*");
        res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        res.set("Access-Control-Allow-Headers", "Content-Type");
        res.status(204).send("");
        return;
      }
      if (req.method === "GET") {
        res
          .status(200)
          .type("text/plain")
          .send(
            "OK: inboundVoice is live. In Twilio Console → Phone Numbers → your number → Voice configuration: " +
              "set A CALL COMES IN to HTTP POST with this function URL (same host as your SMS webhook). " +
              "When someone calls, they should hear the assistant speak first, then can talk or press 1 for voicemail."
          );
        return;
      }
      if (req.method !== "POST") {
        res.status(405).set("Allow", "GET, POST, OPTIONS").send("Method Not Allowed");
        return;
      }

      const accountSid = normalizeAccountSid(TWILIO_ACCOUNT_SID.value());
      const authToken = normalizeAuthToken(TWILIO_AUTH_TOKEN.value());
      const configuredFrom = normalizePhoneE164(TWILIO_PHONE_NUMBER.value());
      const openaiKey = OPENAI_API_KEY.value();
      if (!accountSid || !authToken || !configuredFrom) {
        sendVoiceTwiml(res, (vr) => {
          sayVoiceAssistant(vr, "Voice assistant is misconfigured. Please contact admin.");
        });
        return;
      }

      const params = getTwilioParams(req);
      if (shouldEnforceTwilioValidation()) {
        const v = validateTwilioOrExplained(req, authToken, params);
        if (!v.ok) {
          logger.error("inboundVoice: Twilio signature failed", {
            runId,
            reason: v.reason,
            tried: v.tried,
          });
          res.status(403).send("Forbidden");
          return;
        }
      }

      const from = normalizePhoneE164(String(params.From || "").trim()) || String(params.From || "").trim();
      const to = normalizePhoneE164(String(params.To || "").trim()) || String(params.To || "").trim();
      const speechResult = sanitizeVoiceText(params.SpeechResult || "");
      const recordingUrl = String(params.RecordingUrl || "").trim();
      const recordingDuration = String(params.RecordingDuration || "").trim();
      const recordingContentType = String(params.RecordingContentType || "audio/mpeg").trim();
      const callSid = String(params.CallSid || "").trim();
      const digits = String(params.Digits || "").trim();
      const baseActionUrl = stripQueryFromUrl(buildWebhookUrls(req)[0]);
      const actionUrl = baseActionUrl;
      const recordActionUrl = `${baseActionUrl}?mode=recorded`;
      const mode = String((req.query && req.query.mode) || "").trim().toLowerCase();
      if (!from) {
        sendVoiceTwiml(res, (vr) => {
          sayVoiceAssistant(vr, "I could not read the caller number.");
          vr.hangup();
        });
        return;
      }

      if (!openaiKey) {
        sendVoiceTwiml(res, (vr) => {
          sayVoiceAssistant(vr, "Assistant is offline due to missing AI configuration.");
          vr.hangup();
        });
        return;
      }

      if (mode === "recorded" && recordingUrl) {
        const inboundRef = await db.collection("messages").add({
          direction: "inbound",
          from,
          to,
          body: "Voice recording received. Processing now.",
          messageSid: callSid || null,
          numMedia: 1,
          mediaCountEffective: 1,
          threadKey: from,
          phoneE164: from,
          channel: "voice",
          schemaVersion: MESSAGE_SCHEMA_VERSION,
          audioTranscription: {
            transcript: null,
            mediaIndex: 0,
            contentType: recordingContentType || null,
            model: null,
            status: "queued",
          },
          createdAt: FieldValue.serverTimestamp(),
        });
        await db.collection(COL_VOICE_MESSAGE_QUEUE).add({
          status: "queued",
          from,
          to,
          callSid: callSid || null,
          recordingUrl,
          recordingDuration: recordingDuration || null,
          recordingContentType: recordingContentType || "audio/mpeg",
          inboundMessageId: inboundRef.id,
          replyMessagingServiceSid:
            normalizeTwilioSecret(params.MessagingServiceSid || "") || null,
          runId,
          createdAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        });

        sendVoiceTwiml(res, (vr) => {
          sayVoiceAssistant(
            vr,
            "I got your voice message and I am processing it now. I will text you back with the result."
          );
          vr.hangup();
        });
        return;
      }

      if (digits === "1") {
        sendVoiceTwiml(res, (vr) => {
          sayVoiceAssistant(
            vr,
            "Please leave your voice message after the tone. Press pound when you are done."
          );
          vr.record({
            action: recordActionUrl,
            method: "POST",
            maxLength: 120,
            playBeep: true,
            finishOnKey: "#",
            trim: "do-not-trim",
          });
          sayVoiceAssistant(vr, "No recording was received. Goodbye.");
          vr.hangup();
        });
        return;
      }

      if (!speechResult) {
        sendVoiceTwiml(res, (vr) => {
          sayVoiceAssistant(
            vr,
            "Hi, this is your Gridline construction assistant. I can help with site logs, journal notes, daily reports, or quick questions."
          );
          const gather = vr.gather({
            input: "speech dtmf",
            speechTimeout: "auto",
            numDigits: 1,
            method: "POST",
            action: actionUrl,
          });
          sayVoiceAssistant(
            gather,
            "Go ahead and speak after the tone. Or press 1 to leave a recorded voice message for the team."
          );
          sayVoiceAssistant(vr, "I did not catch that. Please call again when you are ready.");
          vr.hangup();
        });
        return;
      }

      const inboundRef = await db.collection("messages").add({
        direction: "inbound",
        from,
        to,
        body: speechResult,
        messageSid: params.CallSid || null,
        numMedia: 0,
        mediaCountEffective: 0,
        threadKey: from,
        phoneE164: from,
        channel: "voice",
        schemaVersion: MESSAGE_SCHEMA_VERSION,
        createdAt: FieldValue.serverTimestamp(),
      });

      let out;
      try {
        out = await buildReply({
          db,
          openaiApiKey: openaiKey,
          logger,
          runId,
          from,
          body: speechResult,
          relatedMessageId: inboundRef.id,
          numMedia: 0,
          channel: "voice_live",
          models: { primary: OPENAI_MODEL_PRIMARY.value() },
        });
      } catch (err) {
        logger.error("inboundVoice: buildReply failed", { runId, message: err.message });
        out = {
          replyText: "I hit an error processing that. Please try again.",
          outboundMeta: { aiUsed: false, aiError: String(err.message || err), command: "voice_error" },
        };
      }

      const safeReply = sanitizeVoiceText(out.replyText || "OK");
      const outboundMeta = out.outboundMeta || {};
      await inboundRef.update({
        projectSlug: outboundMeta.projectSlug || null,
        command: outboundMeta.command || null,
        aiUsed: Boolean(outboundMeta.aiUsed),
        aiError: outboundMeta.aiError || null,
      });
      await db.collection("messages").add({
        direction: "outbound",
        from: configuredFrom,
        to: from,
        body: safeReply,
        messageSid: null,
        delivery: "twiml_voice",
        replyToInboundDocId: inboundRef.id,
        threadKey: from,
        phoneE164: from,
        channel: "voice",
        schemaVersion: MESSAGE_SCHEMA_VERSION,
        projectSlug: outboundMeta.projectSlug || null,
        aiUsed: Boolean(outboundMeta.aiUsed),
        aiError: outboundMeta.aiError || null,
        command: outboundMeta.command || "voice_ai",
        createdAt: FieldValue.serverTimestamp(),
      });

      sendVoiceTwiml(res, (vr) => {
        sayVoiceAssistant(vr, safeReply);
        const gather = vr.gather({
          input: "speech",
          speechTimeout: "auto",
          method: "POST",
          action: actionUrl,
        });
        sayVoiceAssistant(gather, "If you need anything else, speak now. Or hang up to end the call.");
      });
    } catch (error) {
      logger.error("inboundVoice: error", { runId, message: error.message, stack: error.stack });
      if (!res.headersSent) {
        sendVoiceTwiml(res, (vr) => {
          sayVoiceAssistant(vr, "Temporary error on our side. Please try again.");
          vr.hangup();
        });
      }
    }
  }
);

exports.issueExportCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const access = await getOperatorAccess(db, request, { minimumRole: "management" });
    let projectSlugs = normalizeProjectSlugList(
      Array.isArray(access.projectSlugs) ? access.projectSlugs : []
    );
    const approvedPhoneRaw =
      access.memberData && access.memberData.approvedPhoneE164 != null
        ? String(access.memberData.approvedPhoneE164).trim()
        : "";
    const approvedPhoneE164 = approvedPhoneRaw ? normalizePhoneE164(approvedPhoneRaw) : null;
    if (approvedPhoneE164 && access.via === "app-member" && !access.allProjects) {
      try {
        const smsSnap = await db.collection("smsUsers").doc(approvedPhoneE164).get();
        if (smsSnap.exists) {
          const u = smsSnap.data() || {};
          projectSlugs = normalizeProjectSlugList([
            ...projectSlugs,
            ...(Array.isArray(u.projectSlugs) ? u.projectSlugs : []),
            u.activeProjectSlug,
          ]);
        }
      } catch (_) {}
    }
    return runIssueExport({ db, request, access, projectSlugs });
  }
);

exports.createIssueCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    await assertIssueWriteProjectAccess(operator, request.data?.projectId);
    try {
      return await createDashboardIssue(db, FieldValue, {
        operator: {
          uid: request.auth && request.auth.uid ? request.auth.uid : null,
          email: operator.email || null,
        },
        ...request.data,
      });
    } catch (err) {
      logger.error("createIssueCallable: failed", {
        operator: operator.email || null,
        message: err.message,
        stack: err.stack,
      });
      throw err instanceof HttpsError
        ? err
        : new HttpsError("invalid-argument", err.message || "Issue creation failed.");
    }
  }
);

exports.updateIssueCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    await assertIssueWriteRecordAccess(
      operator,
      request.data?.issueCollection,
      request.data?.issueId
    );
    try {
      return await updateDashboardIssue(db, FieldValue, {
        operator: {
          uid: request.auth && request.auth.uid ? request.auth.uid : null,
          email: operator.email || null,
        },
        ...request.data,
      });
    } catch (err) {
      logger.error("updateIssueCallable: failed", {
        operator: operator.email || null,
        message: err.message,
        stack: err.stack,
      });
      const code = /was not found/i.test(String(err.message || "")) ? "not-found" : "invalid-argument";
      throw err instanceof HttpsError
        ? err
        : new HttpsError(code, err.message || "Issue update failed.");
    }
  }
);

exports.addIssueNoteCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    await assertIssueWriteRecordAccess(
      operator,
      request.data?.issueCollection,
      request.data?.issueId
    );
    try {
      return await addDashboardIssueNote(db, FieldValue, {
        operator: {
          uid: request.auth && request.auth.uid ? request.auth.uid : null,
          email: operator.email || null,
        },
        ...request.data,
      });
    } catch (err) {
      logger.error("addIssueNoteCallable: failed", {
        operator: operator.email || null,
        message: err.message,
        stack: err.stack,
      });
      const code = /was not found/i.test(String(err.message || "")) ? "not-found" : "invalid-argument";
      throw err instanceof HttpsError
        ? err
        : new HttpsError(code, err.message || "Issue note failed.");
    }
  }
);

exports.attachIssuePhotoCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    await assertIssueWriteRecordAccess(
      operator,
      request.data?.issueCollection,
      request.data?.issueId
    );
    try {
      return await attachDashboardIssuePhoto(db, FieldValue, {
        operator: {
          uid: request.auth && request.auth.uid ? request.auth.uid : null,
          email: operator.email || null,
        },
        ...request.data,
      });
    } catch (err) {
      logger.error("attachIssuePhotoCallable: failed", {
        operator: operator.email || null,
        message: err.message,
        stack: err.stack,
      });
      const code = /was not found/i.test(String(err.message || "")) ? "not-found" : "invalid-argument";
      throw err instanceof HttpsError
        ? err
        : new HttpsError(code, err.message || "Issue photo attach failed.");
    }
  }
);

exports.deleteIssueCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    await assertIssueWriteRecordAccess(
      operator,
      request.data?.issueCollection,
      request.data?.issueId
    );
    try {
      return await deleteDashboardIssue(db, {
        issueCollection: request.data?.issueCollection,
        issueId: request.data?.issueId,
      });
    } catch (err) {
      logger.error("deleteIssueCallable: failed", {
        operator: operator.email || null,
        message: err.message,
        stack: err.stack,
      });
      const code = /was not found/i.test(String(err.message || "")) ? "not-found" : "invalid-argument";
      throw err instanceof HttpsError
        ? err
        : new HttpsError(code, err.message || "Issue delete failed.");
    }
  }
);

exports.sendAssistantMessageCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
    secrets: [OPENAI_API_KEY],
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    assertManagementAccess(access);
    assertDashboardToken(request);

    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    const body = String(request.data?.body || "").trim();
    const uploadedMedia = Array.isArray(request.data?.uploadedMedia)
      ? request.data.uploadedMedia
          .map((item) => ({
            storagePath: String(item?.storagePath || "").trim(),
            contentType: String(item?.contentType || "application/octet-stream").trim(),
            fileName: String(item?.fileName || "").trim(),
          }))
          .filter((item) => item.storagePath)
      : [];
    if (!phoneE164) {
      throw new HttpsError("invalid-argument", "phoneE164 is required.");
    }
    if (!body) {
      throw new HttpsError("invalid-argument", "body is required.");
    }

    await assertAccessibleSmsUserForAccess(access, phoneE164);

    const runId = `dash-msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const result = await processAssistantMessage({
      phoneE164,
      body,
      channel: "dashboard",
      replyFrom: "dashboard",
      openaiKey: OPENAI_API_KEY.value() || null,
      runId,
      uploadedMedia,
      uploadedBy: access.email || null,
    }).catch((err) => {
      logger.error("sendAssistantMessageCallable: failed", {
        runId,
        phoneE164,
        message: err.message,
        stack: err.stack,
      });
      throw err instanceof HttpsError
        ? err
        : new HttpsError("internal", err.message || "Assistant message failed.");
    });

    return {
      ok: true,
      replyText: result.replyText,
      command: result.outboundMeta.command || null,
      reportDateKey: result.outboundMeta.reportDateKey || null,
      logEntryId: result.outboundMeta.logEntryId || null,
      outboundMessageId: result.outboundRef.id,
      inboundMessageId: result.inboundRef.id,
      uploadedMediaCount: Array.isArray(result.uploadedMedia) ? result.uploadedMedia.length : 0,
      pdfDownloadURL: result.pdfResult && result.pdfResult.downloadURL ? result.pdfResult.downloadURL : null,
      pdfStoragePath: result.pdfResult && result.pdfResult.storagePath ? result.pdfResult.storagePath : null,
    };
  }
);

exports.transcribeAssistantAudioCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
    secrets: [OPENAI_API_KEY],
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    assertManagementAccess(access);
    assertDashboardToken(request);

    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    const storagePath = String(request.data?.storagePath || "").trim();
    const declaredContentType = String(request.data?.contentType || "audio/mpeg").trim();
    const fileName = String(request.data?.fileName || "voice-note").trim() || "voice-note";
    if (!phoneE164) {
      throw new HttpsError("invalid-argument", "phoneE164 is required.");
    }
    if (!storagePath) {
      throw new HttpsError("invalid-argument", "storagePath is required.");
    }
    await assertAccessibleSmsUserForAccess(access, phoneE164);

    const bucket = admin.storage().bucket();
    const file = bucket.file(storagePath);
    const [exists] = await file.exists();
    if (!exists) {
      throw new HttpsError("not-found", "Uploaded audio file was not found.");
    }
    const [buffer] = await file.download();
    if (!buffer || !buffer.length) {
      throw new HttpsError("failed-precondition", "Uploaded audio file is empty.");
    }
    const [metadata] = await file.getMetadata().catch(() => [{}]);
    const resolvedMime = resolveTranscriptionAudioMime(
      buffer,
      metadata?.contentType || declaredContentType,
      declaredContentType
    );
    const ext = guessExtension(resolvedMime);
    const transcript = await transcribeAudioBufferWithFallback({
      buffer,
      fileName: /\.[a-z0-9]+$/i.test(fileName) ? fileName : `${fileName}.${ext || "bin"}`,
      contentType: resolvedMime,
      openaiKey: OPENAI_API_KEY.value(),
      logger,
      runId: `dash-audio-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      contextLabel: "dashboardAudio",
    });
    const text = String(transcript && transcript.transcript || "").trim();
    if (!text) {
      throw new HttpsError("internal", "Could not transcribe the uploaded audio clearly.");
    }
    return {
      ok: true,
      transcript: text,
      model: transcript && transcript.model ? transcript.model : null,
      contentType: resolvedMime,
    };
  }
);

exports.sendAssistantMessageHttp = onRequest(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
    secrets: [OPENAI_API_KEY],
  },
  async (req, res) => {
    setJsonCors(res);
    if (req.method === "OPTIONS") {
      res.status(204).send("");
      return;
    }
    if (req.method !== "POST") {
      res.status(405).json({
        ok: false,
        error: "method-not-allowed",
        message: "Use POST with a JSON body.",
      });
      return;
    }

    const tokenFromHeader = req.get("X-Dashboard-Token") || req.get("x-dashboard-token") || "";
    const tokenFromBody =
      req.body && typeof req.body === "object" && req.body.token != null
        ? String(req.body.token)
        : "";

    try {
      assertDashboardTokenValue(tokenFromHeader || tokenFromBody);

      const phoneE164 = normalizePhoneE164(String(req.body?.phoneE164 || "").trim());
      let body = String(req.body?.body || "").trim();
      const projectSlug = normalizeProjectSlug(String(req.body?.projectSlug || "").trim());
      const uploadedMedia = Array.isArray(req.body?.uploadedMedia)
        ? req.body.uploadedMedia
            .map((item) => ({
              storagePath: String(item?.storagePath || "").trim(),
              contentType: String(item?.contentType || "application/octet-stream").trim(),
              fileName: String(item?.fileName || "").trim(),
            }))
            .filter((item) => item.storagePath)
        : [];

      if (!phoneE164) {
        throw new HttpsError("invalid-argument", "phoneE164 is required.");
      }
      if (!body) {
        throw new HttpsError("invalid-argument", "body is required.");
      }
      if (projectSlug) {
        body = `project ${projectSlug} ${body}`;
      }

      const userAccess = await getUserProjectAccess(db, phoneE164);
      if (!userAccess.exists) {
        throw new HttpsError(
          "not-found",
          "No smsUsers document for this phone. Text the Twilio number once first."
        );
      }

      const runId = `http-msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const result = await processAssistantMessage({
        phoneE164,
        body,
        channel: "terminal",
        replyFrom: "terminal",
        openaiKey: OPENAI_API_KEY.value() || null,
        runId,
        uploadedMedia,
        uploadedBy: "terminal",
      });

      res.status(200).json({
        ok: true,
        replyText: result.replyText,
        command: result.outboundMeta.command || null,
        projectSlug: result.outboundMeta.projectSlug || null,
        reportDateKey: result.outboundMeta.reportDateKey || null,
        logEntryId: result.outboundMeta.logEntryId || null,
        outboundMessageId: result.outboundRef.id,
        inboundMessageId: result.inboundRef.id,
        uploadedMediaCount: Array.isArray(result.uploadedMedia) ? result.uploadedMedia.length : 0,
        pdfDownloadURL:
          result.pdfResult && result.pdfResult.downloadURL ? result.pdfResult.downloadURL : null,
        pdfStoragePath:
          result.pdfResult && result.pdfResult.storagePath ? result.pdfResult.storagePath : null,
      });
    } catch (err) {
      const code =
        err instanceof HttpsError
          ? err.code
          : /not found/i.test(String(err.message || ""))
            ? "not-found"
            : "internal";
      const status =
        code === "permission-denied"
          ? 403
          : code === "invalid-argument"
            ? 400
            : code === "not-found"
              ? 404
              : 500;
      logger.error("sendAssistantMessageHttp: failed", {
        message: err.message,
        stack: err.stack,
      });
      res.status(status).json({
        ok: false,
        error: code,
        message: err.message || "Assistant message failed.",
      });
    }
  }
);

exports.sendAssistantVoiceCallCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
    secrets: [TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER],
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    assertManagementAccess(access);
    const toPhoneE164 = normalizePhoneE164(String(request.data?.toPhoneE164 || "").trim());
    const message = sanitizeVoiceText(String(request.data?.message || "").trim());
    if (!toPhoneE164) throw new HttpsError("invalid-argument", "toPhoneE164 is required.");
    if (!message) throw new HttpsError("invalid-argument", "message is required.");

    const accountSid = normalizeAccountSid(TWILIO_ACCOUNT_SID.value());
    const authToken = normalizeAuthToken(TWILIO_AUTH_TOKEN.value());
    const fromPhone = normalizePhoneE164(TWILIO_PHONE_NUMBER.value());
    if (!accountSid || !authToken || !fromPhone) {
      throw new HttpsError("failed-precondition", "Twilio voice secrets are not configured.");
    }

    const client = twilio(accountSid, authToken);
    const twiml = `<Response><Say voice="alice">${message}</Say></Response>`;
    const call = await client.calls.create({
      to: toPhoneE164,
      from: fromPhone,
      twiml,
    });

    await db.collection("messages").add({
      direction: "outbound",
      from: fromPhone,
      to: toPhoneE164,
      body: message,
      messageSid: call.sid || null,
      delivery: "twilio_voice_api",
      threadKey: toPhoneE164,
      phoneE164: toPhoneE164,
      channel: "voice",
      schemaVersion: MESSAGE_SCHEMA_VERSION,
      command: "voice_call_outbound",
      createdAt: FieldValue.serverTimestamp(),
      requestedByEmail: access.email || null,
    });

    return {
      ok: true,
      toPhoneE164,
      callSid: call.sid || null,
      status: call.status || null,
    };
  }
);

exports.getDashboardAccessCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    const rawProjectSlugs = Array.isArray(access?.memberData?.projectSlugs)
      ? access.memberData.projectSlugs
      : [];
    const approvedPhoneRaw =
      access.memberData && access.memberData.approvedPhoneE164 != null
        ? String(access.memberData.approvedPhoneE164).trim()
        : "";
    let approvedPhoneE164 = approvedPhoneRaw ? normalizePhoneE164(approvedPhoneRaw) : null;

    // Operator / legacy-admin paths skip memberData on getAppAccess, but the same user may still have
    // an appMembers row with an approved field phone — expose it so the dashboard can scope SMS UI.
    if (!approvedPhoneE164 && access.email) {
      try {
        const memberSnap = await db.collection(COL_APP_MEMBERS).doc(normalizeEmail(access.email)).get();
        if (memberSnap.exists) {
          const md = memberSnap.data() || {};
          if (md.active !== false && md.approvedPhoneE164 != null) {
            const raw = String(md.approvedPhoneE164).trim();
            if (raw) approvedPhoneE164 = normalizePhoneE164(raw);
          }
        }
      } catch (memberErr) {
        logger.warn("getDashboardAccessCallable: appMembers approved phone lookup failed", {
          message: memberErr.message,
        });
      }
    }

    let projectSlugs = normalizeProjectSlugList(
      Array.isArray(access.projectSlugs) ? access.projectSlugs : []
    );
    if (approvedPhoneE164 && access.via === "app-member" && !access.allProjects) {
      try {
        const smsSnap = await db.collection("smsUsers").doc(approvedPhoneE164).get();
        if (smsSnap.exists) {
          const u = smsSnap.data() || {};
          const fromPhone = [
            ...(Array.isArray(u.projectSlugs) ? u.projectSlugs : []),
            u.activeProjectSlug,
          ];
          projectSlugs = normalizeProjectSlugList([...projectSlugs, ...fromPhone]);
        }
      } catch (smsErr) {
        logger.warn("getDashboardAccessCallable: smsUser project merge failed", {
          message: smsErr.message,
        });
      }
    }

    return {
      ok: true,
      email: access.email,
      role: access.role,
      projectSlugs,
      rawProjectSlugs,
      approvedPhoneE164: approvedPhoneE164 || null,
      allProjects: access.allProjects === true,
      canApproveNotes: access.canApproveNotes === true,
      via: access.via || null,
    };
  }
);

exports.issueDebugCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const token = request.auth && request.auth.token ? request.auth.token : {};
    const out = {
      ok: true,
      hasAuth: !!(request.auth && request.auth.uid),
      uid: request.auth && request.auth.uid ? request.auth.uid : null,
      email: token.email || null,
      emailVerified:
        Object.prototype.hasOwnProperty.call(token, "email_verified")
          ? token.email_verified !== false
          : null,
      authRole: token.role || null,
      hasAdminClaim: token.admin === true || token.operator === true || String(token.role || "").trim().toLowerCase() === "admin",
      hasAppCheck: !!request.app,
      appId: request.app && request.app.appId ? request.app.appId : null,
    };
    try {
      const access = await getAppAccess(db, request);
      out.access = {
        ok: true,
        email: access.email || null,
        role: access.role || null,
        allProjects: access.allProjects === true,
        via: access.via || null,
        projectSlugs: Array.isArray(access.projectSlugs) ? access.projectSlugs : [],
      };
    } catch (err) {
      out.access = {
        ok: false,
        code: err && err.code ? err.code : null,
        message: err && err.message ? err.message : String(err),
      };
    }
    logger.info("issueDebugCallable", out);
    return out;
  }
);

exports.issueDebugIssueCountsCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    let projectSlugs = normalizeProjectSlugList(
      Array.isArray(access.projectSlugs) ? access.projectSlugs : []
    );

    const approvedPhoneRaw =
      access.memberData && access.memberData.approvedPhoneE164 != null
        ? String(access.memberData.approvedPhoneE164).trim()
        : "";
    const approvedPhoneE164 = approvedPhoneRaw ? normalizePhoneE164(approvedPhoneRaw) : null;
    if (approvedPhoneE164 && access.via === "app-member" && !access.allProjects) {
      try {
        const smsSnap = await db.collection("smsUsers").doc(approvedPhoneE164).get();
        if (smsSnap.exists) {
          const u = smsSnap.data() || {};
          projectSlugs = normalizeProjectSlugList([
            ...projectSlugs,
            ...(Array.isArray(u.projectSlugs) ? u.projectSlugs : []),
            u.activeProjectSlug,
          ]);
        }
      } catch (_) {}
    }

    const collections = Object.values(COLLECTION_BY_TYPE);
    const result = {
      ok: true,
      role: access.role,
      allProjects: access.allProjects === true,
      projectSlugs,
      counts: {},
    };

    for (const col of collections) {
      result.counts[col] = {
        projectId: 0,
        projectSlug: 0,
        samples: [],
      };

      if (access.allProjects === true || roleAtLeast(access.role, "admin")) {
        const snap = await db.collection(col).limit(20).get();
        result.counts[col].projectId = snap.size;
        result.counts[col].samples = snap.docs.slice(0, 5).map((d) => ({
          id: d.id,
          projectId: d.data()?.projectId || null,
          projectSlug: d.data()?.projectSlug || null,
          title: d.data()?.title || null,
        }));
        continue;
      }

      for (const slugChunk of chunkArray(projectSlugs, 10)) {
        if (!slugChunk.length) continue;
        const byProjectId = await db
          .collection(col)
          .where("projectId", "in", slugChunk)
          .limit(50)
          .get();
        result.counts[col].projectId += byProjectId.size;
        byProjectId.docs.slice(0, Math.max(0, 5 - result.counts[col].samples.length)).forEach((d) => {
          result.counts[col].samples.push({
            id: d.id,
            projectId: d.data()?.projectId || null,
            projectSlug: d.data()?.projectSlug || null,
            title: d.data()?.title || null,
          });
        });

        const byProjectSlug = await db
          .collection(col)
          .where("projectSlug", "in", slugChunk)
          .limit(50)
          .get();
        result.counts[col].projectSlug += byProjectSlug.size;
        byProjectSlug.docs.slice(0, Math.max(0, 5 - result.counts[col].samples.length)).forEach((d) => {
          const already = result.counts[col].samples.some((s) => s.id === d.id);
          if (!already) {
            result.counts[col].samples.push({
              id: d.id,
              projectId: d.data()?.projectId || null,
              projectSlug: d.data()?.projectSlug || null,
              title: d.data()?.title || null,
            });
          }
        });
      }
    }

    logger.info("issueDebugIssueCountsCallable", result);
    return result;
  }
);

exports.listAccessibleIssuesCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    let projectSlugs = normalizeProjectSlugList(
      Array.isArray(access.projectSlugs) ? access.projectSlugs : []
    );

    const approvedPhoneRaw =
      access.memberData && access.memberData.approvedPhoneE164 != null
        ? String(access.memberData.approvedPhoneE164).trim()
        : "";
    const approvedPhoneE164 = approvedPhoneRaw ? normalizePhoneE164(approvedPhoneRaw) : null;
    if (approvedPhoneE164 && access.via === "app-member" && !access.allProjects) {
      try {
        const smsSnap = await db.collection("smsUsers").doc(approvedPhoneE164).get();
        if (smsSnap.exists) {
          const u = smsSnap.data() || {};
          projectSlugs = normalizeProjectSlugList([
            ...projectSlugs,
            ...(Array.isArray(u.projectSlugs) ? u.projectSlugs : []),
            u.activeProjectSlug,
          ]);
        }
      } catch (_) {}
    }

    const collections = Object.values(COLLECTION_BY_TYPE);
    const out = {};
    for (const col of collections) out[col] = [];

    for (const col of collections) {
      const merged = new Map();
      if (access.allProjects === true || roleAtLeast(access.role, "admin")) {
        const snap = await db.collection(col).orderBy("createdAt", "desc").limit(200).get();
        snap.docs.forEach((docSnap) => {
          merged.set(docSnap.id, serializeIssueForClient(docSnap, col));
        });
      } else {
        for (const slugChunk of chunkArray(projectSlugs, 10)) {
          if (!slugChunk.length) continue;
          const snap = await db
            .collection(col)
            .where("projectId", "in", slugChunk)
            .orderBy("createdAt", "desc")
            .limit(200)
            .get();
          snap.docs.forEach((docSnap) => {
            merged.set(docSnap.id, serializeIssueForClient(docSnap, col));
          });
        }
      }
      out[col] = Array.from(merged.values()).sort((a, b) => {
        const aMs = a && a.createdAt && typeof a.createdAt.seconds === "number" ? a.createdAt.seconds * 1000 : 0;
        const bMs = b && b.createdAt && typeof b.createdAt.seconds === "number" ? b.createdAt.seconds * 1000 : 0;
        return bMs - aMs;
      });
    }

    return {
      ok: true,
      role: access.role,
      allProjects: access.allProjects === true,
      projectSlugs,
      collections: out,
    };
  }
);

exports.upsertAppMemberCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request);
    const email = normalizeEmail(request.data?.email || "");
    if (!email) {
      throw new HttpsError("invalid-argument", "email is required.");
    }
    const { approvedPhoneE164 } = await resolveApprovedPhoneSmsUser(request.data?.approvedPhoneE164 || "");

    const role = normalizeRole(request.data?.role || "user");
    const active = request.data?.active !== false;
    const displayName = String(request.data?.displayName || "").trim().slice(0, 120);
    const company = String(request.data?.company || "").trim().slice(0, 120);
    const projectSlugs = normalizeProjectSlugList(request.data?.projectSlugs);
    const allProjects = role === "admin" ? true : request.data?.allProjects === true;
    const canApproveNotes =
      role === "admin" ? true : role === "management" && request.data?.canApproveNotes === true;

    const memberRef = db.collection(COL_APP_MEMBERS).doc(email);
    const memberSnap = await memberRef.get();
    const existing = memberSnap.exists ? memberSnap.data() || {} : {};
    const nextDisplayName = displayName || existing.displayName || "";
    const smsUserPatch = await buildSmsUserMemberSyncPatch({
      approvedPhoneE164,
      displayName: nextDisplayName,
      role,
      projectSlugs,
      allProjects,
    });

    await memberRef.set(
      {
        email,
        displayName: nextDisplayName,
        company: company || existing.company || "",
        approvedPhoneE164,
        role,
        active,
        projectSlugs,
        allProjects,
        canApproveNotes,
        updatedAt: FieldValue.serverTimestamp(),
        updatedByEmail: operator.email,
        ...(memberSnap.exists
          ? {}
          : {
              createdAt: FieldValue.serverTimestamp(),
              createdByEmail: operator.email,
            }),
      },
      { merge: true }
    );

    await db.collection("smsUsers").doc(approvedPhoneE164).set(
      {
        ...smsUserPatch,
        approvedMemberEmail: email,
        approvedMemberRole: role,
        approvedByEmail: operator.email,
      },
      { merge: true }
    );

    return {
      ok: true,
      email,
      approvedPhoneE164,
      role,
      active,
      projectSlugs: smsUserPatch.projectSlugs,
      activeProjectSlug: smsUserPatch.activeProjectSlug,
      allProjects,
      canApproveNotes,
    };
  }
);

exports.deleteAppMemberCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request);
    const email = normalizeEmail(request.data?.email || "");
    if (!email) {
      throw new HttpsError("invalid-argument", "email is required.");
    }
    if (email === operator.email) {
      throw new HttpsError(
        "failed-precondition",
        "You cannot delete your own member record."
      );
    }

    const memberRef = db.collection(COL_APP_MEMBERS).doc(email);
    const memberSnap = await memberRef.get();
    if (!memberSnap.exists) {
      throw new HttpsError("not-found", "Member not found.");
    }

    const memberData = memberSnap.data() || {};
    const approvedPhoneE164 = normalizePhoneE164(
      String(memberData.approvedPhoneE164 || "").trim()
    );
    const batch = db.batch();
    let unlinkedSmsUser = false;

    batch.delete(memberRef);

    if (approvedPhoneE164) {
      const smsUserRef = db.collection("smsUsers").doc(approvedPhoneE164);
      const smsUserSnap = await smsUserRef.get();
      if (smsUserSnap.exists) {
        const smsUserData = smsUserSnap.data() || {};
        const linkedMemberEmail = normalizeEmail(smsUserData.approvedMemberEmail || "");
        if (linkedMemberEmail === email) {
          batch.update(smsUserRef, {
            approvedMemberEmail: FieldValue.delete(),
            approvedMemberRole: FieldValue.delete(),
            approvedByEmail: FieldValue.delete(),
            role: null,
            updatedAt: FieldValue.serverTimestamp(),
          });
          unlinkedSmsUser = true;
        }
      }
    }

    await batch.commit();

    return {
      ok: true,
      email,
      approvedPhoneE164: approvedPhoneE164 || null,
      unlinkedSmsUser,
    };
  }
);

exports.upsertLabourerCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    const name = normalizeLabourerName(request.data?.name || request.data?.displayName || "");
    if (!phoneE164) throw new HttpsError("invalid-argument", "phoneE164 is required.");
    if (!name) throw new HttpsError("invalid-argument", "name is required.");

    const projectSlugs = Array.isArray(request.data?.projectSlugs)
      ? request.data.projectSlugs.map((slug) => normalizeProjectSlug(String(slug || "").trim())).filter(Boolean)
      : [];
    const active = request.data?.active !== false;

    await db.collection(COL_LABOURERS).doc(phoneE164).set(
      {
        phoneE164,
        name,
        displayName: name,
        projectSlugs,
        active,
        updatedAt: FieldValue.serverTimestamp(),
        updatedByEmail: operator.email,
        ...(request.data?.createdAt ? {} : { createdAt: FieldValue.serverTimestamp(), createdByEmail: operator.email }),
      },
      { merge: true }
    );

    return {
      ok: true,
      phoneE164,
      name,
      projectSlugs,
      active,
    };
  }
);

exports.deleteLabourerCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    await getOperatorAccess(db, request, { minimumRole: "management" });
    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    if (!phoneE164) throw new HttpsError("invalid-argument", "phoneE164 is required.");

    const ref = db.collection(COL_LABOURERS).doc(phoneE164);
    const snap = await ref.get();
    if (!snap.exists) throw new HttpsError("not-found", "Labourer not found.");
    await ref.delete();

    return { ok: true, phoneE164 };
  }
);

exports.createProjectTodoCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    const projectSlug = normalizeProjectSlug(String(request.data?.projectSlug || "home").trim()) || "home";
    const taskText = normalizeTodoText(request.data?.taskText || "", 500);
    const dueBy = normalizeTodoDateTime(request.data?.dueBy);
    const priority = normalizeTodoPriority(request.data?.priority);
    const labels = normalizeTodoLabels(request.data?.labels);
    const tags = normalizeTodoTags(request.data?.tags);
    const reminders = normalizeTodoReminders(request.data?.reminders);
    const dependencies = normalizeTodoDependencies(request.data?.dependencies);
    const recurrence = normalizeTodoRecurrence(request.data?.recurrence);

    if (!taskText) throw new HttpsError("invalid-argument", "taskText is required.");
    if (dueBy === "") throw new HttpsError("invalid-argument", "dueBy must be a valid date/time value.");
    if (priority === "") throw new HttpsError("invalid-argument", "priority must be p1, p2, p3, or p4.");
    if (reminders === "") throw new HttpsError("invalid-argument", "reminders must be valid date/time values.");
    if (recurrence === "") throw new HttpsError("invalid-argument", "recurrence is invalid.");
    if (!canAccessProject(operator, projectSlug)) {
      throw new HttpsError("permission-denied", "You cannot create todo items for this project.");
    }

    const todoRef = db.collection(COL_PROJECT_TODOS).doc();
    const createdByName =
      String(operator.memberData?.displayName || operator.email || "").trim() || null;
    await todoRef.set({
      projectSlug,
      scope: "project",
      visibility: "management",
      status: "open",
      taskText,
      sourceText: taskText,
      dueWindow: null,
      dueLabel: null,
      dueBy: dueBy || null,
      startedAt: null,
      finishedAt: null,
      priority: priority || null,
      recurrence,
      labels,
      tags,
      reminders,
      dependencies,
      comments: [],
      subTodos: [],
      createdByPhone: operator.approvedPhoneE164 || null,
      createdByEmail: operator.email || null,
      createdByName,
      source: "dashboard",
      sourceMessageId: null,
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
      updatedByEmail: operator.email || null,
    });

    return { ok: true, todoId: todoRef.id, projectSlug, taskText };
  }
);

exports.updateProjectTodoCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    const todoId = String(request.data?.todoId || "").trim();
    const status = normalizeTodoStatus(request.data?.status);
    const subTodoId = String(request.data?.subTodoId || "").trim();
    const dueByInput = request.data?.dueBy;
    const startedAtInput = request.data?.startedAt;
    const finishedAtInput = request.data?.finishedAt;
    const priorityInput = request.data?.priority;
    const recurrenceInput = request.data?.recurrence;
    const labelsInput = request.data?.labels;
    const tagsInput = request.data?.tags;
    const remindersInput = request.data?.reminders;
    const dependenciesInput = request.data?.dependencies;
    if (!todoId) throw new HttpsError("invalid-argument", "todoId is required.");
    if (
      !status &&
      dueByInput == null &&
      startedAtInput == null &&
      finishedAtInput == null &&
      priorityInput == null &&
      recurrenceInput == null &&
      labelsInput == null &&
      tagsInput == null &&
      remindersInput == null &&
      dependenciesInput == null
    ) {
      throw new HttpsError(
        "invalid-argument",
        "Provide at least one todo update."
      );
    }
    if (request.data?.status != null && !status) {
      throw new HttpsError("invalid-argument", "status must be open, inprogress, or completed.");
    }
    const dueBy = normalizeTodoDateTime(dueByInput);
    const startedAt = normalizeTodoDateTime(startedAtInput);
    const finishedAt = normalizeTodoDateTime(finishedAtInput);
    const priority = normalizeTodoPriority(priorityInput);
    const recurrence = normalizeTodoRecurrence(recurrenceInput);
    const labels = labelsInput == null ? null : normalizeTodoLabels(labelsInput);
    const tags = tagsInput == null ? null : normalizeTodoTags(tagsInput);
    const reminders = remindersInput == null ? null : normalizeTodoReminders(remindersInput);
    const dependencies = dependenciesInput == null ? null : normalizeTodoDependencies(dependenciesInput);
    if (
      dueBy === "" ||
      startedAt === "" ||
      finishedAt === "" ||
      priority === "" ||
      recurrence === "" ||
      tags === "" ||
      reminders === ""
    ) {
      throw new HttpsError("invalid-argument", "Todo date fields must be valid date/time values.");
    }

    const todoRef = db.collection(COL_PROJECT_TODOS).doc(todoId);
    const todoSnap = await todoRef.get();
    if (!todoSnap.exists) throw new HttpsError("not-found", "Todo item not found.");
    const todoData = todoSnap.data() || {};
    const projectSlug = normalizeProjectSlug(String(todoData.projectSlug || "").trim());
    if (!projectSlug || !canAccessProject(operator, projectSlug)) {
      throw new HttpsError("permission-denied", "You cannot update this todo item.");
    }

    const updates = {
      updatedAt: FieldValue.serverTimestamp(),
      updatedByEmail: operator.email || null,
    };
    if (subTodoId) {
      const currentSubTodos = Array.isArray(todoData.subTodos) ? todoData.subTodos : [];
      const nextSubTodos = currentSubTodos.map((item) => {
        if (String(item?.id || "").trim() !== subTodoId) return item;
        const nextStatus = status || String(item?.status || "open").trim().toLowerCase() || "open";
        const nextStartedAt =
          startedAtInput !== undefined
            ? startedAt
            : nextStatus === "inprogress" && !item?.startedAt
              ? new Date().toISOString()
              : item?.startedAt || null;
        const nextFinishedAt =
          finishedAtInput !== undefined
            ? finishedAt
            : nextStatus === "completed"
              ? item?.finishedAt || new Date().toISOString()
              : nextStatus === "open" || nextStatus === "inprogress"
                ? null
                : item?.finishedAt || null;
        return {
          ...item,
          status: nextStatus,
          dueBy: dueByInput !== undefined ? dueBy : item?.dueBy || null,
          startedAt: nextStartedAt,
          finishedAt: nextFinishedAt,
          priority: priorityInput !== undefined ? priority : item?.priority || null,
          recurrence: recurrenceInput !== undefined ? recurrence : item?.recurrence || { mode: "none", customText: "" },
          labels: labelsInput !== undefined ? labels : Array.isArray(item?.labels) ? item.labels : [],
          tags: tagsInput !== undefined ? tags : Array.isArray(item?.tags) ? item.tags : [],
          reminders: remindersInput !== undefined ? reminders : Array.isArray(item?.reminders) ? item.reminders : [],
          dependencies:
            dependenciesInput !== undefined
              ? dependencies
              : Array.isArray(item?.dependencies)
                ? item.dependencies
                : [],
          updatedAt: new Date().toISOString(),
          updatedByEmail: operator.email || null,
          sortKey: String(item?.sortKey || "").trim() || null,
          comments: Array.isArray(item?.comments) ? item.comments : [],
        };
      });
      if (!nextSubTodos.some((item) => String(item?.id || "").trim() === subTodoId)) {
        throw new HttpsError("not-found", "Sub-todo item not found.");
      }
      updates.subTodos = nextSubTodos;
    } else {
      const nextStatus = status || String(todoData.status || "open").trim().toLowerCase() || "open";
      updates.status = nextStatus;
      if (dueByInput !== undefined) updates.dueBy = dueBy;
      if (startedAtInput !== undefined) {
        updates.startedAt = startedAt;
      } else if (nextStatus === "inprogress" && !todoData.startedAt) {
        updates.startedAt = new Date().toISOString();
      }
      if (finishedAtInput !== undefined) {
        updates.finishedAt = finishedAt;
      } else if (nextStatus === "completed") {
        updates.finishedAt = todoData.finishedAt || new Date().toISOString();
      } else if (nextStatus === "open" || nextStatus === "inprogress") {
        updates.finishedAt = null;
      }
      if (priorityInput !== undefined) updates.priority = priority;
      if (recurrenceInput !== undefined) updates.recurrence = recurrence;
      if (labelsInput !== undefined) updates.labels = labels;
      if (tagsInput !== undefined) updates.tags = tags;
      if (remindersInput !== undefined) updates.reminders = reminders;
      if (dependenciesInput !== undefined) updates.dependencies = dependencies;
      if (nextStatus === "completed") {
        updates.completedAt = FieldValue.serverTimestamp();
        updates.completedByEmail = operator.email || null;
      } else {
        updates.completedAt = FieldValue.delete();
        updates.completedByEmail = FieldValue.delete();
      }
    }

    await todoRef.set(updates, { merge: true });
    return { ok: true, todoId, subTodoId: subTodoId || null, status };
  }
);

exports.addProjectSubTodoCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    const todoId = String(request.data?.todoId || "").trim();
    const text = normalizeTodoText(request.data?.text || "", 300);
    const dueBy = normalizeTodoDateTime(request.data?.dueBy);
    if (!todoId) throw new HttpsError("invalid-argument", "todoId is required.");
    if (!text) throw new HttpsError("invalid-argument", "Sub-todo text is required.");
    if (dueBy === "") throw new HttpsError("invalid-argument", "dueBy must be a valid date/time value.");

    const todoRef = db.collection(COL_PROJECT_TODOS).doc(todoId);
    const todoSnap = await todoRef.get();
    if (!todoSnap.exists) throw new HttpsError("not-found", "Todo item not found.");
    const todoData = todoSnap.data() || {};
    const projectSlug = normalizeProjectSlug(String(todoData.projectSlug || "").trim());
    if (!projectSlug || !canAccessProject(operator, projectSlug)) {
      throw new HttpsError("permission-denied", "You cannot update this todo item.");
    }

    const subTodo = {
      id: db.collection("_").doc().id,
      text,
      status: "open",
      dueBy: dueBy || null,
      startedAt: null,
      finishedAt: null,
      priority: null,
      recurrence: { mode: "none", customText: "" },
      labels: [],
      tags: [],
      reminders: [],
      dependencies: [],
      comments: [],
      createdAt: new Date().toISOString(),
      createdByEmail: operator.email || null,
      createdByName: String(operator.memberData?.displayName || operator.email || "").trim() || null,
    };
    const currentSubTodos = Array.isArray(todoData.subTodos) ? todoData.subTodos : [];
    await todoRef.set(
      {
        subTodos: [...currentSubTodos, subTodo],
        updatedAt: FieldValue.serverTimestamp(),
        updatedByEmail: operator.email || null,
      },
      { merge: true }
    );

    return { ok: true, todoId, subTodo };
  }
);

exports.addProjectTodoCommentCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const operator = await getOperatorAccess(db, request, { minimumRole: "management" });
    const todoId = String(request.data?.todoId || "").trim();
    const subTodoId = String(request.data?.subTodoId || "").trim();
    const text = normalizeTodoCommentText(request.data?.text || "");
    if (!todoId) throw new HttpsError("invalid-argument", "todoId is required.");
    if (!text) throw new HttpsError("invalid-argument", "Comment text is required.");

    const todoRef = db.collection(COL_PROJECT_TODOS).doc(todoId);
    const todoSnap = await todoRef.get();
    if (!todoSnap.exists) throw new HttpsError("not-found", "Todo item not found.");
    const todoData = todoSnap.data() || {};
    const projectSlug = normalizeProjectSlug(String(todoData.projectSlug || "").trim());
    if (!projectSlug || !canAccessProject(operator, projectSlug)) {
      throw new HttpsError("permission-denied", "You cannot update this todo item.");
    }

    const comment = {
      id: db.collection("_").doc().id,
      text,
      createdAt: new Date().toISOString(),
      createdByEmail: operator.email || null,
      createdByName: String(operator.memberData?.displayName || operator.email || "").trim() || null,
    };

    if (subTodoId) {
      const currentSubTodos = Array.isArray(todoData.subTodos) ? todoData.subTodos : [];
      const nextSubTodos = currentSubTodos.map((item) => {
        if (String(item?.id || "").trim() !== subTodoId) return item;
        return {
          ...item,
          comments: [...(Array.isArray(item?.comments) ? item.comments : []), comment],
          updatedAt: new Date().toISOString(),
          updatedByEmail: operator.email || null,
        };
      });
      if (!nextSubTodos.some((item) => String(item?.id || "").trim() === subTodoId)) {
        throw new HttpsError("not-found", "Sub-todo item not found.");
      }
      await todoRef.set(
        {
          subTodos: nextSubTodos,
          updatedAt: FieldValue.serverTimestamp(),
          updatedByEmail: operator.email || null,
        },
        { merge: true }
      );
    } else {
      await todoRef.set(
        {
          comments: [...(Array.isArray(todoData.comments) ? todoData.comments : []), comment],
          updatedAt: FieldValue.serverTimestamp(),
          updatedByEmail: operator.email || null,
        },
        { merge: true }
      );
    }

    return { ok: true, todoId, subTodoId: subTodoId || null, comment };
  }
);

exports.generateLabourReportCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
    secrets: [OPENAI_API_KEY],
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    if (!roleAtLeast(access.role, "management")) {
      throw new HttpsError(
        "permission-denied",
        "Management or admin access is required to generate labour reports."
      );
    }
    assertDashboardToken(request);

    const startKey = String(request.data?.startKey || request.data?.startDateKey || "").trim();
    const endKey = String(request.data?.endKey || request.data?.endDateKey || "").trim();
    const phoneE164 = request.data?.labourerPhone
      ? normalizePhoneE164(String(request.data.labourerPhone || "").trim())
      : "";
    const projectSlug = normalizeProjectSlug(String(request.data?.projectSlug || "").trim());
    const labourerName = String(request.data?.labourerName || "").trim();
    const { startKey: normalizedStart, endKey: normalizedEnd } = normalizeLabourRangeKeys(startKey, endKey);
    if (!normalizedStart || !normalizedEnd) {
      throw new HttpsError("invalid-argument", "startKey and endKey must be YYYY-MM-DD.");
    }

    const entries = await loadLabourEntries(db, {
      startKey: normalizedStart,
      endKey: normalizedEnd,
      labourerPhone: phoneE164 || null,
      labourerName: labourerName || null,
      projectSlug: projectSlug || null,
    });
    const summary = buildLabourRollup(entries);
    const labourer = phoneE164 ? await findActiveLabourerByPhone(db, phoneE164) : null;
    const reportTitle = request.data?.reportTitle
      ? String(request.data.reportTitle).trim()
      : "Labour Hours Report";
    const scopeBits = [
      labourer ? labourer.displayName || labourer.phoneE164 : labourerName || "",
      projectSlug || "",
      normalizedStart === normalizedEnd ? normalizedStart : `${normalizedStart} to ${normalizedEnd}`,
    ].filter(Boolean);
    const scopeLabel = scopeBits.join(" · ");
    const runId = `labour-pdf-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const scopeSequenceKey = [
      "labourHours",
      phoneE164 || "",
      labourer ? labourer.displayName || "" : labourerName || "",
      projectSlug || "",
    ].join("|");
    const sameScopeReportsSnap = await db
      .collection("labourReports")
      .where("type", "==", "labourHours")
      .where("startKey", "==", normalizedStart)
      .where("endKey", "==", normalizedEnd)
      .where("scopeSequenceKey", "==", scopeSequenceKey)
      .get();
    const sequence = String((sameScopeReportsSnap.size || 0) + 1).padStart(3, "0");
    const startStamp = normalizedStart.replace(/-/g, "_");
    const endStamp = normalizedEnd.replace(/-/g, "_");
    const fileName = `Labourers_Report_${startStamp}_to_${endStamp}_${sequence}.pdf`;
    const storagePath = fileName;
    const downloadToken = Math.random().toString(36).slice(2) + Date.now().toString(36);
    const bucket = admin.storage().bucket();
    const pdfResult = await generateLabourReportPdf({
      pdfTitle: reportTitle,
      subtitle: scopeLabel,
      summary: {
        ...summary,
        startKey: normalizedStart,
        endKey: normalizedEnd,
      },
      entries,
      storageBucket: bucket,
      storagePath,
      downloadToken,
    });

	    const reportRef = await db.collection("labourReports").add({
	      type: "labourHours",
	      reportTitle,
	      labourerPhone: phoneE164 || null,
	      labourerName: labourer ? labourer.displayName || null : labourerName || null,
	      projectSlug: projectSlug || null,
	      startKey: normalizedStart,
	      endKey: normalizedEnd,
	      totalHours: summary.totalHours,
	      // Keep this equal to actual hours to avoid showing inflated "paid hours" (no 2x/1.5x).
	      totalPaidHours: summary.totalPaidHours,
	      totalPayUnits: summary.totalPayUnits || null,
	      totalEntries: summary.totalEntries,
          fileName,
          fileSequence: Number(sequence),
          scopeSequenceKey,
	      storagePath: pdfResult.storagePath,
	      downloadURL: pdfResult.downloadURL,
	      createdAt: FieldValue.serverTimestamp(),
	      createdByEmail: access.email,
	      runId,
	    });

    return {
      ok: true,
      reportId: reportRef.id,
      reportTitle,
      startKey: normalizedStart,
      endKey: normalizedEnd,
	      totalHours: summary.totalHours,
	      totalPaidHours: summary.totalPaidHours,
	      totalPayUnits: summary.totalPayUnits || null,
	      totalEntries: summary.totalEntries,
	      paidPeriodTotals: summary.paidPeriodTotals,
	      downloadURL: pdfResult.downloadURL,
	      storagePath: pdfResult.storagePath,
	    };
  }
);

exports.backfillProjectJournalEntriesCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    assertManagementAccess(access);
    assertDashboardToken(request);

    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    const projectSlug = normalizeProjectSlug(String(request.data?.projectSlug || "").trim());
    const startKey = String(request.data?.startKey || request.data?.startDateKey || "").trim();
    const endKey = String(request.data?.endKey || request.data?.endDateKey || "").trim();
    const dryRun = request.data?.dryRun === true;
    const includeMedia = request.data?.includeMedia !== false;
    const forceReassign = request.data?.forceReassign === true;
    const sourceMessageIds = Array.isArray(request.data?.sourceMessageIds)
      ? [...new Set(request.data.sourceMessageIds.map((v) => String(v || "").trim()).filter(Boolean))]
      : [];

    if (!phoneE164) throw new HttpsError("invalid-argument", "phoneE164 is required.");
    if (!projectSlug) throw new HttpsError("invalid-argument", "projectSlug is required.");
    if (!canAccessProject(access, projectSlug)) {
      throw new HttpsError("permission-denied", `You are not assigned to project "${projectSlug}".`);
    }
    const hasDateRange = startKey || endKey;
    if (hasDateRange) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(startKey) || !/^\d{4}-\d{2}-\d{2}$/.test(endKey)) {
        throw new HttpsError("invalid-argument", "startKey and endKey must be YYYY-MM-DD when provided.");
      }
      if (startKey > endKey) {
        throw new HttpsError("invalid-argument", "startKey must be before or equal to endKey.");
      }
    }

    const rowsSnap = await db.collection("logEntries").where("senderPhone", "==", phoneE164).limit(5000).get();
    const inSourceSet = sourceMessageIds.length
      ? new Set(sourceMessageIds)
      : null;
    const rows = rowsSnap.docs
      .map((docSnap) => ({ id: docSnap.id, ref: docSnap.ref, ...(docSnap.data() || {}) }))
      .filter((row) => {
        const dk = String(row.reportDateKey || row.dateKey || "").trim();
        const byDate = hasDateRange ? Boolean(dk && dk >= startKey && dk <= endKey) : true;
        const sid = String(row.sourceMessageId || "").trim();
        const bySid = inSourceSet ? inSourceSet.has(sid) : true;
        return byDate && bySid;
      });

    const targetNorm = normalizeProjectSlug(projectSlug);
    const rowsToUpdate = rows.filter((row) => {
      const currentNorm = normalizeProjectSlug(String(row.projectSlug || row.projectId || "").trim());
      if (forceReassign) return currentNorm !== targetNorm;
      return !currentNorm || currentNorm === "_unassigned";
    });

    const changedLogEntryIds = rowsToUpdate.map((row) => row.id);
    let mediaDocsToUpdate = [];
    if (includeMedia) {
      const mediaSnap = await db.collection("media").where("senderPhone", "==", phoneE164).limit(5000).get();
      const linkedIdSet = new Set(changedLogEntryIds);
      mediaDocsToUpdate = mediaSnap.docs
        .map((docSnap) => ({ id: docSnap.id, ref: docSnap.ref, ...(docSnap.data() || {}) }))
        .filter((m) => {
          const dk = String(m.reportDateKey || m.dateKey || "").trim();
          const byDate = hasDateRange ? Boolean(dk && dk >= startKey && dk <= endKey) : true;
          const sid = String(m.sourceMessageId || "").trim();
          const bySid = inSourceSet ? inSourceSet.has(sid) : true;
          const linked = String(m.linkedLogEntryId || "").trim();
          return byDate && (bySid || (linked && linkedIdSet.has(linked)));
        })
        .filter((m) => {
          const currentNorm = normalizeProjectSlug(String(m.projectId || m.projectSlug || "").trim());
          if (forceReassign) return currentNorm !== targetNorm;
          return !currentNorm || currentNorm === "_unassigned";
        });
    }

    if (!dryRun) {
      let batch = db.batch();
      let opCount = 0;
      const commitBatch = async () => {
        if (!opCount) return;
        await batch.commit();
        batch = db.batch();
        opCount = 0;
      };
      for (const row of rowsToUpdate) {
        batch.update(row.ref, {
          projectSlug,
          projectId: projectSlug,
          updatedAt: FieldValue.serverTimestamp(),
          backfillProjectAt: FieldValue.serverTimestamp(),
          backfillProjectByEmail: access.email,
        });
        opCount += 1;
        if (opCount >= 400) await commitBatch();
      }
      for (const media of mediaDocsToUpdate) {
        batch.update(media.ref, {
          projectId: projectSlug,
          projectSlug,
          includeInDailyReport: true,
          updatedAt: FieldValue.serverTimestamp(),
          backfillProjectAt: FieldValue.serverTimestamp(),
          backfillProjectByEmail: access.email,
        });
        opCount += 1;
        if (opCount >= 400) await commitBatch();
      }
      await commitBatch();
    }

    return {
      ok: true,
      dryRun,
      phoneE164,
      projectSlug,
      scopedBy: {
        startKey: hasDateRange ? startKey : null,
        endKey: hasDateRange ? endKey : null,
        sourceMessageIds,
      },
      matchedLogEntries: rows.length,
      updatedLogEntries: rowsToUpdate.length,
      updatedLogEntryIds: rowsToUpdate.slice(0, 100).map((row) => row.id),
      matchedMediaDocs: includeMedia ? mediaDocsToUpdate.length : 0,
      updatedMediaDocs: includeMedia ? mediaDocsToUpdate.length : 0,
      includeMedia,
      forceReassign,
    };
  }
);

exports.createProjectNoteEditRequestCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    if (normalizeRole(access.role) === "viewer") {
      throw new HttpsError(
        "permission-denied",
        "View-only accounts cannot submit note update requests."
      );
    }
    const projectSlug = normalizeProjectSlug(request.data?.projectSlug || "");
    const reportId = String(request.data?.reportId || "").trim();
    const proposedNotes = normalizeProjectNotesText(request.data?.proposedNotes || "");
    const reviewerComment = String(request.data?.comment || "").trim().slice(0, 500);

    if (!projectSlug) {
      throw new HttpsError("invalid-argument", "projectSlug is required.");
    }
    if (!proposedNotes) {
      throw new HttpsError("invalid-argument", "proposedNotes is required.");
    }
    if (!canAccessProject(access, projectSlug)) {
      throw new HttpsError("permission-denied", "You are not assigned to this project.");
    }

    const projectRef = db.collection("projects").doc(projectSlug);
    const projectSnap = await projectRef.get();
    if (!projectSnap.exists) {
      throw new HttpsError("not-found", `Project "${projectSlug}" does not exist.`);
    }
    const projectData = projectSnap.data() || {};

    let reportMeta = {
      reportId: null,
      reportTitle: null,
      reportDateKey: null,
    };
    if (reportId) {
      const reportSnap = await db.collection("dailyReports").doc(reportId).get();
      if (!reportSnap.exists) {
        throw new HttpsError("not-found", "Report not found.");
      }
      const reportData = reportSnap.data() || {};
      if (normalizeProjectSlug(reportData.projectId || "") !== projectSlug) {
        throw new HttpsError(
          "invalid-argument",
          "The selected report does not belong to the selected project."
        );
      }
      reportMeta = {
        reportId,
        reportTitle: String(reportData.reportTitle || "").trim() || null,
        reportDateKey: String(reportData.dateKey || "").trim() || null,
      };
    }

    const docRef = db.collection(COL_PROJECT_NOTE_EDIT_REQUESTS).doc();
    const currentNotes = normalizeProjectNotesText(projectData.notes || "");
    await docRef.set({
      type: "projectNotes",
      status: "pending",
      projectSlug,
      projectName: projectData.name || projectSlug,
      currentNotes,
      proposedNotes,
      requesterComment: reviewerComment || "",
      requestedByEmail: access.email,
      requestedByName: String(request.auth?.token?.name || "").trim() || access.email,
      requestedByRole: access.role,
      reportId: reportMeta.reportId,
      reportTitle: reportMeta.reportTitle,
      reportDateKey: reportMeta.reportDateKey,
      createdAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp(),
    });

    return {
      ok: true,
      requestId: docRef.id,
      projectSlug,
      reportId: reportMeta.reportId,
      status: "pending",
    };
  }
);

exports.reviewProjectNoteEditRequestCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    if (!canApproveProjectNoteRequests(access)) {
      throw new HttpsError(
        "permission-denied",
        "Your account cannot approve project note changes."
      );
    }

    const requestId = String(request.data?.requestId || "").trim();
    const decision = String(request.data?.decision || "").trim().toLowerCase();
    const reviewerComment = String(request.data?.reviewerComment || "").trim().slice(0, 500);
    if (!requestId) {
      throw new HttpsError("invalid-argument", "requestId is required.");
    }
    if (decision !== "approve" && decision !== "reject") {
      throw new HttpsError("invalid-argument", "decision must be approve or reject.");
    }

    const editRef = db.collection(COL_PROJECT_NOTE_EDIT_REQUESTS).doc(requestId);
    await db.runTransaction(async (tx) => {
      const editSnap = await tx.get(editRef);
      if (!editSnap.exists) {
        throw new HttpsError("not-found", "Edit request not found.");
      }
      const editData = editSnap.data() || {};
      if (editData.status !== "pending") {
        throw new HttpsError("failed-precondition", "This request has already been reviewed.");
      }
      const projectSlug = normalizeProjectSlug(editData.projectSlug || "");
      if (!canAccessProject(access, projectSlug)) {
        throw new HttpsError("permission-denied", "You are not assigned to this project.");
      }

      const projectRef = db.collection("projects").doc(projectSlug);
      const projectSnap = await tx.get(projectRef);
      if (!projectSnap.exists) {
        throw new HttpsError("not-found", "Project not found.");
      }
      const projectData = projectSnap.data() || {};
      const liveNotes = normalizeProjectNotesText(projectData.notes || "");

      if (decision === "approve") {
        if (liveNotes !== normalizeProjectNotesText(editData.currentNotes || "")) {
          throw new HttpsError(
            "failed-precondition",
            "Project notes changed after this request was submitted. Refresh and review again."
          );
        }
        tx.set(
          projectRef,
          {
            notes: normalizeProjectNotesText(editData.proposedNotes || ""),
            updatedAt: FieldValue.serverTimestamp(),
            notesUpdatedAt: FieldValue.serverTimestamp(),
            notesUpdatedByEmail: access.email,
          },
          { merge: true }
        );
      }

      tx.set(
        editRef,
        {
          status: decision === "approve" ? "approved" : "rejected",
          reviewerComment: reviewerComment || "",
          reviewedAt: FieldValue.serverTimestamp(),
          reviewedByEmail: access.email,
          updatedAt: FieldValue.serverTimestamp(),
          appliedNotes:
            decision === "approve" ? normalizeProjectNotesText(editData.proposedNotes || "") : null,
        },
        { merge: true }
      );
    });

    return {
      ok: true,
      requestId,
      status: decision === "approve" ? "approved" : "rejected",
    };
  }
);

exports.createUserProjectCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    assertManagementAccess(access);
    assertDashboardToken(request);

    const phoneE164 = normalizePhoneE164(
      String(request.data?.phoneE164 || "").trim()
    );
    const projectSlug = normalizeProjectSlug(
      request.data?.projectSlug || request.data?.slug || ""
    );
    const projectName = normalizeProjectName(
      request.data?.projectName || request.data?.name || ""
    );
    const reportLogoStoragePath = String(request.data?.reportLogoStoragePath || "").trim();
    const location = normalizeProjectLocation(
      request.data?.location || request.data?.siteAddress || request.data?.address || ""
    );

    if (!phoneE164) {
      throw new HttpsError("invalid-argument", "phoneE164 is required.");
    }
    if (!projectSlug) {
      throw new HttpsError(
        "invalid-argument",
        "projectSlug is required and must contain letters or numbers."
      );
    }
    if (!projectName) {
      throw new HttpsError("invalid-argument", "projectName is required.");
    }

    const userAccess = await getUserProjectAccess(db, phoneE164);
    if (!userAccess.exists) {
      throw new HttpsError(
        "not-found",
        "No smsUsers document for this phone. Text the Twilio number once first."
      );
    }

    const projectRef = db.collection("projects").doc(projectSlug);
    try {
      await db.runTransaction(async (tx) => {
        const [userSnap, projectSnap] = await Promise.all([
          tx.get(userAccess.userRef),
          tx.get(projectRef),
        ]);

        if (!userSnap.exists) {
          throw new HttpsError(
            "not-found",
            "No smsUsers document for this phone. Text the Twilio number once first."
          );
        }

        if (projectSnap.exists) {
          const existing = projectSnap.data() || {};
          const ownerPhoneE164 =
            typeof existing.ownerPhoneE164 === "string"
              ? existing.ownerPhoneE164.trim()
              : "";
          if (ownerPhoneE164 === phoneE164) {
            throw new HttpsError(
              "already-exists",
              `Project "${projectSlug}" already exists for this user.`
            );
          }
          if (!ownerPhoneE164) {
            throw new HttpsError(
              "already-exists",
              `Project "${projectSlug}" already exists as a legacy project. Switch to it instead of creating a duplicate.`
            );
          }
          throw new HttpsError(
            "already-exists",
            `Project "${projectSlug}" already exists and belongs to another user.`
          );
        }

        const userData = userSnap.data() || {};
        const userPatch = buildUserProjectPatch(userData, projectSlug, {
          activeProjectSlug: projectSlug,
        });

        tx.set(projectRef, {
          slug: projectSlug,
          name: projectName,
          ...(location ? { location } : {}),
          ...(reportLogoStoragePath ? { reportLogoStoragePath } : {}),
          ownerPhoneE164: phoneE164,
          createdAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
        });
        tx.set(
          userAccess.userRef,
          {
            ...userPatch,
            updatedAt: FieldValue.serverTimestamp(),
          },
          { merge: true }
        );
      });
    } catch (err) {
      if (err instanceof HttpsError) throw err;
      logger.error("createUserProjectCallable: failed", {
        phoneE164,
        projectSlug,
        message: err.message,
        stack: err.stack,
      });
      throw new HttpsError(
        "internal",
        err.message || "Project creation failed."
      );
    }

    return {
      ok: true,
      phoneE164,
      projectSlug,
      projectName,
      reportLogoStoragePath: reportLogoStoragePath || null,
      location: location || null,
      activeProjectSlug: projectSlug,
    };
  }
);

exports.updateProjectReportLogoCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    assertManagementAccess(access);
    assertDashboardToken(request);

    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    const projectSlug = normalizeProjectSlug(request.data?.projectSlug || "");
    const reportLogoStoragePath = String(request.data?.reportLogoStoragePath || "").trim();

    if (!phoneE164) {
      throw new HttpsError("invalid-argument", "phoneE164 is required.");
    }
    if (!projectSlug) {
      throw new HttpsError("invalid-argument", "projectSlug is required.");
    }

    const userAccess = await getUserProjectAccess(db, phoneE164);
    if (!userAccess.exists) {
      throw new HttpsError(
        "not-found",
        "No smsUsers document for this phone. Text the Twilio number once first."
      );
    }

    const projectAccess = await getAccessibleProjectForUser(
      db,
      phoneE164,
      projectSlug,
      userAccess
    );
    if (!projectAccess.exists) {
      throw new HttpsError("not-found", `Project "${projectSlug}" does not exist.`);
    }
    if (!projectAccess.allowed) {
      throw new HttpsError(
        "permission-denied",
        `Project "${projectSlug}" is not assigned to this phone number.`
      );
    }

    await db.collection("projects").doc(projectSlug).set(
      {
        reportLogoStoragePath: reportLogoStoragePath || FieldValue.delete(),
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return {
      ok: true,
      phoneE164,
      projectSlug,
      projectName: projectAccess.projectData.name || projectSlug,
      reportLogoStoragePath: reportLogoStoragePath || null,
    };
  }
);

exports.setActiveProjectCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    await getOperatorAccess(db, request);
    assertDashboardToken(request);

    const phoneE164 = normalizePhoneE164(
      String(request.data?.phoneE164 || "").trim()
    );
    const projectSlug = normalizeProjectSlug(
      request.data?.projectSlug || request.data?.slug || ""
    );

    if (!phoneE164) {
      throw new HttpsError("invalid-argument", "phoneE164 is required.");
    }
    if (!projectSlug) {
      throw new HttpsError("invalid-argument", "projectSlug is required.");
    }

    const userAccess = await getUserProjectAccess(db, phoneE164);
    if (!userAccess.exists) {
      throw new HttpsError(
        "not-found",
        "No smsUsers document for this phone. Text the Twilio number once first."
      );
    }

    const projectAccess = await getAccessibleProjectForUser(
      db,
      phoneE164,
      projectSlug,
      userAccess
    );
    if (!projectAccess.exists) {
      throw new HttpsError(
        "not-found",
        `Project "${projectSlug}" does not exist.`
      );
    }
    if (!projectAccess.allowed) {
      throw new HttpsError(
        "permission-denied",
        `Project "${projectSlug}" is not assigned to this phone number.`
      );
    }

    const userPatch = buildUserProjectPatch(userAccess.userData, projectSlug, {
      activeProjectSlug: projectSlug,
    });
    await userAccess.userRef.set(
      {
        ...userPatch,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return {
      ok: true,
      phoneE164,
      projectSlug,
      projectName: projectAccess.projectData.name || projectSlug,
      activeProjectSlug: projectSlug,
    };
  }
);

exports.generateDailyReportPdfCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
    secrets: [OPENAI_API_KEY],
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    if (!roleAtLeast(access.role, "management")) {
      throw new HttpsError(
        "permission-denied",
        "Management or admin access is required to generate daily PDF reports."
      );
    }
    assertDashboardToken(request);

    const phoneE164 = normalizePhoneE164(
      String(request.data?.phoneE164 || "").trim()
    );
    if (!phoneE164) {
      throw new HttpsError("invalid-argument", "phoneE164 is required.");
    }
    if (access.via === "app-member" && access.memberData) {
      const allowedPhone = normalizePhoneE164(
        String(access.memberData.approvedPhoneE164 || "").trim()
      );
      if (!allowedPhone || phoneE164 !== allowedPhone) {
        throw new HttpsError(
          "permission-denied",
          "You may only generate daily PDF reports for your approved field phone number."
        );
      }
    }
    const reportDateKeyRaw = String(request.data?.reportDateKey || "").trim();
    const reportDateKey =
      reportDateKeyRaw === ""
        ? null
        : /^\d{4}-\d{2}-\d{2}$/.test(reportDateKeyRaw)
          ? reportDateKeyRaw
          : null;
    if (reportDateKeyRaw && !reportDateKey) {
      throw new HttpsError(
        "invalid-argument",
        "reportDateKey must be YYYY-MM-DD."
      );
    }
    const reportTypeRaw = String(request.data?.reportType || "").trim();
    const reportType =
      reportTypeRaw === "" || reportTypeRaw === "dailySiteLog" || reportTypeRaw === "journal"
        ? normalizeReportTypeInput(reportTypeRaw)
        : null;
    if (!reportType) {
      throw new HttpsError(
        "invalid-argument",
        "reportType must be dailySiteLog or journal."
      );
    }
    const includeAllManagementEntries = request.data?.includeAllManagementEntries === true;

    const userAccess = await getUserProjectAccess(db, phoneE164);
    if (!userAccess.exists) {
      throw new HttpsError(
        "not-found",
        "No smsUsers document for this phone. Text the Twilio number once first."
      );
    }

    const projectOverride =
      request.data?.projectSlug != null &&
      String(request.data.projectSlug).trim() !== ""
        ? normalizeProjectSlug(String(request.data.projectSlug).trim())
        : null;
    const requestedSlug =
      projectOverride ??
      userAccess.activeProjectSlug ??
      null;
    const projectRecord = requestedSlug ? await getProjectRecord(db, requestedSlug) : null;
    if (requestedSlug && !projectRecord.exists) {
      throw new HttpsError(
        "not-found",
        `Project "${requestedSlug}" does not exist.`
      );
    }
    if (requestedSlug && !canAccessProject(access, requestedSlug)) {
      throw new HttpsError(
        "permission-denied",
        `Project "${requestedSlug}" is not assigned to your account.`
      );
    }
    const slug = projectRecord ? projectRecord.projectSlug || null : null;
    const projectName = slug
      ? projectRecord.projectData.name || slug
      : null;

    if (reportType === "dailySiteLog" && !slug) {
      throw new HttpsError(
        "invalid-argument",
        "Daily site log PDFs require a project. Enter the project slug in the dashboard (or set the field user’s active project), then generate again."
      );
    }

    if (!canAccessProject(access, slug || "")) {
      throw new HttpsError(
        "permission-denied",
        "You are not assigned to this project."
      );
    }

    const runId = `dash-pdf-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const openaiKey = OPENAI_API_KEY.value();

    let pdfResult;
    try {
      pdfResult = await generateDailyReportPdf({
        db,
        bucket: admin.storage().bucket(),
        phoneE164,
        projectSlug: slug,
        projectName,
        reportDateKey,
        reportType,
        includeAllManagementEntries,
        openaiApiKey: openaiKey || null,
        logger,
        runId,
        modelsOverride: {
          primary: OPENAI_MODEL_PRIMARY.value(),
        },
      });
    } catch (err) {
      logger.error("generateDailyReportPdfCallable: failed", {
        runId,
        message: err.message,
        stack: err.stack,
      });
      throw new HttpsError(
        "internal",
        err.message || "PDF generation failed."
      );
    }

    return {
      ok: true,
      reportId: pdfResult.reportId,
      reportDateKey: pdfResult.reportDateKey || reportDateKey || null,
      reportType: pdfResult.reportType || reportType,
      includeAllManagementEntries,
      downloadURL: pdfResult.downloadURL || null,
      downloadUrlError: pdfResult.downloadUrlError || null,
      storagePath: pdfResult.storagePath,
    };
  }
);

exports.updateDailyReportCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 60,
    memory: "256MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    if (!roleAtLeast(access.role, "management")) {
      throw new HttpsError(
        "permission-denied",
        "Management or admin access is required to update daily reports."
      );
    }
    assertDashboardToken(request);

    const reportId = String(request.data?.reportId || "").trim();
    const reportTitle = String(request.data?.reportTitle ?? "").trim();
    if (!reportId) {
      throw new HttpsError("invalid-argument", "reportId is required.");
    }
    if (!reportTitle) {
      throw new HttpsError("invalid-argument", "reportTitle is required.");
    }
    if (reportTitle.length > 500) {
      throw new HttpsError("invalid-argument", "reportTitle must be 500 characters or less.");
    }

    const reportRef = db.collection("dailyReports").doc(reportId);
    const reportSnap = await reportRef.get();
    if (!reportSnap.exists) {
      throw new HttpsError("not-found", "Report not found.");
    }
    const reportData = reportSnap.data() || {};
    const projectKey = reportData.projectId != null ? String(reportData.projectId).trim() : "";
    if (!canAccessProject(access, projectKey)) {
      throw new HttpsError("permission-denied", "You cannot update this report.");
    }

    await reportRef.set(
      {
        reportTitle,
        titleUpdatedAt: FieldValue.serverTimestamp(),
        titleUpdatedByEmail: access.email,
      },
      { merge: true }
    );

    return { ok: true, reportId, reportTitle };
  }
);

exports.parseLookaheadScheduleCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    await getOperatorAccess(db, request);
    assertDashboardToken(request);

    const storagePath = String(request.data?.storagePath || "").trim();
    if (!storagePath) {
      throw new HttpsError("invalid-argument", "storagePath is required.");
    }
    if (!/^branding\/lookahead\//.test(storagePath)) {
      throw new HttpsError(
        "invalid-argument",
        "storagePath must be under branding/lookahead/."
      );
    }
    if (!/\.xlsx$/i.test(storagePath)) {
      throw new HttpsError("invalid-argument", "Only .xlsx files are supported.");
    }

    const startDateKeyRaw = String(request.data?.startDateKey || "").trim();
    const endDateKeyRaw = String(request.data?.endDateKey || "").trim();
    const startDateKey =
      startDateKeyRaw === "" || /^\d{4}-\d{2}-\d{2}$/.test(startDateKeyRaw)
        ? startDateKeyRaw || ""
        : null;
    const endDateKey =
      endDateKeyRaw === "" || /^\d{4}-\d{2}-\d{2}$/.test(endDateKeyRaw)
        ? endDateKeyRaw || ""
        : null;
    if (startDateKey === null || endDateKey === null) {
      throw new HttpsError(
        "invalid-argument",
        "startDateKey and endDateKey must be blank or YYYY-MM-DD."
      );
    }

    const companyName = String(request.data?.companyName || "Matheson").trim() || "Matheson";
    const projectName =
      String(request.data?.projectName || "Docksteader Paramedic Station").trim() ||
      "Docksteader Paramedic Station";
    const includeHidden = request.data?.includeHidden === true;

    let buffer;
    try {
      [buffer] = await admin.storage().bucket().file(storagePath).download();
    } catch (err) {
      logger.error("parseLookaheadScheduleCallable: download failed", {
        storagePath,
        message: err.message,
      });
      throw new HttpsError("not-found", "Could not download the uploaded Excel file.");
    }

    let parsed;
    try {
      parsed = await parseLookaheadWorkbookBuffer(buffer, {
        fileName: storagePath.split("/").pop() || "lookahead.xlsx",
        sourcePath: storagePath,
        includeHidden,
        includeCompleted: false,
        startDateKey,
        endDateKey,
      });
    } catch (err) {
      logger.error("parseLookaheadScheduleCallable: parse failed", {
        storagePath,
        message: err.message,
        stack: err.stack,
      });
      throw new HttpsError("invalid-argument", err.message || "Could not parse the Excel file.");
    }

    const summary = formatCrewscopeStyleSummary(parsed, {
      companyName,
      projectName,
    });

    return {
      ok: true,
      storagePath,
      summary,
      taskCount: parsed.taskCount,
      window: parsed.window,
      tasks: parsed.tasks,
    };
  }
);

exports.createLookaheadActivitiesReportCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    await getOperatorAccess(db, request);
    assertDashboardToken(request);

    const storagePath = String(request.data?.storagePath || "").trim();
    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    if (!phoneE164) {
      throw new HttpsError("invalid-argument", "phoneE164 is required.");
    }
    if (!storagePath) {
      throw new HttpsError("invalid-argument", "storagePath is required.");
    }
    if (!/^branding\/lookahead\//.test(storagePath)) {
      throw new HttpsError(
        "invalid-argument",
        "storagePath must be under branding/lookahead/."
      );
    }
    if (!/\.xlsx$/i.test(storagePath)) {
      throw new HttpsError("invalid-argument", "Only .xlsx files are supported.");
    }

    const startDateKeyRaw = String(request.data?.startDateKey || "").trim();
    const endDateKeyRaw = String(request.data?.endDateKey || "").trim();
    const startDateKey =
      startDateKeyRaw === "" || /^\d{4}-\d{2}-\d{2}$/.test(startDateKeyRaw)
        ? startDateKeyRaw || ""
        : null;
    const endDateKey =
      endDateKeyRaw === "" || /^\d{4}-\d{2}-\d{2}$/.test(endDateKeyRaw)
        ? endDateKeyRaw || ""
        : null;
    if (startDateKey === null || endDateKey === null) {
      throw new HttpsError(
        "invalid-argument",
        "startDateKey and endDateKey must be blank or YYYY-MM-DD."
      );
    }

    const companyName = String(request.data?.companyName || "Matheson").trim() || "Matheson";
    const projectName =
      String(request.data?.projectName || "Docksteader Paramedic Station").trim() ||
      "Docksteader Paramedic Station";
    const requestedProjectSlug = normalizeProjectSlug(String(request.data?.projectSlug || "").trim());
    const includeHidden = request.data?.includeHidden === true;

    let projectLocation = "";
    if (requestedProjectSlug) {
      const userAccess = await getUserProjectAccess(db, phoneE164);
      if (!userAccess.exists) {
        throw new HttpsError(
          "not-found",
          "No smsUsers document for this phone. Text the Twilio number once first."
        );
      }
      const projectAccess = await getAccessibleProjectForUser(
        db,
        phoneE164,
        requestedProjectSlug,
        userAccess
      );
      if (!projectAccess.exists) {
        throw new HttpsError("not-found", `Project "${requestedProjectSlug}" does not exist.`);
      }
      if (!projectAccess.allowed) {
        throw new HttpsError(
          "permission-denied",
          `Project "${requestedProjectSlug}" is not assigned to this phone number.`
        );
      }
      projectLocation = normalizeProjectLocation(projectAccess.projectData?.location || "");
    }

    let buffer;
    try {
      [buffer] = await admin.storage().bucket().file(storagePath).download();
    } catch (err) {
      logger.error("createLookaheadActivitiesReportCallable: download failed", {
        storagePath,
        message: err.message,
      });
      throw new HttpsError("not-found", "Could not download the uploaded Excel file.");
    }

    let parsed;
    try {
      parsed = await parseLookaheadWorkbookBuffer(buffer, {
        fileName: storagePath.split("/").pop() || "lookahead.xlsx",
        sourcePath: storagePath,
        includeHidden,
        includeCompleted: false,
        startDateKey,
        endDateKey,
      });
    } catch (err) {
      logger.error("createLookaheadActivitiesReportCallable: parse failed", {
        storagePath,
        message: err.message,
        stack: err.stack,
      });
      throw new HttpsError("invalid-argument", err.message || "Could not parse the Excel file.");
    }

    const runId = `lookahead-report-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    let report;
    try {
      report = await createLookaheadActivitiesReportPdf({
        db,
        bucket: admin.storage().bucket(),
        phoneE164,
        projectSlug: requestedProjectSlug || null,
        companyName,
        projectName,
        projectLocation,
        parsed,
        logger,
        runId,
      });
      await saveLookaheadSnapshot({
        db,
        phoneE164,
        projectSlug: requestedProjectSlug || null,
        projectName,
        storagePath,
        parsed,
      });
    } catch (err) {
      logger.error("createLookaheadActivitiesReportCallable: report build failed", {
        runId,
        phoneE164,
        storagePath,
        projectName,
        projectLocation,
        message: err.message,
        stack: err.stack,
      });
      throw err instanceof HttpsError
        ? err
        : new HttpsError(
            "internal",
            err && err.message ? String(err.message) : "Activities report generation failed."
          );
    }

    return {
      ok: true,
      reportId: report.reportId,
      reportTitle: report.reportTitle,
      storagePath: report.storagePath,
      downloadURL: report.downloadURL,
      downloadUrlError: report.downloadUrlError || null,
      reportFileName: report.reportFileName,
      taskCount: report.taskCount,
      window: report.window,
      weatherSummary: report.weatherSummary || null,
    };
  }
);

exports.createLookaheadCloseoutReportCallable = onCall(
  {
    region: "northamerica-northeast1",
    cors: true,
    timeoutSeconds: 120,
    memory: "512MiB",
  },
  async (request) => {
    const access = await getAppAccess(db, request);
    assertManagementAccess(access);
    assertDashboardToken(request);

    const storagePath = String(request.data?.storagePath || "").trim();
    const phoneE164 = normalizePhoneE164(String(request.data?.phoneE164 || "").trim());
    const projectSlug = normalizeProjectSlug(String(request.data?.projectSlug || "").trim());
    const companyName = String(request.data?.companyName || "Matheson").trim() || "Matheson";
    const projectName =
      String(request.data?.projectName || "Docksteader Paramedic Station").trim() ||
      "Docksteader Paramedic Station";

    if (!phoneE164) throw new HttpsError("invalid-argument", "phoneE164 is required.");
    if (!projectSlug) throw new HttpsError("invalid-argument", "projectSlug is required.");
    if (!storagePath) throw new HttpsError("invalid-argument", "storagePath is required.");
    if (!/^branding\/lookahead\//.test(storagePath)) {
      throw new HttpsError("invalid-argument", "storagePath must be under branding/lookahead/.");
    }

    const userAccess = await assertAccessibleSmsUserForAccess(access, phoneE164);
    const projectAccess = await getAccessibleProjectForUser(db, phoneE164, projectSlug, userAccess);
    if (!projectAccess.exists) {
      throw new HttpsError("not-found", `Project "${projectSlug}" does not exist.`);
    }
    if (!projectAccess.allowed) {
      throw new HttpsError(
        "permission-denied",
        `Project "${projectSlug}" is not assigned to this phone number.`
      );
    }

    const previousSnapshot = await loadPreviousLookaheadSnapshot({
      db,
      projectSlug,
      excludeStoragePath: storagePath,
    });
    if (!previousSnapshot) {
      throw new HttpsError(
        "failed-precondition",
        "No previous lookahead schedule was found for this project yet."
      );
    }

    let buffer;
    try {
      [buffer] = await admin.storage().bucket().file(storagePath).download();
    } catch (err) {
      throw new HttpsError("not-found", "Could not download the uploaded Excel file.");
    }

    let parsed;
    try {
      parsed = await parseLookaheadWorkbookBuffer(buffer, {
        fileName: storagePath.split("/").pop() || "lookahead.xlsx",
        sourcePath: storagePath,
        includeHidden: false,
        includeCompleted: false,
      });
    } catch (err) {
      throw new HttpsError("invalid-argument", err.message || "Could not parse the Excel file.");
    }

    const report = await createLookaheadCloseoutReportPdf({
      db,
      bucket: admin.storage().bucket(),
      phoneE164,
      companyName,
      projectName,
      checkedInBy: access.email || null,
      previousSnapshot,
      currentParsed: parsed,
    });

    await saveLookaheadSnapshot({
      db,
      phoneE164,
      projectSlug,
      projectName,
      storagePath,
      parsed,
    });

    return {
      ok: true,
      reportId: report.reportId,
      reportTitle: report.reportTitle,
      storagePath: report.storagePath,
      downloadURL: report.downloadURL || null,
      reportFileName: report.reportFileName,
      summary: report.summary,
    };
  }
);
