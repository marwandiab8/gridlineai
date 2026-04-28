/**
 * Typed issue documents + thin legacy issueLogs rows for daily summaries / admin dashboard.
 */

const { Timestamp } = require("firebase-admin/firestore");
const {
  COLLECTION_BY_TYPE,
  ISSUE_PRIORITIES,
  ISSUE_STATUSES,
  ISSUE_TYPES,
  TYPE_BY_COLLECTION,
} = require("./issueConstants");

const COL_ISSUES_LEGACY = "issueLogs";
const COL_PROJECTS = "projects";
const MAX_SHORT_TEXT = 240;
const MAX_LONG_TEXT = 4000;
const MAX_TAG_COUNT = 20;

function normalizeText(value, max = MAX_SHORT_TEXT) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, max);
}

function normalizeLongText(value, max = MAX_LONG_TEXT) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim()
    .slice(0, max);
}

function normalizeIssueType(value) {
  const type = normalizeText(value, 40).toLowerCase();
  return ISSUE_TYPES.includes(type) ? type : null;
}

function normalizeIssueCollection(value) {
  const collection = normalizeText(value, 80);
  return TYPE_BY_COLLECTION[collection] ? collection : null;
}

function normalizeIssueStatus(value) {
  const status = normalizeText(value, 80);
  return ISSUE_STATUSES.includes(status) ? status : null;
}

function normalizeIssuePriority(value) {
  const priority = normalizeText(value, 40);
  return ISSUE_PRIORITIES.includes(priority) ? priority : null;
}

function normalizeTags(tags) {
  const out = [];
  for (const raw of Array.isArray(tags) ? tags : []) {
    const tag = normalizeText(raw, 50);
    if (tag && !out.includes(tag)) out.push(tag);
    if (out.length >= MAX_TAG_COUNT) break;
  }
  return out;
}

function normalizeDueDateInput(value) {
  const raw = normalizeText(value, 20);
  if (!raw) return { dateKey: "", timestamp: null };
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new Error("dueDate must be blank or YYYY-MM-DD.");
  }
  const asDate = new Date(`${raw}T12:00:00Z`);
  if (Number.isNaN(asDate.getTime())) {
    throw new Error("dueDate must be a valid calendar date.");
  }
  return {
    dateKey: raw,
    timestamp: Timestamp.fromDate(asDate),
  };
}

function buildHistoryEntry({
  action,
  changedBy,
  field = null,
  oldValue = null,
  newValue = null,
  note = "",
}) {
  return {
    at: Timestamp.now(),
    action: normalizeText(action, 80) || "edited",
    changedBy: normalizeText(changedBy, 320) || "system",
    field: field == null ? null : normalizeText(field, 80),
    oldValue: oldValue ?? null,
    newValue: newValue ?? null,
    note: normalizeLongText(note, 500),
  };
}

function buildDashboardChangedBy(operator) {
  if (operator && operator.email) return normalizeText(operator.email, 320);
  if (operator && operator.uid) return normalizeText(operator.uid, 320);
  return "dashboard";
}

function sanitizeDashboardIssueFields(input) {
  const type = normalizeIssueType(input && input.type);
  if (!type) throw new Error("type must be one of safety, delay, deficiency, or general.");

  const status = normalizeIssueStatus(input && input.status);
  if (!status) throw new Error("status is invalid.");

  const priority = normalizeIssuePriority(input && input.priority);
  if (!priority) throw new Error("priority is invalid.");

  const due = normalizeDueDateInput(input && input.dueDate);
  return {
    type,
    projectId: normalizeText(input && input.projectId, 120) || null,
    projectName: normalizeText(input && input.projectName, 200) || null,
    title: normalizeText(input && input.title, 160),
    description: normalizeLongText(input && input.description, 4000),
    location: normalizeText(input && input.location, 240),
    area: normalizeText(input && input.area, 240),
    trade: normalizeText(input && input.trade, 160),
    reference: normalizeText(input && input.reference, 240),
    requestedAction: normalizeText(input && input.requestedAction, 400),
    status,
    priority,
    assignedTo: normalizeText(input && input.assignedTo, 200),
    dueDate: due.timestamp,
    dueDateKey: due.dateKey,
    tags: normalizeTags(input && input.tags),
  };
}

function validateDashboardCreateFields(fields) {
  if (!fields.title || !fields.description) {
    throw new Error("Title and description are required.");
  }
  if (fields.type === "deficiency") {
    if (!fields.projectId) {
      throw new Error("Select a project before creating a deficiency.");
    }
    if (!fields.location && !fields.area) {
      throw new Error("Add a useful location or area / room for the deficiency.");
    }
    if (!fields.requestedAction) {
      throw new Error("Describe the required action so the deficiency is clear and actionable.");
    }
  }
}

async function getIssueSnapshot(db, issueCollection, issueId) {
  const collection = normalizeIssueCollection(issueCollection);
  const id = normalizeText(issueId, 120);
  if (!collection) throw new Error("issueCollection is invalid.");
  if (!id) throw new Error("issueId is required.");
  const ref = db.collection(collection).doc(id);
  const snap = await ref.get();
  if (!snap.exists) {
    throw new Error(`Issue "${id}" was not found.`);
  }
  return {
    issueCollection: collection,
    issueType: TYPE_BY_COLLECTION[collection],
    issueId: id,
    issueRef: ref,
    issueSnap: snap,
    issueData: snap.data() || {},
  };
}

function buildDashboardUpdatePatch(prev, next, changedBy) {
  const historyEntries = [...(Array.isArray(prev.history) ? prev.history : [])];
  const fieldsToCompare = [
    ["status", "status_changed"],
    ["priority", "priority_changed"],
    ["assignedTo", "assigned_changed"],
    ["location", "edited"],
    ["area", "edited"],
    ["trade", "edited"],
    ["reference", "edited"],
    ["requestedAction", "edited"],
    ["description", "edited"],
  ];

  for (const [field, action] of fieldsToCompare) {
    if (field === "description") {
      if ((prev.description || "") !== next.description) {
        historyEntries.push(
          buildHistoryEntry({
            action,
            changedBy,
            field: "description",
            oldValue: "(text)",
            newValue: "(text)",
            note: "Description updated",
          })
        );
      }
      continue;
    }
    const oldValue = prev[field] || "";
    const newValue = next[field] || "";
    if (oldValue !== newValue) {
      historyEntries.push(
        buildHistoryEntry({
          action,
          changedBy,
          field,
          oldValue,
          newValue,
        })
      );
    }
  }

  const prevDue = prev.dueDate && typeof prev.dueDate.toDate === "function"
    ? prev.dueDate.toDate().toISOString().slice(0, 10)
    : "";
  const nextDue = next.dueDateKey || "";
  if (prevDue !== nextDue) {
    historyEntries.push(
      buildHistoryEntry({
        action: "due_changed",
        changedBy,
        field: "dueDate",
        oldValue: prevDue || null,
        newValue: nextDue || null,
      })
    );
  }

  return historyEntries;
}

function inferIssueTypeFromKeywords(text) {
  const t = (text || "").toLowerCase();
  if (
    /\b(safety|unsafe|incident|hazard|ppe|injury|near\s*miss|osha|electrocution|fall)\b/.test(
      t
    )
  ) {
    return "safety";
  }
  if (
    /\b(deficiency|deficient|repair|missing|broken|incorrect|incorrectly|punch|defect|leak)\b/.test(
      t
    )
  ) {
    return "deficiency";
  }
  if (
    /\b(delay|delayed|waiting|no\s*crew|material\s*not|access\s*issue|not\s*here|held\s*up|late\s*delivery|backorder)\b/.test(
      t
    )
  ) {
    return "delay";
  }
  return null;
}

/**
 * Maps legacy log command / AI classifier labels to canonical issue types.
 */
function resolveIssueType({ logParsedType, classifierType, body }) {
  if (logParsedType) {
    if (logParsedType === "safety") return "safety";
    if (logParsedType === "deficiency") return "deficiency";
    if (logParsedType === "delivery") return "delay";
    if (logParsedType === "delay") return "delay";
    if (logParsedType === "inspection") return "general";
    if (logParsedType === "note") return "general";
    if (logParsedType === "issue") return "general";
    if (logParsedType === "progress") return "general";
    if (logParsedType === "daily_log") return "general";
    return "general";
  }
  const kw = inferIssueTypeFromKeywords(body);
  if (kw) return kw;
  if (classifierType === "safety") return "safety";
  if (classifierType === "deficiency") return "deficiency";
  if (classifierType === "delivery") return "delay";
  if (classifierType === "note") return "general";
  return "general";
}

/** Legacy issueLogs.type values used by the original dashboard */
function legacyTypeForIssueLog(issueType) {
  if (issueType === "safety") return "safety";
  if (issueType === "delay") return "delivery";
  if (issueType === "deficiency") return "issue";
  return "issue";
}

function makeTitleFromBody(body, max = 120) {
  const t = (body || "").trim().replace(/\s+/g, " ");
  if (!t) return "Site issue";
  return t.length <= max ? t : t.slice(0, max - 1) + "…";
}

async function getProjectName(db, projectSlug) {
  if (!projectSlug) return null;
  const snap = await db.collection(COL_PROJECTS).doc(projectSlug).get();
  if (!snap.exists) return null;
  const d = snap.data() || {};
  return d.name || projectSlug;
}

async function createDashboardIssue(db, FieldValue, input) {
  const fields = sanitizeDashboardIssueFields(input);
  validateDashboardCreateFields(fields);

  const changedBy = buildDashboardChangedBy(input && input.operator);
  const issueCollection = COLLECTION_BY_TYPE[fields.type];
  const issueRef = db.collection(issueCollection).doc();
  const issueId = issueRef.id;
  const now = FieldValue.serverTimestamp();
  const projectName =
    fields.projectId
      ? await getProjectName(db, fields.projectId) || fields.projectName || fields.projectId
      : fields.projectName;

  await issueRef.set({
    issueId,
    projectId: fields.projectId,
    projectName: projectName || null,
    issueType: fields.type,
    title: fields.title,
    description: fields.description,
    location: fields.location,
    area: fields.area,
    trade: fields.trade,
    reference: fields.reference,
    requestedAction: fields.requestedAction,
    status: fields.status,
    priority: fields.priority,
    source: "dashboard",
    reportedByPhone: "",
    reportedByName: changedBy,
    assignedTo: fields.assignedTo,
    createdAt: now,
    updatedAt: now,
    closedAt:
      fields.status === "Closed" || fields.status === "Archived"
        ? Timestamp.now()
        : null,
    dueDate: fields.dueDate,
    tags: fields.tags,
    aiSummary: null,
    photos: [],
    relatedMessageId: null,
    relatedConversationId: null,
    history: [
      buildHistoryEntry({
        action: "created",
        changedBy,
        note:
          fields.type === "deficiency"
            ? "Deficiency created from dashboard"
            : "Issue created from dashboard",
      }),
    ],
    isArchived: false,
    createdByUid: normalizeText(input && input.operator && input.operator.uid, 200) || null,
    lastUpdatedByUid: normalizeText(input && input.operator && input.operator.uid, 200) || null,
  });

  return {
    issueId,
    issueCollection,
    issueType: fields.type,
  };
}

async function updateDashboardIssue(db, FieldValue, input) {
  const issueRecord = await getIssueSnapshot(db, input && input.issueCollection, input && input.issueId);
  const fields = sanitizeDashboardIssueFields({
    ...input,
    type: issueRecord.issueType,
  });
  const changedBy = buildDashboardChangedBy(input && input.operator);
  const history = buildDashboardUpdatePatch(issueRecord.issueData, fields, changedBy);
  const prevClosedAt = issueRecord.issueData.closedAt || null;
  const nextClosedAt =
    fields.status === "Closed" || fields.status === "Archived"
      ? prevClosedAt || Timestamp.now()
      : prevClosedAt || null;

  await issueRecord.issueRef.set(
    {
      status: fields.status,
      priority: fields.priority,
      assignedTo: fields.assignedTo,
      dueDate: fields.dueDate,
      location: fields.location,
      area: fields.area,
      trade: fields.trade,
      reference: fields.reference,
      requestedAction: fields.requestedAction,
      description: fields.description,
      closedAt: nextClosedAt,
      history,
      updatedAt: FieldValue.serverTimestamp(),
      lastUpdatedByUid: normalizeText(input && input.operator && input.operator.uid, 200) || null,
    },
    { merge: true }
  );

  return {
    issueId: issueRecord.issueId,
    issueCollection: issueRecord.issueCollection,
  };
}

async function addDashboardIssueNote(db, FieldValue, input) {
  const issueRecord = await getIssueSnapshot(db, input && input.issueCollection, input && input.issueId);
  const note = normalizeLongText(input && input.note, 1000);
  if (!note) throw new Error("note is required.");
  const history = [...(Array.isArray(issueRecord.issueData.history) ? issueRecord.issueData.history : [])];
  history.push(
    buildHistoryEntry({
      action: "note_added",
      changedBy: buildDashboardChangedBy(input && input.operator),
      note,
    })
  );

  await issueRecord.issueRef.set(
    {
      history,
      updatedAt: FieldValue.serverTimestamp(),
      lastUpdatedByUid: normalizeText(input && input.operator && input.operator.uid, 200) || null,
    },
    { merge: true }
  );

  return {
    issueId: issueRecord.issueId,
    issueCollection: issueRecord.issueCollection,
  };
}

function sanitizePhotoInput(photo) {
  const storagePath = normalizeText(photo && photo.storagePath, 500);
  const downloadURL = normalizeText(photo && photo.downloadURL, 2000);
  const fileName = normalizeText(photo && photo.fileName, 240);
  const mimeType = normalizeText(photo && photo.mimeType, 120) || "application/octet-stream";
  if (!storagePath) throw new Error("photo.storagePath is required.");
  if (!downloadURL) throw new Error("photo.downloadURL is required.");
  if (!/^https:\/\//i.test(downloadURL)) throw new Error("photo.downloadURL must be https.");
  return {
    storagePath,
    downloadURL,
    fileName: fileName || storagePath.split("/").pop() || "upload",
    mimeType,
  };
}

async function attachDashboardIssuePhoto(db, FieldValue, input) {
  const issueRecord = await getIssueSnapshot(db, input && input.issueCollection, input && input.issueId);
  const photo = sanitizePhotoInput(input && input.photo);
  const changedBy = buildDashboardChangedBy(input && input.operator);
  const photos = [...(Array.isArray(issueRecord.issueData.photos) ? issueRecord.issueData.photos : [])];
  const history = [...(Array.isArray(issueRecord.issueData.history) ? issueRecord.issueData.history : [])];

  photos.push({
    storagePath: photo.storagePath,
    downloadURL: photo.downloadURL,
    fileName: photo.fileName,
    uploadedAt: Timestamp.now(),
    uploadedBy: changedBy,
    source: "dashboard",
    mimeType: photo.mimeType,
  });
  history.push(
    buildHistoryEntry({
      action: "photo_added",
      changedBy,
      field: "photos",
      newValue: photo.storagePath,
      note: `Photo uploaded: ${photo.fileName}`,
    })
  );

  await issueRecord.issueRef.set(
    {
      photos,
      history,
      updatedAt: FieldValue.serverTimestamp(),
      lastUpdatedByUid: normalizeText(input && input.operator && input.operator.uid, 200) || null,
    },
    { merge: true }
  );

  return {
    issueId: issueRecord.issueId,
    issueCollection: issueRecord.issueCollection,
    photoCount: photos.length,
  };
}

async function deleteDashboardIssue(db, input) {
  const issueRecord = await getIssueSnapshot(db, input && input.issueCollection, input && input.issueId);
  const legacySnap = await db
    .collection(COL_ISSUES_LEGACY)
    .where("canonicalIssueId", "==", issueRecord.issueId)
    .where("issueCollection", "==", issueRecord.issueCollection)
    .get();

  for (const docSnap of legacySnap.docs) {
    await docSnap.ref.delete();
  }
  await issueRecord.issueRef.delete();

  return {
    issueId: issueRecord.issueId,
    issueCollection: issueRecord.issueCollection,
    deletedLegacyCount: legacySnap.size || 0,
  };
}

/**
 * Creates a typed issue + legacy issueLogs row.
 * @returns {{ issueId: string, issueCollection: string, legacyIssueLogId: string }}
 */
async function createSmsIssue(db, FieldValue, input) {
  const {
    phoneE164,
    projectSlug,
    projectName: projectNameIn,
    bodyText,
    rawSms,
    source,
    logParsedType,
    classifierType,
    tags = [],
    aiSummary = null,
    relatedMessageId = null,
    titleOverride = null,
    descriptionOverride = null,
    fieldOverrides = null,
  } = input;

  const issueType = resolveIssueType({
    logParsedType,
    classifierType,
    body: bodyText,
  });

  if (!ISSUE_TYPES.includes(issueType)) {
    throw new Error("Invalid issue type");
  }

  const col = COLLECTION_BY_TYPE[issueType];
  const projectName =
    projectNameIn != null
      ? projectNameIn
      : await getProjectName(db, projectSlug);

  const description = (descriptionOverride || bodyText || "").trim();
  const title =
    (titleOverride && String(titleOverride).trim()) || makeTitleFromBody(description);

  const issueRef = db.collection(col).doc();
  const issueId = issueRef.id;
  const now = FieldValue.serverTimestamp();

  const historyEntry = {
    at: Timestamp.now(),
    action: "created",
    changedBy: phoneE164,
    field: null,
    oldValue: null,
    newValue: null,
    note:
      source === "ai"
        ? "Created from SMS (AI-assisted detection)"
        : "Created from SMS",
  };

  const location = String(fieldOverrides && fieldOverrides.location ? fieldOverrides.location : "").trim();
  const area = String(fieldOverrides && fieldOverrides.area ? fieldOverrides.area : "").trim();
  const trade = String(fieldOverrides && fieldOverrides.trade ? fieldOverrides.trade : "").trim();
  const reference = String(fieldOverrides && fieldOverrides.reference ? fieldOverrides.reference : "").trim();
  const requestedAction = String(
    fieldOverrides && fieldOverrides.requestedAction ? fieldOverrides.requestedAction : ""
  ).trim();

  await issueRef.set({
    issueId,
    projectId: projectSlug || null,
    projectName: projectName || null,
    issueType,
    title,
    description,
    location,
    area,
    trade,
    reference,
    requestedAction,
    status: "Open",
    priority: "Medium",
    source: source === "ai" ? "ai" : "sms",
    reportedByPhone: phoneE164,
    reportedByName: null,
    assignedTo: "",
    createdAt: now,
    updatedAt: now,
    closedAt: null,
    dueDate: null,
    tags: Array.isArray(tags) ? tags : [],
    aiSummary: aiSummary || null,
    photos: [],
    relatedMessageId: relatedMessageId || null,
    relatedConversationId: null,
    history: [historyEntry],
    isArchived: false,
    createdByUid: null,
    lastUpdatedByUid: null,
  });

  const legacyRef = await db.collection(COL_ISSUES_LEGACY).add({
    phoneE164,
    projectSlug: projectSlug || null,
    type: legacyTypeForIssueLog(issueType),
    message: title,
    rawSms: rawSms || bodyText || "",
    tags: Array.isArray(tags) ? tags : [],
    canonicalIssueId: issueId,
    issueCollection: col,
    issueType,
    createdAt: FieldValue.serverTimestamp(),
  });

  return {
    issueId,
    issueCollection: col,
    legacyIssueLogId: legacyRef.id,
    issueType,
  };
}

/**
 * MMS-only stub when media exists but no issue was created from text/AI.
 */
async function createMmsPlaceholderIssue(db, FieldValue, input) {
  const {
    phoneE164,
    projectSlug,
    bodyText,
    rawSms,
    relatedMessageId,
  } = input;

  const projectName = await getProjectName(db, projectSlug);
  const col = COLLECTION_BY_TYPE.general;
  const issueRef = db.collection(col).doc();
  const issueId = issueRef.id;
  const now = FieldValue.serverTimestamp();
  const trimmed = (bodyText || "").trim();
  const title = trimmed
    ? makeTitleFromBody(trimmed, 80)
    : "MMS media (pending review)";
  const description = trimmed
    ? trimmed
    : "Photo(s) received without clear issue text. Review, classify, and add notes.";

  const historyEntry = {
    at: Timestamp.now(),
    action: "created",
    changedBy: phoneE164,
    field: null,
    oldValue: null,
    newValue: null,
    note: "Created from inbound MMS (no typed issue from text)",
  };

  await issueRef.set({
    issueId,
    projectId: projectSlug || null,
    projectName: projectName || null,
    issueType: "general",
    title,
    description,
    location: "",
    area: "",
    trade: "",
    reference: "",
    requestedAction: "",
    status: "Open",
    priority: "Medium",
    source: "sms",
    reportedByPhone: phoneE164,
    reportedByName: null,
    assignedTo: "",
    createdAt: now,
    updatedAt: now,
    closedAt: null,
    dueDate: null,
    tags: ["mms"],
    aiSummary: null,
    photos: [],
    relatedMessageId: relatedMessageId || null,
    relatedConversationId: null,
    history: [historyEntry],
    isArchived: false,
    createdByUid: null,
    lastUpdatedByUid: null,
  });

  await db.collection(COL_ISSUES_LEGACY).add({
    phoneE164,
    projectSlug: projectSlug || null,
    type: "issue",
    message: title,
    rawSms: rawSms || "",
    tags: ["mms"],
    canonicalIssueId: issueId,
    issueCollection: col,
    issueType: "general",
    createdAt: FieldValue.serverTimestamp(),
  });

  return { issueId, issueCollection: col };
}

module.exports = {
  addDashboardIssueNote,
  attachDashboardIssuePhoto,
  buildDashboardChangedBy,
  buildDashboardUpdatePatch,
  buildHistoryEntry,
  createSmsIssue,
  createDashboardIssue,
  createMmsPlaceholderIssue,
  deleteDashboardIssue,
  getIssueSnapshot,
  inferIssueTypeFromKeywords,
  resolveIssueType,
  legacyTypeForIssueLog,
  makeTitleFromBody,
  getProjectName,
  normalizeDueDateInput,
  normalizeIssueCollection,
  normalizeIssuePriority,
  normalizeIssueStatus,
  normalizeIssueType,
  normalizeTags,
  sanitizeDashboardIssueFields,
  sanitizePhotoInput,
  updateDashboardIssue,
  validateDashboardCreateFields,
  COL_ISSUES_LEGACY,
};
