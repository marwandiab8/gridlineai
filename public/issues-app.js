import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import {
  initializeAppCheck,
  ReCaptchaV3Provider,
  ReCaptchaEnterpriseProvider,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app-check.js";
import {
  getAuth,
  GoogleAuthProvider,
  onAuthStateChanged,
  signInWithPopup,
  signOut,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import {
  getFirestore,
  collection,
  query,
  orderBy,
  limit,
  onSnapshot,
  doc,
  getDoc,
  getDocs,
  where,
  documentId,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import {
  getStorage,
  ref,
  uploadBytes,
  getDownloadURL,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-storage.js";
import {
  getFunctions,
  httpsCallable,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-functions.js";

const firebaseConfig = {
  apiKey: "AIzaSyBfUA9JCo01N53TTDzMxnqEqzYqy-RJ6qE",
  authDomain: "gridlineai.firebaseapp.com",
  projectId: "gridlineai",
  storageBucket: "gridlineai.firebasestorage.app",
  messagingSenderId: "118761010772",
  appId: "1:118761010772:web:6eee28ee3c09953de0dfc1",
};

const COLLECTION_BY_TYPE = {
  safety: "safetyIssues",
  delay: "delayIssues",
  deficiency: "deficiencyIssues",
  general: "generalIssues",
};

const ALL_COLLECTIONS = Object.values(COLLECTION_BY_TYPE);

const ISSUE_STATUSES = [
  "Open",
  "In Progress",
  "Pending Review",
  "Waiting on Trade",
  "Waiting on Consultant",
  "Waiting on Owner",
  "Closed",
  "Archived",
];

const ISSUE_PRIORITIES = ["Low", "Medium", "High", "Critical"];
const PRIORITY_ORDER = { Critical: 0, High: 1, Medium: 2, Low: 3 };

const CREATE_MODE_COPY = {
  safety: {
    title: "Create safety issue",
    helper:
      "Capture hazards, incidents, and unsafe conditions clearly enough for immediate follow-up.",
    titlePlaceholder: "Example: Missing guardrail at north stair opening",
    descriptionPlaceholder:
      "Describe the hazard, who is exposed, and what immediate action is required.",
  },
  delay: {
    title: "Create delay issue",
    helper:
      "Record the cause, affected area, and what is blocking or slowing progress.",
    titlePlaceholder: "Example: Flooring crew delayed in Level 3 west wing",
    descriptionPlaceholder:
      "Describe the delay, root cause, impact, and what is needed to recover.",
  },
  deficiency: {
    title: "Create deficiency",
    helper:
      "Write it like a field deficiency: exact location, trade responsible, what is wrong, and what must be corrected.",
    titlePlaceholder: "Example: Drywall patch incomplete at Unit 304 kitchen bulkhead",
    descriptionPlaceholder:
      "Describe the observed deficiency, exact location, quality issue, and required correction.",
  },
  general: {
    title: "Create general issue",
    helper:
      "Use this for site issues that do not fit safety, delay, or deficiency workflows.",
    titlePlaceholder: "Example: Consultant requested follow-up site review",
    descriptionPlaceholder:
      "Describe the issue, context, and what follow-up is needed.",
  },
};

const app = initializeApp(firebaseConfig);
function resolveAppCheckSiteKey() {
  const direct =
    (window.__FIREBASE_APPCHECK_SITE_KEY__ &&
      String(window.__FIREBASE_APPCHECK_SITE_KEY__).trim()) ||
    (window.FIREBASE_APPCHECK_SITE_KEY &&
      String(window.FIREBASE_APPCHECK_SITE_KEY).trim()) ||
    "";
  if (direct) {
    try {
      window.localStorage.setItem("firebaseAppCheckSiteKey", direct);
    } catch (_) {}
    return direct;
  }
  try {
    return String(window.localStorage.getItem("firebaseAppCheckSiteKey") || "").trim();
  } catch (_) {
    return "";
  }
}

function resolveAppCheckProvider() {
  return String(window.FIREBASE_APPCHECK_PROVIDER || "enterprise").trim().toLowerCase();
}

const appCheckSiteKey = resolveAppCheckSiteKey();
const appCheckProvider = resolveAppCheckProvider();
let appCheckInstance = null;
if (appCheckSiteKey) {
  try {
    appCheckInstance = initializeAppCheck(app, {
      provider:
        appCheckProvider === "v3" || appCheckProvider === "recaptchav3"
          ? new ReCaptchaV3Provider(appCheckSiteKey)
          : new ReCaptchaEnterpriseProvider(appCheckSiteKey),
      isTokenAutoRefreshEnabled: true,
    });
  } catch (_) {
    // Ignore duplicate initialization if another script already set App Check.
  }
}
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);
const functions = getFunctions(app, "northamerica-northeast1");
const issueExportCallable = httpsCallable(functions, "issueExportCallable");
const createIssueCallable = httpsCallable(functions, "createIssueCallable");
const updateIssueCallable = httpsCallable(functions, "updateIssueCallable");
const addIssueNoteCallable = httpsCallable(functions, "addIssueNoteCallable");
const attachIssuePhotoCallable = httpsCallable(functions, "attachIssuePhotoCallable");
const deleteIssueCallable = httpsCallable(functions, "deleteIssueCallable");
const getDashboardAccessCallable = httpsCallable(functions, "getDashboardAccessCallable");
const listAccessibleIssuesCallable = httpsCallable(functions, "listAccessibleIssuesCallable");

const collectionsData = {
  safetyIssues: [],
  delayIssues: [],
  deficiencyIssues: [],
  generalIssues: [],
};

let unsubscribers = [];
let currentTab = "all";
let projectOptions = [];
let currentAccess = null;
let issueDocsByCollectionScope = {};
let authBootstrapPromise = null;

function resetIssueScopeCache() {
  issueDocsByCollectionScope = {};
  for (const col of ALL_COLLECTIONS) {
    issueDocsByCollectionScope[col] = {};
  }
}

resetIssueScopeCache();

function esc(value) {
  if (value == null) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function byId(id) {
  return document.getElementById(id);
}

function setAuthBusy(isBusy, message = "") {
  const btn = byId("authSignInGoogle");
  const authError = byId("authError");
  if (btn) {
    btn.disabled = !!isBusy;
    btn.textContent = isBusy ? "Signing in..." : "Sign in with Google";
  }
  if (authError) {
    authError.textContent = message || "";
  }
}

function formatUiError(err) {
  if (!err) return "Unknown error.";
  if (typeof err.message === "string" && err.message.trim()) return err.message.trim();
  if (typeof err.code === "string" && err.code.trim()) return err.code.trim();
  return String(err);
}

async function loadIssueBoardAccess() {
  const res = await getDashboardAccessCallable({});
  return res && res.data ? res.data : null;
}

async function loadAccessibleIssues() {
  const res = await listAccessibleIssuesCallable({});
  return res && res.data ? res.data : null;
}

async function refreshScopedIssuesIfNeeded() {
  if (!usesScopedIssueCallableMode()) return;
  const data = await loadAccessibleIssues();
  const collections = data && data.collections ? data.collections : {};
  for (const col of ALL_COLLECTIONS) {
    collectionsData[col] = Array.isArray(collections[col]) ? collections[col] : [];
  }
  renderIssuesList();
}

async function bootstrapSignedInUser(user) {
  if (!user) return;
  if (authBootstrapPromise) return authBootstrapPromise;
  authBootstrapPromise = (async () => {
    const authPanel = byId("authPanel");
    const appPanel = byId("appPanel");
    const authError = byId("authError");
    try {
      setAuthBusy(true, "Completing sign-in...");
      await user.getIdToken(true);
      currentAccess = await loadIssueBoardAccess();
      authPanel.classList.add("hidden");
      appPanel.classList.remove("hidden");
      if (authError) authError.textContent = "";
      setAuthBusy(false);
      byId("userLabel").textContent =
        user.displayName && user.email
          ? `${user.displayName} (${user.email})`
          : user.email || user.displayName || user.uid;
      populateStatusSelects();
      updateCreateFormState();
      await loadProjectsIntoSelects();
      const exportRow = byId("issueExportRow");
      if (exportRow) {
        exportRow.classList.toggle("hidden", !roleAtLeast(currentAccess?.role || "viewer", "management"));
      }
      startListeners();
    } catch (err) {
      stopListeners();
      currentAccess = null;
      setAuthBusy(false);
      authPanel.classList.remove("hidden");
      appPanel.classList.add("hidden");
      if (authError) {
        const rawMessage =
          formatUiError(err) || "Your account is signed in but not approved for issue access yet.";
        const withHint =
          /missing or insufficient permissions/i.test(rawMessage) && !appCheckSiteKey
            ? `${rawMessage} (If App Check enforcement is enabled, set FIREBASE_APPCHECK_SITE_KEY in this page.)`
            : rawMessage;
        authError.textContent = withHint;
      }
    } finally {
      authBootstrapPromise = null;
    }
  })();
  return authBootstrapPromise;
}

function sanitizeStorageSegment(value, fallback = "file") {
  const cleaned = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120);
  return cleaned || fallback;
}

function issuePhotoStoragePath(issue, file) {
  const projectId = sanitizeStorageSegment(issue.projectId || "_shared", "_shared");
  const issueId = sanitizeStorageSegment(issue.issueId || issue.id || "issue", "issue");
  const fileName = sanitizeStorageSegment(file && file.name ? file.name : "upload", "upload");
  return `projects/${projectId}/media/issues/${issueId}/${Date.now()}-${fileName}`;
}

async function uploadIssuePhotoFile(issue, file) {
  const storagePath = issuePhotoStoragePath(issue, file);
  const storageRef = ref(storage, storagePath);
  await uploadBytes(storageRef, file, {
    contentType: file && file.type ? file.type : "application/octet-stream",
  });
  const downloadURL = await getDownloadURL(storageRef);
  return {
    storagePath,
    downloadURL,
    fileName: file && file.name ? file.name : storagePath.split("/").pop() || "upload",
    mimeType: file && file.type ? file.type : "application/octet-stream",
  };
}

function fmtTime(ts) {
  if (!ts) return "";
  try {
    if (typeof ts.toDate === "function") return ts.toDate().toLocaleString();
    if (ts.seconds) return new Date(ts.seconds * 1000).toLocaleString();
  } catch (_) {}
  return esc(String(ts));
}

function toDateObject(ts) {
  if (!ts) return null;
  try {
    if (typeof ts.toDate === "function") return ts.toDate();
    if (ts.seconds) return new Date(ts.seconds * 1000);
  } catch (_) {}
  return null;
}

function tsToInputDate(ts) {
  const d = toDateObject(ts);
  return d ? d.toISOString().slice(0, 10) : "";
}

function formatShortDate(ts) {
  const d = toDateObject(ts);
  return d ? d.toLocaleDateString() : "";
}

function downloadBase64File(b64, mime, filename) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const blob = new Blob([bytes], { type: mime });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

function mergeAllIssues() {
  const out = [];
  for (const col of ALL_COLLECTIONS) {
    for (const row of collectionsData[col]) out.push(row);
  }
  return out;
}

function tabCollections() {
  return currentTab === "all" ? ALL_COLLECTIONS : [COLLECTION_BY_TYPE[currentTab]];
}

function roleRank(role) {
  const order = ["viewer", "user", "management", "admin"];
  return order.indexOf(String(role || "").trim().toLowerCase());
}

function roleAtLeast(role, minimum) {
  return roleRank(role) >= roleRank(minimum);
}

/** Match server `normalizeProjectSlug` so Firestore `projectId` aligns with scoped `in` queries. */
function normalizeIssueProjectKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function currentProjectScope() {
  const base = Array.isArray(currentAccess?.projectSlugs) ? currentAccess.projectSlugs : [];
  const raw = Array.isArray(currentAccess?.rawProjectSlugs) ? currentAccess.rawProjectSlugs : [];
  const expanded = [];
  for (const s of base) {
    const t = String(s || "").trim();
    if (t) expanded.push(t);
  }
  for (const r of raw) {
    const t = String(r || "").trim();
    if (!t) continue;
    if (!expanded.includes(t)) expanded.push(t);
    const n = normalizeIssueProjectKey(t);
    if (n && !expanded.includes(n)) expanded.push(n);
  }
  const slugs = [...new Set(expanded.map((x) => String(x || "").trim()).filter(Boolean))].slice(0, 40);
  return {
    isAdmin: roleAtLeast(currentAccess?.role || "viewer", "admin"),
    isManagement: roleAtLeast(currentAccess?.role || "viewer", "management"),
    allProjects: currentAccess?.allProjects === true,
    projectSlugs: slugs,
  };
}

function usesScopedIssueCallableMode() {
  return true;
}

function chunkArray(values, size) {
  const chunks = [];
  for (let i = 0; i < values.length; i += size) {
    chunks.push(values.slice(i, i + size));
  }
  return chunks;
}

function issueTime(row) {
  const createdAt = row.createdAt;
  if (createdAt && typeof createdAt.toMillis === "function") return createdAt.toMillis();
  if (createdAt && createdAt.seconds) return createdAt.seconds * 1000;
  return 0;
}

function isOpenStatus(status) {
  return !["Closed", "Archived"].includes(String(status || ""));
}

function isOverdue(issue) {
  if (!isOpenStatus(issue.status)) return false;
  const due = toDateObject(issue.dueDate);
  if (!due) return false;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return due.getTime() < today.getTime();
}

function ageInDays(issue) {
  const created = toDateObject(issue.createdAt);
  if (!created) return null;
  return Math.max(0, Math.floor((Date.now() - created.getTime()) / 86400000));
}

function getFilteredIssues() {
  const cols = tabCollections();
  let rows = [];
  for (const col of cols) rows = rows.concat(collectionsData[col]);

  const search = (byId("filterSearch")?.value || "").trim().toLowerCase();
  const project = byId("filterProject")?.value || "";
  const status = byId("filterStatus")?.value || "";
  const assigned = byId("filterAssigned")?.value || "";
  const dateFrom = byId("filterDateFrom")?.value || "";
  const dateTo = byId("filterDateTo")?.value || "";
  const sort = byId("filterSort")?.value || "newest";

  let filtered = rows;
  if (project) filtered = filtered.filter((row) => (row.projectId || "") === project);
  if (status) filtered = filtered.filter((row) => row.status === status);
  if (assigned) {
    filtered = filtered.filter(
      (row) => (row.assignedTo || "").toLowerCase() === assigned.toLowerCase()
    );
  }
  if (dateFrom) {
    const t = new Date(dateFrom).getTime();
    filtered = filtered.filter((row) => issueTime(row) >= t);
  }
  if (dateTo) {
    const t = new Date(dateTo).getTime() + 86400000;
    filtered = filtered.filter((row) => issueTime(row) <= t);
  }
  if (search) {
    filtered = filtered.filter((row) => {
      const blob = [
        row.title,
        row.description,
        row.location,
        row.area,
        row.trade,
        row.reference,
        row.requestedAction,
        row.projectName,
        row.reportedByPhone,
        row.assignedTo,
        row.issueId,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return blob.includes(search);
    });
  }

  const copy = [...filtered];
  if (sort === "newest") {
    copy.sort((a, b) => issueTime(b) - issueTime(a));
  } else if (sort === "oldest") {
    copy.sort((a, b) => issueTime(a) - issueTime(b));
  } else if (sort === "priority") {
    copy.sort((a, b) => (PRIORITY_ORDER[a.priority] ?? 9) - (PRIORITY_ORDER[b.priority] ?? 9));
  } else if (sort === "status") {
    copy.sort((a, b) => String(a.status || "").localeCompare(String(b.status || "")));
  }
  return copy;
}

function renderIssueOverview(rows) {
  const el = byId("issueOverview");
  if (!el) return;
  if (!rows.length) {
    el.innerHTML = '<div class="issue-overview-empty muted small">No matching issues.</div>';
    return;
  }

  const counts = {
    Open: 0,
    "In Progress": 0,
    "Pending Review": 0,
    Closed: 0,
  };
  let overdue = 0;
  let withPhotos = 0;
  for (const row of rows) {
    if (counts[row.status] != null) counts[row.status] += 1;
    if (isOverdue(row)) overdue += 1;
    if (Array.isArray(row.photos) && row.photos.length) withPhotos += 1;
  }

  const cards = [
    ["Open", counts.Open],
    ["In progress", counts["In Progress"]],
    ["Pending review", counts["Pending Review"]],
    ["Closed", counts.Closed],
    ["Overdue", overdue],
    ["With photos", withPhotos],
  ];

  el.innerHTML = cards
    .map(
      ([label, count]) => `
        <div class="issue-overview-card">
          <span class="issue-overview-label">${esc(label)}</span>
          <strong class="issue-overview-value">${esc(String(count))}</strong>
        </div>`
    )
    .join("");
}

function renderIssuesList() {
  const el = byId("issuesList");
  const countEl = byId("issuesCount");
  const rows = getFilteredIssues();
  renderIssueOverview(rows);

  const assignedSet = new Set();
  mergeAllIssues().forEach((row) => {
    if (row.assignedTo && String(row.assignedTo).trim()) {
      assignedSet.add(String(row.assignedTo).trim());
    }
  });

  const assignedSelect = byId("filterAssigned");
  if (!assignedSelect) return;
  const previousAssigned = assignedSelect.value;
  const opts = ['<option value="">Assigned to (any)</option>'];
  [...assignedSet].sort().forEach((assigned) => {
    opts.push(`<option value="${esc(assigned)}">${esc(assigned)}</option>`);
  });
  assignedSelect.innerHTML = opts.join("");
  if ([...assignedSet].includes(previousAssigned)) assignedSelect.value = previousAssigned;

  countEl.textContent = `${rows.length} issue(s) shown (tab + filters).`;
  if (!rows.length) {
    el.innerHTML = '<div class="muted">No issues match.</div>';
    return;
  }

  el.innerHTML = rows
    .map((row) => {
      const photos = Array.isArray(row.photos) ? row.photos : [];
      const src = row.source === "sms" || row.source === "ai" ? "SMS" : "Dash";
      const dueLabel = formatShortDate(row.dueDate);
      const overduePill = isOverdue(row) ? '<span class="pill pill-warn">Overdue</span>' : "";
      const age = ageInDays(row);
      const ageLabel = age == null ? "" : `${age}d open`;
      const locationBits = [
        row.projectName || row.projectId || "—",
        row.location || "",
        row.area || "",
      ].filter(Boolean);
      const detailBits = [
        row.trade ? `Trade: ${row.trade}` : "",
        row.reference ? `Ref: ${row.reference}` : "",
        row.requestedAction ? `Action: ${row.requestedAction}` : "",
      ].filter(Boolean);
      const assignmentBits = [
        `Assigned: ${row.assignedTo || "—"}`,
        dueLabel ? `Due: ${dueLabel}` : "",
        ageLabel,
      ].filter(Boolean);
      const excerpt = (row.description || "").trim();
      return `
        <div class="issue-row" data-col="${esc(row.issueCollection)}" data-id="${esc(row.id)}">
          <div class="issue-row-top">
            <span class="pill pill-type">${esc(row.issueType || "")}</span>
            <span class="pill pill-status">${esc(row.status || "")}</span>
            <span class="pill pill-priority">${esc(row.priority || "")}</span>
            <span class="pill pill-source">${esc(src)}</span>
            ${overduePill}
            ${photos.length ? `<span class="pill pill-photo">Photos ${photos.length}</span>` : ""}
          </div>
          <div class="issue-title">${esc(row.title || "")}</div>
          <div class="muted small">${esc(locationBits.join(" · ") || "—")}</div>
          ${detailBits.length ? `<div class="muted small">${esc(detailBits.join(" · "))}</div>` : ""}
          ${excerpt ? `<div class="issue-excerpt">${esc(excerpt.slice(0, 180))}${excerpt.length > 180 ? "..." : ""}</div>` : ""}
          <div class="muted small">${esc(assignmentBits.join(" · ") || "Assigned: —")} · ${fmtTime(row.createdAt)}</div>
        </div>`;
    })
    .join("");

  el.querySelectorAll(".issue-row").forEach((rowEl) => {
    rowEl.addEventListener("click", () => {
      openDetail(rowEl.getAttribute("data-col"), rowEl.getAttribute("data-id"));
    });
  });
}

function populateStatusSelects() {
  const filterStatus = byId("filterStatus");
  const createStatus = byId("createStatus");
  const options = ['<option value="">All statuses</option>']
    .concat(ISSUE_STATUSES.map((status) => `<option value="${esc(status)}">${esc(status)}</option>`))
    .join("");
  filterStatus.innerHTML = options;
  createStatus.innerHTML = ISSUE_STATUSES.map(
    (status) => `<option value="${esc(status)}">${esc(status)}</option>`
  ).join("");
  byId("createPriority").innerHTML = ISSUE_PRIORITIES.map(
    (priority) => `<option value="${esc(priority)}">${esc(priority)}</option>`
  ).join("");
  createStatus.value = "Open";
  byId("createPriority").value = "Medium";
}

async function loadProjectsIntoSelects() {
  const { isAdmin, allProjects, projectSlugs } = currentProjectScope();
  const docs = [];
  const fallbackOptions = new Map();
  for (const slug of projectSlugs) {
    const clean = String(slug || "").trim();
    if (!clean) continue;
    fallbackOptions.set(clean, { id: clean, name: clean });
  }
  if (isAdmin || allProjects) {
    const snap = await getDocs(collection(db, "projects"));
    docs.push(...snap.docs);
  } else if (projectSlugs.length) {
    try {
      const slugChunks = chunkArray(projectSlugs, 10);
      for (const slugChunk of slugChunks) {
        const snap = await getDocs(
          query(collection(db, "projects"), where(documentId(), "in", slugChunk))
        );
        docs.push(...snap.docs);
      }
    } catch (_) {
      // Scoped users can still work from callable-provided slugs even if project docs are not readable.
    }
  }
  const deduped = new Map();
  for (const docSnap of docs) {
    deduped.set(docSnap.id, docSnap);
  }
  const fetchedOptions = Array.from(deduped.values()).map((docSnap) => ({
    id: docSnap.id,
    name: (docSnap.data() && docSnap.data().name) || docSnap.id,
  }));
  fetchedOptions.forEach((project) => fallbackOptions.set(project.id, project));
  projectOptions = Array.from(fallbackOptions.values());
  const baseOption = '<option value="">— None / TBD —</option>';
  const options = projectOptions
    .map((project) => `<option value="${esc(project.id)}">${esc(project.name)} (${esc(project.id)})</option>`)
    .join("");
  byId("filterProject").innerHTML =
    '<option value="">All projects</option>' +
    projectOptions.map((project) => `<option value="${esc(project.id)}">${esc(project.name)}</option>`).join("");
  byId("createProject").innerHTML = baseOption + options;
}

function startListeners() {
  stopListeners();
  resetIssueScopeCache();
  if (usesScopedIssueCallableMode()) {
    void loadAccessibleIssues()
      .then((data) => {
        const collections = data && data.collections ? data.collections : {};
        for (const col of ALL_COLLECTIONS) {
          collectionsData[col] = Array.isArray(collections[col]) ? collections[col] : [];
        }
        const total = mergeAllIssues().length;
        const hint = byId("issuesCount");
        if (hint) {
          hint.textContent = `${total} issue(s) loaded for your assigned projects.`;
        }
        renderIssuesList();
      })
      .catch((err) => {
        const hint = byId("issuesCount");
        if (hint) {
          hint.textContent = `Could not load scoped issues: ${formatUiError(err)}`;
        }
      });
    return;
  }
  const { isAdmin, allProjects, projectSlugs } = currentProjectScope();
  const scopes = isAdmin || allProjects ? [null] : chunkArray(projectSlugs, 10);
  if (!scopes.length) {
    for (const col of ALL_COLLECTIONS) {
      collectionsData[col] = [];
    }
    const hint = byId("issuesCount");
    if (hint) {
      hint.textContent =
        "No project scope for your account, so issues are not loaded. Ask an admin to assign projects on your app member profile, or ensure your approved field phone has an active project in smsUsers.";
    }
    renderIssuesList();
    return;
  }
  for (const col of ALL_COLLECTIONS) {
    scopes.forEach((scopeSlugs) => {
      const variants = scopeSlugs ? ["projectId", "projectSlug"] : [null];
      variants.forEach((fieldName) => {
        const scopeKey = scopeSlugs
          ? `${fieldName}:${scopeSlugs.join("|")}`
          : "__all__";
        const issueQuery = !scopeSlugs
          ? query(collection(db, col), orderBy("createdAt", "desc"), limit(200))
          : query(
              collection(db, col),
              where(fieldName, "in", scopeSlugs),
              orderBy("createdAt", "desc"),
              limit(200)
            );
        const unsub = onSnapshot(
          issueQuery,
          (snap) => {
            issueDocsByCollectionScope[col][scopeKey] = snap.docs.map((docSnap) => ({
              id: docSnap.id,
              issueCollection: col,
              ...docSnap.data(),
            }));
            const merged = new Map();
            Object.values(issueDocsByCollectionScope[col]).forEach((rows) => {
              rows.forEach((row) => merged.set(row.id, row));
            });
            collectionsData[col] = Array.from(merged.values()).sort((a, b) => issueTime(b) - issueTime(a));
            renderIssuesList();
          },
          (err) => {
            issueDocsByCollectionScope[col][scopeKey] = [];
            const merged = new Map();
            Object.values(issueDocsByCollectionScope[col]).forEach((rows) => {
              rows.forEach((row) => merged.set(row.id, row));
            });
            collectionsData[col] = Array.from(merged.values()).sort((a, b) => issueTime(b) - issueTime(a));
            renderIssuesList();

            const isOptionalProjectSlugVariant = fieldName === "projectSlug" && Array.isArray(scopeSlugs);
            if (isOptionalProjectSlugVariant) {
              return;
            }

            const el = byId("issuesCount");
            if (el) {
              el.textContent = `Could not load ${col}: ${err.message}`;
            }
          }
        );
        unsubscribers.push(unsub);
      });
    });
  }
}

function stopListeners() {
  unsubscribers.forEach((unsub) => unsub());
  unsubscribers = [];
  resetIssueScopeCache();
}

function detailProjectLabel(issue) {
  return issue.projectName || issue.projectId || "—";
}

function findLoadedIssue(col, id) {
  const rows = Array.isArray(collectionsData[col]) ? collectionsData[col] : [];
  return rows.find((row) => String(row.id) === String(id)) || null;
}

function buildDetailHistory(history) {
  if (!history.length) return '<div class="muted">No history.</div>';
  return history
    .slice()
    .reverse()
    .map((entry) => {
      const bits = [];
      if (entry.action) bits.push(entry.action);
      if (entry.field) bits.push(`${entry.field}`);
      if (entry.note) bits.push(entry.note);
      return `<div class="hist-line"><span class="mono">${esc(fmtTime(entry.at))}</span> ${esc(bits.join(" · "))}</div>`;
    })
    .join("");
}

function buildPhotoGrid(photos) {
  if (!photos.length) return '<span class="muted">No photos yet.</span>';
  return photos
    .map((photo) => {
      const href = photo.downloadURL || "#";
      const caption = [photo.fileName, photo.uploadedBy].filter(Boolean).join(" · ");
      return `
        <a href="${esc(href)}" target="_blank" rel="noopener" class="detail-photo-card">
          <img src="${esc(href)}" alt="" />
          ${caption ? `<span class="detail-photo-caption">${esc(caption)}</span>` : ""}
        </a>`;
    })
    .join("");
}

async function openDetail(col, id) {
  let issue = null;
  if (usesScopedIssueCallableMode()) {
    issue = findLoadedIssue(col, id);
  }
  if (!issue) {
    const issueRef = doc(db, col, id);
    const snap = await getDoc(issueRef);
    if (!snap.exists()) return;
    issue = { id: snap.id, issueCollection: col, ...snap.data() };
  }
  const backdrop = byId("detailBackdrop");
  const body = byId("detailBody");
  const photos = Array.isArray(issue.photos) ? issue.photos : [];
  const history = Array.isArray(issue.history) ? issue.history : [];

  body.innerHTML = `
    <div class="detail-header">
      <div>
        <h2 id="dTitleHeading"></h2>
        <p class="muted small" id="dSub"></p>
      </div>
      <div class="detail-meta">
        <div><strong>ID:</strong> <span id="dIssueId"></span></div>
        <div><strong>Project:</strong> <span id="dProjectName"></span></div>
        <div><strong>Created:</strong> <span id="dCreatedAt"></span></div>
      </div>
    </div>

    <div class="detail-grid">
      <label>Status</label>
      <select id="dStatus">${ISSUE_STATUSES.map((status) => `<option value="${esc(status)}">${esc(status)}</option>`).join("")}</select>
      <label>Priority</label>
      <select id="dPriority">${ISSUE_PRIORITIES.map((priority) => `<option value="${esc(priority)}">${esc(priority)}</option>`).join("")}</select>
      <label>Assigned to</label>
      <input type="text" id="dAssigned" />
      <label>Due date</label>
      <input type="date" id="dDue" />
      <label>Location</label>
      <input type="text" id="dLocation" />
      <label>Area / room</label>
      <input type="text" id="dArea" />
      <label>Responsible trade</label>
      <input type="text" id="dTrade" />
      <label>Reference / spec</label>
      <input type="text" id="dReference" />
      <label class="full">Required action</label>
      <input type="text" class="full" id="dAction" />
      <label class="full">Description</label>
      <textarea class="full" id="dDesc" rows="5"></textarea>
    </div>

    <div class="detail-actions">
      <button type="button" class="btn-primary" id="dSave">Save changes</button>
      <button type="button" class="btn-secondary btn-danger" id="dDelete">Delete issue</button>
      <button type="button" class="btn-secondary" id="dPdf">Download PDF</button>
      <button type="button" class="btn-secondary" id="dPdfXlsx">Download Excel</button>
      <button type="button" class="btn-secondary" id="dPrint">Print</button>
    </div>

    <h3>Photos</h3>
    <div class="photo-grid">${buildPhotoGrid(photos)}</div>
    <div class="form-row full">
      <label>Add photos</label>
      <input type="file" id="dPhotos" accept="image/*" multiple />
    </div>

    <h3>Add note</h3>
    <textarea id="dNote" rows="2" placeholder="Coordination note, trade follow-up, or review comment"></textarea>
    <button type="button" class="btn-secondary" id="dAddNote">Add note</button>

    <h3>History</h3>
    <div class="history-scroll" id="dHist">${buildDetailHistory(history)}</div>
  `;

  byId("dTitleHeading").textContent = issue.title || "";
  byId("dSub").textContent = `${issue.issueType || ""} · ${issue.status || ""} · ${issue.priority || ""}`;
  byId("dIssueId").textContent = issue.issueId || issue.id;
  byId("dProjectName").textContent = detailProjectLabel(issue);
  byId("dCreatedAt").textContent = fmtTime(issue.createdAt);
  byId("dStatus").value = issue.status || "Open";
  byId("dPriority").value = issue.priority || "Medium";
  byId("dAssigned").value = issue.assignedTo || "";
  byId("dDue").value = tsToInputDate(issue.dueDate);
  byId("dLocation").value = issue.location || "";
  byId("dArea").value = issue.area || "";
  byId("dTrade").value = issue.trade || "";
  byId("dReference").value = issue.reference || "";
  byId("dAction").value = issue.requestedAction || "";
  byId("dDesc").value = issue.description || "";
  byId("dDelete").classList.toggle(
    "hidden",
    !roleAtLeast(currentAccess?.role || "viewer", "management")
  );

  backdrop.classList.remove("hidden");

  byId("dSave").onclick = async () => {
    try {
      const payload = {
        issueCollection: col,
        issueId: id,
        status: byId("dStatus").value,
        priority: byId("dPriority").value,
        assignedTo: byId("dAssigned").value.trim(),
        dueDate: byId("dDue").value || "",
        location: byId("dLocation").value.trim(),
        area: byId("dArea").value.trim(),
        trade: byId("dTrade").value.trim(),
        reference: byId("dReference").value.trim(),
        requestedAction: byId("dAction").value.trim(),
        description: byId("dDesc").value.trim(),
      };
      await updateIssueCallable(payload);
      await refreshScopedIssuesIfNeeded();
      await openDetail(col, id);
    } catch (err) {
      window.alert(`Failed to save issue: ${formatUiError(err)}`);
    }
  };

  byId("dAddNote").onclick = async () => {
    const note = byId("dNote").value.trim();
    if (!note) return;
    try {
      await addIssueNoteCallable({
        issueCollection: col,
        issueId: id,
        note,
      });
      byId("dNote").value = "";
      await refreshScopedIssuesIfNeeded();
      await openDetail(col, id);
    } catch (err) {
      window.alert(`Failed to add note: ${formatUiError(err)}`);
    }
  };

  byId("dPhotos").onchange = async (event) => {
    const files = event.target.files;
    if (!files || !files.length) return;

    try {
      for (const file of files) {
        const photo = await uploadIssuePhotoFile(issue, file);
        await attachIssuePhotoCallable({
          issueCollection: col,
          issueId: id,
          photo,
        });
      }
      event.target.value = "";
      await refreshScopedIssuesIfNeeded();
      await openDetail(col, id);
    } catch (err) {
      window.alert(`Failed to attach photo: ${formatUiError(err)}`);
    }
  };

  byId("dDelete").onclick = async () => {
    const issueLabel = issue.title || issue.issueId || issue.id || "this issue";
    const confirmed = window.confirm(`Delete "${issueLabel}"? This cannot be undone.`);
    if (!confirmed) return;
    try {
      await deleteIssueCallable({
        issueCollection: col,
        issueId: id,
      });
      backdrop.classList.add("hidden");
      await refreshScopedIssuesIfNeeded();
      renderIssuesList();
    } catch (err) {
      window.alert(`Failed to delete issue: ${formatUiError(err)}`);
    }
  };

  const exportOne = async (format) => {
    if (!roleAtLeast(currentAccess?.role || "viewer", "management")) {
      window.alert("Exports are available to management and administrators only.");
      return;
    }
    const res = await issueExportCallable({
      mode: "single",
      format,
      issueCollection: col,
      issueId: id,
      companyName: "Construction site",
    });
    const data = res.data;
    downloadBase64File(data.fileBase64, data.mimeType, data.filename);
  };

  byId("dPdf").onclick = () => exportOne("pdf");
  byId("dPdfXlsx").onclick = () => exportOne("xlsx");
  byId("dPrint").onclick = () => window.print();
}

function selectedCreateType() {
  return byId("createType")?.value || "general";
}

function updateCreateFormState() {
  const type = selectedCreateType();
  const cfg = CREATE_MODE_COPY[type] || CREATE_MODE_COPY.general;
  if (byId("createCardTitle")) byId("createCardTitle").textContent = cfg.title;
  if (byId("createHelper")) byId("createHelper").textContent = cfg.helper;
  if (byId("createTitle")) byId("createTitle").placeholder = cfg.titlePlaceholder;
  if (byId("createDescription")) byId("createDescription").placeholder = cfg.descriptionPlaceholder;
}

function clearCreateForm() {
  [
    "createTitle",
    "createDescription",
    "createLocation",
    "createArea",
    "createTrade",
    "createReference",
    "createAction",
    "createAssigned",
    "createDue",
    "createTags",
  ].forEach((id) => {
    const input = byId(id);
    if (!input) return;
    input.value = "";
  });
  if (byId("createPhotos")) byId("createPhotos").value = "";
}

function validateCreateForm(payload) {
  if (!payload.title || !payload.description) {
    return "Title and description are required.";
  }
  const scope = currentProjectScope();
  if (!scope.isAdmin && !scope.allProjects && !payload.projectId) {
    return "Select a project for this issue.";
  }
  if (payload.type === "deficiency") {
    if (!payload.projectId) return "Select a project before creating a deficiency.";
    if (!payload.location && !payload.area) {
      return "Add a useful location or area / room for the deficiency.";
    }
    if (!payload.requestedAction) {
      return "Describe the required action so the deficiency is clear and actionable.";
    }
  }
  return "";
}

function buildExportFilters() {
  const filters = {};
  const project = byId("filterProject")?.value || "";
  const status = byId("filterStatus")?.value || "";
  const assigned = byId("filterAssigned")?.value || "";
  const dateFrom = byId("filterDateFrom")?.value || "";
  const dateTo = byId("filterDateTo")?.value || "";
  const search = byId("filterSearch")?.value || "";
  if (project) filters.projectSlug = project;
  if (status) filters.status = status;
  if (assigned) filters.assignedTo = assigned;
  if (dateFrom) filters.dateFrom = dateFrom;
  if (dateTo) filters.dateTo = dateTo;
  if (search) filters.search = search;
  if (currentTab !== "all") filters.issueType = currentTab;
  return filters;
}

async function runListExport(format) {
  if (!roleAtLeast(currentAccess?.role || "viewer", "management")) {
    byId("issuesCount").textContent = "Exports are available to management and administrators only.";
    return;
  }
  const filters = buildExportFilters();
  const scope = currentTab === "all" ? "all" : "typed";
  const issueType = currentTab === "all" ? "general" : currentTab;
  const res = await issueExportCallable({
    mode: "list",
    scope,
    issueType,
    format,
    filters,
    companyName: "Construction site",
    maxPerCollection: 800,
  });
  const data = res.data;
  downloadBase64File(data.fileBase64, data.mimeType, data.filename);
}

byId("detailClose").onclick = () => {
  byId("detailBackdrop").classList.add("hidden");
};

byId("detailBackdrop").onclick = (event) => {
  if (event.target.id === "detailBackdrop") {
    byId("detailBackdrop").classList.add("hidden");
  }
};

byId("tabRow").addEventListener("click", (event) => {
  const btn = event.target.closest(".tab-btn");
  if (!btn) return;
  currentTab = btn.getAttribute("data-tab");
  document.querySelectorAll(".tab-btn").forEach((tabBtn) => tabBtn.classList.remove("active"));
  btn.classList.add("active");
  if (currentTab !== "all" && byId("createType")) {
    byId("createType").value = currentTab;
    updateCreateFormState();
  }
  renderIssuesList();
});

["filterSearch", "filterProject", "filterStatus", "filterAssigned", "filterDateFrom", "filterDateTo", "filterSort"].forEach((id) => {
  byId(id).addEventListener("input", () => renderIssuesList());
  byId(id).addEventListener("change", () => renderIssuesList());
});

byId("createType").addEventListener("change", () => {
  updateCreateFormState();
});

byId("authSignInGoogle").onclick = async () => {
  setAuthBusy(true);
  const provider = new GoogleAuthProvider();
  provider.setCustomParameters({ prompt: "select_account" });
  try {
    const result = await signInWithPopup(auth, provider);
    if (result && result.user) {
      await bootstrapSignedInUser(result.user);
    } else {
      setAuthBusy(false, "Sign-in completed, waiting for session...");
    }
  } catch (err) {
    setAuthBusy(false);
    if (err.code === "auth/popup-closed-by-user") {
      byId("authError").textContent = "Sign-in was cancelled.";
      return;
    }
    byId("authError").textContent = formatUiError(err);
  }
};

byId("authSignOut").onclick = () => signOut(auth);

byId("createSubmit").onclick = async () => {
  const msg = byId("createMsg");
  msg.textContent = "";
  msg.className = "form-msg";

  const type = selectedCreateType();
  const payload = {
    type,
    projectId: byId("createProject").value || null,
    projectName:
      projectOptions.find((project) => project.id === (byId("createProject").value || ""))?.name ||
      byId("createProject").value ||
      null,
    title: byId("createTitle").value.trim(),
    description: byId("createDescription").value.trim(),
    location: byId("createLocation").value.trim(),
    area: byId("createArea").value.trim(),
    trade: byId("createTrade").value.trim(),
    reference: byId("createReference").value.trim(),
    requestedAction: byId("createAction").value.trim(),
    status: byId("createStatus").value,
    priority: byId("createPriority").value,
    assignedTo: byId("createAssigned").value.trim(),
    dueRaw: byId("createDue").value,
    tagsRaw: byId("createTags").value.trim(),
  };

  const validationError = validateCreateForm(payload);
  if (validationError) {
    msg.textContent = validationError;
    msg.className = "form-error";
    return;
  }

  const tags = payload.tagsRaw
    ? payload.tagsRaw.split(",").map((tag) => tag.trim()).filter(Boolean)
    : [];
  const createRes = await createIssueCallable({
    type,
    projectId: payload.projectId,
    projectName: payload.projectName,
    title: payload.title,
    description: payload.description,
    location: payload.location,
    area: payload.area,
    trade: payload.trade,
    reference: payload.reference,
    requestedAction: payload.requestedAction,
    status: payload.status,
    priority: payload.priority,
    assignedTo: payload.assignedTo,
    dueDate: payload.dueRaw || "",
    tags,
  });
  const created = createRes.data;

  const files = byId("createPhotos").files;
  if (files && files.length) {
    const issueForUpload = {
      id: created.issueId,
      issueId: created.issueId,
      projectId: payload.projectId,
    };
    for (const file of files) {
      const photo = await uploadIssuePhotoFile(issueForUpload, file);
      await attachIssuePhotoCallable({
        issueCollection: created.issueCollection,
        issueId: created.issueId,
        photo,
      });
    }
  }

  msg.textContent = type === "deficiency" ? "Deficiency created." : "Issue created.";
  clearCreateForm();
  updateCreateFormState();
  await refreshScopedIssuesIfNeeded();
};

byId("exportXlsx").onclick = () => runListExport("xlsx");
byId("exportPdf").onclick = () => runListExport("pdf");

onAuthStateChanged(auth, async (user) => {
  const authPanel = byId("authPanel");
  const appPanel = byId("appPanel");
  const authError = byId("authError");
  if (!user) {
    stopListeners();
    currentAccess = null;
    setAuthBusy(false);
    authPanel.classList.remove("hidden");
    appPanel.classList.add("hidden");
    return;
  }
  await bootstrapSignedInUser(user);
});
