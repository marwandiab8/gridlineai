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
  doc,
  documentId,
  query,
  orderBy,
  limit,
  onSnapshot,
  where,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import {
  getStorage,
  ref,
  getDownloadURL,
  uploadBytes,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-storage.js";

const firebaseConfig = {
  apiKey: "AIzaSyBfUA9JCo01N53TTDzMxnqEqzYqy-RJ6qE",
  authDomain: "gridlineai.firebaseapp.com",
  projectId: "gridlineai",
  storageBucket: "gridlineai.firebasestorage.app",
  messagingSenderId: "118761010772",
  appId: "1:118761010772:web:6eee28ee3c09953de0dfc1",
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
if (appCheckSiteKey) {
  try {
    initializeAppCheck(app, {
      provider:
        appCheckProvider === "v3" || appCheckProvider === "recaptchav3"
          ? new ReCaptchaV3Provider(appCheckSiteKey)
          : new ReCaptchaEnterpriseProvider(appCheckSiteKey),
      isTokenAutoRefreshEnabled: true,
    });
  } catch (_) {
    // Ignore duplicate initialization if already set.
  }
}
const auth = getAuth(app);
const db = getFirestore(app);
const storage = getStorage(app);

function esc(value) {
  if (value == null) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function fmtTime(ts) {
  if (!ts) return "";
  try {
    if (typeof ts.toDate === "function") return ts.toDate().toLocaleString();
    if (ts.seconds) return new Date(ts.seconds * 1000).toLocaleString();
  } catch (_) {}
  return esc(String(ts));
}

function todayDateKeyEastern() {
  try {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date());
  } catch (_) {
    return new Date().toISOString().slice(0, 10);
  }
}

function parseDateKeyClient(dateKey) {
  const raw = String(dateKey || "").trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  return new Date(Date.UTC(year, month - 1, day));
}

function formatDateKeyClient(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  return date.toISOString().slice(0, 10);
}

function shiftDateKeyClient(dateKey, deltaDays) {
  const date = parseDateKeyClient(dateKey);
  if (!date || !Number.isFinite(deltaDays)) return "";
  date.setUTCDate(date.getUTCDate() + Number(deltaDays));
  return formatDateKeyClient(date);
}

function startOfWeekDateKeyClient(dateKey) {
  const date = parseDateKeyClient(dateKey || todayDateKeyEastern());
  if (!date) return todayDateKeyEastern();
  const day = date.getUTCDay();
  const mondayOffset = day === 0 ? 6 : day - 1;
  return shiftDateKeyClient(dateKey || todayDateKeyEastern(), -mondayOffset) || todayDateKeyEastern();
}

function startOfMonthDateKeyClient(dateKey) {
  const date = parseDateKeyClient(dateKey || todayDateKeyEastern());
  if (!date) return todayDateKeyEastern().slice(0, 8) + "01";
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-01`;
}

function endOfMonthDateKeyClient(dateKey) {
  const date = parseDateKeyClient(dateKey || todayDateKeyEastern());
  if (!date) return todayDateKeyEastern();
  return formatDateKeyClient(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0)));
}

function formatHoursClient(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0.00";
  return n % 1 === 0 ? String(n) : n.toFixed(2).replace(/0+$/, "").replace(/\.$/, "");
}

/** Matches server labourRepository.dayMultiplierFromDateKey (report date = calendar day). */
function dayMultiplierFromDateKeyClient(dateKey) {
  const date = parseDateKeyClient(String(dateKey || "").trim());
  if (!date) return 1;
  const day = date.getUTCDay();
  if (day === 6) return 1.5; // Saturday
  if (day === 0) return 2; // Sunday
  return 1;
}

function sumHoursClient(entries) {
  return (entries || []).reduce((total, item) => total + (Number(item && item.hours) || 0), 0);
}

function sumWeightedPaidHoursClient(entries) {
  const raw = (entries || []).reduce((total, item) => {
    const h = Number(item && item.hours) || 0;
    const m = dayMultiplierFromDateKeyClient(String(item && item.reportDateKey || "").trim());
    return total + h * m;
  }, 0);
  return Math.round(raw * 100) / 100;
}

function groupEntriesClient(entries, keyFn) {
  const grouped = new Map();
  for (const entry of entries || []) {
    const key = keyFn(entry);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(entry);
  }
  return grouped;
}

function labourerLabelClient(item) {
  return String(item?.displayName || item?.name || item?.labourerName || item?.phoneE164 || item?.phone || item?.labourerPhone || "").trim();
}

function renderLabourListItemText(item) {
  const hours = formatHoursClient(item.hours);
  const who = labourerLabelClient(item) || item.labourerPhone || "Unknown";
  return `${item.reportDateKey || "-"} · ${who} · ${hours}h · ${String(item.workOn || "")}`;
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

function normalizeLooseIsoDateClient(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length !== 8) return null;
  const year = digits.slice(0, 4);
  const month = digits.slice(4, 6);
  const day = digits.slice(6, 8);
  const monthNum = Number(month);
  const dayNum = Number(day);
  if (!year || monthNum < 1 || monthNum > 12 || dayNum < 1 || dayNum > 31) return null;
  return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
}

function extractReportDateKeyClient(text) {
  const source = String(text || "");
  const patterns = [
    /\(\s*((?:19|20)\d{2}[-/]\d{2}[-/]\d{2}|(?:19|20)\d{2}[-/]?\d{4})\s*\)/i,
    /\b(?:for|on|dated)\s+((?:19|20)\d{2}[-/]\d{2}[-/]\d{2}|(?:19|20)\d{2}[-/]?\d{4})\b/i,
    /:\s*((?:19|20)\d{2}[-/]\d{2}[-/]\d{2}|(?:19|20)\d{2}[-/]?\d{4})\b/i,
  ];
  for (const pattern of patterns) {
    const match = source.match(pattern);
    if (!match) continue;
    const normalized = normalizeLooseIsoDateClient(match[1]);
    if (normalized) return normalized;
  }
  return null;
}

async function uploadFileToStorage(storagePath, file) {
  const storageRef = ref(storage, storagePath);
  await uploadBytes(storageRef, file, {
    contentType: file && file.type ? file.type : "application/octet-stream",
  });
  return {
    storagePath,
    contentType: file && file.type ? file.type : "application/octet-stream",
    fileName: file && file.name ? file.name : storagePath.split("/").pop() || "upload",
  };
}

function blobToFile(blob, fileName, contentType) {
  return new File([blob], fileName, {
    type: contentType,
    lastModified: Date.now(),
  });
}

function formatUiError(err) {
  if (!err) return "Unknown error";
  const code = err.code ? String(err.code) : "";
  const message = err.message ? String(err.message) : String(err);
  return code ? `${code}: ${message}` : message;
}

async function readImageBitmapForUpload(file) {
  if (typeof createImageBitmap === "function") {
    return createImageBitmap(file);
  }
  const url = URL.createObjectURL(file);
  try {
    const img = await new Promise((resolve, reject) => {
      const node = new Image();
      node.onload = () => resolve(node);
      node.onerror = () => reject(new Error("Could not read image."));
      node.src = url;
    });
    return img;
  } finally {
    URL.revokeObjectURL(url);
  }
}

async function normalizeImageForPdf(file, options = {}) {
  if (!file) return file;
  const preferredType = options.preferredType || "image/jpeg";
  const quality = Number.isFinite(options.quality) ? options.quality : 0.92;
  const originalType = String(file.type || "").toLowerCase();
  if (originalType === "image/jpeg" || originalType === "image/png") return file;

  const bitmap = await readImageBitmapForUpload(file);
  const width = bitmap.width || bitmap.naturalWidth;
  const height = bitmap.height || bitmap.naturalHeight;
  if (!width || !height) {
    throw new Error("Could not read image dimensions.");
  }

  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, width, height);
  ctx.drawImage(bitmap, 0, 0, width, height);
  if (typeof bitmap.close === "function") bitmap.close();

  const blob = await new Promise((resolve, reject) => {
    canvas.toBlob(
      (result) => {
        if (result) resolve(result);
        else reject(new Error("Could not convert image for PDF."));
      },
      preferredType,
      quality
    );
  });

  const sourceName = String(file.name || "upload").replace(/\.[^.]+$/, "");
  const ext = preferredType === "image/png" ? "png" : "jpg";
  return blobToFile(blob, `${sourceName}.${ext}`, preferredType);
}

async function callDashboardFunction(name, payload) {
  const { getFunctions, httpsCallable } = await import(
    "https://www.gstatic.com/firebasejs/10.12.2/firebase-functions.js"
  );
  const fn = httpsCallable(getFunctions(app, "northamerica-northeast1"), name);
  const res = await fn(payload);
  return res.data;
}

const storageUrlCache = new Map();
const storageUrlInflight = new Map();
const TRANSPARENT_PIXEL =
  "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7";

async function getCachedStorageDownloadURL(storagePath) {
  if (!storagePath) return null;
  if (storageUrlCache.has(storagePath)) return storageUrlCache.get(storagePath);
  let inflight = storageUrlInflight.get(storagePath);
  if (!inflight) {
    inflight = getDownloadURL(ref(storage, storagePath))
      .then((url) => {
        storageUrlCache.set(storagePath, url);
        return url;
      })
      .finally(() => {
        storageUrlInflight.delete(storagePath);
      });
    storageUrlInflight.set(storagePath, inflight);
  }
  return inflight;
}

let hydrateMediaScheduled = false;
function scheduleHydrateMediaThumbs() {
  if (hydrateMediaScheduled) return;
  hydrateMediaScheduled = true;
  queueMicrotask(() => {
    hydrateMediaScheduled = false;
    void hydrateAllMediaThumbs();
  });
}

const authPanelEl = document.getElementById("authPanel");
const appPanelEl = document.getElementById("appPanel");
const authSignInBtn = document.getElementById("authSignInGoogle");
const authSignOutBtn = document.getElementById("authSignOut");
const authErrorEl = document.getElementById("authError");
const adminUserLabelEl = document.getElementById("adminUserLabel");
const statusEl = document.getElementById("status");
const statusDetailEl = document.getElementById("statusDetail");
const messagesEl = document.getElementById("messages");
const usersEl = document.getElementById("users");
const issuesEl = document.getElementById("issues");
const summariesEl = document.getElementById("summaries");
const dailyReportsEl = document.getElementById("dailyReports");
const logEntriesEl = document.getElementById("logEntries");
const mediaEl = document.getElementById("media");
const appMembersEl = document.getElementById("appMembers");
const projectNoteEditRequestsEl = document.getElementById("projectNoteEditRequests");
const mediaViewerBackdropEl = document.getElementById("mediaViewerBackdrop");
const mediaViewerCloseEl = document.getElementById("mediaViewerClose");
const mediaViewerPrevEl = document.getElementById("mediaViewerPrev");
const mediaViewerNextEl = document.getElementById("mediaViewerNext");
const mediaViewerImageEl = document.getElementById("mediaViewerImage");
const mediaViewerStatusEl = document.getElementById("mediaViewerStatus");
const mediaViewerCaptionEl = document.getElementById("mediaViewerCaption");
const mediaViewerDetailsEl = document.getElementById("mediaViewerDetails");
const mediaViewerIndexEl = document.getElementById("mediaViewerIndex");
const dashboardUsersCountEl = document.getElementById("dashboardUsersCount");
const dashboardProjectsCountEl = document.getElementById("dashboardProjectsCount");
const dashboardReportsCountEl = document.getElementById("dashboardReportsCount");
const dashboardMessagesCountEl = document.getElementById("dashboardMessagesCount");
const dashboardProjectFocusEl = document.getElementById("dashboardProjectFocus");
const dashboardRecentMessagesEl = document.getElementById("dashboardRecentMessages");
const dashboardRecentReportsEl = document.getElementById("dashboardRecentReports");
const labourersCountEl = document.getElementById("labourersCount");
const labourTodayHoursEl = document.getElementById("labourTodayHours");
const labourTodayEntriesEl = document.getElementById("labourTodayEntries");
const labourWeekHoursEl = document.getElementById("labourWeekHours");
const labourWeekEntriesEl = document.getElementById("labourWeekEntries");
const labourMonthHoursEl = document.getElementById("labourMonthHours");
const labourMonthEntriesEl = document.getElementById("labourMonthEntries");
const labourersEl = document.getElementById("labourers");
const labourEntriesEl = document.getElementById("labourEntries");
const labourReportsEl = document.getElementById("labourReports");
const pageNavLinks = Array.from(document.querySelectorAll("[data-view-link]"));
const pagePanels = Array.from(document.querySelectorAll("[data-view-panel]"));
const quickViewButtons = Array.from(document.querySelectorAll("[data-view-target]"));

let appUnsubscribers = [];
let messagesCache = [];
let smsUsersCache = [];
let projectsCache = [];
let appMembersCache = [];
let issueLogsCache = [];
let summariesCache = [];
let dailyReportsCache = [];
let mediaCache = [];
let logEntriesCache = [];
let projectNoteRequestsCache = [];
let labourersCache = [];
let labourEntriesCache = [];
let labourReportsCache = [];
let labourTodayCache = [];
let labourWeekCache = [];
let labourMonthCache = [];
let mediaViewerItems = [];
let mediaViewerIndex = -1;
let currentAppAccess = null;

function setStatusOk(detail = "") {
  if (statusEl) statusEl.textContent = "Connected";
  if (statusDetailEl) statusDetailEl.textContent = detail;
}

function setStatusInfo(message, detail = "") {
  if (statusEl) statusEl.textContent = message;
  if (statusDetailEl) statusDetailEl.textContent = detail;
}

function setStatusError(message) {
  const rawMessage = String(message || "");
  const withHint =
    /missing or insufficient permissions/i.test(rawMessage) && !appCheckSiteKey
      ? `${rawMessage} (If Firestore App Check enforcement is ON, set FIREBASE_APPCHECK_SITE_KEY in index.html.)`
      : rawMessage;
  if (statusEl) {
    statusEl.innerHTML = `<span class="pill pill-warn">Error</span> ${esc(withHint)}`;
  }
  if (statusDetailEl) {
    statusDetailEl.textContent =
      "If this looks like a rules or index issue, deploy the updated Firebase config and refresh.";
  }
}

function normalizeProjectSlugClient(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function normalizeRoleClient(value) {
  const role = String(value || "").trim().toLowerCase();
  if (role === "admin" || role === "management" || role === "viewer") return role;
  return "user";
}

function roleAtLeastClient(role, minimumRole) {
  const order = ["viewer", "user", "management", "admin"];
  return order.indexOf(normalizeRoleClient(role)) >= order.indexOf(normalizeRoleClient(minimumRole));
}

function currentUserRole() {
  return normalizeRoleClient(currentAppAccess && currentAppAccess.role);
}

function currentUserEmail() {
  return auth.currentUser && auth.currentUser.email ? String(auth.currentUser.email).trim().toLowerCase() : "";
}

function currentUserCanApproveNotes() {
  if (!currentAppAccess) return false;
  if (roleAtLeastClient(currentAppAccess.role, "admin")) return true;
  return normalizeRoleClient(currentAppAccess.role) === "management" && currentAppAccess.canApproveNotes === true;
}

function currentUserAllProjects() {
  return !!(currentAppAccess && currentAppAccess.allProjects === true);
}

function currentUserProjectSlugs() {
  const raw = currentAppAccess && Array.isArray(currentAppAccess.projectSlugs) ? currentAppAccess.projectSlugs : [];
  return raw.map((slug) => normalizeProjectSlugClient(slug)).filter(Boolean);
}

function currentUserCanAccessProject(projectSlug) {
  const slug = normalizeProjectSlugClient(projectSlug);
  if (!slug) return roleAtLeastClient(currentUserRole(), "management");
  if (roleAtLeastClient(currentUserRole(), "admin")) return true;
  if (currentUserAllProjects()) return true;
  return currentUserProjectSlugs().includes(slug);
}

/**
 * Who may pick any smsUsers number on Assistant / Lookahead.
 * App-member org admins see the full list. Legacy clients without `via` keep admin=all.
 * Operator / legacy allowlist accounts use dashboard role "admin" but should not see other field phones
 * when an approved member phone exists (including operator + linked appMembers row).
 * Pure operators with no approved phone on file keep the legacy global picker.
 */
function assistantComposerShowsAllSmsUsers() {
  if (!currentAppAccess) return false;
  const via = String(currentAppAccess.via || "").trim().toLowerCase();
  const role = normalizeRoleClient(currentAppAccess.role);
  const approved = String(currentAppAccess.approvedPhoneE164 || "").trim();
  if (via === "app-member" && role === "admin") return true;
  if (!via && role === "admin") return true;
  if ((via === "custom-claim" || via === "firestore-allowlist") && !approved) return true;
  return false;
}

function userProjectSlugs(user) {
  const out = [];
  const raw = Array.isArray(user?.projectSlugs) ? user.projectSlugs : [];
  for (const value of raw) {
    const slug = normalizeProjectSlugClient(value);
    if (slug && !out.includes(slug)) out.push(slug);
  }
  const active = normalizeProjectSlugClient(user?.activeProjectSlug);
  if (active && !out.includes(active)) out.unshift(active);
  return out;
}

function findUserByPhone(phoneE164) {
  const target = String(phoneE164 || "").trim();
  return smsUsersCache.find((user) => String(user.phoneE164 || user.id || "").trim() === target) || null;
}

/** smsUsers row, or synthetic member context when the approved phone is not in cache yet. */
function resolveSmsUserForAssistant(phoneE164) {
  const phone = String(phoneE164 || "").trim();
  if (!phone) return null;
  const cached = findUserByPhone(phoneE164);
  if (cached) return cached;
  if (assistantComposerShowsAllSmsUsers()) return null;
  const approved = String(currentAppAccess?.approvedPhoneE164 || "").trim();
  if (!approved || phone !== approved) return null;
  const synthetic = syntheticSmsUserForDailyPdf();
  if (synthetic && String(synthetic.phoneE164 || "").trim() === phone) return synthetic;
  return null;
}

function currentUserManagedProjectSlugs() {
  if (roleAtLeastClient(currentUserRole(), "admin")) {
    return projectsCache
      .map((project) => normalizeProjectSlugClient(project.slug || project.id))
      .filter(Boolean);
  }
  return currentUserProjectSlugs().slice(0, 10);
}

function currentUserScopedProjectKeys() {
  const expanded = [];
  const merged = Array.isArray(currentAppAccess?.projectSlugs) ? currentAppAccess.projectSlugs : [];
  const raw = Array.isArray(currentAppAccess?.rawProjectSlugs) ? currentAppAccess.rawProjectSlugs : [];
  for (const value of [...merged, ...raw]) {
    const direct = String(value || "").trim();
    if (direct && !expanded.includes(direct)) expanded.push(direct);
    const normalized = normalizeProjectSlugClient(direct);
    if (normalized && !expanded.includes(normalized)) expanded.push(normalized);
  }
  return expanded.slice(0, 20);
}

function getAccessibleProjectsForUser(user) {
  if (!user) return [];
  const phoneE164 = String(user.phoneE164 || user.id || "").trim();
  const legacySlugs = userProjectSlugs(user);
  return projectsCache
    .filter((project) => {
      const slug = normalizeProjectSlugClient(project.slug || project.id);
      if (!slug) return false;
      const ownerPhoneE164 = String(project.ownerPhoneE164 || "").trim();
      if (ownerPhoneE164) return ownerPhoneE164 === phoneE164;
      return legacySlugs.includes(slug);
    })
    .sort((a, b) =>
      String(a.name || a.slug || a.id || "").localeCompare(String(b.name || b.slug || b.id || ""))
    );
}

function getActiveProjectRecordForUser(user) {
  if (!user) return null;
  const activeSlug = normalizeProjectSlugClient(user.activeProjectSlug);
  if (!activeSlug) return null;
  return (
    getAccessibleProjectsForUser(user).find(
      (project) => normalizeProjectSlugClient(project.slug || project.id) === activeSlug
    ) || null
  );
}

function refreshDailyPdfProjectOptions(user) {
  const list = document.getElementById("dailyPdfProjectOptions");
  if (!list) return;
  list.innerHTML = "";
  for (const project of getAccessibleProjectsForUser(user)) {
    const slug = normalizeProjectSlugClient(project.slug || project.id);
    if (!slug) continue;
    const option = document.createElement("option");
    option.value = slug;
    option.label = `${project.name || slug} (${slug})`;
    list.appendChild(option);
  }
}

function placeholderMiniList(message) {
  return `<div class="mini-item empty">${esc(message)}</div>`;
}

function renderDashboard() {
  if (dashboardUsersCountEl) dashboardUsersCountEl.textContent = String(smsUsersCache.length || 0);
  if (dashboardProjectsCountEl) dashboardProjectsCountEl.textContent = String(projectsCache.length || 0);
  if (dashboardReportsCountEl) dashboardReportsCountEl.textContent = String(dailyReportsCache.length || 0);
  if (dashboardMessagesCountEl) dashboardMessagesCountEl.textContent = String(messagesCache.length || 0);

  if (dashboardProjectFocusEl) {
    dashboardProjectFocusEl.innerHTML = smsUsersCache.length
      ? smsUsersCache
          .slice(0, 5)
          .map((user) => {
            const phone = user.phoneE164 || user.id || "-";
            const activeProject = user.activeProjectSlug || "No active project";
            return `
              <div class="mini-item">
                <div class="mini-item-title">${esc(phone)}</div>
                <div class="mini-item-meta">Active project: ${esc(activeProject)}</div>
                <div class="mini-item-meta">Projects: ${esc(String(userProjectSlugs(user).length || 0))}</div>
              </div>`;
          })
          .join("")
      : placeholderMiniList("No SMS users loaded yet.");
  }

  if (dashboardRecentMessagesEl) {
    dashboardRecentMessagesEl.innerHTML = messagesCache.length
      ? messagesCache
          .slice(0, 5)
          .map((msg) => {
            const direction = msg.direction === "inbound" ? "Inbound" : "Outbound";
            const summary = String(msg.body || "").trim();
            return `
              <div class="mini-item">
                <div class="mini-item-title">${esc(direction)} · ${esc(msg.projectSlug || "no project")}</div>
                <div class="mini-item-meta">${fmtTime(msg.createdAt)}</div>
                <div>${esc(summary.slice(0, 110))}${summary.length > 110 ? "..." : ""}</div>
              </div>`;
          })
          .join("")
      : placeholderMiniList("No recent messages yet.");
  }

  if (dashboardRecentReportsEl) {
    dashboardRecentReportsEl.innerHTML = dailyReportsCache.length
      ? dailyReportsCache
          .slice(0, 5)
          .map((report) => {
            const projectLabel =
              report.projectName ||
              report.projectId ||
              (report.reportType === "journal" ? "Personal journal" : "No project");
            return `
              <div class="mini-item">
                <div class="mini-item-title">${esc(report.reportType || "dailySiteLog")} · ${esc(projectLabel)}</div>
                <div class="mini-item-meta">${fmtTime(report.createdAt)} · ${esc(report.dateKey || "")}</div>
                <div class="mini-item-meta">${report.downloadURL ? "PDF ready" : "Waiting for link"}</div>
              </div>`;
          })
          .join("")
      : placeholderMiniList("No reports yet.");
  }
}

function syncProjectPhoneSelect(docs) {
  const select = document.getElementById("projectPhone");
  const createBtn = document.getElementById("projectCreateBtn");
  if (!select || !createBtn) return;
  const previous = select.value;
  select.innerHTML = "";
  if (!docs.length) {
    select.innerHTML = '<option value="">No users yet - text the Twilio number first</option>';
    createBtn.disabled = true;
    renderProjectManager();
    return;
  }
  select.appendChild(new Option("Select phone...", ""));
  for (const user of docs) {
    const phone = user.phoneE164 || user.id;
    select.appendChild(new Option(phone, phone));
  }
  if (docs.some((user) => (user.phoneE164 || user.id) === previous)) {
    select.value = previous;
  }
  createBtn.disabled = !select.value;
  renderProjectManager();
}

function syncDailyPdfPhoneSelect(docs) {
  const select = document.getElementById("dailyPdfPhone");
  const button = document.getElementById("dailyPdfBtn");
  if (!select || !button) return;
  const previous = select.value;
  select.innerHTML = "";
  if (!docs.length) {
    select.innerHTML = '<option value="">No users yet - text the Twilio number first</option>';
    button.disabled = true;
    return;
  }
  select.appendChild(new Option("Select phone...", ""));
  for (const user of docs) {
    const phone = user.phoneE164 || user.id;
    const option = new Option(`${phone} · ${user.activeProjectSlug || "no project"}`, phone);
    option.dataset.activeProject = user.activeProjectSlug || "";
    select.appendChild(option);
  }
  if (docs.some((user) => (user.phoneE164 || user.id) === previous)) {
    select.value = previous;
  }
  button.disabled = false;
  refreshDailyPdfProjectOptions(findUserByPhone(select.value));
}

function syntheticSmsUserForDailyPdf() {
  const phone = String(currentAppAccess?.approvedPhoneE164 || "").trim();
  if (!phone) return null;
  const slugs = currentUserProjectSlugs();
  return {
    phoneE164: phone,
    id: phone,
    activeProjectSlug: slugs[0] || "",
    projectSlugs: slugs,
  };
}

function assistantComposerPhoneDocsForCurrentUser(docs) {
  if (assistantComposerShowsAllSmsUsers()) {
    return Array.isArray(docs) ? docs : [];
  }
  const approved = String(currentAppAccess?.approvedPhoneE164 || "").trim();
  if (!approved) return [];
  const list = Array.isArray(docs) ? docs : [];
  const filtered = list.filter((user) => String(user.phoneE164 || user.id || "").trim() === approved);
  if (filtered.length) return filtered;
  const synthetic = syntheticSmsUserForDailyPdf();
  if (synthetic && String(synthetic.phoneE164 || "").trim() === approved) return [synthetic];
  return [];
}

function syncDailyPdfPhoneFromAccess() {
  const select = document.getElementById("dailyPdfPhone");
  const button = document.getElementById("dailyPdfBtn");
  if (!select || !button || !currentAppAccess) return;
  if (roleAtLeastClient(currentUserRole(), "admin")) return;
  const phone = String(currentAppAccess.approvedPhoneE164 || "").trim();
  if (!phone) {
    select.innerHTML =
      '<option value="">Add an approved field phone on your member record (Admin → Team) to generate PDFs.</option>';
    button.disabled = true;
    return;
  }
  select.innerHTML = "";
  select.appendChild(new Option(`${phone} · approved field phone`, phone));
  select.value = phone;
  button.disabled = false;
  const projectInput = document.getElementById("dailyPdfProject");
  const slugs = currentUserProjectSlugs();
  if (projectInput && slugs.length === 1 && !String(projectInput.value || "").trim()) {
    projectInput.value = slugs[0];
  }
  refreshDailyPdfProjectOptions(syntheticSmsUserForDailyPdf());
}

function refreshDailyReportTitleEditor() {
  const select = document.getElementById("reportTitleEditSelect");
  if (!select || !roleAtLeastClient(currentUserRole(), "management")) return;
  const prev = select.value;
  select.innerHTML = '<option value="">Select a report...</option>';
  const sorted = [...dailyReportsCache].sort((a, b) => {
    const aTs = a.createdAt && a.createdAt.seconds ? a.createdAt.seconds : 0;
    const bTs = b.createdAt && b.createdAt.seconds ? b.createdAt.seconds : 0;
    return bTs - aTs;
  });
  for (const report of sorted) {
    if (!currentUserCanAccessProject(report.projectId || "")) continue;
    const label = `${report.reportTitle || report.reportType || "Report"} · ${report.dateKey || ""} (${report.id})`;
    select.appendChild(new Option(label, report.id));
  }
  if ([...select.options].some((o) => o.value === prev)) {
    select.value = prev;
  }
}

function syncAssistantComposerPhoneSelect(docs) {
  const selectIds = ["assistantComposerPhone", "assistantSchedulePhone"];
  const scopedDocs = assistantComposerPhoneDocsForCurrentUser(docs);
  const restrictToApprovedPhone = !assistantComposerShowsAllSmsUsers();

  for (const selectId of selectIds) {
    const select = document.getElementById(selectId);
    if (!select) continue;
    const previous = select.value;
    select.innerHTML = "";
    if (!scopedDocs.length) {
      if (restrictToApprovedPhone) {
        const approved = String(currentAppAccess?.approvedPhoneE164 || "").trim();
        select.innerHTML = approved
          ? '<option value="">Your approved phone is not registered as an SMS user yet. Text the Twilio number once from that phone.</option>'
          : '<option value="">Add an approved field phone on your member record (Admin → Team) to use the assistant.</option>';
      } else {
        select.innerHTML = '<option value="">No users yet - text the Twilio number first</option>';
      }
      continue;
    }
    if (restrictToApprovedPhone && scopedDocs.length === 1) {
      const user = scopedDocs[0];
      const phone = user.phoneE164 || user.id;
      select.appendChild(new Option(`${phone} · ${user.activeProjectSlug || "no project"}`, phone));
      select.value = phone;
    } else {
      select.appendChild(new Option("Select phone...", ""));
      for (const user of scopedDocs) {
        const phone = user.phoneE164 || user.id;
        const option = new Option(`${phone} · ${user.activeProjectSlug || "no project"}`, phone);
        select.appendChild(option);
      }
      if (scopedDocs.some((user) => (user.phoneE164 || user.id) === previous)) {
        select.value = previous;
      }
    }
  }
  const button = document.getElementById("assistantComposerSendBtn");
  if (button && !scopedDocs.length) button.disabled = true;
  renderAssistantComposer();
}

function renderProjectManager() {
  const phoneSelect = document.getElementById("projectPhone");
  const activeSelect = document.getElementById("projectActiveSelect");
  const setButton = document.getElementById("projectSetActiveBtn");
  const saveLogoButton = document.getElementById("projectSaveLogoBtn");
  const uploadLogoButton = document.getElementById("projectUploadLogoBtn");
  const logoInput = document.getElementById("projectLogoStoragePath");
  const createButton = document.getElementById("projectCreateBtn");
  const ownedList = document.getElementById("projectOwnedList");
  if (!phoneSelect || !activeSelect || !setButton || !createButton || !ownedList) return;

  const user = findUserByPhone(phoneSelect.value);
  createButton.disabled = !user;
  activeSelect.innerHTML = "";

  if (!user) {
    activeSelect.innerHTML = '<option value="">Select a phone first...</option>';
    activeSelect.disabled = true;
    setButton.disabled = true;
    if (saveLogoButton) saveLogoButton.disabled = true;
    if (uploadLogoButton) uploadLogoButton.disabled = true;
    if (logoInput) logoInput.value = "";
    ownedList.textContent = "Select a phone to view its projects.";
    return;
  }

  const projects = getAccessibleProjectsForUser(user);
  if (!projects.length) {
    activeSelect.innerHTML = '<option value="">No projects assigned yet</option>';
    activeSelect.disabled = true;
    setButton.disabled = true;
    if (saveLogoButton) saveLogoButton.disabled = true;
    if (uploadLogoButton) uploadLogoButton.disabled = true;
    if (logoInput) logoInput.value = "";
    ownedList.textContent = "No projects assigned yet. Create the first one below.";
    refreshDailyPdfProjectOptions(user);
    return;
  }

  for (const project of projects) {
    const slug = normalizeProjectSlugClient(project.slug || project.id);
    activeSelect.appendChild(new Option(`${project.name || slug} (${slug})`, slug));
  }

  const activeProjectSlug = normalizeProjectSlugClient(user.activeProjectSlug);
  activeSelect.value = projects.some(
    (project) => normalizeProjectSlugClient(project.slug || project.id) === activeProjectSlug
  )
    ? activeProjectSlug
    : normalizeProjectSlugClient(projects[0].slug || projects[0].id);
  activeSelect.disabled = false;
  setButton.disabled = false;
  if (saveLogoButton) saveLogoButton.disabled = false;
  if (uploadLogoButton) uploadLogoButton.disabled = false;
  if (logoInput) {
    const selectedProject = projects.find(
      (project) => normalizeProjectSlugClient(project.slug || project.id) === activeSelect.value
    );
    logoInput.value =
      selectedProject && selectedProject.reportLogoStoragePath
        ? String(selectedProject.reportLogoStoragePath)
        : "";
  }

  ownedList.innerHTML = projects
    .map((project) => {
      const slug = normalizeProjectSlugClient(project.slug || project.id);
      const activePill = slug === activeProjectSlug ? '<span class="pill pill-ai">active</span>' : "";
      const ownerLabel = project.ownerPhoneE164 ? "owned" : "legacy assignment";
      const location = project.location
        ? `<div class="project-owned-meta muted small">${esc(project.location)}</div>`
        : "";
      const logo = project.reportLogoStoragePath
        ? `<div class="project-owned-meta muted small">Logo: ${esc(project.reportLogoStoragePath)}</div>`
        : "";
      return `
        <div class="project-owned-item">
          <div class="project-owned-name">${esc(project.name || slug)} ${activePill}</div>
          <div class="project-owned-meta muted small mono">${esc(slug)} · ${esc(ownerLabel)}</div>
          ${location}
          ${logo}
        </div>`;
    })
    .join("");

  refreshDailyPdfProjectOptions(user);
}

function renderAppMembersList() {
  if (!appMembersEl) return;
  if (!appMembersCache.length) {
    appMembersEl.innerHTML = '<div class="row-item muted">No app members configured yet.</div>';
    return;
  }
  appMembersEl.innerHTML = appMembersCache
    .map((member) => {
      const email = String(member.email || member.id || "").trim().toLowerCase();
      const role = normalizeRoleClient(member.role);
      const projects = Array.isArray(member.projectSlugs) ? member.projectSlugs.join(", ") : "";
      const selfDeleteAttrs =
        email && email === currentUserEmail()
          ? 'disabled title="You cannot delete your own member record."'
          : "";
      return `
        <div class="row-item" data-member-email="${esc(email)}">
          <div><span class="pill pill-user">${esc(role)}</span>${member.active === false ? '<span class="pill pill-warn">inactive</span>' : ""}</div>
          <div><strong>${esc(member.displayName || member.email || member.id)}</strong></div>
          <div class="muted small mono">${esc(member.email || member.id || "")}</div>
          <div>Approved phone: ${esc(member.approvedPhoneE164 || "-")}</div>
          <div>Company: ${esc(member.company || "-")}</div>
          <div>Projects: ${esc(member.allProjects ? "all projects" : projects || "-")}</div>
          <div>Can approve notes: ${member.canApproveNotes ? "yes" : "no"}</div>
          <div class="project-manager-actions">
            <button type="button" class="btn-secondary" data-member-edit="${esc(email)}">Edit</button>
            <button type="button" class="btn-secondary btn-danger" data-member-delete="${esc(email)}" ${selfDeleteAttrs}>Delete</button>
          </div>
        </div>`;
    })
    .join("");
}

function renderProjectNotesRequestForm() {
  const reportSelect = document.getElementById("projectNotesReportSelect");
  const metaEl = document.getElementById("projectNotesReportMeta");
  const currentEl = document.getElementById("projectNotesCurrent");
  const proposedEl = document.getElementById("projectNotesProposed");
  const submitBtn = document.getElementById("projectNotesSubmitBtn");
  if (!reportSelect || !metaEl || !currentEl || !proposedEl || !submitBtn) return;

  const previous = reportSelect.value;
  const reports = dailyReportsCache.filter((report) => currentUserCanAccessProject(report.projectId || ""));
  reportSelect.innerHTML = '<option value="">Select a report...</option>';
  for (const report of reports) {
    if (!report.projectId) continue;
    const label = [
      report.projectName || report.projectId,
      report.reportType || "report",
      report.dateKey || "",
    ]
      .filter(Boolean)
      .join(" · ");
    reportSelect.appendChild(new Option(label, report.id));
  }
  if (reports.some((report) => report.id === previous)) {
    reportSelect.value = previous;
  } else if (!reportSelect.value && reports.length) {
    reportSelect.value = reports[0].id;
  }

  const report = reports.find((item) => item.id === reportSelect.value) || null;
  if (!report) {
    metaEl.textContent = "Select a report to load its project notes.";
    currentEl.value = "";
    submitBtn.disabled = true;
    return;
  }

  const project = projectsCache.find(
    (item) => normalizeProjectSlugClient(item.slug || item.id) === normalizeProjectSlugClient(report.projectId || "")
  );
  currentEl.value = project && project.notes ? String(project.notes) : "";
  metaEl.innerHTML = `
    <div><strong>Project:</strong> ${esc(report.projectName || report.projectId || "-")}</div>
    <div><strong>Report:</strong> ${esc(report.reportTitle || report.reportType || report.id)}</div>
    <div><strong>Date:</strong> ${esc(report.dateKey || report.dateRangeStartKey || "-")}</div>
  `;
  submitBtn.disabled = normalizeRoleClient(currentUserRole()) === "viewer";
}

function renderProjectNoteRequests() {
  if (!projectNoteEditRequestsEl) return;
  if (!projectNoteRequestsCache.length) {
    projectNoteEditRequestsEl.innerHTML = '<div class="row-item muted">No note edit requests yet.</div>';
    return;
  }
  projectNoteEditRequestsEl.innerHTML = projectNoteRequestsCache
    .map((item) => {
      const statusClass =
        item.status === "approved" ? "pill-ai" : item.status === "rejected" ? "pill-warn" : "pill-issue";
      const actions =
        currentUserCanApproveNotes() && item.status === "pending"
          ? `<div class="project-manager-actions">
              <button type="button" class="btn-primary" data-approve-request="${esc(item.id)}">Approve</button>
              <button type="button" class="btn-secondary" data-reject-request="${esc(item.id)}">Reject</button>
            </div>`
          : "";
      return `
        <div class="row-item">
          <div><span class="pill ${statusClass}">${esc(item.status || "pending")}</span></div>
          <div><strong>${esc(item.projectName || item.projectSlug || "-")}</strong></div>
          <div class="muted small">Requested by ${esc(item.requestedByName || item.requestedByEmail || "-")} · ${fmtTime(item.createdAt)}</div>
          <div class="muted small">Report: ${esc(item.reportTitle || item.reportId || "-")} · ${esc(item.reportDateKey || "-")}</div>
          <div><strong>Current notes</strong></div>
          <div class="muted small">${esc(String(item.currentNotes || "-").slice(0, 400))}</div>
          <div><strong>Proposed notes</strong></div>
          <div>${esc(String(item.proposedNotes || "").slice(0, 700))}</div>
          ${item.requesterComment ? `<div class="muted small">Requester comment: ${esc(item.requesterComment)}</div>` : ""}
          ${item.reviewerComment ? `<div class="muted small">Reviewer comment: ${esc(item.reviewerComment)}</div>` : ""}
          ${actions}
        </div>`;
    })
    .join("");
}

function syncLabourReportLabourerOptions() {
  const select = document.getElementById("labourReportLabourerPhone");
  if (!select) return;
  const previous = select.value;
  select.innerHTML = '<option value="">All labourers</option>';
  const sorted = [...labourersCache].sort((a, b) =>
    labourerLabelClient(a).localeCompare(labourerLabelClient(b))
  );
  for (const labourer of sorted) {
    const phone = String(labourer.phoneE164 || labourer.id || "").trim();
    if (!phone) continue;
    const label = labourerLabelClient(labourer) || phone;
    const option = new Option(`${label} · ${phone}${labourer.active === false ? " (inactive)" : ""}`, phone);
    select.appendChild(option);
  }
  if ([...select.options].some((option) => option.value === previous)) {
    select.value = previous;
  }
}

function syncLabourReportProjectOptions() {
  const list = document.getElementById("labourReportProjectOptions");
  if (!list) return;
  const input = document.getElementById("labourReportProject");
  const previous = input ? String(input.value || "").trim() : "";
  list.innerHTML = "";
  const projects = [...projectsCache]
    .map((project) => normalizeProjectSlugClient(project.slug || project.id))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
  for (const slug of projects) {
    const option = document.createElement("option");
    option.value = slug;
    list.appendChild(option);
  }
  if (input && previous) input.value = previous;
}

function renderLabourSummary() {
  if (labourersCountEl) labourersCountEl.textContent = String(labourersCache.length || 0);
  const paidT = sumWeightedPaidHoursClient(labourTodayCache);
  const workedT = sumHoursClient(labourTodayCache);
  if (labourTodayHoursEl) labourTodayHoursEl.textContent = `${formatHoursClient(paidT)} paid`;
  if (labourTodayEntriesEl) {
    const nT = labourTodayCache.length || 0;
    labourTodayEntriesEl.textContent =
      Math.abs(paidT - workedT) < 0.01
        ? `${nT} entries`
        : `${nT} entries · ${formatHoursClient(workedT)}h on site`;
  }
  const paidW = sumWeightedPaidHoursClient(labourWeekCache);
  const workedW = sumHoursClient(labourWeekCache);
  if (labourWeekHoursEl) labourWeekHoursEl.textContent = `${formatHoursClient(paidW)} paid`;
  if (labourWeekEntriesEl) {
    const nW = labourWeekCache.length || 0;
    labourWeekEntriesEl.textContent =
      Math.abs(paidW - workedW) < 0.01
        ? `${nW} entries`
        : `${nW} entries · ${formatHoursClient(workedW)}h on site`;
  }
  const paidM = sumWeightedPaidHoursClient(labourMonthCache);
  const workedM = sumHoursClient(labourMonthCache);
  if (labourMonthHoursEl) labourMonthHoursEl.textContent = `${formatHoursClient(paidM)} paid`;
  if (labourMonthEntriesEl) {
    const nM = labourMonthCache.length || 0;
    labourMonthEntriesEl.textContent =
      Math.abs(paidM - workedM) < 0.01
        ? `${nM} entries`
        : `${nM} entries · ${formatHoursClient(workedM)}h on site`;
  }
}

function renderLabourersList() {
  if (!labourersEl) return;
  if (!labourersCache.length) {
    labourersEl.innerHTML = '<div class="row-item muted">No labourers registered yet.</div>';
    return;
  }
  labourersEl.innerHTML = labourersCache
    .map((item) => {
      const phone = String(item.phoneE164 || item.id || "").trim();
      const projects = Array.isArray(item.projectSlugs) ? item.projectSlugs.join(", ") : "";
      const activePill = item.active === false ? '<span class="pill pill-warn">inactive</span>' : '<span class="pill pill-ai">active</span>';
      return `
        <div class="row-item" data-labourer-phone="${esc(phone)}">
          <div>${activePill}</div>
          <div><strong>${esc(labourerLabelClient(item) || phone || "-")}</strong></div>
          <div class="muted small mono">${esc(phone || "-")}</div>
          <div>Projects: ${esc(projects || "-")}</div>
          <div class="muted small">Updated: ${fmtTime(item.updatedAt)}</div>
          <div class="project-manager-actions">
            <button type="button" class="btn-secondary" data-labourer-edit="${esc(phone)}">Edit</button>
            <button type="button" class="btn-secondary btn-danger" data-labourer-delete="${esc(phone)}">Delete</button>
          </div>
        </div>`;
    })
    .join("");
}

function renderLabourEntriesList() {
  if (!labourEntriesEl) return;
  if (!labourEntriesCache.length) {
    labourEntriesEl.innerHTML = '<div class="row-item muted">No labour entries yet. Text hours to the Twilio number or save one here.</div>';
    return;
  }
  labourEntriesEl.innerHTML = labourEntriesCache
    .slice()
    .sort((a, b) => {
      const aTs = a.createdAt && a.createdAt.seconds ? a.createdAt.seconds : 0;
      const bTs = b.createdAt && b.createdAt.seconds ? b.createdAt.seconds : 0;
      return bTs - aTs;
    })
    .map((entry) => {
      const title = labourerLabelClient(entry) || entry.labourerPhone || "Unknown";
      const project = entry.projectSlug ? `Project: ${entry.projectSlug}` : "Project: -";
      const notes = entry.notes ? `<div class="muted small">Notes: ${esc(String(entry.notes).slice(0, 220))}</div>` : "";
      const h = Number(entry.hours) || 0;
      const mult = dayMultiplierFromDateKeyClient(String(entry.reportDateKey || "").trim());
      const paidH = Math.round(h * mult * 100) / 100;
      const hourPill =
        mult !== 1
          ? `${esc(formatHoursClient(h))}h → ${esc(formatHoursClient(paidH))} paid`
          : `${esc(formatHoursClient(h))}h`;
      return `
        <div class="row-item" data-labour-entry-id="${esc(entry.id)}">
          <div><span class="pill pill-issue">${hourPill}</span><span class="pill pill-ai">${esc(entry.reportDateKey || "-")}</span></div>
          <div><strong>${esc(title)}</strong> · ${project}</div>
          <div class="muted small">${esc(String(entry.workOn || "").slice(0, 260))}</div>
          ${notes}
          <div class="muted small mono">${fmtTime(entry.createdAt)} · ${esc(entry.source || "dashboard")} · ${esc(entry.labourerPhone || "")}</div>
        </div>`;
    })
    .join("");
}

function renderLabourReportsList() {
  if (!labourReportsEl) return;
  if (!labourReportsCache.length) {
    labourReportsEl.innerHTML = '<div class="row-item muted">No labour reports generated yet.</div>';
    return;
  }
  labourReportsEl.innerHTML = labourReportsCache
    .slice()
    .sort((a, b) => {
      const aTs = a.createdAt && a.createdAt.seconds ? a.createdAt.seconds : 0;
      const bTs = b.createdAt && b.createdAt.seconds ? b.createdAt.seconds : 0;
      return bTs - aTs;
    })
    .map((report) => {
      const scopeBits = [
        report.reportTitle || "Labour Hours Report",
        report.labourerName || report.labourerPhone || "All labourers",
        report.projectSlug || "All projects",
        report.startKey && report.endKey ? `${report.startKey} to ${report.endKey}` : report.startKey || report.endKey || "",
      ].filter(Boolean);
      const link =
        report.downloadURL && String(report.downloadURL).trim()
          ? `<a href="${esc(report.downloadURL)}" target="_blank" rel="noopener">Open PDF</a>`
          : report.storagePath
            ? `<a href="#" class="daily-report-pdf-pending" data-storage-path="${esc(report.storagePath)}" onclick="return false">Resolving PDF link...</a>`
            : "";
      return `
        <div class="row-item">
          <div class="mono">${fmtTime(report.createdAt)}</div>
          <div><strong>${esc(report.reportTitle || "Labour Hours Report")}</strong></div>
          <div class="muted small">${esc(scopeBits.slice(1).join(" · "))}</div>
          <div class="muted small">${
            Number.isFinite(Number(report.totalPaidHours)) && Math.abs(Number(report.totalPaidHours) - Number(report.totalHours || 0)) > 0.01
              ? `${esc(formatHoursClient(report.totalHours))}h on site · ${esc(formatHoursClient(report.totalPaidHours))} paid`
              : `${esc(formatHoursClient(report.totalHours))} hours`
          } · ${esc(String(report.totalEntries || 0))} entries</div>
          <div>${link}</div>
        </div>`;
    })
    .join("");
  scheduleHydrateMediaThumbs();
}

function renderLabourPanel() {
  syncLabourReportLabourerOptions();
  syncLabourReportProjectOptions();
  renderLabourSummary();
  renderLabourersList();
  renderLabourEntriesList();
  renderLabourReportsList();
}

function syncLabourReportDateDefaults() {
  const startInput = document.getElementById("labourReportStart");
  const endInput = document.getElementById("labourReportEnd");
  if (!startInput || !endInput) return;
  const today = todayDateKeyEastern();
  if (!String(startInput.value || "").trim()) startInput.value = startOfWeekDateKeyClient(today);
  if (!String(endInput.value || "").trim()) endInput.value = today;
}

function setLabourReportRange(mode) {
  const startInput = document.getElementById("labourReportStart");
  const endInput = document.getElementById("labourReportEnd");
  if (!startInput || !endInput) return;
  const today = todayDateKeyEastern();
  if (mode === "today") {
    startInput.value = today;
    endInput.value = today;
  } else if (mode === "week") {
    startInput.value = startOfWeekDateKeyClient(today);
    endInput.value = today;
  } else if (mode === "month") {
    startInput.value = startOfMonthDateKeyClient(today);
    endInput.value = today;
  }
}

function loadLabourerIntoForm(phone) {
  const target = String(phone || "").trim();
  if (!target) return;
  const item = labourersCache.find((row) => String(row.phoneE164 || row.id || "").trim() === target);
  if (!item) return;
  const phoneInput = document.getElementById("labourerPhone");
  const nameInput = document.getElementById("labourerName");
  const projectsInput = document.getElementById("labourerProjects");
  const activeInput = document.getElementById("labourerActive");
  const result = document.getElementById("labourerManagerResult");
  if (phoneInput) phoneInput.value = target;
  if (nameInput) nameInput.value = String(item.displayName || item.name || "").trim();
  if (projectsInput) projectsInput.value = Array.isArray(item.projectSlugs) ? item.projectSlugs.join(", ") : "";
  if (activeInput) activeInput.checked = item.active !== false;
  if (result) {
    result.textContent = `Editing ${item.displayName || item.name || target}`;
    result.className = "project-manager-result muted small";
  }
}

function clearLabourerForm() {
  const phoneInput = document.getElementById("labourerPhone");
  const nameInput = document.getElementById("labourerName");
  const projectsInput = document.getElementById("labourerProjects");
  const activeInput = document.getElementById("labourerActive");
  if (phoneInput) phoneInput.value = "";
  if (nameInput) nameInput.value = "";
  if (projectsInput) projectsInput.value = "";
  if (activeInput) activeInput.checked = true;
}

function renderAssistantComposer() {
  const phoneSelect = document.getElementById("assistantComposerPhone");
  const details = document.getElementById("assistantComposerProject");
  const sendButton = document.getElementById("assistantComposerSendBtn");
  const bodyInput = document.getElementById("assistantComposerBody");
  const photoInput = document.getElementById("assistantComposerPhotos");
  const schedulePhoneSelect = document.getElementById("assistantSchedulePhone");
  const scheduleDetails = document.getElementById("assistantScheduleProjectDetails");
  const scheduleFileInput = document.getElementById("assistantScheduleFile");
  const scheduleParseButton = document.getElementById("assistantScheduleParseBtn");
  const scheduleCreateReportButton = document.getElementById("assistantScheduleCreateReportBtn");
  const scheduleCreateCloseoutButton = document.getElementById("assistantScheduleCreateCloseoutBtn");
  const scheduleProjectInput = document.getElementById("assistantScheduleProject");
  if (!phoneSelect || !details || !sendButton || !bodyInput) return;

  const user = resolveSmsUserForAssistant(phoneSelect.value);
  const hasBody = String(bodyInput.value || "").trim() !== "";
  const photoCount = photoInput && photoInput.files ? photoInput.files.length : 0;
  const scheduleFileCount = scheduleFileInput && scheduleFileInput.files ? scheduleFileInput.files.length : 0;
  const scheduleUser = resolveSmsUserForAssistant(schedulePhoneSelect ? schedulePhoneSelect.value : "");
  // Keep action button clickable so validation messages can explain what is missing.
  sendButton.disabled = false;
  if (scheduleParseButton) scheduleParseButton.disabled = !scheduleFileCount;
  // Keep report button clickable; handler validates requirements and prints explicit messages.
  if (scheduleCreateReportButton) scheduleCreateReportButton.disabled = false;
  if (scheduleCreateCloseoutButton) scheduleCreateCloseoutButton.disabled = false;

  if (!user) {
    details.textContent = "Select a phone to use its active project context.";
  } else {
    const projects = getAccessibleProjectsForUser(user);
    details.innerHTML = `
      <div><strong>Active project:</strong> ${esc(user.activeProjectSlug || "none")}</div>
      <div><strong>Project count:</strong> ${esc(String(projects.length || 0))}</div>
      <div><strong>Photos queued:</strong> ${esc(String(photoCount))}</div>
      <div><strong>Message ready:</strong> ${hasBody ? "yes" : "no"}</div>
      <div class="muted small">Backdated example: log note (2026-04-16) Dewatering complete on east side</div>
    `;
  }

  if (!scheduleDetails) return;

  if (!scheduleUser) {
    scheduleDetails.textContent =
      "Select a phone to use its active project context for lookahead parsing and report generation.";
    return;
  }

  const scheduleProjects = getAccessibleProjectsForUser(scheduleUser);
  const activeProject = getActiveProjectRecordForUser(scheduleUser);
  if (scheduleProjectInput && !String(scheduleProjectInput.value || "").trim() && activeProject) {
    scheduleProjectInput.value = activeProject.name || activeProject.slug || activeProject.id || "";
  }
  scheduleDetails.innerHTML = `
    <div><strong>Active project:</strong> ${esc(scheduleUser.activeProjectSlug || "none")}</div>
    <div><strong>Project count:</strong> ${esc(String(scheduleProjects.length || 0))}</div>
    <div><strong>Schedules queued:</strong> ${esc(String(scheduleFileCount))}</div>
    <div class="muted small">Use this page for parse, activities PDF, and closeout PDF generation.</div>
  `;
}

function clearAdminPanels() {
  const placeholder = '<div class="row-item muted small">Sign in to load this section.</div>';
  if (messagesEl) messagesEl.innerHTML = placeholder;
  if (usersEl) usersEl.innerHTML = placeholder;
  if (appMembersEl) appMembersEl.innerHTML = placeholder;
  if (labourersEl) labourersEl.innerHTML = placeholder;
  if (labourEntriesEl) labourEntriesEl.innerHTML = placeholder;
  if (labourReportsEl) labourReportsEl.innerHTML = placeholder;
  if (issuesEl) issuesEl.innerHTML = placeholder;
  if (summariesEl) summariesEl.innerHTML = placeholder;
  if (dailyReportsEl) dailyReportsEl.innerHTML = placeholder;
  if (logEntriesEl) logEntriesEl.innerHTML = placeholder;
  if (mediaEl) mediaEl.innerHTML = placeholder;
  if (projectNoteEditRequestsEl) projectNoteEditRequestsEl.innerHTML = placeholder;
}

function resetAdminCaches() {
  messagesCache = [];
  smsUsersCache = [];
  projectsCache = [];
  appMembersCache = [];
  issueLogsCache = [];
  summariesCache = [];
  dailyReportsCache = [];
  mediaCache = [];
  logEntriesCache = [];
  projectNoteRequestsCache = [];
  labourersCache = [];
  labourEntriesCache = [];
  labourReportsCache = [];
  labourTodayCache = [];
  labourWeekCache = [];
  labourMonthCache = [];
  currentAppAccess = null;
  renderDashboard();
  renderLabourPanel();
}

function nonEmptyStringErr(value) {
  return typeof value === "string" && value.trim() !== "";
}

function mediaLooksLikeImage(media, contentTypeLower) {
  if (contentTypeLower.startsWith("image/")) return true;
  const storagePath = String(media.storagePath || "").toLowerCase();
  return /\.(jpe?g|png|gif|webp|heic|bmp)(\?.*)?$/i.test(storagePath);
}

function mediaThumbOrFallback(media) {
  const contentType = String(media.contentType || "").trim().toLowerCase();
  const blocked = nonEmptyStringErr(media.downloadError) || nonEmptyStringErr(media.uploadError);
  const directUrl = media.downloadURL && !blocked ? String(media.downloadURL).trim() : "";
  const storagePath = media.storagePath && !blocked ? String(media.storagePath).trim() : "";
  const asImage = mediaLooksLikeImage(media, contentType);
  const mediaId = String(media.id || "");

  if (directUrl && asImage) {
    return `<div class="media-thumb-cell"><a href="#" class="media-thumb-link" data-media-id="${esc(mediaId)}" data-media-url="${esc(directUrl)}" onclick="return false"><img class="media-thumb" src="${esc(directUrl)}" alt="" loading="lazy" referrerpolicy="no-referrer" title="View image" /></a></div>`;
  }
  if (directUrl) {
    return `<div class="media-thumb-cell"><div class="media-thumb-fallback"><a href="${esc(directUrl)}" target="_blank" rel="noopener">Open file</a><br /><span class="muted small">${esc(contentType || "file")}</span></div></div>`;
  }
  if (storagePath && asImage) {
    return `<div class="media-thumb-cell"><a href="#" class="media-thumb-link" data-media-id="${esc(mediaId)}" rel="noopener" onclick="return false" title="Loading from Storage..."><img class="media-thumb media-thumb-pending" data-storage-path="${esc(storagePath)}" src="${TRANSPARENT_PIXEL}" alt="" loading="lazy" /></a></div>`;
  }
  if (storagePath) {
    return `<div class="media-thumb-cell"><div class="media-thumb-fallback muted small"><span data-storage-path="${esc(storagePath)}" class="media-file-pending">Resolving file link...</span></div></div>`;
  }
  return `<div class="media-thumb-cell"><div class="media-thumb-fallback media-thumb-fallback-detail">${esc(String(media.downloadError || media.uploadError || "No image URL or storagePath."))}</div></div>`;
}

function mediaCaptionSummary(media) {
  return String(media?.captionText || "").trim() || "(no caption)";
}

function mediaMetaLine(media) {
  const bits = [
    media?.projectId || "-",
    media?.dateKey || "-",
    media?.senderPhone || "-",
    media?.linkedLogEntryId || "-",
  ];
  return `project ${bits[0]} · date ${bits[1]} · from ${bits[2]} · logEntry ${bits[3]}`;
}

function allImageMediaItems(docs) {
  return (docs || []).filter((media) =>
    mediaLooksLikeImage(media, String(media?.contentType || "").trim().toLowerCase())
  );
}

async function hydrateAllMediaThumbs() {
  const pendingImages = document.querySelectorAll("img.media-thumb-pending[data-storage-path]");
  for (const img of pendingImages) {
    const storagePath = img.getAttribute("data-storage-path");
    if (!storagePath) continue;
    try {
      const url = await getCachedStorageDownloadURL(storagePath);
      if (!url) throw new Error("Empty URL from Storage");
      img.src = url;
      img.classList.remove("media-thumb-pending");
      img.removeAttribute("data-storage-path");
      const anchor = img.closest("a.media-thumb-link");
      if (anchor) {
        anchor.dataset.mediaUrl = url;
      }
    } catch (err) {
      img.classList.remove("media-thumb-pending");
      const fallback = document.createElement("div");
      fallback.className = "media-thumb-fallback media-thumb-fallback-detail";
      fallback.textContent = "Could not resolve image URL from Storage.";
      fallback.title = String(err?.message || err);
      const cell = img.closest(".media-thumb-cell");
      if (cell) {
        cell.innerHTML = "";
        cell.appendChild(fallback);
      } else {
        img.replaceWith(fallback);
      }
    }
  }

  const pendingFiles = document.querySelectorAll("span.media-file-pending[data-storage-path]");
  for (const span of pendingFiles) {
    const storagePath = span.getAttribute("data-storage-path");
    if (!storagePath) continue;
    try {
      const url = await getCachedStorageDownloadURL(storagePath);
      if (!url) throw new Error("Empty URL from Storage");
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.target = "_blank";
      anchor.rel = "noopener";
      anchor.textContent = "Open file";
      span.replaceWith(anchor);
    } catch (err) {
      span.textContent = "Could not resolve file URL";
      span.title = String(err?.message || err);
      span.classList.remove("media-file-pending");
    }
  }

  const pendingReports = document.querySelectorAll("a.daily-report-pdf-pending[data-storage-path]");
  for (const anchor of pendingReports) {
    const storagePath = anchor.getAttribute("data-storage-path");
    if (!storagePath) continue;
    try {
      const url = await getCachedStorageDownloadURL(storagePath);
      if (!url) throw new Error("Empty URL from Storage");
      anchor.href = url;
      anchor.textContent = "Open PDF";
      anchor.classList.remove("daily-report-pdf-pending");
      anchor.removeAttribute("data-storage-path");
      anchor.removeAttribute("onclick");
    } catch (err) {
      const wrap = document.createElement("div");
      wrap.innerHTML = `<div class="muted small">Could not resolve PDF from Storage.</div>`;
      anchor.replaceWith(wrap);
      console.warn("gridline: daily report getDownloadURL failed", storagePath, err);
    }
  }
}

async function resolveMediaDisplayUrl(media) {
  const directUrl = media?.downloadURL ? String(media.downloadURL).trim() : "";
  if (directUrl) return directUrl;
  const storagePath = media?.storagePath ? String(media.storagePath).trim() : "";
  if (!storagePath) return null;
  return getCachedStorageDownloadURL(storagePath);
}

function syncMediaViewerButtons() {
  const hasItems = mediaViewerItems.length > 0;
  if (mediaViewerPrevEl) mediaViewerPrevEl.disabled = !hasItems || mediaViewerIndex <= 0;
  if (mediaViewerNextEl) mediaViewerNextEl.disabled = !hasItems || mediaViewerIndex >= mediaViewerItems.length - 1;
}

async function renderMediaViewer() {
  const media = mediaViewerItems[mediaViewerIndex];
  if (!media) {
    if (mediaViewerImageEl) mediaViewerImageEl.classList.add("hidden");
    if (mediaViewerStatusEl) mediaViewerStatusEl.textContent = "No image selected.";
    if (mediaViewerCaptionEl) mediaViewerCaptionEl.textContent = "";
    if (mediaViewerDetailsEl) mediaViewerDetailsEl.textContent = "";
    if (mediaViewerIndexEl) mediaViewerIndexEl.textContent = "";
    syncMediaViewerButtons();
    return;
  }

  if (mediaViewerImageEl) mediaViewerImageEl.classList.add("hidden");
  if (mediaViewerStatusEl) mediaViewerStatusEl.textContent = "Loading image...";
  if (mediaViewerCaptionEl) mediaViewerCaptionEl.textContent = mediaCaptionSummary(media);
  if (mediaViewerDetailsEl) {
    mediaViewerDetailsEl.textContent =
      `${mediaMetaLine(media)} · message ${media.sourceMessageId || "-"} · media ${media.id || "-"}`;
  }
  if (mediaViewerIndexEl) {
    mediaViewerIndexEl.textContent = `${mediaViewerIndex + 1} / ${mediaViewerItems.length}`;
  }
  syncMediaViewerButtons();

  try {
    const url = await resolveMediaDisplayUrl(media);
    if (!url) throw new Error("No image URL available.");
    if (mediaViewerImageEl) {
      mediaViewerImageEl.src = url;
      mediaViewerImageEl.classList.remove("hidden");
    }
    if (mediaViewerStatusEl) mediaViewerStatusEl.textContent = "";
  } catch (err) {
    if (mediaViewerStatusEl) {
      mediaViewerStatusEl.textContent = `Could not load image: ${err?.message || err}`;
    }
  }
}

function closeMediaViewer() {
  if (mediaViewerBackdropEl) {
    mediaViewerBackdropEl.classList.add("hidden");
    mediaViewerBackdropEl.setAttribute("aria-hidden", "true");
  }
}

async function openMediaViewerById(mediaId) {
  const items = allImageMediaItems(mediaCache);
  const index = items.findIndex((media) => String(media.id || "") === String(mediaId || ""));
  if (index < 0) return;
  mediaViewerItems = items;
  mediaViewerIndex = index;
  if (mediaViewerBackdropEl) {
    mediaViewerBackdropEl.classList.remove("hidden");
    mediaViewerBackdropEl.setAttribute("aria-hidden", "false");
  }
  await renderMediaViewer();
}

async function stepMediaViewer(delta) {
  if (!mediaViewerItems.length) return;
  const nextIndex = mediaViewerIndex + delta;
  if (nextIndex < 0 || nextIndex >= mediaViewerItems.length) return;
  mediaViewerIndex = nextIndex;
  await renderMediaViewer();
}

function groupMediaBySourceMessage(docs) {
  const grouped = new Map();
  for (const media of docs) {
    const key = media.sourceMessageId || `orphan-${media.id}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(media);
  }
  for (const items of grouped.values()) {
    items.sort((a, b) => (a.mediaIndex ?? 0) - (b.mediaIndex ?? 0));
  }
  return [...grouped.entries()].sort((a, b) => {
    const ta = a[1][0]?.createdAt?.seconds ?? 0;
    const tb = b[1][0]?.createdAt?.seconds ?? 0;
    return tb - ta;
  });
}

function renderMediaPanel() {
  if (!mediaEl) return;
  if (!mediaCache.length) {
    mediaEl.innerHTML = '<div class="row-item muted">No media records yet. Send an MMS to the Twilio number.</div>';
    return;
  }
  const imageItems = allImageMediaItems(mediaCache);
  const galleryBlock = `
    <div class="media-library-section">
      <div class="media-library-heading">All uploaded images</div>
      <div class="muted small">${esc(String(imageItems.length))} image(s). Click any image to view it full screen and move next or back.</div>
      <div class="media-gallery-grid">${imageItems.map((media) => mediaThumbOrFallback(media)).join("")}</div>
    </div>`;
  const groupedBlock = `
    <div class="media-library-section">
      <div class="media-library-heading">Grouped by source message</div>
      ${groupMediaBySourceMessage(mediaCache)
        .map(([sourceId, items]) => {
          const first = items[0];
          const meta = [
            `project: ${esc(first.projectId || "-")}`,
            `date: ${esc(first.dateKey || "-")}`,
            `from: ${esc(first.senderPhone || "-")}`,
            `msg doc: ${esc(sourceId)}`,
            `logEntry: ${esc(first.linkedLogEntryId || "-")}`,
          ].join(" · ");
          return `
            <div class="media-group">
              <div class="media-group-head">${meta}</div>
              <div class="muted small">${esc((first.captionText || "").slice(0, 200) || "(no caption)")}</div>
              <div class="media-thumb-grid">${items.map((media) => mediaThumbOrFallback(media)).join("")}</div>
            </div>`;
        })
        .join("")}
    </div>`;
  mediaEl.innerHTML = galleryBlock + groupedBlock;
  scheduleHydrateMediaThumbs();
}

function renderLogEntriesWithMedia() {
  if (!logEntriesEl) return;
  const mediaByLog = {};
  for (const media of mediaCache) {
    if (!media.linkedLogEntryId) continue;
    if (!mediaByLog[media.linkedLogEntryId]) mediaByLog[media.linkedLogEntryId] = [];
    mediaByLog[media.linkedLogEntryId].push(media);
  }
  if (!logEntriesCache.length) {
    logEntriesEl.innerHTML =
      '<div class="row-item muted">No log entries yet. Send field SMS or use log safety:, log delay:, etc.</div>';
    return;
  }
  logEntriesEl.innerHTML = logEntriesCache
    .map((entry) => {
      const ai = entry.aiEnhanced
        ? '<span class="pill pill-ai">AI+</span>'
        : entry.aiError
          ? '<span class="pill pill-warn">AI enh err</span>'
          : "";
      const sections = Array.isArray(entry.dailySummarySections)
        ? entry.dailySummarySections.join(", ")
        : "";
      const inclusion =
        entry.includeInDailySummary === false
          ? '<span class="pill pill-warn">excl. day sum.</span>'
          : '<span class="pill pill-ai">in day log</span>';
      const text = entry.summaryText || entry.normalizedText || entry.rawText || "";
      const thumbs = Array.isArray(mediaByLog[entry.id]) ? mediaByLog[entry.id] : [];
      return `
        <div class="row-item">
          <span class="pill pill-issue">${esc(entry.category || "-")}</span>${ai}${inclusion}
          <div class="mono">${fmtTime(entry.createdAt)} · ${esc(entry.dateKey || "")}</div>
          <div class="muted small">Sections: ${esc(sections || "-")} · status ${esc(entry.status || "-")} · openItem ${entry.openItem ? "yes" : "no"} · id <span class="mono">${esc(entry.id)}</span></div>
          <div>${esc(text.slice(0, 220))}${text.length > 220 ? "..." : ""}</div>
          ${thumbs.length ? `<div class="log-entry-thumbs">${thumbs.map((media) => mediaThumbOrFallback(media)).join("")}</div>` : ""}
          <div class="muted small">${esc(entry.senderPhone || "")} · project ${esc(entry.projectSlug || "-")}</div>
        </div>`;
    })
    .join("");
  scheduleHydrateMediaThumbs();
}

function bindQuery(q, targetEl, render, onDocs, label = "") {
  return onSnapshot(
    q,
    (snap) => {
      setStatusOk();
      const docs = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      targetEl.innerHTML = render(docs);
      if (typeof onDocs === "function") onDocs(docs);
    },
    (err) => {
      const message = label ? `${label}: ${err.message}` : err.message;
      setStatusError(message);
      targetEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
    }
  );
}

function stopAdminListeners() {
  for (const unsub of appUnsubscribers) {
    try {
      if (typeof unsub === "function") unsub();
    } catch (_) {}
  }
  appUnsubscribers = [];
}

function startAdminListeners() {
  stopAdminListeners();
  const isAdmin = roleAtLeastClient(currentUserRole(), "admin");
  const isManagement = roleAtLeastClient(currentUserRole(), "management");
  const projectSlugs = currentUserProjectSlugs().slice(0, 10);
  const userEmail = currentUserEmail();
  const managedProjectSlugs = currentUserManagedProjectSlugs();
  const managedProjectKeys = currentUserScopedProjectKeys();

  const renderReports = (docs) => {
    const sorted = [...docs].sort((a, b) => {
      const aTs = a.createdAt && a.createdAt.seconds ? a.createdAt.seconds : 0;
      const bTs = b.createdAt && b.createdAt.seconds ? b.createdAt.seconds : 0;
      return bTs - aTs;
    });
    if (!sorted.length) {
      return '<div class="row-item muted">No PDF reports yet.</div>';
    }
    return sorted
      .map((report) => {
        const storagePath = report.storagePath && String(report.storagePath).trim();
        let link = "";
        if (report.downloadURL && String(report.downloadURL).trim()) {
          link = `<a href="${esc(report.downloadURL)}" target="_blank" rel="noopener">Open PDF</a>`;
        } else if (storagePath) {
          link = `<div class="daily-report-pdf-row"><a href="#" class="daily-report-pdf-pending" data-storage-path="${esc(storagePath)}" target="_blank" rel="noopener" onclick="return false">Resolving PDF link...</a></div>`;
        }
        const err =
          report.downloadUrlError && !report.downloadURL
            ? `<div class="muted small">Signed URL error: ${esc(String(report.downloadUrlError).slice(0, 200))}</div>`
            : "";
        const projectLabel =
          report.projectName || report.projectId || (report.reportType === "journal" ? "personal journal" : "-");
        return `
          <div class="row-item">
            <div class="mono">${fmtTime(report.createdAt)}</div>
            <div class="muted small">${esc(report.phoneE164 || "-")} · ${esc(report.reportType || "dailySiteLog")} · ${esc(projectLabel)} · ${esc(report.dateKey || report.dateRangeStartKey || "")}</div>
            <div>${link}</div>
            ${err}
          </div>`;
      })
      .join("");
  };

  if (isAdmin) {
    appUnsubscribers.push(
      bindQuery(
        query(collection(db, "messages"), orderBy("createdAt", "desc"), limit(40)),
        messagesEl,
        (docs) => {
          if (!docs.length) {
            return '<div class="row-item muted">No messages yet.</div>';
          }
          return docs
            .map((msg) => {
              const dir = msg.direction === "inbound" ? "inbound" : "outbound";
              const pillClass = dir === "inbound" ? "pill-inbound" : "pill-outbound";
              const aiPill =
                msg.aiError || (msg.command === "ai_error" && msg.aiError)
                  ? '<span class="pill pill-warn">AI error</span>'
                  : msg.aiUsed
                    ? '<span class="pill pill-ai">AI</span>'
                    : msg.command
                      ? `<span class="pill pill-ai">${esc(msg.command)}</span>`
                      : "";
              const proj = msg.projectSlug
                ? `<div><strong>Project:</strong> ${esc(msg.projectSlug)}</div>`
                : "";
              const err =
                msg.aiError && dir === "outbound"
                  ? `<div class="muted small">Error: ${esc(msg.aiError)}</div>`
                  : "";
              const mms =
                dir === "inbound" &&
                (msg.numMedia > 0 ||
                  (msg.mediaIds && msg.mediaIds.length) ||
                  msg.mediaAttachedCount > 0)
                  ? `<span class="pill pill-ai">MMS ${msg.numMedia || msg.mediaAttachedCount || (msg.mediaIds && msg.mediaIds.length) || ""}</span>`
                  : "";
              const photoUrls =
                dir === "inbound" && Array.isArray(msg.photoPreviewUrls) ? msg.photoPreviewUrls.filter(Boolean) : [];
              const photoPaths =
                dir === "inbound" && Array.isArray(msg.photoStoragePaths) ? msg.photoStoragePaths.filter(Boolean) : [];
              const photoStrip =
                dir === "inbound" && photoUrls.length
                  ? `<div class="message-photo-strip">${photoUrls
                      .map(
                        (url) =>
                          `<a href="${esc(url)}" target="_blank" rel="noopener" title="Full size"><img class="media-thumb" src="${esc(url)}" alt="" loading="lazy" referrerpolicy="no-referrer" /></a>`
                      )
                      .join("")}</div>`
                  : dir === "inbound" && photoPaths.length
                    ? `<div class="message-photo-strip">${photoPaths
                        .map(
                          (storagePath) =>
                            `<a href="#" class="media-thumb-link" rel="noopener" onclick="return false" title="Loading..."><img class="media-thumb media-thumb-pending" data-storage-path="${esc(storagePath)}" src="${TRANSPARENT_PIXEL}" alt="" loading="lazy" /></a>`
                        )
                        .join("")}</div>`
                    : "";
              return `
                <div class="row-item">
                  <div><span class="pill ${pillClass}">${esc(dir)}</span>${mms}${aiPill}</div>
                  <div class="mono">${fmtTime(msg.createdAt)}</div>
                  <div><strong>From:</strong> ${esc(msg.from)} -> <strong>To:</strong> ${esc(msg.to)}</div>
                  ${proj}
                  <div>${esc(msg.body)}</div>
                  ${photoStrip}
                  ${err}
                  <div class="muted small mono">sid ${esc(msg.messageSid || "")} · doc ${esc(msg.id)}</div>
                </div>`;
            })
            .join("");
      },
      (docs) => {
        messagesCache = docs;
        renderDashboard();
        scheduleHydrateMediaThumbs();
      },
      "messages"
    )
  );

    appUnsubscribers.push(
      onSnapshot(
        query(collection(db, "smsUsers"), orderBy("updatedAt", "desc"), limit(50)),
        (snap) => {
          setStatusOk();
          const docs = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          smsUsersCache = docs;
          syncDailyPdfPhoneSelect(docs);
          syncProjectPhoneSelect(docs);
          syncAssistantComposerPhoneSelect(docs);
          usersEl.innerHTML = !docs.length
            ? '<div class="row-item muted">No users yet (text the Twilio number once).</div>'
            : docs
                .map(
                  (user) => `
                    <div class="row-item">
                      <span class="pill pill-user">user</span>
                      <div class="mono"><strong>${esc(user.phoneE164 || user.id)}</strong></div>
                      <div>Active project: ${esc(user.activeProjectSlug || "-")}</div>
                      <div>Projects: ${esc(String(userProjectSlugs(user).length || 0))}</div>
                      <div>Role: ${esc(user.role || "-")}</div>
                      <div class="muted small">Updated: ${fmtTime(user.updatedAt)}</div>
                    </div>`
                )
                .join("");
          renderProjectManager();
          renderDashboard();
        },
        (err) => {
          const message = `smsUsers: ${err.message}`;
          setStatusError(message);
          usersEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
        }
      )
    );

    appUnsubscribers.push(
      bindQuery(
        query(collection(db, "issueLogs"), orderBy("createdAt", "desc"), limit(30)),
        issuesEl,
        (docs) =>
          !docs.length
            ? '<div class="row-item muted">No issue logs yet. SMS: log issue: ...</div>'
            : docs
                .map(
                  (log) => `
                    <div class="row-item">
                      <span class="pill pill-issue">${esc(log.type)}</span>
                      <div class="mono">${fmtTime(log.createdAt)}</div>
                      <div>${esc(log.message)}</div>
                      <div class="muted small">${esc(log.phoneE164)} · project ${esc(log.projectSlug || "-")}</div>
                    </div>`
                )
                .join(""),
        (docs) => {
          issueLogsCache = docs;
        },
        "issueLogs"
      )
    );

    appUnsubscribers.push(
      bindQuery(
        query(collection(db, "summaries"), orderBy("createdAt", "desc"), limit(15)),
        summariesEl,
        (docs) =>
          !docs.length
            ? '<div class="row-item muted">No saved rollups yet. SMS: daily summary</div>'
            : docs
                .map(
                  (summary) => `
                    <div class="row-item">
                      <div class="mono">${fmtTime(summary.createdAt)}</div>
                      <div class="muted small">${esc(summary.phoneE164)} · ${esc(summary.projectSlug || "no project")}</div>
                      <div>${esc(summary.summaryText)}</div>
                    </div>`
                )
                .join(""),
        (docs) => {
          summariesCache = docs;
        },
        "summaries"
      )
    );

    appUnsubscribers.push(
      onSnapshot(
        query(collection(db, "media"), orderBy("createdAt", "desc")),
        (snap) => {
          setStatusOk();
          mediaCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderMediaPanel();
          renderLogEntriesWithMedia();
        },
        (err) => {
          const message = `media: ${err.message}`;
          setStatusError(message);
          mediaEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
        }
      )
    );

    appUnsubscribers.push(
      onSnapshot(
        query(collection(db, "logEntries"), orderBy("createdAt", "desc"), limit(60)),
        (snap) => {
          setStatusOk();
          logEntriesCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderLogEntriesWithMedia();
        },
        (err) => {
          const message = `logEntries: ${err.message}`;
          setStatusError(message);
          logEntriesEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
        }
      )
    );

    if (appMembersEl) {
      appUnsubscribers.push(
        onSnapshot(
          query(collection(db, "appMembers"), orderBy("updatedAt", "desc"), limit(100)),
          (snap) => {
            setStatusOk();
            appMembersCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
            renderAppMembersList();
          },
          (err) => {
            const message = `appMembers: ${err.message}`;
            setStatusError(message);
            appMembersEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
          }
        )
      );
    }
  } else {
    messagesCache = [];
    smsUsersCache = [];
    issueLogsCache = [];
    summariesCache = [];
    mediaCache = [];
    logEntriesCache = [];
    if (messagesEl) messagesEl.innerHTML = '<div class="row-item muted small">Admin-only section.</div>';
    if (usersEl) usersEl.innerHTML = '<div class="row-item muted small">Admin-only section.</div>';
    if (issuesEl) issuesEl.innerHTML = '<div class="row-item muted small">Admin-only section.</div>';
    if (summariesEl) summariesEl.innerHTML = '<div class="row-item muted small">Admin-only section.</div>';
    if (mediaEl) {
      mediaEl.innerHTML = isManagement
        ? '<div class="row-item muted small">Loading project media...</div>'
        : '<div class="row-item muted small">Admin-only section.</div>';
    }
    if (logEntriesEl) logEntriesEl.innerHTML = '<div class="row-item muted small">Admin-only section.</div>';
    if (appMembersEl) appMembersEl.innerHTML = '<div class="row-item muted small">Admin-only section.</div>';
  }

  if (!isAdmin && isManagement) {
    const smsUsersQuery = currentUserAllProjects()
      ? query(collection(db, "smsUsers"), limit(100))
      : projectSlugs.length
        ? query(collection(db, "smsUsers"), where("activeProjectSlug", "in", projectSlugs), limit(100))
        : null;
    if (smsUsersQuery) {
      appUnsubscribers.push(
        onSnapshot(
          smsUsersQuery,
          (snap) => {
            setStatusOk();
            smsUsersCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
            syncAssistantComposerPhoneSelect(smsUsersCache);
            renderAssistantComposer();
          },
          (err) => {
            const message = `smsUsers: ${err.message}`;
            setStatusError(message);
            smsUsersCache = [];
            syncAssistantComposerPhoneSelect([]);
          }
        )
      );
    } else {
      smsUsersCache = [];
      syncAssistantComposerPhoneSelect([]);
    }

    const mergeScopedDocs = (byQuery) => {
      const merged = new Map();
      for (const docs of byQuery.values()) {
        for (const row of docs) {
          if (!row || !row.id) continue;
          merged.set(row.id, row);
        }
      }
      return Array.from(merged.values()).sort((a, b) => {
        const aTs = a.createdAt && a.createdAt.seconds ? a.createdAt.seconds : 0;
        const bTs = b.createdAt && b.createdAt.seconds ? b.createdAt.seconds : 0;
        return bTs - aTs;
      });
    };

    if (currentUserAllProjects()) {
      appUnsubscribers.push(
        onSnapshot(
          query(collection(db, "media"), orderBy("createdAt", "desc")),
          (snap) => {
            setStatusOk();
            mediaCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
            renderMediaPanel();
          },
          (err) => {
            const message = `media: ${err.message}`;
            setStatusError(message);
            mediaCache = [];
            if (mediaEl) mediaEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
          }
        )
      );
    } else if (managedProjectKeys.length) {
      const mediaByQuery = new Map();
      const syncScopedMedia = () => {
        mediaCache = mergeScopedDocs(mediaByQuery);
        renderMediaPanel();
      };
      const watchScopedMedia = (queryKey, mediaQuery) => {
        appUnsubscribers.push(
          onSnapshot(
            mediaQuery,
            (snap) => {
              setStatusOk();
              mediaByQuery.set(
                queryKey,
                snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }))
              );
              syncScopedMedia();
            },
            (err) => {
              const message = `media(${queryKey}): ${err.message}`;
              setStatusError(message);
              mediaByQuery.delete(queryKey);
              syncScopedMedia();
              if (mediaEl) mediaEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
            }
          )
        );
      };

      for (const projectKey of managedProjectKeys) {
        watchScopedMedia(
          `projectId:${projectKey}`,
          query(collection(db, "media"), where("projectId", "==", projectKey), limit(100))
        );
        watchScopedMedia(
          `projectSlug:${projectKey}`,
          query(collection(db, "media"), where("projectSlug", "==", projectKey), limit(100))
        );
      }
    } else {
      mediaCache = [];
      renderMediaPanel();
    }
  }

  if (isManagement) {
    const todayKey = todayDateKeyEastern();
    const weekStartKey = startOfWeekDateKeyClient(todayKey);
    const monthStartKey = startOfMonthDateKeyClient(todayKey);

    appUnsubscribers.push(
      onSnapshot(
        query(collection(db, "labourers"), orderBy("updatedAt", "desc"), limit(100)),
        (snap) => {
          setStatusOk();
          labourersCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderLabourPanel();
        },
        (err) => {
          const message = `labourers: ${err.message}`;
          setStatusError(message);
          labourersCache = [];
          renderLabourPanel();
          if (labourersEl) labourersEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
        }
      )
    );

    appUnsubscribers.push(
      onSnapshot(
        query(collection(db, "labourReports"), orderBy("createdAt", "desc"), limit(50)),
        (snap) => {
          setStatusOk();
          labourReportsCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderLabourPanel();
        },
        (err) => {
          const message = `labourReports: ${err.message}`;
          setStatusError(message);
          labourReportsCache = [];
          renderLabourPanel();
          if (labourReportsEl) labourReportsEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
        }
      )
    );

    appUnsubscribers.push(
      onSnapshot(
        query(collection(db, "labourEntries"), orderBy("createdAt", "desc"), limit(100)),
        (snap) => {
          setStatusOk();
          labourEntriesCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderLabourPanel();
        },
        (err) => {
          const message = `labourEntries: ${err.message}`;
          setStatusError(message);
          labourEntriesCache = [];
          renderLabourPanel();
          if (labourEntriesEl) labourEntriesEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
        }
      )
    );

    appUnsubscribers.push(
      onSnapshot(
        query(
          collection(db, "labourEntries"),
          where("reportDateKey", "==", todayKey),
          orderBy("createdAt", "asc"),
          limit(5000)
        ),
        (snap) => {
          setStatusOk();
          labourTodayCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderLabourSummary();
        },
        (err) => {
          const message = `labourEntries(today): ${err.message}`;
          setStatusError(message);
          labourTodayCache = [];
          renderLabourSummary();
        }
      )
    );

    appUnsubscribers.push(
      onSnapshot(
        query(
          collection(db, "labourEntries"),
          where("reportDateKey", ">=", weekStartKey),
          where("reportDateKey", "<=", todayKey),
          orderBy("reportDateKey", "asc"),
          orderBy("createdAt", "asc"),
          limit(5000)
        ),
        (snap) => {
          setStatusOk();
          labourWeekCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderLabourSummary();
        },
        (err) => {
          const message = `labourEntries(week): ${err.message}`;
          setStatusError(message);
          labourWeekCache = [];
          renderLabourSummary();
        }
      )
    );

    appUnsubscribers.push(
      onSnapshot(
        query(
          collection(db, "labourEntries"),
          where("reportDateKey", ">=", monthStartKey),
          where("reportDateKey", "<=", todayKey),
          orderBy("reportDateKey", "asc"),
          orderBy("createdAt", "asc"),
          limit(5000)
        ),
        (snap) => {
          setStatusOk();
          labourMonthCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          renderLabourSummary();
        },
        (err) => {
          const message = `labourEntries(month): ${err.message}`;
          setStatusError(message);
          labourMonthCache = [];
          renderLabourSummary();
        }
      )
    );
  } else {
    labourersCache = [];
    labourEntriesCache = [];
    labourReportsCache = [];
    labourTodayCache = [];
    labourWeekCache = [];
    labourMonthCache = [];
    renderLabourPanel();
  }

  const projectQuery = isAdmin || currentUserAllProjects()
    ? collection(db, "projects")
    : null;
  if (projectQuery) {
    appUnsubscribers.push(
      onSnapshot(
        projectQuery,
          (snap) => {
            setStatusOk();
            projectsCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
            renderProjectManager();
            renderProjectNotesRequestForm();
            refreshDailyPdfProjectOptions(findUserByPhone(document.getElementById("dailyPdfPhone")?.value));
            renderDashboard();
            renderLabourPanel();
          },
        (err) => {
          const message = `projects: ${err.message}`;
          setStatusError(message);
          const ownedList = document.getElementById("projectOwnedList");
          const result = document.getElementById("projectManagerResult");
          if (ownedList) ownedList.innerHTML = `<div class="muted small">${esc(message)}</div>`;
          if (result) {
            result.textContent = `Could not load projects: ${err.message}`;
            result.className = "project-manager-result err";
          }
        }
      )
    );
  } else if (projectSlugs.length) {
    const scopedProjectDocs = new Map();
    const renderScopedProjects = () => {
      projectsCache = Array.from(scopedProjectDocs.values());
      renderProjectManager();
      renderProjectNotesRequestForm();
      refreshDailyPdfProjectOptions(findUserByPhone(document.getElementById("dailyPdfPhone")?.value));
      syncDailyPdfPhoneFromAccess();
      renderDashboard();
      renderLabourPanel();
    };
    for (const projectSlug of projectSlugs) {
      appUnsubscribers.push(
        onSnapshot(
          doc(db, "projects", projectSlug),
          (snap) => {
            setStatusOk();
            if (snap.exists()) {
              scopedProjectDocs.set(snap.id, { id: snap.id, ...snap.data() });
            } else {
              scopedProjectDocs.delete(projectSlug);
            }
            renderScopedProjects();
          },
          (err) => {
            const message = `projects/${projectSlug}: ${err.message}`;
            setStatusError(message);
            const ownedList = document.getElementById("projectOwnedList");
            const result = document.getElementById("projectManagerResult");
            if (ownedList) ownedList.innerHTML = `<div class="muted small">${esc(message)}</div>`;
            if (result) {
              result.textContent = `Could not load project ${projectSlug}: ${err.message}`;
              result.className = "project-manager-result err";
            }
          }
        )
      );
    }
  } else {
    projectsCache = [];
    renderProjectNotesRequestForm();
  }

  const reportsQuery = isAdmin
    ? query(collection(db, "dailyReports"), orderBy("createdAt", "desc"))
    : currentUserAllProjects()
      ? query(collection(db, "dailyReports"), limit(100))
      : null;
  if (reportsQuery) {
    appUnsubscribers.push(
      bindQuery(
        reportsQuery,
        dailyReportsEl,
        renderReports,
        (docs) => {
          dailyReportsCache = docs;
          renderProjectNotesRequestForm();
          renderDashboard();
          scheduleHydrateMediaThumbs();
          refreshDailyReportTitleEditor();
        },
        "dailyReports"
      )
    );
  } else if (projectSlugs.length) {
    const scopedReports = new Map();
    const renderScopedReports = () => {
      const mergedDocs = Array.from(scopedReports.values());
      dailyReportsCache = mergedDocs;
      if (dailyReportsEl) dailyReportsEl.innerHTML = renderReports(mergedDocs);
      renderProjectNotesRequestForm();
      renderDashboard();
      scheduleHydrateMediaThumbs();
      refreshDailyReportTitleEditor();
      syncDailyPdfPhoneFromAccess();
    };
    for (const projectSlug of projectSlugs) {
      const reportsByProjectIdQuery = query(
        collection(db, "dailyReports"),
        where("projectId", "==", projectSlug),
        limit(100)
      );
      appUnsubscribers.push(
        onSnapshot(
          reportsByProjectIdQuery,
          (snap) => {
            setStatusOk();
            for (const docSnap of snap.docs) {
              scopedReports.set(docSnap.id, { id: docSnap.id, ...docSnap.data() });
            }
            renderScopedReports();
          },
          (err) => {
            const message = `dailyReports(projectId=${projectSlug}): ${err.message}`;
            setStatusError(message);
            if (dailyReportsEl) dailyReportsEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
          }
        )
      );
    }
  } else {
    dailyReportsCache = [];
    if (dailyReportsEl) dailyReportsEl.innerHTML = '<div class="row-item muted">No assigned project reports yet.</div>';
    renderProjectNotesRequestForm();
    refreshDailyReportTitleEditor();
  }

  let noteRequestsQuery = null;
  if (isAdmin) {
    noteRequestsQuery = query(collection(db, "projectNoteEditRequests"), orderBy("createdAt", "desc"), limit(100));
  } else if (currentUserCanApproveNotes() && (projectSlugs.length || currentUserAllProjects())) {
    noteRequestsQuery = currentUserAllProjects()
      ? query(collection(db, "projectNoteEditRequests"), orderBy("createdAt", "desc"), limit(100))
      : query(collection(db, "projectNoteEditRequests"), where("projectSlug", "in", projectSlugs), limit(100));
  } else if (userEmail) {
    noteRequestsQuery = query(collection(db, "projectNoteEditRequests"), where("requestedByEmail", "==", userEmail), limit(100));
  }
  if (noteRequestsQuery) {
    appUnsubscribers.push(
      onSnapshot(
        noteRequestsQuery,
        (snap) => {
          setStatusOk();
          projectNoteRequestsCache = snap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
          projectNoteRequestsCache.sort((a, b) => {
            const aTs = a.createdAt && a.createdAt.seconds ? a.createdAt.seconds : 0;
            const bTs = b.createdAt && b.createdAt.seconds ? b.createdAt.seconds : 0;
            return bTs - aTs;
          });
          renderProjectNoteRequests();
        },
        (err) => {
          const message = `projectNoteEditRequests: ${err.message}`;
          setStatusError(message);
          if (projectNoteEditRequestsEl) {
            projectNoteEditRequestsEl.innerHTML = `<div class="row-item muted small">${esc(message)}</div>`;
          }
        }
      )
    );
  } else {
    projectNoteRequestsCache = [];
    renderProjectNoteRequests();
  }
}

function currentViewFromHash() {
  const requested = String(window.location.hash || "").replace(/^#/, "").trim().toLowerCase();
  const allowed = new Set([
    "dashboard",
    "assistant",
    "lookahead",
    "messages",
    "reports",
    "approvals",
    "labour",
    "projects",
    "team",
    "tools",
  ]);
  return allowed.has(requested) ? requested : "dashboard";
}

function viewAllowedForCurrentRole(viewName) {
  const panel = pagePanels.find((item) => item.getAttribute("data-view-panel") === viewName);
  const minimumRole = panel ? panel.getAttribute("data-min-role") : "";
  return !minimumRole || roleAtLeastClient(currentUserRole(), minimumRole);
}

function syncAccessControlledUi() {
  const role = currentUserRole();
  for (const link of pageNavLinks) {
    const minimumRole = link.getAttribute("data-min-role");
    link.classList.toggle("hidden", !!minimumRole && !roleAtLeastClient(role, minimumRole));
  }
  for (const panel of pagePanels) {
    const minimumRole = panel.getAttribute("data-min-role");
    panel.dataset.accessHidden = minimumRole && !roleAtLeastClient(role, minimumRole) ? "true" : "false";
  }
  for (const node of Array.from(document.querySelectorAll("[data-min-role]"))) {
    const minimumRole = node.getAttribute("data-min-role");
    node.classList.toggle("hidden", !!minimumRole && !roleAtLeastClient(role, minimumRole));
  }
}

function applyView(viewName) {
  const fallbacks = ["dashboard", "assistant", "lookahead", "reports", "labour", "tools", "approvals"];
  const fallbackView = fallbacks.find((candidate) => viewAllowedForCurrentRole(candidate)) || "reports";
  const resolvedView = viewAllowedForCurrentRole(viewName) ? viewName : fallbackView;
  for (const panel of pagePanels) {
    const panelView = panel.getAttribute("data-view-panel");
    const blocked = panel.dataset.accessHidden === "true";
    panel.classList.toggle("hidden", blocked || panelView !== resolvedView);
  }
  for (const link of pageNavLinks) {
    link.classList.toggle("active", link.getAttribute("data-view-link") === resolvedView && !link.classList.contains("hidden"));
  }
}

function initNavigation() {
  syncAccessControlledUi();
  applyView(currentViewFromHash());
  window.addEventListener("hashchange", () => {
    applyView(currentViewFromHash());
  });
  for (const button of quickViewButtons) {
    button.addEventListener("click", () => {
      const view = button.getAttribute("data-view-target");
      if (view) window.location.hash = view;
    });
  }
}

function initMediaViewer() {
  if (!mediaEl || !mediaViewerBackdropEl) return;

  mediaEl.addEventListener("click", async (event) => {
    const link = event.target.closest("a.media-thumb-link[data-media-id]");
    if (!link) return;
    event.preventDefault();
    await openMediaViewerById(link.getAttribute("data-media-id"));
  });

  if (mediaViewerCloseEl) {
    mediaViewerCloseEl.addEventListener("click", () => closeMediaViewer());
  }
  if (mediaViewerPrevEl) {
    mediaViewerPrevEl.addEventListener("click", () => {
      void stepMediaViewer(-1);
    });
  }
  if (mediaViewerNextEl) {
    mediaViewerNextEl.addEventListener("click", () => {
      void stepMediaViewer(1);
    });
  }
  mediaViewerBackdropEl.addEventListener("click", (event) => {
    if (event.target === mediaViewerBackdropEl) closeMediaViewer();
  });
  window.addEventListener("keydown", (event) => {
    if (mediaViewerBackdropEl.classList.contains("hidden")) return;
    if (event.key === "Escape") {
      closeMediaViewer();
    } else if (event.key === "ArrowLeft") {
      event.preventDefault();
      void stepMediaViewer(-1);
    } else if (event.key === "ArrowRight") {
      event.preventDefault();
      void stepMediaViewer(1);
    }
  });
}

function initDailyReportTitleEditor() {
  const btn = document.getElementById("reportTitleSaveBtn");
  const select = document.getElementById("reportTitleEditSelect");
  const input = document.getElementById("reportTitleEditInput");
  const result = document.getElementById("reportTitleEditResult");
  if (!btn || !select || !input) return;
  btn.addEventListener("click", async () => {
    const reportId = String(select.value || "").trim();
    const reportTitle = String(input.value || "").trim();
    if (!reportId) {
      if (result) {
        result.textContent = "Select a report first.";
        result.className = "daily-pdf-result err";
      }
      return;
    }
    if (!reportTitle) {
      if (result) {
        result.textContent = "Enter a title.";
        result.className = "daily-pdf-result err";
      }
      return;
    }
    btn.disabled = true;
    if (result) {
      result.textContent = "Saving...";
      result.className = "daily-pdf-result muted small";
    }
    try {
      const tokenInput = document.getElementById("dailyPdfToken");
      const token = tokenInput && tokenInput.value ? tokenInput.value.trim() : "";
      const payload = { reportId, reportTitle };
      if (token) payload.token = token;
      await callDashboardFunction("updateDailyReportCallable", payload);
      if (result) {
        result.textContent = "Title updated.";
        result.className = "daily-pdf-result ok";
      }
      input.value = "";
    } catch (err) {
      if (result) {
        result.textContent = `Failed: ${err?.message || err}`;
        result.className = "daily-pdf-result err";
      }
    } finally {
      btn.disabled = false;
    }
  });
}

function initDailyPdfFromDashboard() {
  const phoneSelect = document.getElementById("dailyPdfPhone");
  const projectInput = document.getElementById("dailyPdfProject");
  const reportTypeSelect = document.getElementById("dailyPdfReportType");
  const dateInput = document.getElementById("dailyPdfDate");
  const tokenInput = document.getElementById("dailyPdfToken");
  const button = document.getElementById("dailyPdfBtn");
  const output = document.getElementById("dailyPdfResult");
  if (!phoneSelect || !projectInput || !reportTypeSelect || !dateInput || !button || !output) return;

  if (!dateInput.value) dateInput.value = todayDateKeyEastern();

  phoneSelect.addEventListener("change", () => {
    const user = findUserByPhone(phoneSelect.value);
    projectInput.value = user ? user.activeProjectSlug || "" : "";
    refreshDailyPdfProjectOptions(user);
  });

  button.addEventListener("click", async () => {
    const phoneE164 = String(phoneSelect.value || "").trim();
    if (!phoneE164) {
      output.textContent = "Select a phone number.";
      output.className = "daily-pdf-result err";
      return;
    }

    button.disabled = true;
    output.textContent = "Generating...";
    output.className = "daily-pdf-result muted small";

    const user = findUserByPhone(phoneSelect.value);
    let projectSlug =
      normalizeProjectSlugClient(projectInput.value.trim()) ||
      (user ? normalizeProjectSlugClient(user.activeProjectSlug || "") : "");
    const accessible = user ? getAccessibleProjectsForUser(user).map((p) => normalizeProjectSlugClient(p.slug || p.id)) : [];
    if (!projectSlug && accessible.length === 1) {
      projectSlug = accessible[0];
    }
    if (!projectSlug) {
      output.textContent =
        "Enter or pick a project slug (datalist). Daily site logs need a project so all field data for that site is included.";
      output.className = "daily-pdf-result err";
      button.disabled = false;
      return;
    }
    projectInput.value = projectSlug;

    const payload = {
      phoneE164,
      projectSlug,
      reportType: reportTypeSelect.value || "dailySiteLog",
      reportDateKey: dateInput.value || todayDateKeyEastern(),
    };
    const token = tokenInput.value.trim();
    if (token) payload.token = token;

    try {
      const result = await callDashboardFunction("generateDailyReportPdfCallable", payload);
      const lines = [`${payload.reportType} report created for ${payload.reportDateKey}.`];
      if (result.downloadURL) lines.push(`Download: ${result.downloadURL}`);
      if (result.downloadUrlError) lines.push(`Signed URL failed: ${result.downloadUrlError}`);
      if (!result.downloadURL && result.storagePath) lines.push(`Storage path: ${result.storagePath}`);
      lines.push(`Report id: ${result.reportId || "-"}`);
      output.textContent = lines.join("\n");
      output.className = "daily-pdf-result ok";
    } catch (err) {
      output.textContent = `Failed: ${err?.message || err}`;
      output.className = "daily-pdf-result err";
    } finally {
      button.disabled = false;
    }
  });
}

function initProjectManager() {
  const phoneSelect = document.getElementById("projectPhone");
  const activeSelect = document.getElementById("projectActiveSelect");
  const setButton = document.getElementById("projectSetActiveBtn");
  const saveLogoButton = document.getElementById("projectSaveLogoBtn");
  const uploadLogoButton = document.getElementById("projectUploadLogoBtn");
  const slugInput = document.getElementById("projectCreateSlug");
  const nameInput = document.getElementById("projectCreateName");
  const locationInput = document.getElementById("projectCreateLocation");
  const logoInput = document.getElementById("projectLogoStoragePath");
  const logoFileInput = document.getElementById("projectLogoUpload");
  const tokenInput = document.getElementById("projectActionToken");
  const createButton = document.getElementById("projectCreateBtn");
  const result = document.getElementById("projectManagerResult");
  if (!phoneSelect || !activeSelect || !setButton || !slugInput || !nameInput || !createButton || !result) return;

  let slugTouched = false;
  slugInput.addEventListener("input", () => {
    slugTouched = slugInput.value.trim() !== "";
    slugInput.value = normalizeProjectSlugClient(slugInput.value);
  });
  nameInput.addEventListener("input", () => {
    if (!slugTouched) slugInput.value = normalizeProjectSlugClient(nameInput.value);
  });
  phoneSelect.addEventListener("change", () => renderProjectManager());
  activeSelect.addEventListener("change", () => {
    setButton.disabled = !(phoneSelect.value && activeSelect.value);
    if (saveLogoButton) saveLogoButton.disabled = !(phoneSelect.value && activeSelect.value);
    if (uploadLogoButton) uploadLogoButton.disabled = !(phoneSelect.value && activeSelect.value);
    renderProjectManager();
  });
  if (logoFileInput) logoFileInput.addEventListener("change", () => renderProjectManager());

  setButton.addEventListener("click", async () => {
    const phoneE164 = String(phoneSelect.value || "").trim();
    const projectSlug = normalizeProjectSlugClient(activeSelect.value);
    if (!phoneE164 || !projectSlug) {
      result.textContent = "Select a phone and project first.";
      result.className = "project-manager-result err";
      return;
    }
    setButton.disabled = true;
    result.textContent = "Saving active project...";
    result.className = "project-manager-result muted small";
    try {
      const payload = { phoneE164, projectSlug };
      const token = tokenInput.value.trim();
      if (token) payload.token = token;
      const data = await callDashboardFunction("setActiveProjectCallable", payload);
      result.textContent = `Active project set to ${data.projectName || data.projectSlug} (${data.projectSlug}).`;
      result.className = "project-manager-result ok";
      const dailyPhone = document.getElementById("dailyPdfPhone");
      const dailyProject = document.getElementById("dailyPdfProject");
      if (dailyPhone && dailyProject && dailyPhone.value === phoneE164) {
        dailyProject.value = data.projectSlug;
      }
    } catch (err) {
      result.textContent = `Failed: ${err?.message || err}`;
      result.className = "project-manager-result err";
    } finally {
      setButton.disabled = false;
    }
  });

  createButton.addEventListener("click", async () => {
    const phoneE164 = String(phoneSelect.value || "").trim();
    const projectSlug = normalizeProjectSlugClient(slugInput.value);
    const projectName = String(nameInput.value || "").trim();
    const location = String(locationInput?.value || "").trim();
    if (!phoneE164) {
      result.textContent = "Select a phone first.";
      result.className = "project-manager-result err";
      return;
    }
    if (!projectSlug) {
      result.textContent = "Enter a project slug.";
      result.className = "project-manager-result err";
      return;
    }
    if (!projectName) {
      result.textContent = "Enter a project name.";
      result.className = "project-manager-result err";
      return;
    }

    createButton.disabled = true;
    result.textContent = "Creating project...";
    result.className = "project-manager-result muted small";
    try {
      const payload = { phoneE164, projectSlug, projectName, location };
      if (logoInput && String(logoInput.value || "").trim()) {
        payload.reportLogoStoragePath = String(logoInput.value || "").trim();
      }
      const token = tokenInput.value.trim();
      if (token) payload.token = token;
      const data = await callDashboardFunction("createUserProjectCallable", payload);
      result.textContent = `Created ${data.projectName || data.projectSlug} (${data.projectSlug}) and made it active.`;
      result.className = "project-manager-result ok";
      slugInput.value = "";
      nameInput.value = "";
      if (locationInput) locationInput.value = "";
      if (logoInput) logoInput.value = "";
      slugTouched = false;
      const dailyPhone = document.getElementById("dailyPdfPhone");
      const dailyProject = document.getElementById("dailyPdfProject");
      if (dailyPhone && dailyProject && dailyPhone.value === phoneE164) {
        dailyProject.value = data.projectSlug;
      }
    } catch (err) {
      result.textContent = `Failed: ${err?.message || err}`;
      result.className = "project-manager-result err";
    } finally {
      createButton.disabled = false;
      renderProjectManager();
    }
  });

  if (saveLogoButton) {
    saveLogoButton.addEventListener("click", async () => {
      const phoneE164 = String(phoneSelect.value || "").trim();
      const projectSlug = normalizeProjectSlugClient(activeSelect.value);
      if (!phoneE164 || !projectSlug) {
        result.textContent = "Select a phone and project first.";
        result.className = "project-manager-result err";
        return;
      }
      saveLogoButton.disabled = true;
      result.textContent = "Saving report logo...";
      result.className = "project-manager-result muted small";
      try {
        const payload = {
          phoneE164,
          projectSlug,
          reportLogoStoragePath: logoInput ? String(logoInput.value || "").trim() : "",
        };
        const token = tokenInput.value.trim();
        if (token) payload.token = token;
        const data = await callDashboardFunction("updateProjectReportLogoCallable", payload);
        result.textContent = data.reportLogoStoragePath
          ? `Saved logo path for ${data.projectName || data.projectSlug}.`
          : `Cleared logo path for ${data.projectName || data.projectSlug}.`;
        result.className = "project-manager-result ok";
      } catch (err) {
        result.textContent = `Failed: ${err?.message || err}`;
        result.className = "project-manager-result err";
      } finally {
        renderProjectManager();
      }
    });
  }

      if (uploadLogoButton && logoFileInput) {
    uploadLogoButton.addEventListener("click", async () => {
      const phoneE164 = String(phoneSelect.value || "").trim();
      const projectSlug = normalizeProjectSlugClient(activeSelect.value);
      const file = logoFileInput.files && logoFileInput.files[0] ? logoFileInput.files[0] : null;
      if (!phoneE164 || !projectSlug) {
        result.textContent = "Select a phone and project first.";
        result.className = "project-manager-result err";
        return;
      }
      if (!file) {
        result.textContent = "Choose a logo file first.";
        result.className = "project-manager-result err";
        return;
      }

      uploadLogoButton.disabled = true;
      result.textContent = "Uploading logo...";
      result.className = "project-manager-result muted small";
      try {
        const safeFile = await normalizeImageForPdf(file, { preferredType: "image/png" });
        const storagePath = [
          "branding",
          "projects",
          sanitizeStorageSegment(projectSlug, "project"),
          `logo-${Date.now()}-${sanitizeStorageSegment(safeFile.name || "logo.png", "logo.png")}`,
        ].join("/");
        const uploaded = await uploadFileToStorage(storagePath, safeFile);
        if (logoInput) logoInput.value = uploaded.storagePath;
        const payload = {
          phoneE164,
          projectSlug,
          reportLogoStoragePath: uploaded.storagePath,
        };
        const token = tokenInput.value.trim();
        if (token) payload.token = token;
        const data = await callDashboardFunction("updateProjectReportLogoCallable", payload);
        result.textContent = `Uploaded and saved logo for ${data.projectName || data.projectSlug}.`;
        result.className = "project-manager-result ok";
        logoFileInput.value = "";
      } catch (err) {
        result.textContent = `Failed: ${err?.message || err}`;
        result.className = "project-manager-result err";
      } finally {
        renderProjectManager();
      }
    });
  }
}

function initMemberManager() {
  const emailInput = document.getElementById("memberEmail");
  const displayNameInput = document.getElementById("memberDisplayName");
  const companyInput = document.getElementById("memberCompany");
  const approvedPhoneInput = document.getElementById("memberApprovedPhone");
  const roleInput = document.getElementById("memberRole");
  const projectsInput = document.getElementById("memberProjects");
  const activeInput = document.getElementById("memberActive");
  const allProjectsInput = document.getElementById("memberAllProjects");
  const canApproveNotesInput = document.getElementById("memberCanApproveNotes");
  const saveButton = document.getElementById("memberSaveBtn");
  const result = document.getElementById("memberManagerResult");
  if (!emailInput || !approvedPhoneInput || !roleInput || !saveButton || !result) return;

  const loadMemberIntoForm = (email) => {
    const target = String(email || "").trim().toLowerCase();
    if (!target) return;
    const member = appMembersCache.find((item) => String(item.email || item.id || "").trim().toLowerCase() === target);
    if (!member) return;
    emailInput.value = String(member.email || member.id || "").trim().toLowerCase();
    displayNameInput.value = String(member.displayName || "").trim();
    companyInput.value = String(member.company || "").trim();
    approvedPhoneInput.value = String(member.approvedPhoneE164 || "").trim();
    roleInput.value = normalizeRoleClient(member.role);
    projectsInput.value = Array.isArray(member.projectSlugs) ? member.projectSlugs.join(", ") : "";
    activeInput.checked = member.active !== false;
    allProjectsInput.checked = member.allProjects === true;
    canApproveNotesInput.checked = member.canApproveNotes === true;
    result.textContent = `Editing ${emailInput.value}`;
    result.className = "project-manager-result muted small";
  };

  const clearMemberForm = () => {
    emailInput.value = "";
    if (displayNameInput) displayNameInput.value = "";
    if (companyInput) companyInput.value = "";
    approvedPhoneInput.value = "";
    if (projectsInput) projectsInput.value = "";
    if (activeInput) activeInput.checked = true;
    if (allProjectsInput) allProjectsInput.checked = false;
    if (canApproveNotesInput) canApproveNotesInput.checked = false;
    roleInput.value = "user";
  };

  const deleteMember = async (email) => {
    const target = String(email || "").trim().toLowerCase();
    if (!target) return;
    if (target === currentUserEmail()) {
      result.textContent = "You cannot delete your own member record.";
      result.className = "project-manager-result err";
      return;
    }
    const member = appMembersCache.find((item) => String(item.email || item.id || "").trim().toLowerCase() === target);
    const label = (member && (member.displayName || member.email || member.id)) || target;
    const confirmed = window.confirm(
      `Delete ${label} from Team? This removes their app member access. SMS history stays in Firestore.`
    );
    if (!confirmed) return;

    result.textContent = `Deleting ${target}...`;
    result.className = "project-manager-result muted small";
    try {
      const data = await callDashboardFunction("deleteAppMemberCallable", { email: target });
      if (String(emailInput.value || "").trim().toLowerCase() === target) {
        clearMemberForm();
      }
      result.textContent = `Deleted ${data.email}.`;
      result.className = "project-manager-result ok";
    } catch (err) {
      result.textContent = `Failed: ${formatUiError(err)}`;
      result.className = "project-manager-result err";
    }
  };

  if (appMembersEl && !appMembersEl.dataset.editBinding) {
    const handleMemberPick = (target) => {
      if (target.closest("[data-member-delete]")) return;
      const editButton = target.closest("[data-member-edit]");
      if (editButton) {
        loadMemberIntoForm(editButton.getAttribute("data-member-edit"));
        return;
      }
      if (target.closest("button")) return;
      const row = target.closest("[data-member-email]");
      if (!row) return;
      loadMemberIntoForm(row.getAttribute("data-member-email"));
    };
    appMembersEl.addEventListener("click", async (event) => {
      const deleteButton = event.target.closest("[data-member-delete]");
      if (deleteButton) {
        await deleteMember(deleteButton.getAttribute("data-member-delete"));
        return;
      }
      handleMemberPick(event.target);
    });
    appMembersEl.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      if (event.target.closest("button")) return;
      handleMemberPick(event.target);
    });
    appMembersEl.dataset.editBinding = "true";
  }

  saveButton.addEventListener("click", async () => {
    const email = String(emailInput.value || "").trim().toLowerCase();
    if (!email) {
      result.textContent = "Enter an email address.";
      result.className = "project-manager-result err";
      return;
    }
    const approvedPhoneE164 = String(approvedPhoneInput.value || "").trim();
    if (!approvedPhoneE164) {
      result.textContent = "Enter an approved phone number.";
      result.className = "project-manager-result err";
      return;
    }
    saveButton.disabled = true;
    result.textContent = "Saving member...";
    result.className = "project-manager-result muted small";
    try {
      const payload = {
        email,
        displayName: String(displayNameInput.value || "").trim(),
        company: String(companyInput.value || "").trim(),
        approvedPhoneE164,
        role: String(roleInput.value || "user").trim(),
        active: !!activeInput.checked,
        allProjects: !!allProjectsInput.checked,
        canApproveNotes: !!canApproveNotesInput.checked,
        projectSlugs: String(projectsInput.value || "")
          .split(",")
          .map((value) => normalizeProjectSlugClient(value))
          .filter(Boolean),
      };
      const data = await callDashboardFunction("upsertAppMemberCallable", payload);
      result.textContent = `Saved ${data.email} as ${data.role} with approved phone ${data.approvedPhoneE164}.`;
      result.className = "project-manager-result ok";
      clearMemberForm();
    } catch (err) {
      result.textContent = `Failed: ${formatUiError(err)}`;
      result.className = "project-manager-result err";
    } finally {
      saveButton.disabled = false;
    }
  });
}

function initLabourPage() {
  const phoneInput = document.getElementById("labourerPhone");
  const nameInput = document.getElementById("labourerName");
  const projectsInput = document.getElementById("labourerProjects");
  const activeInput = document.getElementById("labourerActive");
  const saveButton = document.getElementById("labourerSaveBtn");
  const deleteButton = document.getElementById("labourerDeleteBtn");
  const labourReportPhoneSelect = document.getElementById("labourReportLabourerPhone");
  const labourReportProjectInput = document.getElementById("labourReportProject");
  const labourReportStartInput = document.getElementById("labourReportStart");
  const labourReportEndInput = document.getElementById("labourReportEnd");
  const labourReportTitleInput = document.getElementById("labourReportTitle");
  const labourReportTokenInput = document.getElementById("labourReportToken");
  const labourReportTodayBtn = document.getElementById("labourReportTodayBtn");
  const labourReportWeekBtn = document.getElementById("labourReportWeekBtn");
  const labourReportMonthBtn = document.getElementById("labourReportMonthBtn");
  const labourReportGenerateBtn = document.getElementById("labourReportGenerateBtn");
  const labourReportResult = document.getElementById("labourReportResult");
  const labourerResult = document.getElementById("labourerManagerResult");
  if (!phoneInput || !nameInput || !saveButton || !deleteButton || !labourReportGenerateBtn || !labourReportResult) return;

  syncLabourReportDateDefaults();
  if (!String(labourReportTitleInput?.value || "").trim()) {
    labourReportTitleInput.value = "Labour Hours Report";
  }

  if (labourersEl && !labourersEl.dataset.labourBinding) {
    labourersEl.addEventListener("click", async (event) => {
      const editButton = event.target.closest("[data-labourer-edit]");
      const deleteBtn = event.target.closest("[data-labourer-delete]");
      if (editButton) {
        loadLabourerIntoForm(editButton.getAttribute("data-labourer-edit"));
        return;
      }
      if (deleteBtn) {
        const targetPhone = String(deleteBtn.getAttribute("data-labourer-delete") || "").trim();
        if (!targetPhone) return;
        const item = labourersCache.find((row) => String(row.phoneE164 || row.id || "").trim() === targetPhone);
        const label = labourerLabelClient(item) || targetPhone;
        const confirmed = window.confirm(
          `Delete ${label} from Labour? This removes their labourer profile, but keeps past entries and reports.`
        );
        if (!confirmed) return;
        try {
          deleteBtn.disabled = true;
          if (labourerResult) {
            labourerResult.textContent = `Deleting ${label}...`;
            labourerResult.className = "project-manager-result muted small";
          }
          await callDashboardFunction("deleteLabourerCallable", { phoneE164: targetPhone });
          if (String(phoneInput.value || "").trim() === targetPhone) {
            clearLabourerForm();
          }
          if (labourerResult) {
            labourerResult.textContent = `Deleted ${label}.`;
            labourerResult.className = "project-manager-result ok";
          }
        } catch (err) {
          if (labourerResult) {
            labourerResult.textContent = `Failed: ${formatUiError(err)}`;
            labourerResult.className = "project-manager-result err";
          }
        } finally {
          deleteBtn.disabled = false;
        }
      }
    });
    labourersEl.dataset.labourBinding = "true";
  }

  saveButton.addEventListener("click", async () => {
    const phoneE164 = String(phoneInput.value || "").trim();
    const name = String(nameInput.value || "").trim();
    if (!phoneE164) {
      if (labourerResult) {
        labourerResult.textContent = "Enter a phone number.";
        labourerResult.className = "project-manager-result err";
      }
      return;
    }
    if (!name) {
      if (labourerResult) {
        labourerResult.textContent = "Enter a name.";
        labourerResult.className = "project-manager-result err";
      }
      return;
    }

    saveButton.disabled = true;
    if (labourerResult) {
      labourerResult.textContent = "Saving labourer...";
      labourerResult.className = "project-manager-result muted small";
    }
    try {
      const payload = {
        phoneE164,
        name,
        projectSlugs: String(projectsInput.value || "")
          .split(",")
          .map((value) => normalizeProjectSlugClient(value))
          .filter(Boolean),
        active: !!activeInput.checked,
      };
      const data = await callDashboardFunction("upsertLabourerCallable", payload);
      if (labourerResult) {
        labourerResult.textContent = `Saved ${data.name || data.phoneE164}.`;
        labourerResult.className = "project-manager-result ok";
      }
      clearLabourerForm();
    } catch (err) {
      if (labourerResult) {
        labourerResult.textContent = `Failed: ${formatUiError(err)}`;
        labourerResult.className = "project-manager-result err";
      }
    } finally {
      saveButton.disabled = false;
    }
  });

  deleteButton.addEventListener("click", async () => {
    const phoneE164 = String(phoneInput.value || "").trim();
    if (!phoneE164) {
      if (labourerResult) {
        labourerResult.textContent = "Enter a labourer phone to delete.";
        labourerResult.className = "project-manager-result err";
      }
      return;
    }
    const item = labourersCache.find((row) => String(row.phoneE164 || row.id || "").trim() === phoneE164);
    const label = labourerLabelClient(item) || phoneE164;
    const confirmed = window.confirm(
      `Delete ${label} from Labour? This removes their labourer profile, but keeps past entries and reports.`
    );
    if (!confirmed) return;
    deleteButton.disabled = true;
    if (labourerResult) {
      labourerResult.textContent = `Deleting ${label}...`;
      labourerResult.className = "project-manager-result muted small";
    }
    try {
      await callDashboardFunction("deleteLabourerCallable", { phoneE164 });
      clearLabourerForm();
      if (labourerResult) {
        labourerResult.textContent = `Deleted ${label}.`;
        labourerResult.className = "project-manager-result ok";
      }
    } catch (err) {
      if (labourerResult) {
        labourerResult.textContent = `Failed: ${formatUiError(err)}`;
        labourerResult.className = "project-manager-result err";
      }
    } finally {
      deleteButton.disabled = false;
    }
  });

  if (labourReportTodayBtn) {
    labourReportTodayBtn.addEventListener("click", () => {
      setLabourReportRange("today");
    });
  }
  if (labourReportWeekBtn) {
    labourReportWeekBtn.addEventListener("click", () => {
      setLabourReportRange("week");
    });
  }
  if (labourReportMonthBtn) {
    labourReportMonthBtn.addEventListener("click", () => {
      setLabourReportRange("month");
    });
  }

  labourReportGenerateBtn.addEventListener("click", async () => {
    const startKey = String(labourReportStartInput?.value || "").trim();
    const endKey = String(labourReportEndInput?.value || "").trim();
    const reportTitle = String(labourReportTitleInput?.value || "").trim() || "Labour Hours Report";
    const labourerPhone = String(labourReportPhoneSelect?.value || "").trim();
    const projectSlug = normalizeProjectSlugClient(String(labourReportProjectInput?.value || "").trim());
    const token = String(labourReportTokenInput?.value || "").trim();
    const normalizedStart = startKey || startOfWeekDateKeyClient(todayDateKeyEastern());
    const normalizedEnd = endKey || todayDateKeyEastern();
    if (startKey && endKey && startKey > endKey) {
      labourReportResult.textContent = "Start date must be before end date.";
      labourReportResult.className = "daily-pdf-result err";
      return;
    }

    labourReportGenerateBtn.disabled = true;
    labourReportResult.textContent = "Generating labour report...";
    labourReportResult.className = "daily-pdf-result muted small";

    try {
      const payload = {
        startKey: normalizedStart,
        endKey: normalizedEnd,
        reportTitle,
      };
      if (labourerPhone) payload.labourerPhone = labourerPhone;
      if (projectSlug) payload.projectSlug = projectSlug;
      if (token) payload.token = token;
      const data = await callDashboardFunction("generateLabourReportCallable", payload);
      const lines = [`Labour report created for ${payload.startKey} to ${payload.endKey}.`];
      if (Number.isFinite(Number(data.totalPaidHours))) {
        lines.push(`Paid tally (weighted): ${formatHoursClient(data.totalPaidHours)} hours`);
      }
      if (Array.isArray(data.paidPeriodTotals) && data.paidPeriodTotals.length) {
        lines.push("Paid periods:");
        for (const period of data.paidPeriodTotals) {
          const start = String(period?.periodStartKey || "-");
          const end = String(period?.periodEndKey || "-");
          const total = formatHoursClient(Number(period?.totalHours) || 0);
          const weighted = formatHoursClient(Number(period?.totalPaidHours) || 0);
          const sat = formatHoursClient(Number(period?.saturdayHours) || 0);
          const sun = formatHoursClient(Number(period?.sundayHours) || 0);
          lines.push(`${start} to ${end}: ${weighted} paid (base ${total}; Sat ${sat} x1.5; Sun ${sun} x2)`);
        }
      }
      if (data.downloadURL) lines.push(`Download: ${data.downloadURL}`);
      if (data.downloadUrlError) lines.push(`Signed URL failed: ${data.downloadUrlError}`);
      if (!data.downloadURL && data.storagePath) lines.push(`Storage path: ${data.storagePath}`);
      lines.push(`Report id: ${data.reportId || "-"}`);
      labourReportResult.textContent = lines.join("\n");
      labourReportResult.className = "daily-pdf-result ok";
    } catch (err) {
      labourReportResult.textContent = `Failed: ${formatUiError(err)}`;
      labourReportResult.className = "daily-pdf-result err";
    } finally {
      labourReportGenerateBtn.disabled = false;
    }
  });
}

function initProjectNotesRequestForm() {
  const reportSelect = document.getElementById("projectNotesReportSelect");
  const proposedEl = document.getElementById("projectNotesProposed");
  const commentEl = document.getElementById("projectNotesComment");
  const submitBtn = document.getElementById("projectNotesSubmitBtn");
  const result = document.getElementById("projectNotesRequestResult");
  if (!reportSelect || !proposedEl || !submitBtn || !result) return;

  reportSelect.addEventListener("change", () => renderProjectNotesRequestForm());

  submitBtn.addEventListener("click", async () => {
    const report = dailyReportsCache.find((item) => item.id === reportSelect.value);
    if (!report || !report.projectId) {
      result.textContent = "Select a project report first.";
      result.className = "project-manager-result err";
      return;
    }
    const proposedNotes = String(proposedEl.value || "").trim();
    if (!proposedNotes) {
      result.textContent = "Enter the proposed project notes.";
      result.className = "project-manager-result err";
      return;
    }

    submitBtn.disabled = true;
    result.textContent = "Submitting note update request...";
    result.className = "project-manager-result muted small";
    try {
      const payload = {
        reportId: report.id,
        projectSlug: report.projectId,
        proposedNotes,
        comment: commentEl ? String(commentEl.value || "").trim() : "",
      };
      const data = await callDashboardFunction("createProjectNoteEditRequestCallable", payload);
      result.textContent = `Request ${data.requestId} submitted for approval.`;
      result.className = "project-manager-result ok";
      proposedEl.value = "";
      if (commentEl) commentEl.value = "";
      window.location.hash = "approvals";
    } catch (err) {
      result.textContent = `Failed: ${formatUiError(err)}`;
      result.className = "project-manager-result err";
    } finally {
      submitBtn.disabled = false;
    }
  });
}

function initApprovals() {
  if (!projectNoteEditRequestsEl) return;
  projectNoteEditRequestsEl.addEventListener("click", async (event) => {
    const approveButton = event.target.closest("[data-approve-request]");
    const rejectButton = event.target.closest("[data-reject-request]");
    const requestId = approveButton
      ? approveButton.getAttribute("data-approve-request")
      : rejectButton
        ? rejectButton.getAttribute("data-reject-request")
        : "";
    if (!requestId) return;

    const decision = approveButton ? "approve" : "reject";
    const reviewerComment =
      window.prompt(
        decision === "approve" ? "Optional approval comment:" : "Reason for rejection:",
        ""
      ) || "";
    try {
      await callDashboardFunction("reviewProjectNoteEditRequestCallable", {
        requestId,
        decision,
        reviewerComment,
      });
    } catch (err) {
      window.alert(`Failed: ${formatUiError(err)}`);
    }
  });
}

function initAssistantComposer() {
  const phoneSelect = document.getElementById("assistantComposerPhone");
  const schedulePhoneSelect = document.getElementById("assistantSchedulePhone");
  const bodyInput = document.getElementById("assistantComposerBody");
  const photoInput = document.getElementById("assistantComposerPhotos");
  const scheduleFileInput = document.getElementById("assistantScheduleFile");
  const scheduleStartInput = document.getElementById("assistantScheduleStart");
  const scheduleEndInput = document.getElementById("assistantScheduleEnd");
  const scheduleCompanyInput = document.getElementById("assistantScheduleCompany");
  const scheduleProjectInput = document.getElementById("assistantScheduleProject");
  const scheduleParseButton = document.getElementById("assistantScheduleParseBtn");
  const scheduleCreateReportButton = document.getElementById("assistantScheduleCreateReportBtn");
  const scheduleCreateCloseoutButton = document.getElementById("assistantScheduleCreateCloseoutBtn");
  const scheduleResult = document.getElementById("assistantScheduleResult");
  const scheduleSummary = document.getElementById("assistantScheduleSummary");
  const composerTokenInput = document.getElementById("assistantComposerToken");
  const scheduleTokenInput = document.getElementById("assistantScheduleToken");
  const sendButton = document.getElementById("assistantComposerSendBtn");
  const result = document.getElementById("assistantComposerResult");
  if (!phoneSelect || !bodyInput || !sendButton || !result) return;
  let lastParsedSchedule = null;

  const refresh = () => {
    renderAssistantComposer();
  };
  phoneSelect.addEventListener("change", refresh);
  if (schedulePhoneSelect) schedulePhoneSelect.addEventListener("change", refresh);
  bodyInput.addEventListener("input", refresh);
  if (photoInput) photoInput.addEventListener("change", refresh);
  if (scheduleFileInput) scheduleFileInput.addEventListener("change", refresh);

  sendButton.addEventListener("click", async () => {
    const phoneE164 = String(phoneSelect.value || "").trim();
    const body = String(bodyInput.value || "").trim();
    if (!phoneE164 || !body) {
      result.textContent = "Select a phone and enter a message first.";
      result.className = "project-manager-result err";
      return;
    }

    sendButton.disabled = true;
    result.textContent = "Sending to assistant...";
    result.className = "project-manager-result muted small";

    try {
      const payload = { phoneE164, body };
      const user = resolveSmsUserForAssistant(phoneE164);
      const reportDateKey = extractReportDateKeyClient(body) || todayDateKeyEastern();
      const projectSlug = normalizeProjectSlugClient(user && user.activeProjectSlug ? user.activeProjectSlug : "");
      const files = photoInput && photoInput.files ? Array.from(photoInput.files) : [];
      if (files.length) {
        result.textContent = `Uploading ${files.length} photo${files.length === 1 ? "" : "s"}...`;
        const uploadBatchId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        payload.uploadedMedia = [];
        for (let i = 0; i < files.length; i += 1) {
          const file = await normalizeImageForPdf(files[i], { preferredType: "image/jpeg", quality: 0.9 });
          const storagePath = [
            "projects",
            sanitizeStorageSegment(projectSlug || "_unassigned", "_unassigned"),
            "media",
            sanitizeStorageSegment(reportDateKey, todayDateKeyEastern()),
            `dashboard-${uploadBatchId}`,
            `${String(i + 1).padStart(2, "0")}-${sanitizeStorageSegment(file.name || `image-${i + 1}`, `image-${i + 1}`)}`,
          ].join("/");
          payload.uploadedMedia.push(await uploadFileToStorage(storagePath, file));
        }
      }
      const token = composerTokenInput && composerTokenInput.value ? composerTokenInput.value.trim() : "";
      if (token) payload.token = token;
      const data = await callDashboardFunction("sendAssistantMessageCallable", payload);
      result.textContent = data.replyText || "Saved.";
      result.className = "project-manager-result ok";
      bodyInput.value = "";
      if (photoInput) photoInput.value = "";
    } catch (err) {
      result.textContent = `Failed: ${err?.message || err}`;
      result.className = "project-manager-result err";
    } finally {
      renderAssistantComposer();
    }
  });

  if (scheduleParseButton && scheduleFileInput && scheduleResult && scheduleSummary) {
    scheduleParseButton.addEventListener("click", async () => {
      const file =
        scheduleFileInput.files && scheduleFileInput.files[0] ? scheduleFileInput.files[0] : null;
      if (!file) {
        scheduleResult.textContent = "Choose an Excel workbook first.";
        scheduleResult.className = "project-manager-result err";
        scheduleSummary.classList.add("hidden");
        scheduleSummary.textContent = "";
        return;
      }
      if (!/\.xlsx$/i.test(String(file.name || ""))) {
        scheduleResult.textContent = "Only .xlsx workbooks are supported right now.";
        scheduleResult.className = "project-manager-result err";
        scheduleSummary.classList.add("hidden");
        scheduleSummary.textContent = "";
        return;
      }

      scheduleParseButton.disabled = true;
      scheduleResult.textContent = "Uploading schedule workbook...";
      scheduleResult.className = "project-manager-result muted small";
      scheduleSummary.classList.add("hidden");
      scheduleSummary.textContent = "";

      try {
        const user = resolveSmsUserForAssistant(schedulePhoneSelect ? schedulePhoneSelect.value : "");
        const activeProject = getActiveProjectRecordForUser(user);
        const fallbackProjectName =
          activeProject && (activeProject.name || activeProject.slug || activeProject.id)
            ? activeProject.name || activeProject.slug || activeProject.id
            : "";
        const storagePath = [
          "branding",
          "lookahead",
          `${Date.now()}-${sanitizeStorageSegment(file.name || "lookahead.xlsx", "lookahead.xlsx")}`,
        ].join("/");
        const uploaded = await uploadFileToStorage(storagePath, file);

        scheduleResult.textContent = "Parsing uploaded workbook...";
        const payload = {
          storagePath: uploaded.storagePath,
          includeHidden: false,
          companyName: String(scheduleCompanyInput?.value || "").trim() || "Matheson",
          projectName: String(scheduleProjectInput?.value || "").trim() || fallbackProjectName || "Docksteader Paramedic Station",
        };
        const startDateKey = String(scheduleStartInput?.value || "").trim();
        const endDateKey = String(scheduleEndInput?.value || "").trim();
        if (startDateKey) payload.startDateKey = startDateKey;
        if (endDateKey) payload.endDateKey = endDateKey;
        const token = scheduleTokenInput && scheduleTokenInput.value ? scheduleTokenInput.value.trim() : "";
        if (token) payload.token = token;

        const data = await callDashboardFunction("parseLookaheadScheduleCallable", payload);
        scheduleResult.textContent = `Parsed ${data.taskCount || 0} tasks from ${data.window?.startDateKey || "?"} to ${data.window?.endDateKey || "?"}.`;
        scheduleResult.className = "project-manager-result ok";
        scheduleSummary.textContent = data.summary || "";
        scheduleSummary.classList.remove("hidden");
        lastParsedSchedule = {
          storagePath: uploaded.storagePath,
          companyName: payload.companyName,
          projectName: payload.projectName,
          startDateKey: startDateKey || "",
          endDateKey: endDateKey || "",
          includeHidden: false,
          taskCount: data.taskCount || 0,
        };
      } catch (err) {
        scheduleResult.textContent = `Failed: ${err?.message || err}`;
        scheduleResult.className = "project-manager-result err";
        scheduleSummary.classList.add("hidden");
        scheduleSummary.textContent = "";
        lastParsedSchedule = null;
      } finally {
        renderAssistantComposer();
      }
    });
  }

  if (scheduleCreateReportButton && scheduleResult) {
    scheduleCreateReportButton.addEventListener("click", async () => {
      const phoneE164 = String((schedulePhoneSelect && schedulePhoneSelect.value) || "").trim();
      if (!phoneE164) {
        scheduleResult.textContent = "Select a phone before creating the report.";
        scheduleResult.className = "project-manager-result err";
        return;
      }

      scheduleCreateReportButton.disabled = true;
      scheduleResult.textContent = "Creating Activities report PDF...";
      scheduleResult.className = "project-manager-result muted small";
      try {
        let storagePath = lastParsedSchedule && lastParsedSchedule.storagePath
          ? String(lastParsedSchedule.storagePath)
          : "";
        if (!storagePath) {
          const file =
            scheduleFileInput && scheduleFileInput.files && scheduleFileInput.files[0]
              ? scheduleFileInput.files[0]
              : null;
          if (!file) {
            scheduleResult.textContent = "Parse a schedule first or choose an Excel file.";
            scheduleResult.className = "project-manager-result err";
            return;
          }
          if (!/\.xlsx$/i.test(String(file.name || ""))) {
            scheduleResult.textContent = "Only .xlsx workbooks are supported.";
            scheduleResult.className = "project-manager-result err";
            return;
          }
          scheduleResult.textContent = "Uploading schedule workbook...";
          const uploaded = await uploadFileToStorage(
            [
              "branding",
              "lookahead",
              `${Date.now()}-${sanitizeStorageSegment(file.name || "lookahead.xlsx", "lookahead.xlsx")}`,
            ].join("/"),
            file
          );
          storagePath = uploaded.storagePath;
        }

        const payload = {
          phoneE164,
          storagePath,
          companyName:
            String(scheduleCompanyInput?.value || "").trim() ||
            (lastParsedSchedule && lastParsedSchedule.companyName) ||
            "Matheson",
          projectName:
            String(scheduleProjectInput?.value || "").trim() ||
            (lastParsedSchedule && lastParsedSchedule.projectName) ||
            "Docksteader Paramedic Station",
          includeHidden: false,
        };
        const activeUser = resolveSmsUserForAssistant(phoneE164);
        const activeProject = getActiveProjectRecordForUser(activeUser);
        const activeProjectSlug = normalizeProjectSlugClient(
          activeProject && (activeProject.slug || activeProject.id) ? activeProject.slug || activeProject.id : ""
        );
        if (activeProjectSlug) payload.projectSlug = activeProjectSlug;
        const startDateKey =
          String(scheduleStartInput?.value || "").trim() ||
          (lastParsedSchedule && lastParsedSchedule.startDateKey) ||
          "";
        const endDateKey =
          String(scheduleEndInput?.value || "").trim() ||
          (lastParsedSchedule && lastParsedSchedule.endDateKey) ||
          "";
        if (startDateKey) payload.startDateKey = startDateKey;
        if (endDateKey) payload.endDateKey = endDateKey;
        const token = scheduleTokenInput && scheduleTokenInput.value ? scheduleTokenInput.value.trim() : "";
        if (token) payload.token = token;
        scheduleResult.textContent = "Creating Activities report PDF...";
        const data = await callDashboardFunction("createLookaheadActivitiesReportCallable", payload);
        const title = data.reportTitle || "Activities report";
        const linkLine = data.downloadURL ? ` Download: ${data.downloadURL}` : "";
        const signWarn =
          data.downloadUrlError && data.downloadURL
            ? ` Note: signed download URL failed (${String(data.downloadUrlError).slice(0, 120)}); link may save with a long auto-generated name.`
            : "";
        const weatherLine =
          data.weatherSummary && Array.isArray(data.weatherSummary.summaryItems) && data.weatherSummary.summaryItems.length
            ? ` Weather: ${data.weatherSummary.summaryItems[0]}`
            : "";
        scheduleResult.textContent =
          `${title} saved.${linkLine}${signWarn}${weatherLine}`.trim();
        scheduleResult.className = "project-manager-result ok";
        lastParsedSchedule = {
          storagePath,
          companyName: payload.companyName,
          projectName: payload.projectName,
          startDateKey,
          endDateKey,
          includeHidden: false,
          taskCount: Number(data.taskCount || 0),
        };
      } catch (err) {
        scheduleResult.textContent = `Failed: ${formatUiError(err)}`;
        scheduleResult.className = "project-manager-result err";
      } finally {
        scheduleCreateReportButton.disabled = false;
      }
    });
  }

  if (scheduleCreateCloseoutButton && scheduleResult) {
    scheduleCreateCloseoutButton.addEventListener("click", async () => {
      const phoneE164 = String((schedulePhoneSelect && schedulePhoneSelect.value) || "").trim();
      if (!phoneE164) {
        scheduleResult.textContent = "Select a phone before creating the closeout report.";
        scheduleResult.className = "project-manager-result err";
        return;
      }

      scheduleCreateCloseoutButton.disabled = true;
      scheduleResult.textContent = "Creating Closeout report PDF...";
      scheduleResult.className = "project-manager-result muted small";

      try {
        let storagePath = lastParsedSchedule && lastParsedSchedule.storagePath
          ? String(lastParsedSchedule.storagePath)
          : "";
        if (!storagePath) {
          const file =
            scheduleFileInput && scheduleFileInput.files && scheduleFileInput.files[0]
              ? scheduleFileInput.files[0]
              : null;
          if (!file) {
            scheduleResult.textContent = "Parse a schedule first or choose an Excel file.";
            scheduleResult.className = "project-manager-result err";
            return;
          }
          const uploaded = await uploadFileToStorage(
            [
              "branding",
              "lookahead",
              `${Date.now()}-${sanitizeStorageSegment(file.name || "lookahead.xlsx", "lookahead.xlsx")}`,
            ].join("/"),
            file
          );
          storagePath = uploaded.storagePath;
        }

        const activeUser = resolveSmsUserForAssistant(phoneE164);
        const activeProject = getActiveProjectRecordForUser(activeUser);
        const activeProjectSlug = normalizeProjectSlugClient(
          activeProject && (activeProject.slug || activeProject.id) ? activeProject.slug || activeProject.id : ""
        );
        if (!activeProjectSlug) {
          scheduleResult.textContent = "Set an active project first. The closeout report compares schedules within one project.";
          scheduleResult.className = "project-manager-result err";
          return;
        }

        const payload = {
          phoneE164,
          projectSlug: activeProjectSlug,
          storagePath,
          companyName:
            String(scheduleCompanyInput?.value || "").trim() ||
            (lastParsedSchedule && lastParsedSchedule.companyName) ||
            "Matheson",
          projectName:
            String(scheduleProjectInput?.value || "").trim() ||
            (lastParsedSchedule && lastParsedSchedule.projectName) ||
            "Docksteader Paramedic Station",
        };
        const token = scheduleTokenInput && scheduleTokenInput.value ? scheduleTokenInput.value.trim() : "";
        if (token) payload.token = token;

        const data = await callDashboardFunction("createLookaheadCloseoutReportCallable", payload);
        const summary = data.summary
          ? ` Completed ${data.summary.completed}, ongoing ${data.summary.ongoing}, delayed ${data.summary.delayed}.`
          : "";
        scheduleResult.textContent =
          `${data.reportTitle || "Closeout report"} saved.${data.downloadURL ? ` Download: ${data.downloadURL}` : ""}${summary}`.trim();
        scheduleResult.className = "project-manager-result ok";
      } catch (err) {
        scheduleResult.textContent = `Failed: ${formatUiError(err)}`;
        scheduleResult.className = "project-manager-result err";
      } finally {
        scheduleCreateCloseoutButton.disabled = false;
      }
    });
  }
}

function formatAuthLabel(user) {
  if (!user) return "-";
  if (user.displayName && user.email) return `${user.displayName} (${user.email})`;
  return user.email || user.displayName || user.uid;
}

function showSignedOutState() {
  currentAppAccess = null;
  stopAdminListeners();
  resetAdminCaches();
  clearAdminPanels();
  syncAccessControlledUi();
  if (authPanelEl) authPanelEl.classList.remove("hidden");
  if (appPanelEl) appPanelEl.classList.add("hidden");
  if (adminUserLabelEl) adminUserLabelEl.textContent = "-";
  setStatusInfo("Sign in required", "Admin data stays locked until you authenticate.");
  applyView(currentViewFromHash());
}

async function showSignedInState(user) {
  if (authPanelEl) authPanelEl.classList.add("hidden");
  if (appPanelEl) appPanelEl.classList.remove("hidden");
  if (adminUserLabelEl) adminUserLabelEl.textContent = formatAuthLabel(user);
  setStatusInfo("Connecting...", "Loading account access.");
  currentAppAccess = await callDashboardFunction("getDashboardAccessCallable", {});
  syncDailyPdfPhoneFromAccess();
  syncAccessControlledUi();
  applyView(currentViewFromHash());
  const roleLabel = currentAppAccess && currentAppAccess.role ? String(currentAppAccess.role) : "user";
  setStatusInfo("Connecting...", `Access level: ${roleLabel}. Starting secure Firestore subscriptions.`);
  startAdminListeners();
}

function initAuthGate() {
  if (authSignInBtn) {
    authSignInBtn.addEventListener("click", async () => {
      if (authErrorEl) authErrorEl.textContent = "";
      const provider = new GoogleAuthProvider();
      provider.setCustomParameters({ prompt: "select_account" });
      try {
        await signInWithPopup(auth, provider);
      } catch (err) {
        if (authErrorEl) authErrorEl.textContent = err?.message || String(err);
      }
    });
  }

  if (authSignOutBtn) {
    authSignOutBtn.addEventListener("click", async () => {
      await signOut(auth);
    });
  }

  onAuthStateChanged(auth, async (user) => {
    if (!user) {
      showSignedOutState();
      return;
    }
    try {
      await showSignedInState(user);
    } catch (err) {
      if (authErrorEl) authErrorEl.textContent = err?.message || String(err);
      showSignedOutState();
    }
  });
}

initNavigation();
initMediaViewer();
initDailyReportTitleEditor();
initDailyPdfFromDashboard();
initProjectManager();
initMemberManager();
initLabourPage();
initProjectNotesRequestForm();
initApprovals();
initAssistantComposer();
initAuthGate();
