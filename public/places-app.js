// Known Places dashboard: list, rename, retune matching radius, delete, and merge the places
// learned by the "Log this place" Shortcut (see docs/quick-log-shortcuts.md and
// functions/placeLearning.js). Mirrors public/issues-app.js's auth/App Check/Firestore setup.
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
  where,
  onSnapshot,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import {
  getFunctions,
  httpsCallable,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-functions.js";

const DEFAULT_FIREBASE_CONFIG = {
  apiKey: "AIzaSyBfUA9JCo01N53TTDzMxnqEqzYqy-RJ6qE",
  authDomain: "gridlineai.firebaseapp.com",
  projectId: "gridlineai",
  storageBucket: "gridlineai.firebasestorage.app",
  messagingSenderId: "118761010772",
  appId: "1:118761010772:web:6eee28ee3c09953de0dfc1",
};

function resolveFirebaseWebConfig(defaultConfig) {
  const runtime =
    window.FIREBASE_WEB_CONFIG && typeof window.FIREBASE_WEB_CONFIG === "object"
      ? window.FIREBASE_WEB_CONFIG
      : {};
  return { ...defaultConfig, ...runtime };
}

const firebaseConfig = resolveFirebaseWebConfig(DEFAULT_FIREBASE_CONFIG);
const app = initializeApp(firebaseConfig);

function resolveAppCheckSiteKey() {
  const direct = String(window.FIREBASE_APPCHECK_SITE_KEY || "").trim();
  if (direct) return direct;
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
    // Ignore duplicate initialization if another script already set App Check.
  }
}

const auth = getAuth(app);
const db = getFirestore(app);
const functions = getFunctions(app, "northamerica-northeast1");
const getDashboardAccessCallable = httpsCallable(functions, "getDashboardAccessCallable");
const renameKnownPlaceCallable = httpsCallable(functions, "renameKnownPlaceCallable");
const updateKnownPlaceRadiusCallable = httpsCallable(functions, "updateKnownPlaceRadiusCallable");
const deleteKnownPlaceCallable = httpsCallable(functions, "deleteKnownPlaceCallable");
const mergeKnownPlacesCallable = httpsCallable(functions, "mergeKnownPlacesCallable");

const MIN_RADIUS_METERS = 20;
const MAX_RADIUS_METERS = 1000;

let places = [];
let selectedIds = new Set();
let unsubscribe = null;
let authBootstrapPromise = null;

function byId(id) {
  return document.getElementById(id);
}
function esc(value) {
  if (value == null) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
function formatUiError(err) {
  if (!err) return "Unknown error.";
  if (typeof err.message === "string" && err.message.trim()) return err.message.trim();
  if (typeof err.code === "string" && err.code.trim()) return err.code.trim();
  return String(err);
}
function setMsg(text, isError = false) {
  const el = byId("placesMsg");
  if (!el) return;
  el.textContent = text || "";
  el.classList.toggle("form-error", !!isError);
}
function setAuthBusy(isBusy, message = "") {
  const btn = byId("authSignInGoogle");
  const authError = byId("authError");
  if (btn) {
    btn.disabled = !!isBusy;
    btn.textContent = isBusy ? "Signing in..." : "Sign in with Google";
  }
  if (authError) authError.textContent = message || "";
}
function formatWhen(value) {
  const ms = value && typeof value.toMillis === "function" ? value.toMillis() : null;
  if (!ms) return "—";
  try {
    return new Date(ms).toLocaleString("en-CA", { dateStyle: "medium", timeStyle: "short" });
  } catch (_) {
    return "—";
  }
}

function formatMinutes(minutes) {
  const total = Math.max(0, Math.round(Number(minutes) || 0));
  const hours = Math.floor(total / 60);
  const mins = total % 60;
  if (!hours) return `${mins} min`;
  return mins ? `${hours} h ${mins} min` : `${hours} h`;
}

function formatStayStats(place) {
  if (place.currentVisitStartedAt) return `here now since ${formatWhen(place.currentVisitStartedAt)} · `;
  const timed = Number(place.timedVisitCount) || 0;
  if (!timed) return "";
  const avg = (Number(place.totalMinutesSpent) || 0) / timed;
  return `last stay ${formatMinutes(place.lastVisitDurationMinutes)} · avg ${formatMinutes(avg)} · `;
}

function stopListeners() {
  if (unsubscribe) {
    unsubscribe();
    unsubscribe = null;
  }
}

function startListeners(memberEmail) {
  stopListeners();
  const q = query(collection(db, "knownPlaces"), where("memberEmail", "==", memberEmail));
  unsubscribe = onSnapshot(
    q,
    (snap) => {
      places = snap.docs
        .map((d) => ({ id: d.id, ...d.data() }))
        .sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
      selectedIds = new Set([...selectedIds].filter((id) => places.some((p) => p.id === id)));
      renderPlaces();
    },
    (err) => setMsg(formatUiError(err), true)
  );
}

function matchesSearch(place, term) {
  if (!term) return true;
  return String(place.name || "").toLowerCase().includes(term);
}

function updateMergeButtonState() {
  const btn = byId("mergeButton");
  if (btn) btn.disabled = selectedIds.size < 2;
}

function renderPlaces() {
  const list = byId("placesList");
  const countEl = byId("placesCount");
  const term = String(byId("filterSearch")?.value || "").trim().toLowerCase();
  const visible = places.filter((place) => matchesSearch(place, term));
  if (countEl) countEl.textContent = `${visible.length} of ${places.length} place${places.length === 1 ? "" : "s"}`;
  updateMergeButtonState();

  if (!places.length) {
    list.innerHTML = `<p class="muted small">No places learned yet. Run the "Log this place" Shortcut somewhere new to get started.</p>`;
    return;
  }
  if (!visible.length) {
    list.innerHTML = `<p class="muted small">No places match "${esc(term)}".</p>`;
    return;
  }

  list.innerHTML = visible
    .map(
      (place) => `
    <div class="place-row" data-id="${esc(place.id)}">
      <label class="place-select">
        <input type="checkbox" class="place-checkbox" data-id="${esc(place.id)}" ${selectedIds.has(place.id) ? "checked" : ""} />
      </label>
      <div class="place-main">
        <div class="place-name-row">
          <input type="text" class="place-name-input" data-id="${esc(place.id)}" value="${esc(place.name)}" />
          <button type="button" class="btn-inline place-save-name" data-id="${esc(place.id)}">Save</button>
        </div>
        <div class="place-meta muted small">
          ${place.visitCount || 0} visit${(place.visitCount || 0) === 1 ? "" : "s"} · last ${formatWhen(place.lastVisitAt)} ·
          ${formatStayStats(place)}
          ${Number(place.latitude).toFixed(5)}, ${Number(place.longitude).toFixed(5)}
        </div>
        <div class="place-radius-row muted small">
          Matching radius:
          <input type="number" class="place-radius-input" data-id="${esc(place.id)}" min="${MIN_RADIUS_METERS}" max="${MAX_RADIUS_METERS}" step="10" value="${Number(place.radiusMeters) || 120}" />
          m
          <button type="button" class="btn-inline place-save-radius" data-id="${esc(place.id)}">Save</button>
          <span class="muted">(how close a future visit must be to count as this same place)</span>
        </div>
      </div>
      <button type="button" class="btn-danger place-delete" data-id="${esc(place.id)}">Delete</button>
    </div>`
    )
    .join("");
}

async function withBusyMessage(action, busyText, doneText) {
  setMsg(busyText);
  try {
    await action();
    setMsg(doneText);
  } catch (err) {
    setMsg(formatUiError(err), true);
  }
}

document.addEventListener("click", async (event) => {
  const saveNameBtn = event.target.closest(".place-save-name");
  if (saveNameBtn) {
    const id = saveNameBtn.dataset.id;
    const input = document.querySelector(`.place-name-input[data-id="${CSS.escape(id)}"]`);
    const name = String(input?.value || "").trim();
    if (!name) {
      setMsg("Name cannot be empty.", true);
      return;
    }
    await withBusyMessage(
      () => renameKnownPlaceCallable({ placeId: id, name }),
      "Saving name…",
      "Saved."
    );
    return;
  }

  const saveRadiusBtn = event.target.closest(".place-save-radius");
  if (saveRadiusBtn) {
    const id = saveRadiusBtn.dataset.id;
    const input = document.querySelector(`.place-radius-input[data-id="${CSS.escape(id)}"]`);
    const radiusMeters = Number(input?.value);
    if (!Number.isFinite(radiusMeters) || radiusMeters < MIN_RADIUS_METERS || radiusMeters > MAX_RADIUS_METERS) {
      setMsg(`Radius must be between ${MIN_RADIUS_METERS} and ${MAX_RADIUS_METERS} meters.`, true);
      return;
    }
    await withBusyMessage(
      () => updateKnownPlaceRadiusCallable({ placeId: id, radiusMeters }),
      "Saving radius…",
      "Saved."
    );
    return;
  }

  const deleteBtn = event.target.closest(".place-delete");
  if (deleteBtn) {
    const id = deleteBtn.dataset.id;
    const place = places.find((p) => p.id === id);
    if (!window.confirm(`Delete "${place?.name || "this place"}"? Future visits there will be treated as somewhere new.`)) return;
    await withBusyMessage(() => deleteKnownPlaceCallable({ placeId: id }), "Deleting…", "Deleted.");
    return;
  }

  if (event.target.id === "mergeButton" && !event.target.disabled) {
    const ids = [...selectedIds];
    if (ids.length < 2) return;
    const names = ids.map((id) => places.find((p) => p.id === id)?.name || id);
    const survivorName = window.prompt(
      `Merging: ${names.join(", ")}\n\nType the exact name of the one to KEEP (the others will be deleted and their visits added to it):`,
      names[0]
    );
    if (survivorName == null) return;
    const survivor = ids.find((id) => (places.find((p) => p.id === id)?.name || "") === survivorName.trim());
    if (!survivor) {
      setMsg("That name did not match any of the selected places exactly. Nothing was merged.", true);
      return;
    }
    const mergeIds = ids.filter((id) => id !== survivor);
    await withBusyMessage(
      () => mergeKnownPlacesCallable({ survivorId: survivor, mergeIds }),
      "Merging…",
      "Merged."
    );
    selectedIds = new Set();
  }
});

document.addEventListener("change", (event) => {
  if (event.target.classList.contains("place-checkbox")) {
    const id = event.target.dataset.id;
    if (event.target.checked) selectedIds.add(id);
    else selectedIds.delete(id);
    updateMergeButtonState();
  }
});

byId("filterSearch")?.addEventListener("input", renderPlaces);

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
      const res = await getDashboardAccessCallable({});
      const access = res && res.data ? res.data : null;
      authPanel.classList.add("hidden");
      appPanel.classList.remove("hidden");
      if (authError) authError.textContent = "";
      setAuthBusy(false);
      byId("userLabel").textContent =
        user.displayName && user.email ? `${user.displayName} (${user.email})` : user.email || user.displayName || user.uid;
      startListeners((access && access.email) || String(user.email || "").toLowerCase());
    } catch (err) {
      stopListeners();
      setAuthBusy(false);
      authPanel.classList.remove("hidden");
      appPanel.classList.add("hidden");
      if (authError) {
        const rawMessage = formatUiError(err) || "Your account is signed in but not approved for app access yet.";
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

byId("authSignInGoogle").onclick = async () => {
  setAuthBusy(true);
  try {
    await signInWithPopup(auth, new GoogleAuthProvider());
  } catch (err) {
    setAuthBusy(false, formatUiError(err));
  }
};
byId("authSignOut").onclick = () => signOut(auth);

onAuthStateChanged(auth, async (user) => {
  const authPanel = byId("authPanel");
  const appPanel = byId("appPanel");
  if (!user) {
    stopListeners();
    setAuthBusy(false);
    authPanel.classList.remove("hidden");
    appPanel.classList.add("hidden");
    return;
  }
  await bootstrapSignedInUser(user);
});
