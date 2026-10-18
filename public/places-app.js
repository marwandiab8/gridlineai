// Known Places dashboard: add places from an address, coordinates or a spreadsheet (functions/placesAdd.js),
// then list, rename, set what you do there (activity), retune matching radius, delete, and merge them -
// including the places learned by the "Log this place" Shortcut (see docs/quick-log-shortcuts.md and
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
import { PLACE_ACTIVITY_CHOICES } from "./place-activity-choices.js?v=2026-10-11-activities";

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
const updateKnownPlaceActivityCallable = httpsCallable(functions, "updateKnownPlaceActivityCallable");
const updateKnownPlaceRadiusCallable = httpsCallable(functions, "updateKnownPlaceRadiusCallable");
const deleteKnownPlaceCallable = httpsCallable(functions, "deleteKnownPlaceCallable");
const mergeKnownPlacesCallable = httpsCallable(functions, "mergeKnownPlacesCallable");
const lookupPlaceAddressCallable = httpsCallable(functions, "lookupPlaceAddressCallable");
const addKnownPlaceCallable = httpsCallable(functions, "addKnownPlaceCallable");
// Address-only rows are looked up about one a second on the server, so allow the callable its full time.
const importKnownPlacesCallable = httpsCallable(functions, "importKnownPlacesCallable", { timeout: 310000 });

const MIN_RADIUS_METERS = 20;
const MAX_RADIUS_METERS = 1000;
const MAX_IMPORT_FILE_BYTES = 2 * 1024 * 1024;

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
    list.innerHTML = `<p class="muted small">No places yet. Add one above, import a spreadsheet, or run the "Log this place" Shortcut somewhere new.</p>`;
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
          Activity:
          <input type="text" class="place-activity-input" data-id="${esc(place.id)}" list="placeActivityChoices" maxlength="40" value="${esc(place.activity || "")}" placeholder="Visit" />
          <button type="button" class="btn-inline place-save-activity" data-id="${esc(place.id)}">Save</button>
          <span class="muted">${place.activitySource === "set" ? "" : "(suggested from the name) "}what a visit here is - Time Left counts your time here as this. A change applies from the next visit.</span>
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

  const saveActivityBtn = event.target.closest(".place-save-activity");
  if (saveActivityBtn) {
    const id = saveActivityBtn.dataset.id;
    const input = document.querySelector(`.place-activity-input[data-id="${CSS.escape(id)}"]`);
    const activity = String(input?.value || "").trim();
    await withBusyMessage(
      () => updateKnownPlaceActivityCallable({ placeId: id, activity }),
      "Saving activity…",
      activity ? "Saved. Visits from now on count as this." : "Back to the activity the name suggests."
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

// --- Add a place from an address or coordinates ---------------------------------------------------

let addCandidates = [];

function setAddMsg(text, isError = false) {
  const el = byId("addMsg");
  if (!el) return;
  el.textContent = text || "";
  el.classList.toggle("form-error", !!isError);
}

function selectedAddCandidate() {
  const checked = document.querySelector('input[name="addCandidate"]:checked');
  return checked ? addCandidates[Number(checked.value)] : null;
}

function renderAddCandidates() {
  const list = byId("addCandidates");
  list.innerHTML = addCandidates
    .map(
      (candidate, index) => `
    <label class="places-candidate">
      <input type="radio" name="addCandidate" value="${index}" ${index === 0 ? "checked" : ""} />
      <span>${esc(candidate.label)}<br /><span class="muted small">${candidate.latitude.toFixed(5)}, ${candidate.longitude.toFixed(5)}
        · <a href="https://www.openstreetmap.org/?mlat=${candidate.latitude}&mlon=${candidate.longitude}#map=18/${candidate.latitude}/${candidate.longitude}" target="_blank" rel="noopener">check on map</a></span></span>
    </label>`
    )
    .join("");
  byId("addSave").disabled = !addCandidates.length;
}

function resetAddCandidates() {
  addCandidates = [];
  renderAddCandidates();
}

async function findAddCandidates() {
  const query = String(byId("addQuery").value || "").trim();
  if (!query) {
    setAddMsg("Type an address or coordinates first.", true);
    return;
  }
  setAddMsg("Looking up…");
  resetAddCandidates();
  try {
    const res = await lookupPlaceAddressCallable({ query });
    addCandidates = (res.data && res.data.candidates) || [];
    renderAddCandidates();
    setAddMsg(
      addCandidates.length
        ? addCandidates.length > 1
          ? "Pick the right match, then add the place."
          : "Check the match, then add the place."
        : "No match found. Try a fuller address (street, city), or paste coordinates.",
      !addCandidates.length
    );
  } catch (err) {
    setAddMsg(formatUiError(err), true);
  }
}

async function saveAddedPlace() {
  const name = String(byId("addName").value || "").trim();
  const candidate = selectedAddCandidate();
  const radiusMeters = Number(byId("addRadius").value);
  if (!name) {
    setAddMsg("Give the place a name.", true);
    return;
  }
  if (!candidate) {
    setAddMsg("Find the address or coordinates first.", true);
    return;
  }
  if (!Number.isFinite(radiusMeters) || radiusMeters < MIN_RADIUS_METERS || radiusMeters > MAX_RADIUS_METERS) {
    setAddMsg(`Radius must be between ${MIN_RADIUS_METERS} and ${MAX_RADIUS_METERS} meters.`, true);
    return;
  }
  setAddMsg("Saving…");
  byId("addSave").disabled = true;
  try {
    const res = await addKnownPlaceCallable({
      name,
      latitude: candidate.latitude,
      longitude: candidate.longitude,
      activity: String(byId("addActivity").value || "").trim(),
      radiusMeters,
      address: candidate.fromCoordinates ? "" : candidate.label,
    });
    const saved = res.data || {};
    setAddMsg(
      saved.action === "updated"
        ? `"${saved.name}" was already saved there - updated it.`
        : `Added "${saved.name}"${saved.activity ? ` (${saved.activity})` : ""}. Your next visit there will be recognized.`
    );
    byId("addName").value = "";
    byId("addQuery").value = "";
    byId("addActivity").value = "";
    resetAddCandidates();
  } catch (err) {
    setAddMsg(formatUiError(err), true);
    byId("addSave").disabled = false;
  }
}

byId("addFind")?.addEventListener("click", findAddCandidates);
byId("addQuery")?.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    findAddCandidates();
  }
});
byId("addQuery")?.addEventListener("input", resetAddCandidates);
byId("addSave")?.addEventListener("click", saveAddedPlace);

// --- Import from a spreadsheet ------------------------------------------------------------------------

const IMPORT_STATUS_LABELS = {
  add: "Will add",
  update: "Will update",
  lookup: "Will look up address",
  added: "Added",
  updated: "Updated",
  error: "Skipped",
};

let importFilePayload = null;

function setImportMsg(text, isError = false) {
  const el = byId("importMsg");
  if (!el) return;
  el.textContent = text || "";
  el.classList.toggle("form-error", !!isError);
}

function readFileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || "").replace(/^data:[^,]*,/, ""));
    reader.onerror = () => reject(reader.error || new Error("Could not read the file."));
    reader.readAsDataURL(file);
  });
}

function renderImportRows(rows) {
  const el = byId("importRows");
  if (!rows || !rows.length) {
    el.innerHTML = "";
    return;
  }
  el.innerHTML = `
    <table class="places-import-table">
      <thead><tr><th>Row</th><th>Name</th><th>Result</th><th>Details</th></tr></thead>
      <tbody>
        ${rows
          .map((row) => {
            const details =
              row.message ||
              (row.foundAddress ? `Found: ${row.foundAddress}` : "") ||
              (row.status === "lookup" ? row.address : "") ||
              (Number.isFinite(row.latitude) ? `${row.latitude.toFixed(5)}, ${row.longitude.toFixed(5)}` : "");
            return `<tr class="import-${esc(row.status)}">
              <td>${esc(row.rowNumber)}</td>
              <td>${esc(row.name || "—")}</td>
              <td>${esc(IMPORT_STATUS_LABELS[row.status] || row.status)}</td>
              <td>${esc(details)}${row.activity ? ` <span class="muted">· ${esc(row.activity)}</span>` : ""}</td>
            </tr>`;
          })
          .join("")}
      </tbody>
    </table>`;
}

function resetImport() {
  importFilePayload = null;
  byId("importRun").classList.add("hidden");
  renderImportRows([]);
  setImportMsg("");
}

byId("importFile")?.addEventListener("change", () => {
  resetImport();
  byId("importPreview").disabled = !byId("importFile").files.length;
});

byId("importPreview")?.addEventListener("click", async () => {
  const file = byId("importFile").files[0];
  if (!file) return;
  resetImport();
  if (!/\.(xlsx|csv)$/i.test(file.name)) {
    setImportMsg("Choose an .xlsx or .csv file. (An older .xls file can be saved as .xlsx from Excel.)", true);
    return;
  }
  if (file.size > MAX_IMPORT_FILE_BYTES) {
    setImportMsg("That file is larger than 2 MB. Split it into smaller files.", true);
    return;
  }
  setImportMsg("Reading the spreadsheet…");
  try {
    importFilePayload = { fileName: file.name, fileBase64: await readFileAsBase64(file) };
    const res = await importKnownPlacesCallable({ ...importFilePayload, dryRun: true });
    const { summary, rows } = res.data || {};
    renderImportRows(rows);
    const ready = summary.toAdd + summary.toUpdate + summary.toLookUp;
    const parts = [
      summary.toAdd && `${summary.toAdd} to add`,
      summary.toUpdate && `${summary.toUpdate} to update`,
      summary.toLookUp && `${summary.toLookUp} address${summary.toLookUp === 1 ? "" : "es"} to look up`,
      summary.errors && `${summary.errors} with problems (skipped)`,
    ].filter(Boolean);
    setImportMsg(`${summary.rows} row${summary.rows === 1 ? "" : "s"}: ${parts.join(", ")}.`, !ready);
    if (ready) {
      const run = byId("importRun");
      run.textContent = `Import ${ready} place${ready === 1 ? "" : "s"}`;
      run.classList.remove("hidden");
    }
  } catch (err) {
    importFilePayload = null;
    setImportMsg(formatUiError(err), true);
  }
});

byId("importRun")?.addEventListener("click", async () => {
  if (!importFilePayload) return;
  const run = byId("importRun");
  run.disabled = true;
  setImportMsg("Importing… addresses take about a second each to look up.");
  try {
    const res = await importKnownPlacesCallable({ ...importFilePayload, dryRun: false });
    const { summary, rows } = res.data || {};
    renderImportRows(rows);
    setImportMsg(
      `Done: ${summary.added} added, ${summary.updated} updated${summary.errors ? `, ${summary.errors} skipped (see below)` : ""}.`,
      summary.errors > 0 && !summary.added && !summary.updated
    );
    importFilePayload = null;
    run.classList.add("hidden");
  } catch (err) {
    setImportMsg(formatUiError(err), true);
  } finally {
    run.disabled = false;
  }
});

byId("importTemplate")?.addEventListener("click", (event) => {
  event.preventDefault();
  const csv = [
    "Name,Latitude,Longitude,Address,Activity,Radius",
    "Raging Bull,43.5471,-80.2968,,Eating out,120",
    "Cottage,,,\"12 Lake Rd, Muskoka, ON\",Visit,",
  ].join("\r\n");
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  link.download = "known-places-template.csv";
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 1000);
});

// The activity choices offered as you type (any other short text works too).
const activityChoices = document.createElement("datalist");
activityChoices.id = "placeActivityChoices";
activityChoices.innerHTML = PLACE_ACTIVITY_CHOICES.map((choice) => `<option value="${esc(choice)}"></option>`).join("");
document.body.appendChild(activityChoices);

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
