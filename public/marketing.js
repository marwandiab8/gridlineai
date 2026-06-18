import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import {
  initializeAppCheck,
  ReCaptchaEnterpriseProvider,
  ReCaptchaV3Provider,
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app-check.js";
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

function resolveAppCheckSiteKey() {
  return String(
    window.__FIREBASE_APPCHECK_SITE_KEY__ ||
      window.FIREBASE_APPCHECK_SITE_KEY ||
      ""
  ).trim();
}

function resolveAppCheckProvider() {
  return String(window.FIREBASE_APPCHECK_PROVIDER || "enterprise")
    .trim()
    .toLowerCase();
}

const app = initializeApp(resolveFirebaseWebConfig(DEFAULT_FIREBASE_CONFIG));
const appCheckSiteKey = resolveAppCheckSiteKey();

if (appCheckSiteKey) {
  try {
    const providerName = resolveAppCheckProvider();
    initializeAppCheck(app, {
      provider:
        providerName === "v3" || providerName === "recaptchav3"
          ? new ReCaptchaV3Provider(appCheckSiteKey)
          : new ReCaptchaEnterpriseProvider(appCheckSiteKey),
      isTokenAutoRefreshEnabled: true,
    });
  } catch (_) {
    // Duplicate initialization can happen in hot reload or browser restore cases.
  }
}

const functions = getFunctions(app, "northamerica-northeast1");
const submitDemoRequest = httpsCallable(functions, "submitDemoRequestCallable");

function setStatus(el, message, kind) {
  if (!el) return;
  el.textContent = message || "";
  el.className = `form-status${kind ? ` ${kind}` : ""}`;
}

function formValue(form, name) {
  return String(new FormData(form).get(name) || "").trim();
}

function validateClientPayload(payload) {
  if (!payload.name) return "Enter your name.";
  if (!payload.email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(payload.email)) {
    return "Enter a valid work email.";
  }
  if (!payload.company) return "Enter your company.";
  if (!payload.consent) return "Please confirm that we can contact you about a demo.";
  if (!appCheckSiteKey) {
    return "Demo protection is not configured yet. Add the Firebase App Check site key before using this form.";
  }
  return "";
}

function initDemoForm() {
  const form = document.getElementById("demoRequestForm");
  const status = document.getElementById("demoFormStatus");
  const loadedAt = document.getElementById("demoLoadedAt");
  if (!form) return;

  if (loadedAt) loadedAt.value = String(Date.now());

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const submitButton = form.querySelector('button[type="submit"]');
    const payload = {
      name: formValue(form, "name"),
      email: formValue(form, "email"),
      company: formValue(form, "company"),
      phone: formValue(form, "phone"),
      role: formValue(form, "role"),
      teamSize: formValue(form, "teamSize"),
      message: formValue(form, "message"),
      consent: Boolean(form.querySelector('input[name="consent"]')?.checked),
      website: formValue(form, "website"),
      loadedAt: Number(formValue(form, "loadedAt")) || 0,
      pagePath: window.location.pathname,
    };

    const error = validateClientPayload(payload);
    if (error) {
      setStatus(status, error, "err");
      return;
    }

    if (submitButton) submitButton.disabled = true;
    setStatus(status, "Sending request...", "");

    try {
      await submitDemoRequest(payload);
      form.reset();
      if (loadedAt) loadedAt.value = String(Date.now());
      setStatus(status, "Thanks. Your demo request was sent.", "ok");
    } catch (err) {
      const message =
        err && err.message
          ? String(err.message).replace(/^FirebaseError:\s*/i, "")
          : "Could not send the request. Please try again.";
      setStatus(status, message, "err");
    } finally {
      if (submitButton) submitButton.disabled = false;
    }
  });
}

initDemoForm();
