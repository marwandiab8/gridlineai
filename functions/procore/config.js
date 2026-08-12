"use strict";

const DEFAULTS_BY_ENV = {
  sandbox: {
    authBaseUrl: "https://login-sandbox.procore.com",
    apiBaseUrl: "https://sandbox.procore.com",
  },
  production: {
    authBaseUrl: "https://login.procore.com",
    apiBaseUrl: "https://api.procore.com",
  },
};

function cleanString(value) {
  return String(value || "").trim();
}

function requireUrl(name, value) {
  const raw = cleanString(value);
  if (!raw) return "";
  try {
    const url = new URL(raw);
    if (!/^https?:$/.test(url.protocol)) {
      throw new Error("URL must start with http:// or https://");
    }
    return url.toString().replace(/\/$/, "");
  } catch (err) {
    throw new Error(`${name} must be a valid URL.`);
  }
}

function loadProcoreConfig(env = process.env) {
  const procoreEnv = cleanString(env.PROCORE_ENV || "sandbox").toLowerCase();
  if (!Object.prototype.hasOwnProperty.call(DEFAULTS_BY_ENV, procoreEnv)) {
    throw new Error("PROCORE_ENV must be either sandbox or production.");
  }

  const defaults = DEFAULTS_BY_ENV[procoreEnv];
  const config = {
    env: procoreEnv,
    enabled: cleanString(env.PROCORE_ENABLED || "false").toLowerCase() === "true",
    clientId: cleanString(env.PROCORE_CLIENT_ID),
    clientSecret: cleanString(env.PROCORE_CLIENT_SECRET),
    redirectUri: cleanString(env.PROCORE_REDIRECT_URI),
    authBaseUrl: requireUrl("PROCORE_AUTH_BASE_URL", env.PROCORE_AUTH_BASE_URL || defaults.authBaseUrl),
    apiBaseUrl: requireUrl("PROCORE_API_BASE_URL", env.PROCORE_API_BASE_URL || defaults.apiBaseUrl),
    companyId: cleanString(env.PROCORE_COMPANY_ID),
    projectName: cleanString(env.PROCORE_PROJECT_NAME),
  };

  if (!config.enabled) throw new Error("Procore integration is disabled. Set PROCORE_ENABLED=true only for an approved sandbox setup.");
  if (config.env === "production") throw new Error("Procore production integration is disabled until encrypted token storage is approved.");

  const missing = [];
  for (const [key, value] of [
    ["PROCORE_CLIENT_ID", config.clientId],
    ["PROCORE_CLIENT_SECRET", config.clientSecret],
    ["PROCORE_REDIRECT_URI", config.redirectUri],
    ["PROCORE_AUTH_BASE_URL", cleanString(env.PROCORE_AUTH_BASE_URL)],
    ["PROCORE_API_BASE_URL", cleanString(env.PROCORE_API_BASE_URL)],
  ]) {
    if (!value) missing.push(key);
  }
  if (missing.length) {
    throw new Error(`Missing required Procore env vars: ${missing.join(", ")}`);
  }

  if (config.companyId && !/^\d+$/.test(config.companyId)) {
    throw new Error("PROCORE_COMPANY_ID must be a numeric Procore company id.");
  }

  return config;
}

module.exports = {
  DEFAULTS_BY_ENV,
  loadProcoreConfig,
};
