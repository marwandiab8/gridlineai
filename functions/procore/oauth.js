"use strict";

const { createHash, randomBytes } = require("crypto");

const STATE_COOKIE_NAME = "procore_oauth_state";
const STATE_TTL_MS = 10 * 60 * 1000;
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;

function base64Url(buffer) {
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function generateState() {
  return base64Url(randomBytes(32));
}

function hashState(state) {
  return createHash("sha256").update(String(state || "")).digest("hex");
}

function parseCookies(header) {
  const out = {};
  for (const part of String(header || "").split(";")) {
    const index = part.indexOf("=");
    if (index < 0) continue;
    const key = part.slice(0, index).trim();
    const value = part.slice(index + 1).trim();
    if (key) out[key] = decodeURIComponent(value);
  }
  return out;
}

function buildStateCookie(state, { maxAgeSeconds = 600, secure = false } = {}) {
  const parts = [
    `${STATE_COOKIE_NAME}=${encodeURIComponent(state)}`,
    "HttpOnly",
    "SameSite=Lax",
    "Path=/",
    `Max-Age=${maxAgeSeconds}`,
  ];
  if (secure) parts.push("Secure");
  return parts.join("; ");
}

function buildClearStateCookie() {
  return `${STATE_COOKIE_NAME}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0`;
}

function buildAuthorizationUrl(config, state) {
  const url = new URL("/oauth/authorize", config.authBaseUrl);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", config.clientId);
  url.searchParams.set("redirect_uri", config.redirectUri);
  url.searchParams.set("state", state);
  return url.toString();
}

async function saveOAuthState(db, state, owner = {}, now = Date.now()) {
  if (typeof owner === "number") {
    now = owner;
    owner = {};
  }
  const stateHash = hashState(state);
  await db.collection("procoreOAuthStates").doc(stateHash).set({
    stateHash,
    ownerUid: owner && owner.uid ? String(owner.uid) : null,
    ownerEmail: owner && owner.email ? String(owner.email) : null,
    createdAt: new Date(now).toISOString(),
    expiresAt: new Date(now + STATE_TTL_MS).toISOString(),
    usedAt: null,
  });
  return stateHash;
}

async function consumeOAuthState(db, state, cookieState, now = Date.now()) {
  if (!state) {
    throw new Error("Procore OAuth callback did not include state. Start the connection again.");
  }
  if (!cookieState || String(cookieState) !== String(state)) {
    throw new Error("Procore OAuth state cookie did not match. Start the connection again.");
  }
  const stateHash = hashState(state);
  const ref = db.collection("procoreOAuthStates").doc(stateHash);
  const consume = async (tx) => {
    const snap = await tx.get(ref);
    if (!snap.exists) throw new Error("Procore OAuth state was not found. Start the connection again.");
    const data = snap.data() || {};
    if (data.usedAt) throw new Error("Procore OAuth state has already been used. Start the connection again.");
    const expiresAt = Date.parse(data.expiresAt || "");
    if (!Number.isFinite(expiresAt) || expiresAt < now) throw new Error("Procore OAuth state expired. Start the connection again.");
    tx.update(ref, { usedAt: new Date(now).toISOString() });
    return data;
  };
  if (typeof db.runTransaction === "function") return db.runTransaction(consume);
  throw new Error("Procore OAuth state requires transactional Firestore support.");
}

function sanitizeTokenPayload(payload) {
  const expiresIn = Number(payload && payload.expires_in);
  return {
    accessToken: String(payload && payload.access_token ? payload.access_token : ""),
    refreshToken: String(payload && payload.refresh_token ? payload.refresh_token : ""),
    tokenType: String(payload && payload.token_type ? payload.token_type : "Bearer"),
    expiresIn: Number.isFinite(expiresIn) && expiresIn > 0 ? expiresIn : 3600,
  };
}

async function parseTokenResponse(response) {
  const text = await response.text();
  let data = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (_) {
      data = { error: "invalid_json", error_description: text.slice(0, 500) };
    }
  }
  if (!response.ok) {
    const message = String(data.error_description || data.error || "Procore token request failed.");
    const err = new Error(message);
    err.status = response.status;
    err.detail = sanitizeErrorDetail(data);
    throw err;
  }
  const token = sanitizeTokenPayload(data);
  if (!token.accessToken || !token.refreshToken) {
    throw new Error("Procore token response did not include both access_token and refresh_token.");
  }
  return token;
}

function sanitizeErrorDetail(value) {
  if (value == null) return value;
  if (Array.isArray(value)) return value.map(sanitizeErrorDetail);
  if (typeof value === "object") {
    const out = {};
    for (const [key, item] of Object.entries(value)) {
      if (/token|secret|code|authorization/i.test(key)) {
        out[key] = "[redacted]";
      } else {
        out[key] = sanitizeErrorDetail(item);
      }
    }
    return out;
  }
  if (typeof value === "string") {
    return value.replace(/[A-Za-z0-9._~+/=-]{32,}/g, "[redacted]").slice(0, 1000);
  }
  return value;
}

async function exchangeAuthorizationCode(config, code, fetchImpl = fetch) {
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    client_id: config.clientId,
    client_secret: config.clientSecret,
    code,
    redirect_uri: config.redirectUri,
  });
  const response = await fetchImpl(new URL("/oauth/token", config.authBaseUrl), {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/json" },
    body,
  });
  return parseTokenResponse(response);
}

async function refreshAccessToken(config, refreshToken, fetchImpl = fetch) {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: config.clientId,
    client_secret: config.clientSecret,
    refresh_token: refreshToken,
  });
  const response = await fetchImpl(new URL("/oauth/token", config.authBaseUrl), {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/json" },
    body,
  });
  return parseTokenResponse(response);
}

function tokenExpiresAt(now, expiresInSeconds) {
  return new Date(now + Number(expiresInSeconds || 3600) * 1000).toISOString();
}

function shouldRefreshToken(connection, now = Date.now()) {
  const expiresAt = Date.parse(connection && connection.expiresAt ? connection.expiresAt : "");
  return !Number.isFinite(expiresAt) || expiresAt - now <= TOKEN_REFRESH_BUFFER_MS;
}

module.exports = {
  STATE_COOKIE_NAME,
  TOKEN_REFRESH_BUFFER_MS,
  buildAuthorizationUrl,
  buildClearStateCookie,
  buildStateCookie,
  consumeOAuthState,
  exchangeAuthorizationCode,
  generateState,
  hashState,
  parseCookies,
  refreshAccessToken,
  saveOAuthState,
  sanitizeErrorDetail,
  shouldRefreshToken,
  tokenExpiresAt,
};
