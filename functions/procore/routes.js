"use strict";

const { loadProcoreConfig } = require("./config");
const { ProcoreApiError, ProcoreClient } = require("./client");
const {
  STATE_COOKIE_NAME,
  buildAuthorizationUrl,
  buildClearStateCookie,
  buildStateCookie,
  consumeOAuthState,
  exchangeAuthorizationCode,
  generateState,
  parseCookies,
  refreshAccessToken,
  saveOAuthState,
  shouldRefreshToken,
  tokenExpiresAt,
  sanitizeErrorDetail,
} = require("./oauth");

const CONNECTION_DOC_ID = "default";
const SELECTION_DOC_ID = "default";

function scopedDocId(ownerUid) {
  const uid = String(ownerUid || "").trim();
  return uid ? uid.replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 120) : CONNECTION_DOC_ID;
}

function getConnectionRef(db, ownerUid) {
  return db.collection("procoreOAuthConnections").doc(scopedDocId(ownerUid));
}

function getSelectionRef(db, ownerUid) {
  return db.collection("procoreSettings").doc(scopedDocId(ownerUid));
}

function publicError(err) {
  const status = Number(err && err.status) || 500;
  const code = err && err.code ? err.code : status === 500 ? "procore-error" : "procore-request-failed";
  return {
    status,
    body: {
      connected: false,
      ok: false,
      error: code,
      message: err && err.message ? err.message : "Procore request failed.",
      detail: err && err.detail ? err.detail : undefined,
    },
  };
}

function safeLogMessage(message) {
  return sanitizeErrorDetail(String(message || "Procore request failed."));
}

function safeMe(me) {
  if (!me || typeof me !== "object") return null;
  return {
    id: me.id ?? null,
    name: me.name || [me.first_name, me.last_name].filter(Boolean).join(" ") || null,
    email: me.email_address || me.email || null,
    login: me.login || null,
  };
}

function safeCompany(company) {
  return {
    id: company && company.id != null ? company.id : null,
    name: company && company.name ? company.name : null,
  };
}

function safeProject(project) {
  if (!project) return null;
  return {
    id: project.id ?? null,
    name: project.name || null,
    project_number: project.project_number || project.projectNumber || null,
  };
}

function safeSelection(selection) {
  if (!selection || typeof selection !== "object") return null;
  return {
    company_id: selection.companyId != null && selection.companyId !== "" ? Number(selection.companyId) : null,
    company_name: selection.companyName || null,
    project_id: selection.projectId != null && selection.projectId !== "" ? Number(selection.projectId) : null,
    project_name: selection.projectName || null,
    project_number: selection.projectNumber || null,
    updated_at: selection.updatedAt || null,
    updated_by: selection.updatedBy || null,
  };
}

async function saveConnection(db, config, token, now = Date.now(), ownerUid = null) {
  await getConnectionRef(db, ownerUid).set(
    {
      env: config.env,
      accessToken: token.accessToken,
      refreshToken: token.refreshToken,
      tokenType: token.tokenType,
      expiresAt: tokenExpiresAt(now, token.expiresIn),
      updatedAt: new Date(now).toISOString(),
      // TODO: For production multi-user use, store per-user connections and add app-level KMS encryption.
    },
    { merge: true }
  );
}

async function loadConnection(db, ownerUid = null) {
  const snap = await getConnectionRef(db, ownerUid).get();
  if (!snap.exists) return null;
  return snap.data() || null;
}

async function loadSelection(db, ownerUid = null) {
  const snap = await getSelectionRef(db, ownerUid).get();
  if (!snap.exists) return null;
  return snap.data() || null;
}

async function saveSelection(db, selection, ownerUid = null) {
  await getSelectionRef(db, ownerUid).set(selection, { merge: true });
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function hasStoredConnection(db) {
  const connection = await loadConnection(db);
  return Boolean(connection && connection.refreshToken);
}

async function waitForStoredConnection(db, attempts = 4, delayMs = 750) {
  for (let i = 0; i < attempts; i += 1) {
    if (await hasStoredConnection(db)) return true;
    if (i < attempts - 1) await sleep(delayMs);
  }
  return false;
}

function isDuplicateStateError(err) {
  return /state has already been used/i.test(String(err && err.message ? err.message : ""));
}

async function getFreshAccessToken(db, config, fetchImpl = fetch, ownerUid = null) {
  const connection = await loadConnection(db, ownerUid);
  if (!connection || !connection.accessToken || !connection.refreshToken) {
    const err = new ProcoreApiError("Reconnect Procore before testing the API.", {
      status: 401,
      code: "not-connected",
    });
    throw err;
  }
  if (!shouldRefreshToken(connection)) {
    return connection.accessToken;
  }
  try {
    const token = await refreshAccessToken(config, connection.refreshToken, fetchImpl);
    await saveConnection(db, config, token);
    return token.accessToken;
  } catch (err) {
    err.status = err.status || 401;
    err.code = "reconnect-required";
    err.message = "Procore refresh token is invalid or expired. Reconnect Procore.";
    throw err;
  }
}

function json(res, status, body) {
  res.status(status).set("Cache-Control", "no-store").json(body);
}

function redirect(res, location) {
  res.set("Cache-Control", "no-store");
  res.redirect(302, location);
}

function errorHtml(title, message) {
  const escape = (value) => String(value || "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  const safeTitle = escape(title);
  const safeMessage = escape(message);
  return `<!doctype html><html><head><meta charset="utf-8"><title>${safeTitle}</title></head><body><h1>${safeTitle}</h1><p>${safeMessage}</p><p><a href="/procore">Back to Procore status</a></p></body></html>`;
}

function readBearerToken(req) {
  const header = String(req.get("Authorization") || req.get("authorization") || "").trim();
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : "";
}

function normalizeProcoreDataPath(value) {
  const raw = String(value || "").trim();
  if (!raw.startsWith("/rest/")) {
    throw new ProcoreApiError("Procore path must start with /rest/.", {
      status: 400,
      code: "invalid-path",
    });
  }
  if (/[\r\n]/.test(raw) || raw.includes("://")) {
    throw new ProcoreApiError("Procore path is not valid.", {
      status: 400,
      code: "invalid-path",
    });
  }
  return raw;
}

function queryObjectFromRequest(req) {
  const out = {};
  for (const [key, value] of Object.entries((req && req.query) || {})) {
    if (key === "path") continue;
    if (/token|secret|code|authorization/i.test(key)) {
      throw new ProcoreApiError("Sensitive OAuth fields are not allowed in Procore data queries.", {
        status: 400,
        code: "invalid-query",
      });
    }
    out[key] = value;
  }
  return out;
}

function readJsonBody(req) {
  const body = req && req.body;
  if (!body) return {};
  if (typeof body === "object") return body;
  if (typeof body === "string") {
    try {
      return JSON.parse(body);
    } catch (_) {
      throw new ProcoreApiError("Request body must be valid JSON.", {
        status: 400,
        code: "invalid-json",
      });
    }
  }
  return {};
}

function normalizeNumericField(name, value, { required = true } = {}) {
  const raw = String(value == null ? "" : value).trim();
  if (!raw) {
    if (required) {
      throw new ProcoreApiError(`${name} is required.`, {
        status: 400,
        code: "invalid-selection",
      });
    }
    return "";
  }
  if (!/^\d+$/.test(raw)) {
    throw new ProcoreApiError(`${name} must be numeric.`, {
      status: 400,
      code: "invalid-selection",
    });
  }
  return raw;
}

function createProcoreHandlers({ db, logger, fetchImpl = fetch, env = process.env, authorizeRequest = null }) {
  async function login(req, res) {
    if (req.method !== "GET") {
      res.status(405).set("Allow", "GET").send("Method Not Allowed");
      return;
    }
    try {
      if (!authorizeRequest) throw new ProcoreApiError("Procore access is not configured.", { status: 403, code: "access-not-configured" });
      const access = await authorizeRequest(req);
      const config = loadProcoreConfig(env);
      const state = generateState();
      await saveOAuthState(db, state, { uid: access && access.uid, email: access && access.email });
      res.set("Set-Cookie", buildStateCookie(state, {
        secure: config.redirectUri.startsWith("https://"),
      }));
      const authorizationUrl = buildAuthorizationUrl(config, state);
      if (String(req.query && req.query.format || "").toLowerCase() === "json") {
        json(res, 200, { ok: true, authorization_url: authorizationUrl });
      } else {
        redirect(res, authorizationUrl);
      }
    } catch (err) {
      logger.error("procoreLogin failed", { reason: safeLogMessage(err.message) });
      const out = publicError(err);
      json(res, out.status, out.body);
    }
  }

  async function callback(req, res) {
    if (req.method !== "GET") {
      res.status(405).set("Allow", "GET").send("Method Not Allowed");
      return;
    }
    try {
      const config = loadProcoreConfig(env);
      const code = String(req.query && req.query.code ? req.query.code : "").trim();
      const state = String(req.query && req.query.state ? req.query.state : "").trim();
      const error = String(req.query && req.query.error ? req.query.error : "").trim();
      const errorDescription = String(req.query && req.query.error_description ? req.query.error_description : "").trim();
      if (error) {
        throw new Error(errorDescription || `Procore OAuth error: ${error}`);
      }
      if (!code) {
        throw new Error("Procore callback did not include an authorization code.");
      }
      const cookies = parseCookies(req.get("Cookie"));
      const stateData = await consumeOAuthState(db, state, cookies[STATE_COOKIE_NAME]);
      const token = await exchangeAuthorizationCode(config, code, fetchImpl);
      await saveConnection(db, config, token, Date.now(), stateData && stateData.ownerUid);
      res.set("Set-Cookie", buildClearStateCookie());
      redirect(res, "/procore?connected=1");
    } catch (err) {
      logger.error("procoreCallback failed", { reason: safeLogMessage(err.message), status: err.status || null });
      res.set("Set-Cookie", buildClearStateCookie());
      res.status(err.status || 400).set("Cache-Control", "no-store").type("html").send(
        errorHtml("Procore connection failed", String(err.message || "Unable to connect Procore."))
      );
    }
  }

  async function status(req, res) {
    if (req.method !== "GET") {
      res.status(405).set("Allow", "GET").send("Method Not Allowed");
      return;
    }
    try {
      if (!authorizeRequest) throw new ProcoreApiError("Procore status access is not configured.", { status: 403, code: "access-not-configured" });
      const access = await authorizeRequest(req);
      const config = loadProcoreConfig(env);
      const ownerUid = access && access.uid;
      const connection = await loadConnection(db, ownerUid);
      const selection = await loadSelection(db, ownerUid);
      const effectiveCompanyId = config.companyId || (selection && selection.companyId) || "";
      const effectiveProjectName = config.projectName || (selection && selection.projectName) || "";
      if (!connection || !connection.refreshToken) {
        json(res, 200, {
          connected: false,
          company_id: effectiveCompanyId ? Number(effectiveCompanyId) : null,
          project_name: effectiveProjectName,
          selected: safeSelection(selection),
          message: `Procore is not connected. Use Connect Procore ${config.env === "production" ? "Production" : "Sandbox"}.`,
        });
        return;
      }
      const client = new ProcoreClient({
        config,
        fetchImpl,
        getAccessToken: () => getFreshAccessToken(db, config, fetchImpl, ownerUid),
      });

      const me = await client.get("/rest/v1.0/me");
      const companies = await client.get("/rest/v1.0/companies");
      const projects = effectiveCompanyId
        ? await client.get("/rest/v1.1/projects", {
          companyScoped: true,
          query: { company_id: effectiveCompanyId },
        })
        : [];
      const projectList = Array.isArray(projects) ? projects : [];
      const foundProject = selection && selection.projectId
        ? projectList.find((project) => String(project && project.id) === String(selection.projectId))
        : effectiveProjectName
        ? projectList.find(
          (project) => String(project && project.name ? project.name : "").trim() === effectiveProjectName
        )
        : null;

      json(res, 200, {
        connected: true,
        me: safeMe(me),
        company_id: effectiveCompanyId ? Number(effectiveCompanyId) : null,
        companies_found: Array.isArray(companies) ? companies.map(safeCompany) : [],
        project: safeProject(foundProject),
        project_found: Boolean(foundProject),
        selected: safeSelection(selection),
        message: effectiveCompanyId
          ? undefined
          : "Connected. Choose the production company id from companies_found, then set PROCORE_COMPANY_ID to discover projects.",
      });
    } catch (err) {
      logger.error("procoreStatus failed", {
        reason: safeLogMessage(err.message),
        status: err.status || null,
        code: err.code || null,
      });
      const out = publicError(err);
      json(res, out.status, out.body);
    }
  }

  async function data(req, res) {
    if (req.method !== "GET") {
      res.status(405).set("Allow", "GET").send("Method Not Allowed");
      return;
    }
    try {
      if (!authorizeRequest) {
        throw new ProcoreApiError("Procore data access is not configured.", {
          status: 403,
          code: "data-access-not-configured",
        });
      }
      const access = await authorizeRequest(req);
      const ownerUid = access && access.uid;
      const config = loadProcoreConfig(env);
      const path = normalizeProcoreDataPath(req.query && req.query.path);
      const query = queryObjectFromRequest(req);
      const client = new ProcoreClient({
        config,
        fetchImpl,
        getAccessToken: () => getFreshAccessToken(db, config, fetchImpl, ownerUid),
      });
      const payload = await client.get(path, {
        companyScoped: true,
        query,
      });
      json(res, 200, {
        ok: true,
        path,
        query,
        requested_by: access && access.email ? access.email : null,
        data: payload,
      });
    } catch (err) {
      logger.error("procoreData failed", {
        reason: safeLogMessage(err.message),
        status: err.status || null,
        code: err.code || null,
      });
      const out = publicError(err);
      json(res, out.status, out.body);
    }
  }

  async function selection(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
      res.status(405).set("Allow", "GET, POST").send("Method Not Allowed");
      return;
    }
    try {
      if (!authorizeRequest) {
        throw new ProcoreApiError("Procore selection access is not configured.", {
          status: 403,
          code: "selection-access-not-configured",
        });
      }
      const access = await authorizeRequest(req);
      const ownerUid = access && access.uid;
      const config = loadProcoreConfig(env);
      if (req.method === "GET") {
        const stored = await loadSelection(db, ownerUid);
        json(res, 200, {
          ok: true,
          env: config.env,
          configured_company_id: config.companyId ? Number(config.companyId) : null,
          configured_project_name: config.projectName || null,
          selected: safeSelection(stored),
        });
        return;
      }

      const body = readJsonBody(req);
      const companyId = normalizeNumericField("company_id", body.company_id);
      const projectId = normalizeNumericField("project_id", body.project_id);
      const now = new Date().toISOString();
      const next = {
        env: config.env,
        companyId,
        companyName: String(body.company_name || "").trim(),
        projectId,
        projectName: String(body.project_name || "").trim(),
        projectNumber: String(body.project_number || "").trim(),
        updatedAt: now,
        updatedBy: access && access.email ? access.email : null,
      };
      await saveSelection(db, next, ownerUid);
      json(res, 200, {
        ok: true,
        selected: safeSelection(next),
      });
    } catch (err) {
      logger.error("procoreSelection failed", {
        reason: safeLogMessage(err.message),
        status: err.status || null,
        code: err.code || null,
      });
      const out = publicError(err);
      json(res, out.status, out.body);
    }
  }

  return {
    callback,
    data,
    getFreshAccessToken,
    login,
    selection,
    status,
  };
}

module.exports = {
  CONNECTION_DOC_ID,
  createProcoreHandlers,
  getFreshAccessToken,
  loadConnection,
  loadSelection,
  publicError,
  safeCompany,
  safeMe,
  safeProject,
  safeSelection,
  saveConnection,
  saveSelection,
};
