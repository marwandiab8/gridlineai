"use strict";

const { test } = require("node:test");
const assert = require("node:assert/strict");
const { loadProcoreConfig } = require("./procore/config");
const { ProcoreClient } = require("./procore/client");
const {
  buildAuthorizationUrl,
  consumeOAuthState,
  exchangeAuthorizationCode,
  saveOAuthState,
} = require("./procore/oauth");
const { createProcoreHandlers, saveConnection } = require("./procore/routes");

const env = {
  PROCORE_ENV: "sandbox",
  PROCORE_ENABLED: "true",
  PROCORE_CLIENT_ID: "client-id",
  PROCORE_CLIENT_SECRET: "client-secret",
  PROCORE_REDIRECT_URI: "http://localhost:3000/callback",
  PROCORE_AUTH_BASE_URL: "https://login-sandbox.procore.com",
  PROCORE_API_BASE_URL: "https://sandbox.procore.com",
  PROCORE_COMPANY_ID: "4286302",
  PROCORE_PROJECT_NAME: "Sandbox Test Project",
};

function makeJsonResponse(body, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async text() {
      return JSON.stringify(body);
    },
  };
}

function makeDb() {
  const store = new Map();
  let transactionTail = Promise.resolve();
  return {
    collection(name) {
      return {
        doc(id) {
          const key = `${name}/${id}`;
          return {
            async set(data, options) {
              const prev = options && options.merge ? store.get(key) || {} : {};
              store.set(key, { ...prev, ...data });
            },
            async get() {
              return {
                exists: store.has(key),
                data: () => store.get(key),
              };
            },
            async update(data) {
              store.set(key, { ...(store.get(key) || {}), ...data });
            },
          };
        },
      };
    },
    async runTransaction(fn) {
      const run = transactionTail.then(async () => {
        const tx = {
          async get(ref) { return ref.get(); },
          update(ref, data) { return ref.update(data); },
        };
        return fn(tx);
      });
      transactionTail = run.catch(() => {});
      return run;
    },
  };
}

function makeReq(method = "GET", options = {}) {
  return {
    method,
    query: options.query || {},
    body: options.body,
    get(name) {
      return (options.headers || {})[name] || (options.headers || {})[String(name || "").toLowerCase()] || "";
    },
  };
}

function makeRes() {
  return {
    statusCode: 200,
    headers: {},
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    set(key, value) {
      this.headers[key] = value;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
    send(body) {
      this.body = body;
      return this;
    },
    type() {
      return this;
    },
    redirect(code, location) {
      this.statusCode = code;
      this.headers.Location = location;
      return this;
    },
  };
}

test("loadProcoreConfig validates required env vars and sandbox endpoints", () => {
  const config = loadProcoreConfig(env);
  assert.equal(config.env, "sandbox");
  assert.equal(config.companyId, "4286302");
  assert.equal(config.authBaseUrl, "https://login-sandbox.procore.com");
  assert.throws(() => loadProcoreConfig({ ...env, PROCORE_CLIENT_SECRET: "" }), /PROCORE_CLIENT_SECRET/);
});

test("loadProcoreConfig fails closed for production token storage", () => {
  assert.throws(() => loadProcoreConfig({
    ...env,
    PROCORE_ENV: "production",
    PROCORE_AUTH_BASE_URL: "https://login.procore.com",
    PROCORE_API_BASE_URL: "https://api.procore.com",
    PROCORE_COMPANY_ID: "",
    PROCORE_PROJECT_NAME: "",
  }), /production integration is disabled/);
});

test("buildAuthorizationUrl includes required OAuth params", () => {
  const url = new URL(buildAuthorizationUrl(loadProcoreConfig(env), "state-123"));
  assert.equal(url.origin, "https://login-sandbox.procore.com");
  assert.equal(url.pathname, "/oauth/authorize");
  assert.equal(url.searchParams.get("response_type"), "code");
  assert.equal(url.searchParams.get("client_id"), "client-id");
  assert.equal(url.searchParams.get("redirect_uri"), "http://localhost:3000/callback");
  assert.equal(url.searchParams.get("state"), "state-123");
});

test("exchangeAuthorizationCode posts x-www-form-urlencoded token request", async () => {
  const calls = [];
  const token = await exchangeAuthorizationCode(loadProcoreConfig(env), "auth-code", async (url, init) => {
    calls.push({ url, init });
    return makeJsonResponse({
      access_token: "access-token",
      refresh_token: "refresh-token",
      expires_in: 3600,
      token_type: "Bearer",
    });
  });
  assert.equal(token.accessToken, "access-token");
  assert.equal(calls[0].url.toString(), "https://login-sandbox.procore.com/oauth/token");
  assert.equal(calls[0].init.method, "POST");
  assert.equal(calls[0].init.headers["Content-Type"], "application/x-www-form-urlencoded");
  assert.equal(calls[0].init.body.get("grant_type"), "authorization_code");
  assert.equal(calls[0].init.body.get("client_secret"), "client-secret");
});

test("consumeOAuthState requires matching cookie and atomically consumes state", async () => {
  const db = makeDb();
  await saveOAuthState(db, "state-without-cookie", Date.now());
  await assert.rejects(
    () => consumeOAuthState(db, "state-without-cookie", ""),
    /cookie did not match/
  );
  await assert.doesNotReject(() => consumeOAuthState(db, "state-without-cookie", "state-without-cookie"));
  await assert.rejects(() => consumeOAuthState(db, "state-without-cookie", "state-without-cookie"), /already been used/);
});

test("concurrent OAuth callbacks cannot consume the same state twice", async () => {
  const db = makeDb();
  await saveOAuthState(db, "concurrent-state", Date.now());
  const results = await Promise.allSettled([
    consumeOAuthState(db, "concurrent-state", "concurrent-state"),
    consumeOAuthState(db, "concurrent-state", "concurrent-state"),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  assert.equal(results.filter((r) => r.status === "rejected").length, 1);
});

test("Procore login and status reject unauthenticated and insufficient callers", async () => {
  const db = makeDb();
  const handlers = createProcoreHandlers({ db, env, logger: { error() {} }, authorizeRequest: async () => {
    const err = new Error("Sign in required"); err.status = 401; err.code = "unauthenticated"; throw err;
  } });
  const loginRes = makeRes();
  await handlers.login(makeReq(), loginRes);
  assert.equal(loginRes.statusCode, 401);
  const statusRes = makeRes();
  await handlers.status(makeReq(), statusRes);
  assert.equal(statusRes.statusCode, 401);
  const denied = createProcoreHandlers({ db, env, logger: { error() {} }, authorizeRequest: async () => {
    const err = new Error("Management access is required"); err.status = 403; err.code = "permission-denied"; throw err;
  } });
  const deniedRes = makeRes();
  await denied.login(makeReq(), deniedRes);
  assert.equal(deniedRes.statusCode, 403);
});

test("OAuth callback escapes external error text in HTML", async () => {
  const handlers = createProcoreHandlers({ db: makeDb(), env, logger: { error() {} } });
  const res = makeRes();
  await handlers.callback(makeReq("GET", { query: { error: "<script>alert(1)</script>", error_description: "<img src=x onerror=alert(1)>" } }), res);
  assert.equal(res.statusCode, 400);
  assert.doesNotMatch(String(res.body), /<script>|<img/i);
  assert.match(String(res.body), /&lt;/);
});

test("ProcoreClient attaches auth and company headers", async () => {
  const calls = [];
  const client = new ProcoreClient({
    config: loadProcoreConfig(env),
    getAccessToken: async () => "access-token",
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      return makeJsonResponse([{ id: 1 }]);
    },
  });
  await client.get("/rest/v1.1/projects", {
    companyScoped: true,
    query: { company_id: 4286302 },
  });
  assert.equal(calls[0].url.toString(), "https://sandbox.procore.com/rest/v1.1/projects?company_id=4286302");
  assert.equal(calls[0].init.headers.Authorization, "Bearer access-token");
  assert.equal(calls[0].init.headers.Accept, "application/json");
  assert.equal(calls[0].init.headers["Procore-Company-Id"], "4286302");
  assert.equal(calls[0].init.headers["Content-Type"], undefined);
});

test("procore status returns safe user/company/project fields", async () => {
  const db = makeDb();
  const config = loadProcoreConfig(env);
  await saveConnection(db, config, {
    accessToken: "access-token",
    refreshToken: "refresh-token",
    tokenType: "Bearer",
    expiresIn: 3600,
  }, Date.now(), "test-user");
  const responses = [
    { id: 10, name: "Sandbox User", email_address: "user@example.com" },
    [{ id: 4286302, name: "Sandbox Company" }],
    [
      { id: 111, name: "Other", project_number: "9999" },
      { id: 222, name: "Sandbox Test Project", project_number: "1234" },
    ],
  ];
  const handlers = createProcoreHandlers({
    db,
    env,
    logger: { error() {} },
    authorizeRequest: async () => ({ uid: "test-user", email: "user@example.com" }),
    fetchImpl: async () => makeJsonResponse(responses.shift()),
  });
  const res = makeRes();
  await handlers.status(makeReq(), res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.connected, true);
  assert.equal(res.body.me.email, "user@example.com");
  assert.equal(res.body.project.id, 222);
  assert.equal(res.body.project.project_number, "1234");
  assert.equal(res.body.access_token, undefined);
  assert.equal(res.body.refresh_token, undefined);
});

test("procore status returns companies without projects when company id is blank", async () => {
  const discoveryEnv = {
    ...env,
    PROCORE_ENV: "production",
    PROCORE_AUTH_BASE_URL: "https://login.procore.com",
    PROCORE_API_BASE_URL: "https://api.procore.com",
    PROCORE_COMPANY_ID: "",
    PROCORE_PROJECT_NAME: "",
  };
  discoveryEnv.PROCORE_ENABLED = "true";
  assert.throws(() => loadProcoreConfig(discoveryEnv), /production integration is disabled/);
});

test("procore data endpoint requires auth and proxies safe GET paths", async () => {
  const db = makeDb();
  const config = loadProcoreConfig(env);
  await saveConnection(db, config, {
    accessToken: "access-token",
    refreshToken: "refresh-token",
    tokenType: "Bearer",
    expiresIn: 3600,
  });
  const handlers = createProcoreHandlers({
    db,
    env,
    logger: { error() {} },
    authorizeRequest: async () => ({ email: "manager@example.com" }),
    fetchImpl: async (url, init) => {
      assert.equal(url.toString(), "https://sandbox.procore.com/rest/v1.1/projects?company_id=4286302");
      assert.equal(init.headers.Authorization, "Bearer access-token");
      return makeJsonResponse([{ id: 222 }]);
    },
  });
  const res = makeRes();
  await handlers.data(makeReq("GET", {
    query: { path: "/rest/v1.1/projects", company_id: "4286302" },
  }), res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body.data, [{ id: 222 }]);
});

test("procore selection endpoint saves and returns selected project", async () => {
  const db = makeDb();
  const handlers = createProcoreHandlers({
    db,
    env,
    logger: { error() {} },
    authorizeRequest: async () => ({ email: "manager@example.com" }),
  });
  const saveRes = makeRes();
  await handlers.selection(makeReq("POST", {
    body: {
      company_id: "51994",
      company_name: "Matheson Constructors Limited",
      project_id: "123456",
      project_name: "Main Project",
      project_number: "P-100",
    },
  }), saveRes);
  assert.equal(saveRes.statusCode, 200);
  assert.equal(saveRes.body.selected.company_id, 51994);
  assert.equal(saveRes.body.selected.project_id, 123456);
  assert.equal(saveRes.body.selected.project_name, "Main Project");

  const getRes = makeRes();
  await handlers.selection(makeReq("GET"), getRes);
  assert.equal(getRes.statusCode, 200);
  assert.equal(getRes.body.selected.company_name, "Matheson Constructors Limited");
  assert.equal(getRes.body.selected.updated_by, "manager@example.com");
});
