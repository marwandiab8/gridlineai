"use strict";

const { sanitizeErrorDetail } = require("./oauth");

class ProcoreApiError extends Error {
  constructor(message, { status = 500, code = "procore-api-error", detail = null } = {}) {
    super(message);
    this.name = "ProcoreApiError";
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

function statusMessage(status) {
  if (status === 401) return ["unauthorized", "Procore authorization is missing, expired, or invalid. Reconnect Procore."];
  if (status === 403) return ["forbidden", "Procore denied access. Check that the Procore app is installed for this company and that your Procore user/app has project permissions."];
  if (status === 404) return ["not-found", "Procore endpoint or id was not found."];
  if (status === 429) return ["rate-limited", "Procore rate limit reached. Try again later."];
  if (status >= 400 && status < 500) return ["procore-client-error", "Procore rejected the request."];
  return ["procore-server-error", "Procore API returned a server error."];
}

async function readResponseBody(response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (_) {
    return { body: text.slice(0, 1000) };
  }
}

class ProcoreClient {
  constructor({ config, getAccessToken, fetchImpl = fetch }) {
    this.config = config;
    this.getAccessToken = getAccessToken;
    this.fetchImpl = fetchImpl;
  }

  async request(path, { method = "GET", body, companyScoped = false, query } = {}) {
    const accessToken = await this.getAccessToken();
    if (!accessToken) {
      throw new ProcoreApiError("Reconnect Procore before calling the API.", {
        status: 401,
        code: "missing-token",
      });
    }

    const url = new URL(path, this.config.apiBaseUrl);
    if (query && typeof query === "object") {
      for (const [key, value] of Object.entries(query)) {
        if (value != null && value !== "") url.searchParams.set(key, String(value));
      }
    }

    const headers = {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
    };
    const companyHeaderId = query && query.company_id != null && query.company_id !== ""
      ? query.company_id
      : this.config.companyId;
    if (companyScoped && companyHeaderId) {
      headers["Procore-Company-Id"] = String(companyHeaderId);
    }

    const init = { method, headers };
    if (body != null) {
      headers["Content-Type"] = "application/json";
      init.body = typeof body === "string" ? body : JSON.stringify(body);
    }

    const response = await this.fetchImpl(url, init);
    const payload = await readResponseBody(response);
    if (!response.ok) {
      const [code, message] = statusMessage(response.status);
      throw new ProcoreApiError(message, {
        status: response.status,
        code,
        detail: sanitizeErrorDetail(payload),
      });
    }
    return payload;
  }

  get(path, options = {}) {
    return this.request(path, { ...options, method: "GET" });
  }
}

module.exports = {
  ProcoreApiError,
  ProcoreClient,
  statusMessage,
};
