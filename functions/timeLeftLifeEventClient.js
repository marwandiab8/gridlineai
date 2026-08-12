const {
  requireValidTimeLeftLifeEventConfig,
} = require("./timeLeftLifeEventConfig");

const RESULT_STATUS = {
  delivered: "delivered",
  duplicate: "duplicate",
  off: "off",
  permanentFailure: "permanent_failure",
  authenticationFailure: "authentication_failure",
  conflict: "conflict",
  retryableFailure: "retryable_failure",
};

function normalizeHeaders(headers = {}) {
  const out = {};
  for (const [key, value] of Object.entries(headers)) {
    out[key] = String(value);
  }
  return out;
}

async function safeParseJson(response) {
  if (!response || typeof response.text !== "function") return {};
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_) {
    return { __invalidJson: true, raw: String(text).slice(0, 2000) };
  }
}

function sanitizeErrorSummary(error) {
  const raw = String(error && error.message ? error.message : error).trim();
  if (!raw) return "request failure";
  return raw.slice(0, 500);
}

async function fetchWithTimeout(fetchFn, url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new Error("request timeout"));
  }, Math.max(250, Number(timeoutMs) || 5000));

  try {
    return await fetchFn(url, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }
}

function isTimeoutError(error) {
  return (
    error &&
    (error.name === "AbortError" ||
      String(error.message || "").toLowerCase().includes("timeout"))
  );
}

function isNetworkError(error) {
  if (!error) return false;
  const msg = String(error.message || "").toLowerCase();
  return (
    /network/i.test(msg) ||
    /fetch failed/i.test(msg) ||
    error.name === "TypeError"
  );
}

function classifyResponse(httpStatus, body) {
  if (httpStatus === 200) {
    if (body && body.duplicate) {
      return {
        status: RESULT_STATUS.duplicate,
        retryable: false,
        lifeEventId: body.lifeEventId || null,
        idempotencyKey: body.idempotencyKey || null,
      };
    }
    return {
      status: RESULT_STATUS.delivered,
      retryable: false,
      lifeEventId: body.lifeEventId || null,
      idempotencyKey: body.idempotencyKey || null,
    };
  }

  if (httpStatus === 400 || httpStatus === 413) {
    return {
      status: RESULT_STATUS.permanentFailure,
      retryable: false,
      errorCode: String(body && body.code ? body.code : `http_${httpStatus}`),
      summary: sanitizeErrorSummary(body && body.error ? body.error : body && body.message),
    };
  }

  if (httpStatus === 401 || httpStatus === 403) {
    return {
      status: RESULT_STATUS.authenticationFailure,
      retryable: false,
      errorCode: String(body && body.code ? body.code : `http_${httpStatus}`),
      summary: sanitizeErrorSummary(body && body.error ? body.error : body && body.message),
    };
  }

  if (httpStatus === 409) {
    return {
      status: RESULT_STATUS.conflict,
      retryable: false,
      existingLifeEventId: body && body.existingLifeEventId ? body.existingLifeEventId : null,
      lifeEventId: body && body.lifeEventId ? body.lifeEventId : null,
      idempotencyKey: body && body.idempotencyKey ? body.idempotencyKey : null,
      errorCode: String(body && body.code ? body.code : `http_${httpStatus}`),
      summary: sanitizeErrorSummary(body && body.error ? body.error : body && body.message),
    };
  }

  if (httpStatus === 429 || (httpStatus >= 500 && httpStatus < 600)) {
    return {
      status: RESULT_STATUS.retryableFailure,
      retryable: true,
      errorCode: String(body && body.code ? body.code : `http_${httpStatus}`),
      summary: sanitizeErrorSummary(body && body.error ? body.error : body && body.message),
    };
  }

  return {
    status: RESULT_STATUS.retryableFailure,
    retryable: true,
    errorCode: `http_${httpStatus}`,
    summary: sanitizeErrorSummary(body && body.error ? body.error : body && body.message),
  };
}

function createTimeLeftLifeEventClient(configInput = {}, options = {}) {
  const config = requireValidTimeLeftLifeEventConfig(configInput);
  const fetchImpl = options.fetch || globalThis.fetch;
  const logger = options.logger || null;
  const safeMode = config.mode;

  async function sendLifeEvent(lifeEvent) {
    if (safeMode === "off") {
      return {
        status: RESULT_STATUS.off,
        retryable: false,
        lifeEventId: null,
        idempotencyKey: null,
        summary: "life event delivery is disabled",
      };
    }

    if (!lifeEvent || typeof lifeEvent !== "object") {
      return {
        status: RESULT_STATUS.retryableFailure,
        retryable: true,
        errorCode: "invalid_life_event",
        summary: "lifeEvent must be an object",
      };
    }

    if (logger && typeof logger.debug === "function") {
      logger.debug("TimeLeft LifeEvent client request prepared", {
        mode: safeMode,
        targetProjectId: config.targetProjectId,
        endpointUrl: config.endpointUrl,
      });
    }

    const body = {
      ...lifeEvent,
      calendarId: config.calendarId,
      connectionId: config.connectionId,
      integrationId: config.integrationId,
    };

    try {
      const response = await fetchWithTimeout(
        fetchImpl,
        config.endpointUrl,
        {
          method: "POST",
          headers: normalizeHeaders({
            "Content-Type": "application/json",
            Authorization: `Bearer ${config.bearerToken}`,
          }),
          body: JSON.stringify(body),
        },
        config.timeoutMs
      );

      const parsed = await safeParseJson(response);
      if (parsed && parsed.__invalidJson) {
        return {
          status: RESULT_STATUS.retryableFailure,
          retryable: true,
          errorCode: "invalid_json",
          summary: "Could not parse TimeLeft response JSON.",
        };
      }
      return classifyResponse(response.status, parsed);
    } catch (error) {
      if (isTimeoutError(error)) {
        return {
          status: RESULT_STATUS.retryableFailure,
          retryable: true,
          errorCode: "timeout",
          summary: "Timed out waiting for TimeLeft LifeEvent endpoint.",
        };
      }
      if (isNetworkError(error)) {
        return {
          status: RESULT_STATUS.retryableFailure,
          retryable: true,
          errorCode: "network_error",
          summary: sanitizeErrorSummary(error),
        };
      }

      if (logger && typeof logger.error === "function") {
        logger.error("TimeLeft LifeEvent client failed", {
          status: error && error.status ? error.status : null,
          summary: sanitizeErrorSummary(error),
          code: error && error.code ? error.code : null,
        });
      }
      return {
        status: RESULT_STATUS.retryableFailure,
        retryable: true,
        errorCode: error && error.code ? error.code : "delivery_error",
        summary: sanitizeErrorSummary(error),
      };
    }
  }

  return {
    sendLifeEvent,
    config,
  };
}

module.exports = {
  RESULT_STATUS,
  classifyResponse,
  createTimeLeftLifeEventClient,
  fetchWithTimeout,
  sanitizeErrorSummary,
};
