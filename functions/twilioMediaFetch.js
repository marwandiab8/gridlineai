/**
 * Download Twilio-hosted MMS URLs (Basic auth) + content-type → extension.
 * Prefers Account SID embedded in the media URL (authoritative for that resource).
 */

const http = require("http");
const https = require("https");
const { URL } = require("url");
const {
  normalizeAccountSid,
  normalizeAuthToken,
  extractAccountSidFromTwilioMediaUrl,
  TWILIO_ACCOUNT_SID_RE,
} = require("./twilioSecrets");

const MAX_REDIRECTS = 5;

function guessExtension(contentType) {
  const c = (contentType || "").toLowerCase();
  if (c.includes("jpeg") || c.includes("jpg")) return "jpg";
  if (c.includes("png")) return "png";
  if (c.includes("gif")) return "gif";
  if (c.includes("webp")) return "webp";
  if (c.includes("heic")) return "heic";
  return "bin";
}

class TwilioMediaFetchError extends Error {
  /**
   * @param {string} message
   * @param {Record<string, unknown>} [diagnostics]
   */
  constructor(message, diagnostics = {}) {
    super(message);
    this.name = "TwilioMediaFetchError";
    this.diagnostics = diagnostics;
  }
}

/**
 * Resolve which Account SID to use for Basic auth (username).
 * Prefer SID from media URL when valid; else configured (if valid).
 */
function resolveAuthUsername(configuredSid, mediaUrl) {
  const cfg = normalizeAccountSid(configuredSid);
  const urlSid = extractAccountSidFromTwilioMediaUrl(mediaUrl);

  const urlOk = Boolean(urlSid && TWILIO_ACCOUNT_SID_RE.test(urlSid));
  const cfgOk = Boolean(cfg && TWILIO_ACCOUNT_SID_RE.test(cfg));

  if (urlOk) {
    return {
      chosenSid: urlSid,
      sidSource: "mediaUrl",
      urlSid,
      configuredSid: cfg || null,
      configuredLooksValid: cfgOk,
      urlConfiguredMismatch: cfgOk && urlSid !== cfg,
    };
  }
  if (cfgOk) {
    return {
      chosenSid: cfg,
      sidSource: "configured",
      urlSid: urlSid || null,
      configuredSid: cfg,
      configuredLooksValid: true,
      urlConfiguredMismatch: false,
    };
  }
  return {
    chosenSid: null,
    sidSource: "none",
    urlSid: urlSid || null,
    configuredSid: cfg || null,
    configuredLooksValid: cfgOk,
    urlConfiguredMismatch: false,
  };
}

/**
 * @param {string} currentUrl
 * @param {Record<string, string>} headers
 * @param {{ logger: object, runId: string }} ctx
 */
function httpsGetOnce(currentUrl, headers, ctx) {
  const { logger, runId } = ctx;
  const u = new URL(currentUrl);
  const lib = u.protocol === "https:" ? https : http;
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: u.hostname,
      port: u.port || (u.protocol === "https:" ? 443 : 80),
      path: u.pathname + u.search,
      method: "GET",
      headers: { ...headers },
    };
    const req = lib.request(opts, (res) => {
      const code = res.statusCode || 0;
      const ct = (res.headers["content-type"] || "").split(";")[0].trim();
      const loc = res.headers.location;
      logger.info("twilioMediaFetch: HTTP response", {
        runId,
        statusCode: code,
        contentType: ct || null,
        host: u.hostname,
        hasLocation: Boolean(loc),
      });
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        resolve({
          statusCode: code,
          contentType: ct,
          location: loc || null,
          buffer: Buffer.concat(chunks),
        });
      });
    });
    req.on("error", (err) => {
      logger.warn("twilioMediaFetch: request error", {
        runId,
        message: err.message,
        code: err.code,
      });
      reject(
        new TwilioMediaFetchError(`network error: ${err.message}`, {
          httpReached: false,
          networkError: true,
          errno: err.code || null,
        })
      );
    });
    req.end();
  });
}

/**
 * GET with Basic auth; follow 301/302/303/307/308 up to MAX_REDIRECTS.
 */
async function getBinaryWithRedirects(
  startUrl,
  authHeader,
  ctx
) {
  let url = startUrl;
  const chain = [];
  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    const res = await httpsGetOnce(
      url,
      {
        Authorization: authHeader,
        Accept: "*/*",
        "User-Agent": "gridline-inbound-sms/1.1 (Twilio media fetch)",
      },
      ctx
    );
    chain.push({
      statusCode: res.statusCode,
      contentType: res.contentType || null,
    });

    const code = res.statusCode;
    if (code >= 300 && code < 400 && res.location) {
      if (hop === MAX_REDIRECTS) {
        throw new TwilioMediaFetchError(
          `too many redirects (max ${MAX_REDIRECTS})`,
          {
            httpReached: true,
            redirectLoop: true,
            chain,
            lastStatusCode: code,
          }
        );
      }
      url = new URL(res.location, url).href;
      continue;
    }

    if (code >= 400) {
      let msg = `Media HTTP ${code}`;
      if (code === 401) {
        msg +=
          ": Twilio rejected Basic auth — Auth Token must match the Account SID used as username (check secret TWILIO_AUTH_TOKEN for that account).";
      } else if (code === 404) {
        msg += ": media not found (expired URL or wrong message).";
      }
      throw new TwilioMediaFetchError(msg, {
        httpReached: true,
        statusCode: code,
        chain,
        contentType: res.contentType || null,
      });
    }

    if (!res.buffer || !res.buffer.length) {
      throw new TwilioMediaFetchError(
        "empty response body from Twilio media URL after successful HTTP status",
        {
          httpReached: true,
          statusCode: code,
          chain,
          emptyBody: true,
          contentType: res.contentType || null,
        }
      );
    }

    return {
      buffer: res.buffer,
      statusCode: code,
      contentType: res.contentType || null,
      chain,
    };
  }
  throw new TwilioMediaFetchError("redirect handling failed", {
    httpReached: true,
    redirectLoop: true,
    chain,
  });
}

/**
 * @param {string} mediaUrl
 * @param {string} configuredAccountSid
 * @param {string} authToken
 * @param {{ logger?: object, runId?: string }} [opts]
 * @returns {Promise<Buffer>}
 */
async function fetchTwilioMediaBuffer(
  mediaUrl,
  configuredAccountSid,
  authToken,
  opts = {}
) {
  const logger = opts.logger || console;
  const runId = opts.runId || "";

  const token = normalizeAuthToken(authToken);
  const resolution = resolveAuthUsername(configuredAccountSid, mediaUrl);

  if (resolution.urlConfiguredMismatch) {
    logger.warn(
      "twilioMediaFetch: configured TWILIO_ACCOUNT_SID differs from media URL Account SID — using media URL SID for download",
      {
        runId,
        urlSidStart: resolution.urlSid ? resolution.urlSid.slice(0, 4) : null,
        cfgSidStart: resolution.configuredSid
          ? resolution.configuredSid.slice(0, 4)
          : null,
      }
    );
  }

  if (!resolution.chosenSid) {
    throw new TwilioMediaFetchError(
      "no valid Twilio Account SID: media URL had no parseable AC… SID and TWILIO_ACCOUNT_SID secret is missing or not 34 chars (AC + 32 hex).",
      {
        sidSource: "none",
        configuredLooksValid: resolution.configuredLooksValid,
        urlSidFound: Boolean(resolution.urlSid),
        invalidFormat: true,
      }
    );
  }

  if (!token.length) {
    throw new TwilioMediaFetchError(
      "missing TWILIO_AUTH_TOKEN (empty after normalization)",
      {
        sidSource: resolution.sidSource,
        missingToken: true,
      }
    );
  }

  const auth = Buffer.from(
    `${resolution.chosenSid}:${token}`,
    "utf8"
  ).toString("base64");
  const authHeader = `Basic ${auth}`;

  logger.info("twilioMediaFetch: using Basic auth username", {
    runId,
    sidSource: resolution.sidSource,
    chosenSidStart: resolution.chosenSid.slice(0, 4),
    chosenSidEnd: resolution.chosenSid.slice(-4),
  });

  try {
    const out = await getBinaryWithRedirects(
      mediaUrl,
      authHeader,
      { logger, runId }
    );
    return out.buffer;
  } catch (e) {
    if (e instanceof TwilioMediaFetchError) {
      throw new TwilioMediaFetchError(e.message, {
        ...e.diagnostics,
        sidSource: resolution.sidSource,
        chosenSidStart: resolution.chosenSid.slice(0, 4),
        chosenSidEnd: resolution.chosenSid.slice(-4),
        urlSidFound: Boolean(resolution.urlSid),
        configuredSidValid: resolution.configuredLooksValid,
      });
    }
    throw e;
  }
}

/** Max MMS attachments Twilio sends per message (documented limit). */
const TWILIO_MAX_MEDIA = 10;

/**
 * Effective media count: max(NumMedia, highest MediaUrlN present).
 */
function countTwilioMediaParams(params) {
  const p = params || {};
  const declared = parseInt(String(p.NumMedia || "0"), 10);
  const nDeclared = Number.isFinite(declared) && declared >= 0 ? declared : 0;
  let maxUrlIndex = -1;
  for (let i = 0; i < TWILIO_MAX_MEDIA; i++) {
    const u = p[`MediaUrl${i}`];
    if (u != null && String(u).trim()) maxUrlIndex = i;
  }
  const fromUrls = maxUrlIndex + 1;
  return Math.min(TWILIO_MAX_MEDIA, Math.max(nDeclared, fromUrls, 0));
}

/** @deprecated use extractAccountSidFromTwilioMediaUrl from twilioSecrets */
function accountSidFromMediaUrl(url) {
  return extractAccountSidFromTwilioMediaUrl(url);
}

module.exports = {
  guessExtension,
  fetchTwilioMediaBuffer,
  countTwilioMediaParams,
  TWILIO_MAX_MEDIA,
  accountSidFromMediaUrl,
  TwilioMediaFetchError,
};
