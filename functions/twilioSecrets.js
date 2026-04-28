/**
 * Normalize Twilio credentials from Firebase secrets / env (quotes, CRLF, zero-width).
 * Masked diagnostics for logs (never log full tokens).
 */

/** Zero-width and common invisible chars */
const INVISIBLE_RE = /[\u200B-\u200D\uFEFF\u00A0]/g;

/**
 * @param {string|null|undefined} raw
 * @returns {string}
 */
function normalizeTwilioSecret(raw) {
  if (raw == null) return "";
  let s = String(raw);
  s = s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  s = s.replace(INVISIBLE_RE, "");
  s = s.trim();
  if (s.length >= 2) {
    const q0 = s[0];
    const q1 = s[s.length - 1];
    if (
      (q0 === '"' && q1 === '"') ||
      (q0 === "'" && q1 === "'") ||
      (q0 === "\u201c" && q1 === "\u201d")
    ) {
      s = s.slice(1, -1).trim();
    }
  }
  return s;
}

function normalizeAccountSid(raw) {
  return normalizeTwilioSecret(raw);
}

function normalizeAuthToken(raw) {
  return normalizeTwilioSecret(raw);
}

function normalizePhoneE164(raw) {
  return normalizeTwilioSecret(raw);
}

const TWILIO_ACCOUNT_SID_RE = /^AC[0-9a-f]{32}$/i;

/**
 * Account SID in Twilio REST URLs: /Accounts/AC…/
 * @param {string} mediaUrl
 * @returns {string|null}
 */
function extractAccountSidFromTwilioMediaUrl(mediaUrl) {
  const m = String(mediaUrl || "").match(/\/Accounts\/(AC[0-9a-f]{32})\//i);
  return m ? m[1] : null;
}

/**
 * @param {string} sid
 */
function maskSidParts(sid) {
  const s = String(sid || "");
  if (!s.length) {
    return { sidLength: 0, sidStart: "", sidEnd: "", sidLooksValid: false };
  }
  return {
    sidLength: s.length,
    sidStart: s.length >= 4 ? s.slice(0, 4) : s,
    sidEnd: s.length > 4 ? s.slice(-4) : "",
    sidLooksValid: TWILIO_ACCOUNT_SID_RE.test(s),
  };
}

/**
 * @param {string} token
 */
function maskTokenParts(token) {
  const t = String(token || "");
  return { tokenLength: t.length };
}

/**
 * Safe subset for Firestore on failed fetch (no full secrets).
 * @param {string} sid
 */
function sidMaskForDoc(sid) {
  const m = maskSidParts(sid);
  return {
    sidLength: m.sidLength,
    sidStart: m.sidStart,
    sidEnd: m.sidEnd,
    sidLooksValid: m.sidLooksValid,
  };
}

/**
 * One line per inbound request — proves what runtime saw after normalization.
 * @param {import('firebase-functions/logger')} logger
 * @param {string} runId
 * @param {{ accountSid: string, authToken: string, configuredFrom: string, sampleMediaUrl?: string }} normalized
 */
function logInboundTwilioSecrets(logger, runId, normalized) {
  let firebaseProjectId = null;
  try {
    if (process.env.FIREBASE_CONFIG) {
      firebaseProjectId =
        JSON.parse(process.env.FIREBASE_CONFIG).projectId || null;
    }
  } catch (_) {
    /* ignore */
  }

  const sid = maskSidParts(normalized.accountSid);
  const tok = maskTokenParts(normalized.authToken);
  const sampleUrl = normalized.sampleMediaUrl || "";
  const urlSid = extractAccountSidFromTwilioMediaUrl(sampleUrl);
  logger.info("inboundSms: twilio credentials (masked)", {
    runId,
    gcpProjectId: process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || null,
    firebaseProjectId,
    ...sid,
    ...tok,
    configuredPhoneLength: (normalized.configuredFrom || "").length,
    sampleUrlSidFound: Boolean(urlSid),
    sampleUrlSidMatchesConfigured:
      urlSid && sid.sidLooksValid
        ? urlSid === normalized.accountSid
        : null,
  });
}

module.exports = {
  normalizeTwilioSecret,
  normalizeAccountSid,
  normalizeAuthToken,
  normalizePhoneE164,
  extractAccountSidFromTwilioMediaUrl,
  maskSidParts,
  maskTokenParts,
  sidMaskForDoc,
  logInboundTwilioSecrets,
  TWILIO_ACCOUNT_SID_RE,
};
