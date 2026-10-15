// Twilio waits about 15 s for the inbound SMS webhook to answer. Building a reply can take longer (the AI routes the
// message, then the journal entry is written), and that work carries on regardless. So instead of giving up after
// 12 s - which told people their message failed, made them resend it (duplicate entries) and left their photos
// unattached - the webhook answers Twilio with no message, keeps waiting for the real result, files it as usual
// and texts the reply when it's ready.

const REPLY_TIMEOUT_ERROR = "__BUILD_REPLY_TIMEOUT__";

const after = (ms, value) =>
  new Promise((resolve) => {
    const t = setTimeout(() => resolve(value), ms);
    if (t && typeof t.unref === "function") t.unref();
  });

/**
 * Waits for `work` (a promise of the reply). If it isn't done within `fastMs`, calls `onSlow()` once and keeps
 * waiting up to `slowMs` more. Resolves with the work's value; rejects with the work's own error, or with
 * Error(REPLY_TIMEOUT_ERROR) when it takes longer than both.
 */
async function awaitInboundReply(work, { fastMs, slowMs, onSlow = () => {} }) {
  const SLOW = Symbol("slow");
  const first = await Promise.race([work, after(fastMs, SLOW)]);
  if (first !== SLOW) return first;
  onSlow();
  const TOO_SLOW = Symbol("too slow");
  const second = await Promise.race([work, after(slowMs, TOO_SLOW)]);
  if (second === TOO_SLOW) throw new Error(REPLY_TIMEOUT_ERROR);
  return second;
}

module.exports = { REPLY_TIMEOUT_ERROR, awaitInboundReply };
