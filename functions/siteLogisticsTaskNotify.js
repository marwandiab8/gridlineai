// Texts labourers about tasks just added in Site Logistics (Tasks). Site Logistics works out who gets what (each
// labourer's new tasks, with the link to their Tasks page) and posts the finished texts here; this endpoint only
// sends them, from gridlineai's Twilio number, and logs each one in `messages`.
//
// Trust: as for siteLogisticsLabourHours, the request must carry a Google-signed ID token for Site Logistics'
// Cloud Functions service account, with this endpoint as its audience.
const { verifySiteLogisticsCaller } = require("./siteLogisticsLabour");
const { normalizePhoneE164 } = require("./twilioSecrets");

const MAX_RECIPIENTS = 100;
const MAX_BODY = 640;
const E164 = /^\+\d{8,15}$/;

/** The texts to send, [{ phoneE164, label, body }] with one per phone (the first wins), or { error }. */
function parseTaskNotifyBody(body) {
  const list = body && Array.isArray(body.recipients) ? body.recipients : null;
  if (!list) return { error: "Missing recipients." };
  if (list.length > MAX_RECIPIENTS) return { error: `At most ${MAX_RECIPIENTS} texts at a time.` };
  const seen = new Set();
  const recipients = [];
  for (const r of list) {
    const phoneE164 = normalizePhoneE164(String((r && r.phoneE164) || ""));
    const text = String((r && r.body) || "").trim().slice(0, MAX_BODY);
    if (!E164.test(phoneE164) || !text || seen.has(phoneE164)) continue;
    seen.add(phoneE164);
    recipients.push({ phoneE164, label: String((r && r.label) || "").trim().slice(0, 120) || phoneE164, body: text });
  }
  return { recipients };
}

/** Sends each text; a text that went out counts as sent even if logging it fails. Returns { sent, failed }. */
async function sendTaskTexts({ db, FieldValue, smsClient, fromPhone, recipients, site, messageSchemaVersion = null, logger = console }) {
  const sent = [];
  const failed = [];
  for (const r of recipients) {
    let tw;
    try {
      tw = await smsClient.messages.create({ to: r.phoneE164, from: fromPhone, body: r.body });
    } catch (error) {
      failed.push({ phoneE164: r.phoneE164, label: r.label, error: String((error && error.message) || error || "send_failed").slice(0, 180) });
      continue;
    }
    sent.push({ phoneE164: r.phoneE164, label: r.label, sid: tw.sid || null });
    try {
      await db.collection("messages").add({
        direction: "outbound",
        from: fromPhone,
        to: r.phoneE164,
        body: r.body,
        messageSid: tw.sid || null,
        delivery: "twilio_api_notification",
        threadKey: r.phoneE164,
        phoneE164: r.phoneE164,
        channel: "sms",
        schemaVersion: messageSchemaVersion,
        command: "site_logistics_task_notify",
        siteLogistics: site,
        twilioMessageStatus: tw.status || null,
        twilioSenderPhoneE164: fromPhone,
        twilioDestinationPhoneE164: r.phoneE164,
        createdAt: FieldValue.serverTimestamp(),
      });
    } catch (error) {
      logger.warn("siteLogisticsTaskNotify: could not log a sent text", { message: error.message });
    }
  }
  return { sent, failed };
}

/**
 * HTTP handler. `getTwilio()` returns { smsClient, fromPhone } (or nulls when not configured). Body:
 * { siteId, siteName, recipients: [{ phoneE164, label, body }] }. Replies { ok, sent, failed }.
 */
function createSiteLogisticsTaskNotifyHandler({ db, FieldValue, logger = console, audience, getTwilio, verifyCaller = verifySiteLogisticsCaller, messageSchemaVersion = null }) {
  return async (req, res) => {
    res.set("Cache-Control", "no-store");
    if (req.method !== "POST") return res.status(405).set("Allow", "POST").json({ ok: false, code: "method" });
    const aud = typeof audience === "function" ? audience(req) : audience;
    if (!(await verifyCaller(req, aud))) {
      logger.warn("siteLogisticsTaskNotify: rejected caller");
      return res.status(401).json({ ok: false, code: "unauthorized", message: "Not allowed." });
    }
    const body = req.body || {};
    const { recipients, error } = parseTaskNotifyBody(body);
    if (error) return res.status(400).json({ ok: false, code: "bad_request", message: error });
    if (!recipients.length) return res.status(200).json({ ok: true, sent: 0, failed: 0 });
    try {
      const { smsClient, fromPhone } = getTwilio();
      if (!smsClient || !fromPhone) {
        logger.error("siteLogisticsTaskNotify: Twilio is not configured");
        return res.status(503).json({ ok: false, code: "not_configured", message: "gridlineai can't send texts right now." });
      }
      const site = { siteId: String(body.siteId || "").slice(0, 64), siteName: String(body.siteName || "").slice(0, 120) };
      const out = await sendTaskTexts({ db, FieldValue, smsClient, fromPhone, recipients, site, messageSchemaVersion, logger });
      logger.info("siteLogisticsTaskNotify", { siteId: site.siteId, sent: out.sent.length, failed: out.failed.length });
      return res.status(200).json({ ok: true, sent: out.sent.length, failed: out.failed.length });
    } catch (err) {
      logger.error("siteLogisticsTaskNotify: failed", { message: err.message });
      return res.status(500).json({ ok: false, code: "error", message: "gridlineai couldn't send the texts. Try again." });
    }
  };
}

module.exports = {
  createSiteLogisticsTaskNotifyHandler,
  parseTaskNotifyBody,
  sendTaskTexts,
};
