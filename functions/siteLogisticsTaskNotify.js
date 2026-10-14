// Texts labourers about tasks just added in Site Logistics (Tasks). Site Logistics works out who gets what (each
// labourer's new tasks, with the link to their Tasks page) and posts the finished texts here; this endpoint only
// sends them, from gridlineai's Twilio number, and logs each one in `messages`.
//
// A text is addressed to a phone number, or - for a labourer Site Logistics has no number for - to their Site
// Logistics sign-in, which is sent to the phone of the labourer it's linked to here (Labour -> Labourer manager,
// the same link their hours use).
//
// Trust: as for siteLogisticsLabourHours, the request must carry a Google-signed ID token for Site Logistics'
// Cloud Functions service account, with this endpoint as its audience.
const { normalizeSiteLogisticsId, verifySiteLogisticsCaller } = require("./siteLogisticsLabour");
const { normalizePhoneE164 } = require("./twilioSecrets");

const MAX_RECIPIENTS = 100;
const MAX_BODY = 640;
const E164 = /^\+\d{8,15}$/;

const signInOf = (value) => {
  try {
    return normalizeSiteLogisticsId(value);
  } catch (_) {
    return "";
  }
};

/**
 * The texts to send: { recipients: [{ phoneE164, label, body }], bySignIn: [{ siteLogisticsId, label, body }] }
 * (texts for a phone number, and texts for a Site Logistics sign-in still to be matched to a phone), or { error }.
 */
function parseTaskNotifyBody(body) {
  const list = body && Array.isArray(body.recipients) ? body.recipients : null;
  if (!list) return { error: "Missing recipients." };
  if (list.length > MAX_RECIPIENTS) return { error: `At most ${MAX_RECIPIENTS} texts at a time.` };
  const seenPhones = new Set();
  const seenSignIns = new Set();
  const recipients = [];
  const bySignIn = [];
  for (const r of list) {
    const text = String((r && r.body) || "").trim().slice(0, MAX_BODY);
    if (!text) continue;
    const label = String((r && r.label) || "").trim().slice(0, 120);
    const phoneE164 = normalizePhoneE164(String((r && r.phoneE164) || ""));
    if (E164.test(phoneE164)) {
      if (seenPhones.has(phoneE164)) continue;
      seenPhones.add(phoneE164);
      recipients.push({ phoneE164, label: label || phoneE164, body: text });
      continue;
    }
    const siteLogisticsId = signInOf(r && r.siteLogisticsId);
    if (!siteLogisticsId || seenSignIns.has(siteLogisticsId)) continue;
    seenSignIns.add(siteLogisticsId);
    bySignIn.push({ siteLogisticsId, label: label || siteLogisticsId, body: text });
  }
  return { recipients, bySignIn };
}

/**
 * The phone of the labourer each Site Logistics sign-in is linked to (an active labourer with that sign-in in
 * siteLogisticsIds; exactly one). { recipients: [{ phoneE164, label, body }], unlinked: number of sign-ins with none }.
 */
async function phonesForSignIns(db, bySignIn) {
  const recipients = [];
  let unlinked = 0;
  for (const r of bySignIn) {
    const snap = await db.collection("labourers").where("siteLogisticsIds", "array-contains", r.siteLogisticsId).get();
    const active = snap.docs.filter((d) => d.get("active") !== false);
    const phoneE164 = active.length === 1 ? normalizePhoneE164(String(active[0].get("phoneE164") || active[0].id)) : "";
    if (!E164.test(phoneE164)) {
      unlinked += 1;
      continue;
    }
    const name = String(active[0].get("displayName") || active[0].get("name") || "").trim();
    recipients.push({ phoneE164, label: name || r.label, body: r.body });
  }
  return { recipients, unlinked };
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
 * { siteId, siteName, recipients: [{ phoneE164 | siteLogisticsId, label, body }] }. Replies { ok, sent, failed,
 * unlinked } (unlinked: sign-ins not linked to a labourer with a phone, so not texted).
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
    const { recipients: direct, bySignIn, error } = parseTaskNotifyBody(body);
    if (error) return res.status(400).json({ ok: false, code: "bad_request", message: error });
    try {
      const linked = await phonesForSignIns(db, bySignIn);
      // One text per phone, a number Site Logistics sent winning over the same number found by sign-in.
      const recipients = [...direct, ...linked.recipients.filter((r) => !direct.some((d) => d.phoneE164 === r.phoneE164))];
      if (!recipients.length) return res.status(200).json({ ok: true, sent: 0, failed: 0, unlinked: linked.unlinked });
      const { smsClient, fromPhone } = getTwilio();
      if (!smsClient || !fromPhone) {
        logger.error("siteLogisticsTaskNotify: Twilio is not configured");
        return res.status(503).json({ ok: false, code: "not_configured", message: "gridlineai can't send texts right now." });
      }
      const site = { siteId: String(body.siteId || "").slice(0, 64), siteName: String(body.siteName || "").slice(0, 120) };
      const out = await sendTaskTexts({ db, FieldValue, smsClient, fromPhone, recipients, site, messageSchemaVersion, logger });
      logger.info("siteLogisticsTaskNotify", { siteId: site.siteId, sent: out.sent.length, failed: out.failed.length, bySignIn: bySignIn.length, unlinked: linked.unlinked });
      return res.status(200).json({ ok: true, sent: out.sent.length, failed: out.failed.length, unlinked: linked.unlinked });
    } catch (err) {
      logger.error("siteLogisticsTaskNotify: failed", { message: err.message });
      return res.status(500).json({ ok: false, code: "error", message: "gridlineai couldn't send the texts. Try again." });
    }
  };
}

module.exports = {
  createSiteLogisticsTaskNotifyHandler,
  parseTaskNotifyBody,
  phonesForSignIns,
  sendTaskTexts,
};
