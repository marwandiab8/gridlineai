/**
 * Google Cloud Speech-to-Text for formats OpenAI often rejects (e.g. MMS AMR from Android).
 * Uses Application Default Credentials (Firebase Functions service account).
 * Enable API: Cloud Console → APIs & Services → Speech-to-Text API.
 */

const { SpeechClient } = require("@google-cloud/speech");

/**
 * @param {Buffer} buffer Raw AMR audio bytes
 * @param {{ logger?: object, runId?: string }} [ctx]
 * @returns {Promise<string|null>} Transcript text or null
 */
async function transcribeAmrBuffer(buffer, ctx = {}) {
  const logger = ctx.logger || console;
  const runId = ctx.runId || "";
  if (!buffer || !buffer.length) return null;

  const client = new SpeechClient();
  const content = buffer.toString("base64");
  const head = buffer.slice(0, 16).toString("ascii");
  const preferWb = /#!AMR-WB/i.test(head);

  const attempts = preferWb
    ? [
        { encoding: "AMR_WB", sampleRateHertz: 16000, label: "amr-wb-16k" },
        { encoding: "AMR", sampleRateHertz: 8000, label: "amr-nb-8k" },
      ]
    : [
        { encoding: "AMR", sampleRateHertz: 8000, label: "amr-nb-8k" },
        { encoding: "AMR_WB", sampleRateHertz: 16000, label: "amr-wb-16k" },
      ];

  let lastErr = null;
  for (const a of attempts) {
    try {
      const [response] = await client.recognize({
        config: {
          encoding: a.encoding,
          sampleRateHertz: a.sampleRateHertz,
          languageCode: "en-US",
          enableAutomaticPunctuation: true,
        },
        audio: { content },
      });
      const parts = (response.results || [])
        .map((r) => (r.alternatives && r.alternatives[0] && r.alternatives[0].transcript) || "")
        .filter(Boolean);
      const text = parts.join(" ").replace(/\s+/g, " ").trim();
      if (text) {
        logger.info("googleSpeechTranscribe: transcript ok", {
          runId,
          label: a.label,
          chars: text.length,
        });
        return text;
      }
      logger.warn("googleSpeechTranscribe: empty transcript", { runId, label: a.label });
    } catch (err) {
      lastErr = err;
      logger.warn("googleSpeechTranscribe: recognize failed", {
        runId,
        label: a.label,
        message: err.message,
        code: err.code,
      });
    }
  }
  if (lastErr) {
    logger.warn("googleSpeechTranscribe: all AMR attempts failed", {
      runId,
      message: lastErr.message,
    });
  }
  return null;
}

module.exports = { transcribeAmrBuffer };
