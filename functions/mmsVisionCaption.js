/**
 * Optional vision caption for the first photo on an issue — never throws to caller.
 * Uses the same primary model as the assistant (aiConfig + openaiHelpers).
 */

const OpenAI = require("openai");
const { FieldValue, Timestamp } = require("firebase-admin/firestore");
const {
  chatCompletionWithFallback,
  completionText,
} = require("./openaiHelpers");

/**
 * @param {{ db: import('firebase-admin').firestore.Firestore, issueCollection: string, issueId: string, openaiApiKey: string, modelsOverride?: { primary?: string }, logger: object, runId: string }} opts
 */
async function maybeCaptionFirstMmsPhoto(opts) {
  const {
    db,
    issueCollection,
    issueId,
    openaiApiKey,
    modelsOverride,
    logger,
    runId,
  } = opts;
  if (!openaiApiKey) return;

  const ref = db.collection(issueCollection).doc(issueId);
  const snap = await ref.get();
  if (!snap.exists) return;
  const data = snap.data() || {};
  if (data.aiImageCaption) return;

  const photos = Array.isArray(data.photos) ? data.photos : [];
  const first = photos[0];
  if (!first || !first.downloadURL) return;

  try {
    const client = new OpenAI({ apiKey: openaiApiKey });
    const completion = await chatCompletionWithFallback(
      client,
      {
        messages: [
          {
            role: "user",
            content: [
              {
                type: "text",
                text: `Construction site photo. Issue context: "${String(data.title || data.description || "").slice(0, 400)}". Reply with one short field observation (max 120 characters).`,
              },
              { type: "image_url", image_url: { url: first.downloadURL } },
            ],
          },
        ],
        max_completion_tokens: 120,
      },
      logger,
      runId,
      modelsOverride
    );
    const cap = completionText(completion).trim().slice(0, 200);
    if (!cap) return;

    const prevHist = Array.isArray(data.history) ? data.history : [];
    prevHist.push({
      at: Timestamp.now(),
      action: "ai_image_caption",
      changedBy: "system",
      field: "aiImageCaption",
      oldValue: null,
      newValue: cap,
      note: "Optional AI caption for first MMS image",
    });

    await ref.update({
      aiImageCaption: cap,
      history: prevHist,
      updatedAt: FieldValue.serverTimestamp(),
    });
    logger.info("mmsVisionCaption: captioned", { runId, issueId });
  } catch (e) {
    logger.warn("mmsVisionCaption: skipped", { runId, message: e.message });
  }
}

module.exports = { maybeCaptionFirstMmsPhoto };
