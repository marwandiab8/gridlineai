const { getModels, sanitizeChatCompletionParams } = require("./aiConfig");

function completionText(completion) {
  const msg = completion?.choices?.[0]?.message;
  if (!msg) return "";
  const c = msg.content;
  if (typeof c === "string") return c;
  if (Array.isArray(c)) {
    return c
      .map((part) => (part && part.type === "text" ? part.text || "" : ""))
      .join("");
  }
  return "";
}

async function chatCompletionWithFallback(
  client,
  params,
  logger,
  runId,
  modelsOverride
) {
  const models = getModels(modelsOverride);
  const cap = params.max_completion_tokens ?? params.max_tokens ?? 500;
  const primary = {
    ...params,
    model: models.primary,
    max_completion_tokens: cap,
  };
  delete primary.max_tokens;
  sanitizeChatCompletionParams(primary, models.primary);
  return await client.chat.completions.create(primary);
}

module.exports = {
  completionText,
  chatCompletionWithFallback,
};
