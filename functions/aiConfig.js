/**
 * Central OpenAI model configuration (override via Firebase params / env).
 */

const DEFAULT_MODELS = {
  /** GPT-5.2 family — single model; no alternate fallback model. */
  primary: "gpt-5.2-chat-latest",
};

/**
 * @param {{ primary?: string } | null | undefined} override
 */
function getModels(override) {
  if (!override || typeof override !== "object") {
    return { ...DEFAULT_MODELS };
  }
  return {
    primary: override.primary || DEFAULT_MODELS.primary,
  };
}

/**
 * GPT-5 family models reject `temperature` / `top_p` on Chat Completions (400 unsupported_parameter).
 * Mutates `params` in place.
 * @param {Record<string, unknown>} params
 * @param {string} modelId
 */
function sanitizeChatCompletionParams(params, modelId) {
  if (!modelId || typeof modelId !== "string" || !/^gpt-5/i.test(modelId)) {
    return params;
  }
  delete params.temperature;
  delete params.top_p;
  return params;
}

module.exports = {
  DEFAULT_MODELS,
  getModels,
  sanitizeChatCompletionParams,
};
