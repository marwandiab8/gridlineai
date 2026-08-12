function getDefaultClock() {
  return new Date();
}

function toSafeErrorSummary(error) {
  const raw = String(error && error.message ? error.message : error || "delivery failure");
  return raw.trim().slice(0, 500);
}

function isObjectLike(value) {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

function resolveConfigMode(client) {
  const mode = client && client.config && client.config.mode;
  return typeof mode === "string" && mode.trim() ? mode.trim() : "off";
}

function resolveConfigTargetProjectId(client) {
  const value = client && client.config && client.config.targetProjectId;
  const text = String(value == null ? "" : value).trim();
  return text || null;
}

function isTerminalStatus(status) {
  return (
    status === "delivered" ||
    status === "duplicate" ||
    status === "off" ||
    status === "conflict" ||
    status === "unsupported_event_type" ||
    status === "permanent_failure" ||
    status === "authentication_failure"
  );
}

function normalizeResult(input = {}) {
  if (!isObjectLike(input)) return {};
  return {
    status: String(input.status || "off"),
    retryable: Boolean(input.retryable),
    lifeEventId: input.lifeEventId ? String(input.lifeEventId) : null,
    idempotencyKey: input.idempotencyKey ? String(input.idempotencyKey) : null,
    existingLifeEventId: input.existingLifeEventId
      ? String(input.existingLifeEventId)
      : null,
    errorCode: input.errorCode ? String(input.errorCode) : null,
    summary: input.summary ? String(input.summary) : null,
  };
}

function isRecoverableFailure(status) {
  return status === "retryable_failure" || !isTerminalStatus(status);
}

function createTimeLeftLifeEventDelivery({
  mapper,
  client,
  repository,
  logger,
  clock = getDefaultClock,
}) {
  const mapShortcutEvent = typeof mapper === "function" ? mapper : null;
  const sendLifeEvent = client && typeof client.sendLifeEvent === "function" ? client.sendLifeEvent.bind(client) : null;
  const recordDeliveryState =
    repository && typeof repository.upsertTimeLeftDeliveryState === "function"
      ? repository.upsertTimeLeftDeliveryState
      : null;
  const safeMode = resolveConfigMode(client);
  const safeTargetProjectId = resolveConfigTargetProjectId(client);
  const safeClock = typeof clock === "function" ? clock : getDefaultClock;

  async function persist(eventId, state = {}) {
    if (!recordDeliveryState) return { ok: false, error: "repository_unavailable" };
    return recordDeliveryState(
      eventId,
      {
        mode: safeMode,
        targetProjectId: safeTargetProjectId,
        ...state,
      },
      { clock: safeClock }
    );
  }

  return async function deliverTimeLeftLifeEvent({ event, eventId }) {
    const resolvedId = String(eventId || "").trim();
    if (!resolvedId) {
      return {
        ok: false,
        status: "invalid_event_id",
        retryable: false,
        summary: "Missing eventId for TimeLeftToLive delivery.",
      };
    }

    if (!mapShortcutEvent || !sendLifeEvent) {
      const result = {
        ok: false,
        status: "delivery_not_configured",
        retryable: false,
        summary: "TimeLeftToLive delivery dependencies are missing.",
      };
      await persist(resolvedId, {
        status: result.status,
        retryable: result.retryable,
        lastErrorCode: "delivery_not_configured",
        lastErrorSummary: result.summary,
      });
      return result;
    }

    let mapped;
    try {
      mapped = mapShortcutEvent({ ...event, id: resolvedId });
    } catch (error) {
      const mappedError = {
        ok: false,
        status: "mapping_exception",
        retryable: false,
        summary: toSafeErrorSummary(error),
      };
      await persist(resolvedId, {
        status: mappedError.status,
        retryable: mappedError.retryable,
        lastErrorCode: "mapping_error",
        lastErrorSummary: mappedError.summary,
      });
      return mappedError;
    }

    if (!isObjectLike(mapped) || mapped.ok !== true) {
      const summary = mapped && mapped.status ? `${mapped.status}: ${mapped.reason || "unsupported event"}` : "Unable to map event";
      const mappedError = {
        ok: false,
        status: mapped && mapped.status ? mapped.status : "unsupported_event",
        retryable: false,
        summary,
        sourceEventType: mapped && mapped.sourceEventType ? mapped.sourceEventType : event && event.eventType ? event.eventType : null,
      };
      await persist(resolvedId, {
        status: mappedError.status,
        retryable: mappedError.retryable,
        lastErrorCode: "unsupported_event",
        lastErrorSummary: mappedError.summary,
      });
      return mappedError;
    }

    try {
      const outcome = normalizeResult(await sendLifeEvent(mapped.event));
      const payload = {
        status: outcome.status,
        lifeEventId: outcome.lifeEventId,
        idempotencyKey: outcome.idempotencyKey,
        retryable: outcome.retryable,
        lastErrorCode: outcome.errorCode,
        lastErrorSummary: outcome.summary,
      };
      const persistence = await persist(resolvedId, payload);
      return {
        ok: !isRecoverableFailure(outcome.status),
        status: outcome.status,
        retryable: outcome.retryable,
        lifeEventId: outcome.lifeEventId,
        existingLifeEventId: outcome.existingLifeEventId,
        idempotencyKey: outcome.idempotencyKey,
        lastErrorCode: outcome.errorCode,
        lastErrorSummary: outcome.summary,
        persistence,
      };
    } catch (error) {
      const failure = {
        ok: false,
        status: "delivery_error",
        retryable: true,
        summary: toSafeErrorSummary(error),
        lastErrorCode: "delivery_error",
        lastErrorSummary: toSafeErrorSummary(error),
      };
      const persistence = await persist(resolvedId, {
        status: failure.status,
        retryable: failure.retryable,
        lastErrorCode: failure.lastErrorCode,
        lastErrorSummary: failure.lastErrorSummary,
      });
      return { ...failure, persistence };
    }
  };
}

module.exports = {
  createTimeLeftLifeEventDelivery,
  getDefaultClock,
  isRecoverableFailure,
  normalizeResult,
  toSafeErrorSummary,
};
