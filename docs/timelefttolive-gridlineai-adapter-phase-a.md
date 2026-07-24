# TimeLeftToLive GridlineAI Adapter — Phase A

Verified repository: `/home/marwan/Documents/ChatBot`

Verified Firebase project mapping:

- `.firebaserc` default project: `gridlineai`

## 1) Scope completed in this phase

Phase A implements a pure mapper + delivery client + validation for GridlineAI
`iosShortcutEvents` to TimeLeftToLive LifeEvent v1, plus focused tests.

It does **not**:

- connect the durable adapter into current `iosShortcutsEvents` write path
- change existing `logEntries` behavior
- change `activitySessions`
- implement delivery-state records
- add retries/schedulers
- deploy either repository
- hit live staging endpoints

## 2) Mapper contract

Module: `functions/timeLeftLifeEventMapper.js`

Export: `mapShortcutEventToTimeLeftLifeEvent(eventDoc)`

Pure, side-effect free function:

- Accepts persisted/normalized Shortcut event with stable document identifier (`id` or `sourceRecordId`)
- Returns:
  - `{ ok: true, event: { ...LifeEvent fields... } }` on supported events
  - `{ ok: false, status: "unsupported_event_type" | ... }` on unsupported payloads
- Output is deterministic and does not mutate input

Required LifeEvent fields:

- `schemaVersion: 1`
- `sourceApp: "gridlineai"`
- `sourceFirebaseProjectId: "gridlineai"`
- `sourceRecordId`
- `eventType`
- `eventClass`
- `activityFamily`
- `categoryId`
- `title`
- `occurredAt`
- `timezone`
- `privacyLevel: "ownerOnly"`

Optional fields included when available:

- `sourceEventId`
- `sourceProjectId` (uses `sourceProjectId` or `projectSlug`)
- `location` (preserves label and includes coordinates only when valid pair exists)
- `metadata` (sanitized)

### Time source + timezone

- `occurredAt` uses source persisted time (`eventAtIso` → `eventAtMs`) so it is
  stable across retries.
- Timezone defaults to `America/Toronto` when missing.
- Invalid timezone is rejected.

### Location rules

- Coordinate validation:
  - latitude must be `-90..90`
  - longitude must be `-180..180`
  - both must be present together
- Invalid or incomplete coordinates are omitted (location label is preserved when present).
- A non-empty location label is preserved; no invented locations.

### Metadata / redaction rules

- Source `memberEmail` is never added to output.
- Secret-like metadata keys are removed (`token`, `apiKey`, `secret`, `webhook`, etc.).
- Useful context (`projectSlug`, `reportDateKey`) is preserved in `metadata` when available.

## 3) Event mapping

Supported event set:

- `arrive_work` → `activity_boundary / work / work`
- `leave_work` → `activity_boundary / work / work`
- `arrive_home` → `activity_boundary / home / home`
- `leave_home` → `activity_boundary / home / home`
- `arrive_gym` → `activity_boundary / gym / gym`
- `leave_gym` → `activity_boundary / gym / gym`
- `start_workout` → `activity_boundary / workout / workout`
- `finish_workout` → `activity_boundary / workout / workout`
- `arrive_location` → `location / location / other_location`
- `leave_location` → `location / location / other_location`
- `start_spotify` → `activity_boundary / spotify / spotify`

`start_spotify` does not invent `endAt` or `durationSeconds`.

## 4) Configuration validation

Module: `functions/timeLeftLifeEventConfig.js`

Names (all in `functions/.env.example`):

- `TLTL_LIFE_EVENTS_URL`
- `TLTL_CALENDAR_ID`
- `TLTL_CONNECTION_ID`
- `TLTL_INTEGRATION_ID`
- `TLTL_BEARER_TOKEN`
- `TLTL_DUAL_WRITE_MODE`
- `TLTL_TARGET_PROJECT_ID`
- `TLTL_REQUEST_TIMEOUT_MS`

Validation rules:

- default mode is `off`
- staging mode requires endpoint, calendar ID, connection ID, integration ID, target token
- staging mode accepts only `https://timelefttolive-stg-go.web.app/api/v1/life-events`
- `production` is rejected in this phase
- disallowed target projects: `timelefttolive`, `timelefttolive-stg-marwan`

No bearer token is included in validation errors.

## 5) HTTP client behavior

Module: `functions/timeLeftLifeEventClient.js`

Factory: `createTimeLeftLifeEventClient(config, { fetch, logger })`

Common behavior:

- injects fetch for tests
- bounded timeout support
- never throws for normal delivery outcomes
- safe JSON parsing
- Authorization header set as `Bearer <token>`
- logs do not include token

Result classifications:

- `off` (skip): no HTTP request
- 200 with `duplicate:false`: `delivered`, `retryable:false`
- 200 with `duplicate:true`: `duplicate`, `retryable:false`
- 400 or 413: `permanent_failure`, `retryable:false`
- 401 or 403: `authentication_failure`, `retryable:false`
- 409: `conflict`, `retryable:false`, preserve `existingLifeEventId` if provided
- 429 / 5xx: `retryable_failure`, `retryable:true`
- timeout/network/parse failures: `retryable_failure`, `retryable:true`

## 6) Tests added

Modules:

- `functions/timeLeftLifeEventMapper.test.js` (mapper mapping + validation + redaction)
- `functions/timeLeftLifeEventClient.test.js` (mode, host checks, every requested response path,
  timeout/network/json parsing, and header/payload assertions)

Additional tests run:

- existing `functions/iosShortcutsIntegration.test.js`
- overall `npm test` in `functions`

## 7) Next phase (exact)

- integrate the mapper/client after durable `iosShortcutEvents` persistence
- preserve existing `assistant/logEntries/activitySessions` behavior
- add delivery-state storage
- add bounded retry processing
- keep dual-write off by default
