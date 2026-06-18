# Time Left To Live gridlineai Sync

This document explains how to configure, test, deploy, verify, troubleshoot, and backfill the backend-only gridlineai to Time Left To Live sync.

The local gridlineai repo is:

```text
/home/marwan/Documents/ChatBot
```

The Firebase project alias is:

```text
gridlineai
```

## Overview

Data flow:

```text
gridlineai Firestore/Storage metadata
-> gridlineai backend Firebase Functions
-> Time Left ingestion HTTPS endpoint
-> Time Left externalItems
```

The integration is intentionally server-side only:

- The Time Left ingestion token is a Firebase Functions secret.
- The frontend must never receive, store, or log the token.
- gridlineai does not write directly to Time Left Firestore.
- gridlineai sends records only through the Time Left ingestion endpoints.
- Synced items default to `visibility: "ownerOnly"`.
- Firestore triggers can run more than once, so idempotency depends on stable `sourceDocumentPath` and `sourceStoragePath`.

Time Left endpoints:

```text
Single item:
https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItem

Batch:
https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItemsBatch
```

## Setup Checklist

1. In Time Left To Live, create a source connection for `gridlineai`.
2. Copy the Time Left `calendarId`.
3. Copy the Time Left `connectionId` for the gridlineai source connection.
4. Generate the ingestion token from Time Left External Sources.
5. Store the token as a Firebase Functions secret in the `gridlineai` Firebase project.
6. Set the non-secret runtime config values listed below.
7. Run local validation.
8. Deploy only the new sync functions.
9. Test one report payload.
10. Verify the item in Time Left Day Detail.
11. Send the same payload twice and confirm it upserts instead of duplicating.
12. Run backfill only after live trigger validation succeeds.

Do not deploy until the real Time Left `calendarId`, Time Left gridlineai `connectionId`, and ingestion token are configured.

## Firebase Functions Setup

This repo uses Firebase Functions v2, JavaScript, npm, and Node 22.

Required secret:

```bash
firebase functions:secrets:set TIME_LEFT_INGESTION_TOKEN --project gridlineai
```

Do not put `TIME_LEFT_INGESTION_TOKEN` in:

- frontend code
- `public/runtime-config.js`
- Vite/React/browser environment variables
- Firestore
- Remote Config
- committed `.env` files
- example env files
- logs

Non-secret config values match [functions/timeLeftGridline/env.example](/home/marwan/Documents/ChatBot/functions/timeLeftGridline/env.example):

```text
TIME_LEFT_SINGLE_INGEST_ENDPOINT=https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItem
TIME_LEFT_BATCH_INGEST_ENDPOINT=https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItemsBatch
TIME_LEFT_CALENDAR_ID=REPLACE_WITH_TIME_LEFT_CALENDAR_ID
TIME_LEFT_CONNECTION_ID=REPLACE_WITH_TIME_LEFT_GRIDLINEAI_CONNECTION_ID
GRIDLINE_FIREBASE_PROJECT_ID=gridlineai
GRIDLINE_APP_BASE_URL=REPLACE_WITH_GRIDLINE_APP_BASE_URL
GRIDLINE_DEFAULT_TIME_ZONE=America/Toronto
GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS=PROJECT_ID_1,PROJECT_ID_2
```

`GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS` is optional. If set, it is a comma-separated allowlist of gridlineai logical project IDs. Leave it empty to skip local allowlist validation and rely on Time Left source connection validation.

Important distinction:

- `GRIDLINE_FIREBASE_PROJECT_ID` is the Firebase project ID, usually `gridlineai`.
- `sourceProjectId` is the logical gridlineai project ID from a source document, such as a project slug or `projectId` field.

## Actual Firestore Source Mapping

The implementation watches root-level collections. Reports, logs, and media are not nested under `projects`.

| Source type | Firestore path | Time Left category | Stable `sourceDocumentPath` | `sourceStoragePath` | `sourceProjectId` |
| --- | --- | --- | --- | --- | --- |
| project record | `projects/{projectId}` | `projectRecord` | `projects/{projectId}` | `null` unless the project record has `storagePath` | `projectId` |
| report | `dailyReports/{reportId}` | `projectReport` | `dailyReports/{reportId}` | `dailyReports.storagePath` if present, otherwise `null` | derived from `dailyReports.projectId` or `dailyReports.projectSlug` |
| journal/log entry | `logEntries/{entryId}` | `journalEntry` | `logEntries/{entryId}` | `null` | derived from `logEntries.projectId` or `logEntries.projectSlug` |
| uploaded image/file | `media/{mediaId}` | `uploadedImage` or `uploadedFile` | `media/{mediaId}` | stable Cloud Storage object path from `media.storagePath` | derived from `media.projectId` or `media.projectSlug` |

Current mapper source project logic:

```text
sourceProjectId = record.projectId || record.projectSlug || options.projectId || options.id || ""
```

For project records, `options.id` is the `{projectId}` document ID, so `sourceProjectId` is stable even if the project document does not contain a `projectId` field.

For root-level `dailyReports`, `logEntries`, and `media`, the source document must carry `projectId` or `projectSlug` for Time Left project allowlist checks to work.

## Exported Functions

The sync functions are exported from [functions/timeLeftGridline/triggers.js](/home/marwan/Documents/ChatBot/functions/timeLeftGridline/triggers.js) and wired from [functions/index.js](/home/marwan/Documents/ChatBot/functions/index.js):

```text
syncGridlineProjectRecordToTimeLeft
syncGridlineReportToTimeLeft
syncGridlineJournalEntryToTimeLeft
syncGridlineFileToTimeLeft
```

Trigger paths:

```text
projects/{projectId}
dailyReports/{reportId}
logEntries/{entryId}
media/{mediaId}
```

The triggers run on document writes. Create and update events send `syncStatus: "active"`. Delete events use the before snapshot and send `syncStatus: "deletedFromSource"`. Time Left must decide how to display or hide deleted-source items.

## Canonical Categories

Mapping:

- `projects` -> `projectRecord`
- `dailyReports` -> `projectReport`
- `logEntries` -> `journalEntry`
- `media` with `contentType` starting `image/` -> `uploadedImage`
- `media` with other `contentType` -> `uploadedFile`

All synced items use:

```text
sourceApp=gridlineai
visibility=ownerOnly
```

## Date Behavior

Time Left date IDs must be:

```text
YYYY-MM-DD
```

The mapper sends `dateId` only when the source record has a reliable date. If no reliable date exists, `dateId` is omitted so Time Left can place the item into date review.

Date priority in the code:

```text
dateId
entryDate
journalDate
reportDateKey
dateKey
reportDate
capturedAt
takenAt
exifTakenAt
occurredAt
documentDate
uploadedAt
```

The mapper does not use `updatedAt` as `dateId`.

`createdAt` is not part of date resolution for `dateId` in the current code. It is still sent separately as `originalCreatedAt`.

Timezone fallback order:

```text
record.timeZone
record.projectTimeZone
trigger/backfill option timeZone
GRIDLINE_DEFAULT_TIME_ZONE
America/Toronto
```

When a timestamp is converted to a date ID, the conversion uses this timezone. A literal `YYYY-MM-DD` value is preserved as-is.

## Idempotency

Firestore triggers may run more than once. Time Left upsert/dedupe behavior depends on stable source identity.

Good:

```text
sourceDocumentPath=dailyReports/REPORT_ID
sourceStoragePath=null
```

Good:

```text
sourceDocumentPath=media/MEDIA_ID
sourceStoragePath=projects/PROJECT_ID/uploads/MEDIA_ID/photo.jpg
```

Bad:

```text
sourceDocumentPath=dailyReports/REPORT_ID/updatedAt/2028-07-25T10:30:00Z
sourceDocumentPath=syncRuns/RANDOM_ID
sourceStoragePath=https://signed-url.example.com/private-file?expires=...
sourceStoragePath=https://firebasestorage.googleapis.com/...&token=...
```

Do not put signed URLs, Firebase download URLs with token query params, `updatedAt`, or random sync IDs in idempotency fields.

## Local Validation

Run from repo root:

```bash
cd /home/marwan/Documents/ChatBot
node --check functions/index.js
node --test functions/timeLeftGridline/mappers.test.js
npm test --prefix functions
```

Expected result after this implementation: all function tests pass.

## Test One Report With Curl

Use the committed sample:

```bash
cd /home/marwan/Documents/ChatBot/functions
cp timeLeftGridline/payload-report.example.json /tmp/payload-report.json
```

Edit `/tmp/payload-report.json` and replace:

- `REPLACE_WITH_TIME_LEFT_CALENDAR_ID`
- `REPLACE_WITH_TIME_LEFT_GRIDLINEAI_CONNECTION_ID`
- `PROJECT_ID`
- `REPORT_ID`
- sample title/summary/storage fields as needed

Current implementation uses:

```text
Authorization: Bearer TOKEN
```

Curl:

```bash
read -s TIME_LEFT_INGESTION_TOKEN

curl -i \
  -X POST "https://northamerica-northeast1-timelefttolive.cloudfunctions.net/ingestExternalDailyItem" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TIME_LEFT_INGESTION_TOKEN}" \
  --data-binary @/tmp/payload-report.json

unset TIME_LEFT_INGESTION_TOKEN
```

The ingestion client also supports an alternate header if Time Left changes:

```text
x-ingestion-token: TOKEN
```

The deployed trigger client currently sends `Authorization: Bearer TOKEN`.

## Duplicate Ingestion Test

Send the same payload twice. Time Left should update the same external item instead of creating duplicates.

Confirm the stable fields did not change:

```text
sourceDocumentPath
sourceStoragePath
sourceApp
sourceFirebaseProjectId
sourceProjectId
```

## Backfill

The backfill implementation is a local/server-side script:

```text
functions/timeLeftGridline/backfillTimeLeft.js
```

It is run from the `functions` directory. It is not a deployed public endpoint.

Dry run:

```bash
cd /home/marwan/Documents/ChatBot/functions
npm run backfill:timeleft:gridline -- --source=all --dryRun --limit=25
npm run backfill:timeleft:gridline -- --source=dailyReports --dryRun --limit=25
npm run backfill:timeleft:gridline -- --source=logEntries --dryRun --limit=25
npm run backfill:timeleft:gridline -- --source=media --dryRun --limit=25
npm run backfill:timeleft:gridline -- --source=projects --dryRun --limit=25
```

Live mode:

```bash
cd /home/marwan/Documents/ChatBot/functions
npm run backfill:timeleft:gridline -- --source=all --limit=25
```

The script uses the Time Left batch endpoint and sends at most 100 items per request. Start with small `--limit` values.

Run live backfill only after live triggers have been deployed and verified with one record.

## Deploy

This integration has not been deployed by this documentation update.

Deploy only the new functions:

```bash
cd /home/marwan/Documents/ChatBot
firebase deploy --only "functions:syncGridlineProjectRecordToTimeLeft,functions:syncGridlineReportToTimeLeft,functions:syncGridlineJournalEntryToTimeLeft,functions:syncGridlineFileToTimeLeft" --project gridlineai
```

Do not deploy hosting for this backend-only sync.

## Verify In Time Left

After sending one report:

1. Open the Time Left app.
2. Open the configured calendar.
3. Open the matching `dateId`.
4. Open Day Detail.
5. Check external items for the new gridlineai item.
6. Confirm `visibility` is `ownerOnly`.
7. Confirm title, summary, description, source link, and file metadata if applicable.

Firestore verification path inside Time Left:

```text
lifeCalendars/{calendarId}/dailyEntries/{dateId}/externalItems/{externalItemId}
```

Check these fields:

```text
sourceApp
category
sourceFirebaseProjectId
sourceProjectId
sourceDocumentPath
sourceStoragePath
visibility
```

Expected values include:

```text
sourceApp=gridlineai
sourceFirebaseProjectId=gridlineai
visibility=ownerOnly
```

## Troubleshooting

### Invalid token

Check:

- `TIME_LEFT_INGESTION_TOKEN` secret exists in the `gridlineai` Firebase project.
- The sync functions were redeployed after setting the secret.
- The header format matches Time Left. Current implementation uses `Authorization: Bearer TOKEN`.
- The token was generated from the correct Time Left source connection.
- The token has not been revoked or rotated.

### Source connection inactive

Check in Time Left:

- source connection status is active
- `TIME_LEFT_CONNECTION_ID` matches the gridlineai connection
- `TIME_LEFT_CALENDAR_ID` matches the target calendar

### `sourceProjectId` not allowed

Do not confuse:

- `GRIDLINE_FIREBASE_PROJECT_ID=gridlineai`
- `sourceProjectId`, which is a logical gridlineai project ID from source documents

Root-level `dailyReports`, `logEntries`, and `media` need a valid `projectId` or `projectSlug` field so the mapper can set `sourceProjectId`.

Check:

- `GRIDLINE_ALLOWED_SOURCE_PROJECT_IDS`
- the Time Left source connection project allowlist
- the source document’s `projectId` or `projectSlug`

### Missing date

This is expected when no reliable date exists.

The item should go to Time Left date review. Do not “fix” missing dates by using `updatedAt`.

### Owner-only item not visible to viewers

Expected behavior.

The Time Left calendar owner should see the item. Viewers should not see `ownerOnly` external items.

### Duplicate items

Check that these are stable:

- `sourceDocumentPath`
- `sourceStoragePath`

Do not include timestamps, signed URLs, download URLs with tokens, or random sync IDs in those fields.

### Media item did not appear

Check:

- `media/{mediaId}` exists.
- `media.contentType` is set.
- `media.storagePath` is a stable Cloud Storage object path.
- `media.projectId` or `media.projectSlug` maps to the expected Time Left allowed source project.
- File previews may be absent if the source file is private and `fileUrl` is intentionally `null`.

### Report or log item did not appear

Check:

- the record is in the root-level collection, not nested under `projects`
- `dailyReports/{reportId}` or `logEntries/{entryId}` exists
- the corresponding sync function is deployed
- Firebase Functions logs for the sync function
- required `projectId` or `projectSlug` fields are present when Time Left uses project allowlists

### Backfill sends zero records

Check:

- `--source` is one of `all`, `projects`, `dailyReports`, `logEntries`, or `media`
- `--limit` is greater than zero
- you are running from `/home/marwan/Documents/ChatBot/functions`
- required config is set for live mode
- dry-run mode is not expected to write anything

## Known Assumptions And Open Questions

- The actual Time Left `calendarId` must come from the Time Left app.
- The actual Time Left gridlineai `connectionId` must come from the Time Left app.
- The allowed `sourceProjectId` list must match gridlineai logical project IDs, not the Firebase project ID.
- If Time Left later adds a dedicated delete endpoint, delete/soft-delete syncing can be extended.
- If media files need direct preview inside Time Left, use secure auth-aware URLs. Do not make private files public by default.
- Reports can include `sourceStoragePath` from `dailyReports.storagePath`; this is useful for idempotency and file metadata.
