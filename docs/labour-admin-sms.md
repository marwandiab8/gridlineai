# Administrator Labour Queries over SMS

GridlineAI administrators can query current labour totals and request protected PDF reports by texting the existing `inboundSms` number.

## Authorization

The sender must match an active `appMembers.approvedPhoneE164` record whose normalized role is exactly `admin`. Management, viewer, worker-only, inactive, and unknown phones cannot use cross-worker or cross-project labour queries. Authorization is resolved from the normalized E.164 sender; no phone number is hard-coded.

Unauthorized requests receive a generic denial that does not confirm whether a worker, project, date, or labour record exists. Existing worker self-service and ordinary labour submissions remain separate.

## Canonical source of truth

Every answer is calculated at request time from canonical `labourEntries` documents:

- `reportDateKey` is the work date.
- A positive integer `minutesWorked` is authoritative.
- If `minutesWorked` exists, legacy `hours` is ignored and never added.
- A legitimate legacy row without `minutesWorked` is converted once using the established `labourMinutesFromHours()` normalization.
- Invalid minutes, invalid dates, deleted/test/duplicate rows, unresolved workers, and missing, malformed, contradictory, or unknown project ownership fail closed.
- Each Firestore document ID is counted once.
- Arithmetic remains integer minutes until SMS/PDF formatting.

The aggregation audit retains the exact project and worker scope, inclusive date range, entry and worker counts, total minutes, excluded counts/reasons, and source document IDs. Ordinary SMS replies do not expose document IDs, notes, work descriptions, phone numbers, or private content.

## Project and worker identity

Projects resolve through the canonical `projects` registry and shared report-ownership rules. Both `projectSlug` and `projectId`, when present, must agree. Project names and configured aliases are matched exactly after canonical normalization. Ambiguity produces one clarification question.

All-work reports include active construction projects whose labour tracking is not explicitly disabled. Personal/Home projects are excluded unless the exact project is explicitly requested and its registry record has `labourEnabled: true` (or `labourTrackingEnabled: true`).

Workers resolve through active canonical `labourers` records. The document/phone identity is primary; current names and configured aliases or previous names support SMS resolution. Shared first names produce a clarification rather than combining people. Historical entries with the same canonical phone are grouped under the current registered name.

## Dates and pay periods

Natural-language date boundaries use `America/Toronto`. ISO ranges are inclusive.

The established GridlineAI payroll calendar is reused from `LABOUR_PAY_PERIOD_CONFIG` in `functions/labourRepository.js`:

- Anchor start: `2026-04-25`
- Cycle: 14 days
- Time zone: `America/Toronto`

Production `adminSettings/company` currently contains no separate payroll-period setting. If the server calendar definition is unavailable or invalid, pay-period queries fail safely and ask for explicit start/end dates.

Defaults:

- Project-only total: project-to-date.
- Worker-only total: current pay period.
- Project PDF: current pay period.
- PDF without a project: current pay period across separately validated work projects.

## Supported examples

```text
How many labour hours for the Docksteader job?
How many hours for the current pay period for all labourers?
How many hours did Ethan log?
How many hours did Ethan log on 2026-08-06?
Who has hours on 2026-08-06?
How many total labour hours were entered on 2026-08-06?
How many hours did Shawn Jones work from 2026-08-12 to 2026-08-14?
Send me a labour report for Docksteader.
Send me a PDF labour report for Docksteader for the current pay period.
Labour report Docksteader 2026-08-01 to 2026-08-15.
Labour help
```

## PDF and idempotency

Text and PDF output use the same canonical aggregation result. Administrator PDFs contain project-separated subtotals, worker totals, day totals, worker-by-day totals, exact dates, Toronto generation time, entry/worker counts, integer-minute totals, and excluded-record warnings. They never include journal, Shortcut, media, personal activity, labour notes, or work descriptions.

PDF metadata is stored in the dedicated `adminLabourReports` collection, which has no client read rule; ordinary management and project access to `labourReports` cannot expose administrator report metadata. PDF objects are stored without public download tokens under the separate deterministic `adminLabourReports/` Storage namespace, which has no client read rule and therefore does not inherit management access to `labourReports/**`. Delivery reuses the existing `dailyReportDownload` protected-link handler and `reportAccessGrants` collection.

The inbound Twilio `MessageSid` is hashed into deterministic request, queue, report, storage, access-grant, and outbound-message identities. Duplicate webhook deliveries are transactionally consumed without a second TwiML message, queue document, report document, PDF path, access grant, or report-link SMS.

For PDF requests, the canonical aggregation is persisted in the durable queue transaction before GridlineAI acknowledges that a protected link will follow. The delivery trigger validates that persisted result, creates the private PDF and access grant once, and marks the queue delivered only after Twilio accepts the link message. Pre-send failures remain retryable for three delivery attempts; an exhausted job sends one administrator-safe failure message. Provider-uncertain sends fail closed rather than risking a duplicate link.

Successful project acknowledgement and link messages use readable inclusive dates, for example:

```text
Okay. Generating the administrator labour PDF for Docksteader, August 15–28, 2026. A protected link will follow shortly.
Docksteader labour report — August 15–28, 2026: [protected link]
```

Privacy-safe logs record only the structured intent, hashed identity references, project/worker scope, inclusive dates, entry/exclusion counts, total minutes, clarification/failure reason, and request idempotency key. Raw SMS text, notes, full phone numbers, grant tokens, and PDF URLs are not logged by this workflow.
