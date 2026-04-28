# Daily Construction Report and Daily Journal Rules

This document describes the report-generation rules implemented in this codebase. It is based on the current Firebase Functions, dashboard, PDF builders, repositories, security rules, and tests.

Primary source files reviewed:

- `functions/index.js`
- `functions/assistant.js`
- `functions/logClassifier.js`
- `functions/logEntryRepository.js`
- `functions/mediaRepository.js`
- `functions/dailyReportPdf.js`
- `functions/dailyReportContent.js`
- `functions/dailyReportIntegrity.js`
- `functions/dailySectionMapper.js`
- `functions/dailyReportAiJson.js`
- `functions/dailyPdfReportBuilder.js`
- `functions/dailyPdfCompact.js`
- `public/index.html`
- `public/app.js`
- `firestore.rules`
- `storage.rules`
- related `*.test.js` files under `functions/`

## Report Types

There are two daily PDF report types:

- `dailySiteLog`: the formal daily construction report / daily site log.
- `journal`: the daily journal PDF.

If a report type is missing or blank in backend generation, it defaults to `dailySiteLog`. The only accepted explicit values in the dashboard callable are `dailySiteLog` and `journal`.

## Creation Entry Points

### Dashboard report form

The dashboard Reports view lets management users generate daily PDFs by selecting:

- phone number from `smsUsers`
- project slug
- report style (`dailySiteLog` or `journal`)
- report date
- optional dashboard token

The frontend defaults the report date to today's `America/New_York` date.

The frontend currently requires a project slug before it calls the backend, even when the selected type is `journal`. The backend itself can create a projectless personal journal for management users, but the dashboard UI blocks that path.

### Dashboard callable

`generateDailyReportPdfCallable` enforces:

- caller must have at least management access
- dashboard token must pass when configured
- `phoneE164` is required
- app-member callers can only generate for their own approved field phone
- `reportDateKey`, if supplied, must be `YYYY-MM-DD`
- `reportType` must be `dailySiteLog` or `journal`
- the selected `smsUsers` document must exist
- project comes from explicit `projectSlug`, otherwise the field user's `activeProjectSlug`
- requested project must exist
- caller must be assigned to the requested project, unless their role/all-project access permits it
- `dailySiteLog` requires a project
- `journal` can be project-scoped or projectless at the backend layer

The callable passes `includeAllManagementEntries` only when the request explicitly sets it to `true`.

### SMS request

Inbound SMS can request daily PDF creation without OpenAI. Supported request phrases include:

- `daily report`
- `daily pdf`
- `daily pdf report`
- `pdf report today`
- `eod report`
- `end of day report`
- `generate report`
- `generate daily report`
- `daily journal pdf`
- `journal pdf`
- `journal report`

Optional polite prefixes are accepted, such as `please`, `can you`, `send me`, `text me`, `give me`, `I need`, and similar.

SMS report requests can include:

- date: `YYYY-MM-DD`, `today`, or `yesterday`
- project hints: `project docksteader`, `docksteader project`, `for project docksteader`, `on docksteader project`, and similar
- style hint: `journal`, `personal journal`, `daily site log`, or `site log`

Invalid SMS request rules:

- a request cannot ask for both `journal` and `dailySiteLog`
- a request cannot include more than one date hint
- unclear leftover words after parsing cause an invalid request response
- `dailySiteLog` SMS generation fails if no explicit or active project is available

When SMS requests a report, the HTTP reply is immediate. A `dailyPdfDeliveryQueue` document is created, and a separate function generates the PDF and texts the download link.

### Dashboard assistant message

Dashboard assistant messages can also trigger daily PDF creation. When a daily PDF is requested through that path:

- the selected phone must be accessible to the dashboard user
- `dailySiteLog` requires an active or requested project
- the PDF is generated immediately in the callable workflow and returned with a download URL or storage path

## Dates and Time Zone

All daily boundaries use `America/New_York`.

Rules:

- new log entries default to today's Eastern date unless a report date is parsed
- report dates are stored as `dateKey` and `reportDateKey`
- PDF titles, filenames, captions, and report windows use Eastern dates
- report generation defaults to today's Eastern date when no valid date is supplied internally
- dashboard callable rejects malformed `reportDateKey`
- SMS accepts `today`, `yesterday`, and strict `YYYY-MM-DD`
- structured log messages can include backdated dates as `(YYYY-MM-DD)`, `for YYYY-MM-DD`, `on YYYY-MM-DD`, `dated YYYY-MM-DD`, or `date YYYY-MM-DD`
- loose date typos such as `2026-04016` are normalized when possible

Legacy and backdated lookup rules:

- log entries with explicit report dates can be found even if their `createdAt` falls one or two days after the report day
- media lookups include the report date and adjacent legacy windows so photos tied to backdated reports can still appear
- daily site log generation also merges media from the previous Eastern date for the selected report date
- journal generation uses only media whose Eastern `dateKey` is the same as the journal report date

## Log Entry Creation Rules

Every saved inbound field update becomes a `logEntries` document unless it is classified as a meta command, UI request, or conversation control message.

Saved `logEntries` include:

- `dateKey`
- `reportDateKey`
- normalized `projectId`
- normalized `projectSlug`
- sender and author phone/name/email/label fields
- `rawText`
- `normalizedText`
- `category`
- `subtype`
- `tags`
- `status`
- `openItem`
- `includeInDailySummary`
- `dailySummarySections`
- linked media IDs
- optional AI enhancement fields

`includeInDailySummary` defaults to `true`. A document is excluded from daily reports when this flag is explicitly `false`.

Structured SMS commands that create field log entries include:

- `log safety`
- `log safety issue`
- `log deficiency`
- `log punch` or `log punch item`
- `log delay`
- `log inspection`
- `log issue`
- `log delivery`
- `log note`
- `log manpower`
- `log progress`
- `daily log`
- shorthand forms such as `safety:`, `delay:`, `deficiency:`, `punch item`, `issue:`, `note:`, `manpower:`, and `progress:`

Common manpower command typos are normalized, including `load manpower`, `manpwer`, `manpoewer`, and `manpwower`.

If a `log note` body starts with `manpower` or `progress`, it is reclassified into the matching report intent while preserving `dayLog`.

## Section Mapping Rules

Every saved reportable log entry includes `dayLog`.

Base category mapping:

- `safety`: `safety`, `openItems`
- `delay`: `delays`
- `deficiency`: `deficiencies`, `openItems`
- `issue`: `issues`, `openItems`
- `note`: `notes`
- `progress`: `workInProgress`, `workCompleted`
- `delivery`: `deliveries`
- `inspection`: `inspections`, `openItems`
- `journal`: `journal`

Keyword-based section additions:

- concrete: `pour`, `concrete`, `slab`, `pump`, `ready mix`, `mud mat`, `curing`, `mixer`, `placement`
- weather: `rain`, `snow`, `wind`, `forecast`, `weather`, `humid`, `hot day`, `cold`, `freezing`, `icy`
- manpower: `crew`, `manpower`, `labour`, `labor`, `workers`, `short staff`, `headcount`, `sub crew`, or recognized roll-call patterns
- inspections: `inspect`, `inspection`, `consultant`, `englobe`, `geotech`, `third party`
- delays: `delay`, `late`, `waiting`, `cancelled`, `canceled`, `held up`, `backorder`, `no show`
- deficiencies/open items: `defect`, `deficiency`, `punch`, `missed`, `wrong`, `broken`, `hazard`, `unsafe`
- deliveries: `deliver`, `delivery`, `truck load`, `material arrived`, `drop off`
- completed work: `complete`, `completed`, `done`, `wrapped`, `finished` when category is `progress`
- in-progress work: `ongoing`, `in progress`, `underway`, `continuing`, `tomorrow`
- photos: `mms` or `photo` tags

Open item signals:

- categories `deficiency`, `issue`, `safety`, `delay`, and `inspection` are treated as likely open items
- text containing `pending`, `open`, `follow up`, `unresolved`, `waiting`, `tomorrow`, or `must fix` is treated as likely open item content

## Daily Construction Report Rules

The daily construction report is `dailySiteLog`.

### Source scope

When a project is selected, the report aggregates all log entries, messages, and media for that project across all senders. It is not limited to the phone used to request the report.

When `includeAllManagementEntries` is true and management phones can be resolved for the project, messages, log entries, and media are narrowed to active management app members assigned to that project.

### Required project

A `dailySiteLog` requires a project:

- dashboard callable rejects projectless site logs
- SMS delivery rejects projectless site logs
- dashboard UI requires a project before any daily PDF generation request

### Field entry inclusion

An entry can appear in the site log only when:

- `includeInDailySummary` is not `false`
- it matches the report project by normalized `projectSlug` or `projectId`
- it is not control/meta chatter
- it survives second-pass field curation
- after bulk-wrapper/date cleanup, it still has text for the report date

Project rescue rule:

- an unassigned legacy entry can be included if its text explicitly says `Project: <slug>` for the requested project
- an entry explicitly labeled for another project is excluded

### Excluded from site log body and appendix

The report excludes:

- daily report/PDF requests
- daily rollup requests
- `continue`, `ok`, `yes`, `no`, `thanks`, and similar one-word replies
- photo gallery/view/send commands
- project switch commands
- requests to add/reassign photos to a project
- report complaints, report fix requests, layout/title/header change requests
- requests to include/show/attach photos in the report
- link requests
- menu/link tap instructions
- short greetings with no field context
- personal home/family notes that are not site-related
- no-detail placeholders
- weather-unavailable placeholders
- ChatGPT/OpenAI keyword requests
- previous-conversation continuation requests
- manpower clarification chatter
- empty or wrapper-only bulk update text

Field notes that contain words like "show photos" can remain reportable when they also include real construction context such as grid, pour, slab, rebar, concrete, crew, deficiency, inspection, crane, formwork, backfill, or similar.

### Bulk text cleanup

PDF and AI input text removes:

- lines like `Add the below updates...`
- lines like `Add the following...`
- project/date wrapper headers such as `Project Docksteader Monday April...`
- embedded weekday/month dates that conflict with the selected report date

Raw Firestore data is not changed by this cleanup.

### Section promotion

Journal or note entries can be promoted into formal site-log sections when field content is detected:

- weather/forecast/temperature lines can become weather entries
- roofing, waterproofing, excavation, backfill, rebar, piers, temporary power, electrical rough-in, Coreydale, O'Connor, installing, and similar work terms can become work-in-progress entries
- pour, concrete slab, mud mat, placement, and ready mix terms can become concrete entries

### Weather

Daily site logs fetch automated weather for the report day.

Weather rules:

- provider is Open-Meteo
- the project address is used when available
- Docksteader uses `6 Docksteader Rd, Brampton, ON L6R 3Y2`
- missing or unresolved addresses fall back to Brampton-area coordinates
- the PDF renders report-day weather only
- if automated weather fails, the PDF says weather was unavailable
- AI is explicitly instructed to use only the authoritative weather prefix and not invent temperatures, wind, precipitation, or conditions
- weekly forecast rows are not used in the current rendered daily site log

Weather log text only goes into the weather section if it is weather-only. Mixed text such as a weather delay plus waterproofing work is treated as work instead of pure weather.

### Workforce Summary

The workforce section renders a table with:

- Trade
- Foreman
- Workers
- Notes

Manpower rows come from:

- AI-extracted manpower rows on log entries
- `manpower` report sections
- text containing crew/workers/labour/manpower/headcount/foreman/on-site signals
- recognized multi-trade roll-call lines
- parsed contractor/foreman/worker patterns

AI-extracted manpower rows take precedence for an entry. Generic trade labels such as `Journal`, `Site / General`, `General`, and `Notes` are rejected.

If no manpower is stated, the table shows a placeholder row saying manpower was not stated in log entries.

The PDF adds a total row when worker counts are parseable. Narrative text is omitted when it is redundant with the table or is a simple roll-call line.

### Work Completed / In Progress

Work entries are grouped by trade or scope.

Work-like entries include:

- `progress`
- `note`
- `journal`
- `delivery`
- `inspection`
- sections matching work, deliveries, inspections, notes, or journal

Trade inference rules:

- named contractors are preferred when detected: `ALC`, `Road-Ex`, `Coreydale`, `O'Connor`, `SteelCon`
- common trade labels are inferred from text, including Formwork, Reinforcing, Concrete, Earthworks, Waterproofing, Masonry, Roofing, Glazing, Mechanical / Plumbing, Electrical, Interior Framing / Drywall, Painting / Coatings, Fireproofing, Landscaping, and Site Civil / Paving
- invalid headings, verbs, command words, and generic starters fall back to `Site / General`
- AI structured work sections are used only when they contain valid real trade names and non-empty items
- adjacent AI work sections for the same trade are merged
- source updates can be shown beneath AI trade sections

The PDF may render an executive/superintendent summary when it adds useful content beyond the stitched source log.

### Issues and Deficiencies

Issue content comes from sections/buckets:

- `deficiencies`
- `issues`
- `delays`
- `safety`

The PDF renders `Issues & Deficiencies`, removes duplicate issue paragraphs when they already appear in work/executive content, and shows issue-linked source updates or photos when not redundant.

### Inspections

Inspections render only when there is real inspection text or inspection chunks.

The section is omitted when all content is empty or placeholder text such as `Not stated in field messages.`

AI is instructed to include consultant/inspector name, scope inspected, result, and follow-up when present.

### Concrete Summary

The concrete section always renders a table with:

- Pour location / scope
- Volume
- Status

Concrete rows come from concrete sections or concrete keywords such as pour, concrete, m3, mud mat, slab, ready mix, and placement.

Concrete parsing rules:

- volume recognizes `m3`, cubic meters, `cy`, yards, and similar units, including approximate quantities
- location/scope is extracted from `for`, `re:`, `pour at`, `placed at`, `at`, `@`, `location`, `scope`, `zone`, `bay`, `grid`, `level`, and `area`
- status recognizes complete/placed, in progress, cancelled/no pour, delayed, postponed, rescheduled, on hold, pending, done, tentative, scrubbed, and related words
- if no concrete is stated, the table shows a placeholder row

### Open Items / Action Required

Open items can come from deterministic parsing or AI JSON.

Deterministic open item rules:

- `entry.openItem === true` qualifies
- `assignedTo`, `dueDate`, or `status` qualifies
- text with open item/action/follow-up/responsible/assigned/owner/status/due/target-date language qualifies
- status words such as open, pending, awaiting, in progress, on hold, monitoring, resolved, and closed qualify

Open item rows include:

- number
- action item
- responsible
- status

Owner/responsible can be parsed from `owner`, `assigned to`, `reported by`, `by`, `pm`, `super`, `gc`, `responsible`, or `action by`.

If no open items are flagged, the table shows `No open items flagged in log entries.`

Weak AI open-item rows can be removed when they duplicate issue, executive, work, or concrete text.

### Site photos

Site-log media rules:

- media must have `storagePath`
- media project must match the report project
- linked media is included only if linked to an included log entry
- unlinked media is included only when it has `includeInDailyReport: true`, shares an allowed source message ID, or has a field-log-like caption
- wrong-project media is excluded
- media linked to an entry outside the final report entry set is excluded

Unlinked captions are considered reportable when they contain strong construction signals such as:

- `log:`, `daily log:`, `safety:`, `delay:`, `inspection:`, `note:`, `progress:`, or `delivery:`
- pour, concrete, rebar, slab, footing, crane, waterproofing, excavation, trench, backfill
- crew, foreman, manpower, headcount, GC, subcontractor, trade
- grid, GL, bay, zone, level, core, shaft
- units such as `cy`, yards, `m3`, `cfm`, `psi`
- drone, UAV, aerial, orthophoto, site photo, progress photo

Personal/family captions are rejected unless they also have field-log context.

Photo rendering rules:

- photos are deduped by media ID
- photos with `includeInDailyReport: true` are prioritized
- linked photos are prioritized over unlinked photos
- longer captions sort ahead of shorter captions
- near-duplicate captions are capped
- section photo caps are 4 for manpower, work, issues, concrete, and inspections
- remaining site photos are capped at 8
- issue photos are not repeated under work sections
- already-rendered photos move out of remaining site photos
- captions can be shortened or removed when they repeat nearby report text

## Daily Journal Rules

The daily journal is `journal`.

### Source scope

Journal source rules depend on project scope:

- project-scoped journal: loads project messages, project log entries, and project media across all senders
- projectless journal: loads the selected phone's messages across all projects, that phone's log entries for the day, and that phone's media for the day

When a journal is requested by SMS and the phone has an active project, the journal becomes project-scoped unless the request resolves otherwise. Without an active or requested project, it is a personal journal.

### Journal entry inclusion

Journal entries must pass the daily summary flag and journal-specific meta filter.

Excluded journal content includes:

- empty text
- `continue`, `ok`, `yes`, `no`, `thanks`, `help`, `status`, `reset`, `contacts`
- `photo attachment`
- project-switch commands
- bare daily report/daily summary/daily log requests
- report/PDF generation requests
- weather requests and weather-unavailable placeholders
- photo gallery commands
- tap/reply/menu instructions
- report-generation chatter and app/debug chatter

Important preservation rule:

- a journal entry is kept if any text layer is real diary content, even if another layer, such as `summaryText`, contains report-meta wording

### Journal author and contributor rules

Author labels are resolved from:

- active `appMembers` by `approvedPhoneE164`
- `smsUsers` by phone
- entry author fields

Phone-like author labels are replaced with display names when available.

When more than one contributor appears:

- journal bundles list contributors
- timeline rows retain author labels
- AI is instructed to write shared/co-authored journal text
- AI must not assign one contributor's action, feeling, errand, meal, or purchase to another contributor

### Journal model

The deterministic journal model builds:

- chronological timeline sorted by entry time
- contributor list
- `isCoauthored`
- day overview
- key moments
- reflections
- closing note

Rules:

- timeline rows include time, author label, text, and linked photos
- key moments are unique lines in chronological order, capped at 8
- reflections prefer text with reflective/emotional words such as feel, thought, worried, grateful, tired, hope, frustrated, excited, anxious, learned, noticed, and appreciated
- if no reflective text is found, reflections fall back to recent journal lines
- reflections are capped at 6
- overview uses up to the first 3 key moments and is capped around 900 characters
- if no journal-worthy notes exist, overview says no journal-worthy notes were captured
- closing note uses the last journal line and is capped around 320 characters

### Journal AI rules

OpenAI is optional. If unavailable or JSON parsing fails, deterministic journal content is used.

AI journal output must be strict JSON with:

- `overview`
- `keyMoments`
- `reflections`
- `closingNote`

AI journal writing rules:

- write like a thoughtful personal day journal
- do not write like a superintendent or construction site log
- keep tone reflective, specific, and human
- use only actual moments from input
- treat authorship as factual context
- when multi-contributor, avoid unqualified first-person singular in overview, reflections, and closing note
- site/work events can appear only as lived experience, not formal contractor reporting
- do not output section labels inside arrays
- keep sparse days honest and concise
- do not invent events, feelings, or facts
- do not echo commands, report/PDF requests, weather requests, project switches, or app/debug chatter

Sanitization caps:

- overview: about 900 characters
- key moments: 12 items max
- reflections: 10 items max from AI, then renderer/model rules apply
- closing note: about 360 characters

### Journal media

Journal media rules:

- media must have `storagePath`
- media must have `dateKey` equal to the journal report date
- media from the previous Eastern day is not included
- linked media is kept when linked to a source log entry ID for the journal day
- linked media can survive even if strict journal text filtering removes that entry from the visible timeline
- project-scoped unlinked media is kept when its `projectId`/`projectSlug` matches the project or `_unassigned`
- projectless journal unlinked media is kept for the selected phone/day

The journal PDF embeds:

- linked photos under chronological journal entries
- additional photos not already rendered
- all uploaded photos for the same journal day, with no journal photo cap

### Journal PDF sections

The journal renderer can include:

- cover
- Day Overview
- Project Notes, when meaningful project notes exist
- Chronological Journal
- linked photos beneath timeline entries
- Key Moments
- Reflections
- Additional Photos
- Closing Note

## AI Daily Site Log Rules

OpenAI is optional for `dailySiteLog`. If unavailable, the deterministic model is used.

Daily site log AI must return strict JSON with:

- `executiveSummary`
- `weather.todaySummary`
- `weather.weeklyForecastRows`
- `manpower.rows`
- `manpower.summaryNote`
- `workCompletedInProgress.sections`
- `issuesDeficienciesDelays.items`
- `inspections.items`
- `concreteSummary.rows`
- `concreteSummary.narrativeNote`
- `openItems.rows`
- `openItems.narrativeNote`

AI constraints:

- use only curated input facts
- write like a superintendent daily site log
- do not write like a chatbot or SMS recap
- do not invent quantities, locations, trades, events, weather, or open items
- use authoritative weather only when supplied
- if weather lookup failed, say weather was unavailable
- do not echo commands, report requests, placeholders, bulk wrappers, or wrong embedded dates
- prefer trade-by-trade detail with locations, progress, constraints, coordination, and inspections
- avoid vague bullets when the input supports specifics
- keep manpower table clean
- do not repeat tabular manpower rows in `summaryNote`
- surface consultant/inspector details when present
- surface concrete location/scope, quantity, and status when present
- use empty arrays or `Not stated in field messages.` when no fact exists
- keep `weather.weeklyForecastRows` empty in current behavior

AI sanitization rejects:

- generic work trade labels
- fake trade headings
- admin chatter
- report-generation requests
- report layout/fix requests
- photo-in-report instructions
- bulk ingestion wrappers

## PDF, Storage, and Firestore Output Rules

Generated PDFs are saved to:

`dailyReports/{encodedPhoneE164}/{dateKey}/{reportType}/{fileName}`

Filenames:

- construction report: `Construction_Report_{Weekday}_{Month}_{Day}_{Year}_{NNN}.pdf`
- journal: `Journal_{Weekday}_{Month}_{Day}_{Year}_{NNN}.pdf`

Sequence numbers:

- sequence document ID is `{encodedPhoneE164}__{dateKey}`
- sequence is per phone and date, not per report type
- construction reports and journals generated for the same phone/date share the same sequence counter

Storage metadata includes:

- `phoneE164`
- `projectSlug`
- `reportDateKey`
- `reportType`
- `reportFileName`
- `reportSequence`
- Firebase download token

A `dailyReports` Firestore document is created with:

- phone
- project ID and name
- report type
- title
- filename
- sequence
- report date timestamp
- date key
- storage path
- download URL or download URL error
- message count
- log entry count
- media count
- whether unified day log exists
- whether AI narrative was applied
- weather snapshot for `dailySiteLog`
- created timestamp

Download URL behavior:

- the function first tries to create a signed URL
- if signing fails, it builds a Firebase token download URL
- dashboard and SMS responses return the URL when available
- otherwise they return the storage path

## Security and Read Rules

Firestore:

- clients cannot write `dailyReports`, `dailyPdfDeliveryQueue`, `dailyReportSequences`, or `logEntries`
- `dailyReports` can be read by admins or by members who can access the report's `projectId`
- `dailyPdfDeliveryQueue` and `dailyReportSequences` are not client-readable
- `logEntries` are admin-readable only

Storage:

- `dailyReports/**` can be read by approved operators
- clients cannot write `dailyReports/**`
- project media can be read by approved operators or management users with access to the project
- project media writes are limited to approved operators

## Behavior Confirmed by Tests

Tests confirm these important rules:

- construction report filenames use the `Construction_Report` prefix and zero-padded sequence
- journal filenames use the `Journal` prefix and zero-padded sequence
- daily report sequence IDs are stable by phone and date
- journal media keeps linked photos even when project ID is `_unassigned`
- journal media accepts a `Set` of entry IDs so linked photos survive strict journal text filtering
- journal media excludes photos whose `dateKey` is not the report day
- mixed weather/work notes do not pollute the weather section
- only actionable items enter the open items table
- journal timeline order is chronological and linked photos attach to timeline rows
- journal photo placeholders are kept as journal entries while AI/app chat is dropped
- real diary text survives when another text layer contains report-meta wording
- co-authored journal bundles include contributor labels
- source chunks preserve author labels for report updates
- daily journal SMS requests are parsed as `journal`
- structured logs parse backdated dates and manpower roll calls
- project prefixes are parsed from natural language
- meta inbound messages such as journal review follow-ups are not saved as logs
- wrong-project media and orphan-linked media are excluded from site logs
- field notes with photo wording remain reportable when they contain construction context
- Docksteader regression cases keep mis-tagged Home photos and meta lines out of the site report

## Implementation Notes

- Raw Firestore records are preserved; cleanup happens for PDF display and AI input.
- Deterministic report generation works without OpenAI.
- AI can enrich narrative and structured tables, but final output still passes local sanitizers.
- Project-scoped site logs are intentionally aggregate reports across all contributors on that project.
- Journal reports are intentionally less formal and preserve authorship and chronology.
- The dashboard UI's project requirement is stricter than the backend journal capability.
