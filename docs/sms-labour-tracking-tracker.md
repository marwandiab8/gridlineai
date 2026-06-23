# SMS Labour Tracking Tracker

## Purpose

Track brainstormed, planned, implemented, and discarded ideas for SMS-first labour hours, labour cost, material usage, and Excel reporting workflows.

Primary goal: make labour tracking useful through text messages alone for labourers, owners, project managers, supervisors, and management, while keeping records structured enough for budget review, cost tracking, dashboard use, and reports.

## Status Legend

- `proposed`: captured from brainstorm, not yet designed
- `accepted`: agreed direction, ready for design or implementation
- `designing`: needs data model, command flow, or UI/report design
- `implementing`: active code work
- `done`: implemented and verified
- `discarded`: intentionally dropped

## Tracker

| ID | Status | Idea | Primary Users | SMS Examples | Main Work Areas | Notes / Open Questions |
| --- | --- | --- | --- | --- | --- | --- |
| LAB-001 | done | Query total labour hours across all projects for a date range. | Owners, PMs, management | "give me all total hours on my all projects for today" | SMS parser, permissions, labour query service, reply formatting | Implemented deterministic parsing for all-project wording, today, exact dates, last week, and past 2 weeks. Access remains membership/all-project scoped. |
| LAB-002 | done | Query total labour hours for one specific project. | Owners, PMs, management, supervisors | "give me hours for Docksteader today" | SMS parser, project resolution, labour query service | Existing project-scope totals now benefit from exact date, last week, and past 2 weeks parsing. |
| LAB-003 | designing | Query total labour hours for one specific labourer. | Owners, PMs, management, supervisors | "give me hours for labourer John Smith last week" | SMS parser, labourer lookup, labour query service | Implemented explicit "for labourer/worker/person NAME" parsing and permission-scoped filtering. Still needs friendlier bare-name matching like "hours for John". |
| LAB-004 | proposed | Generate an Excel sheet for any labour-hours query. | Owners, PMs, management | "send me Excel for labour hours today" / "Excel for John's hours last week" | Excel generation, storage, SMS link delivery, dashboard/report integration | Use existing Excel dependencies where available. Need expiry/access rules for download links. |
| LAB-005 | proposed | Add cost codes to labour entries. | Management, PMs, supervisors, labourers | "2 hours cost code 03100 installing forms" | Data model, SMS parser, dashboard edit/review, reports | Need cost-code setup: free text vs managed list. Should support optional requirement by project/management. |
| LAB-006 | proposed | Allow labourers to include material usage inside labour/report text. | Labourers, supervisors | "2 hours installing safety railing on level 2 used 5 lengths of 2x4 lumber" | SMS parser, data model, material records, labour entry linkage | Need parsing for quantity, unit, material name, location, project, date, and relation to labour entry. |
| LAB-007 | proposed | Query material usage for today or a date range. | Labourers, owners, PMs, management | "show me the material I used today" / "show materials used last week" / "show material used 2026-06-15" | SMS parser, material query service, date parsing, reply formatting | Needs "I used" to resolve to sender's records; management may need all-user/project queries. |
| LAB-008 | proposed | Query material usage for a specific project, labourer, or date. | Owners, PMs, management, supervisors | "show materials for Docksteader past 2 weeks" | SMS parser, project/labourer filters, material query service | Should share query/filter logic with labour-hours queries. |
| LAB-009 | proposed | Include labour costs in labour summaries when rates/cost codes are available. | Owners, PMs, management | "what did labour cost today" | Labour cost model, permissions, rate storage, reporting | Sensitive data. Labourers probably should not see cost unless permitted. |
| LAB-010 | proposed | Make labour and material query results report-ready. | Owners, PMs, management | "send weekly labour and materials report" | PDF/Excel reports, dashboard report view, SMS link delivery | Could become a combined labour/material report. |

## Actionable Function Breakdown

### SMS Intent Functions

- Parse labour summary requests by date range, project, labourer, and all visible projects.
- Parse Excel export requests attached to any labour/material query.
- Parse cost-code-bearing labour entries.
- Parse material usage embedded in labour entries or standalone material messages.
- Parse material summary requests by date range, project, labourer, sender, and all visible projects.
- Ask short follow-up questions when project, labourer, date, or export format is ambiguous.

### Data / Backend Functions

- Normalize date ranges from natural language: today, yesterday, last week, past 2 weeks, and exact dates such as `2026-06-15`.
- Resolve sender role and project visibility before returning all-project, cost, or management-level summaries.
- Query labour entries by project, labourer, sender, date range, and cost code.
- Query material usage by project, labourer/sender, date range, material name, and linked labour entry.
- Store optional cost code on labour entries.
- Store material usage records with quantity, unit, material name, location, project, date, author, source message, and linked labour entry when applicable.
- Generate Excel files from the same query filters used by SMS replies.
- Deliver downloadable report/export links by SMS with appropriate access controls.

### Dashboard / Reporting Functions

- Show cost code on labour entries and allow management edits where permitted.
- Show material usage linked to labour entries and reports.
- Add filters for project, labourer, date range, material, and cost code.
- Add Excel export for labour and material result sets.
- Include material usage and cost codes in relevant PDFs/reports when requested.

## Implementation Order Draft

1. Design shared query model for labour/material filters and permissions.
2. Add cost code field support to labour entries without requiring it everywhere.
3. Add material usage data model and parser support.
4. Build labour/material SMS query replies.
5. Add Excel export for the same queries.
6. Add dashboard/report visibility and filters.
7. Add cost totals where rates and permissions exist.

## Implementation Log

- 2026-06-23: Implemented first labour SMS query slice in both repos. Added deterministic parsing for all-project wording, exact dates like `2026-06-15`, `last week`, `yesterday`, and `past 2 weeks`; wired management labour totals to support explicit labourer-name filters; added focused parser tests.

## Open Decisions

- Should cost codes be free text, a project-specific managed list, or both?
- Who can see labour cost dollars versus only labour hours?
- Should material tracking be attached to labour entries, daily reports, standalone material records, or all three?
- Should Excel exports be generated on demand only, or saved as report records?
- How long should SMS export links remain valid?
- What exact roles count as "owner", "management", and "project manager" in permissions?
