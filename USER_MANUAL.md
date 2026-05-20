# User Manual: `gridlineai` and `aigridline`

This manual explains how to use both projects as day-to-day field reporting and management systems.

The two projects are very similar:

- `gridlineai` repo: `/home/marwan/Documents/ChatBot`
- `aigridline` repo: `/home/marwan/Documents/aigridline`

Both support:

- SMS-based reporting
- assistant-driven workflows
- daily PDF reports
- labour hour tracking
- lookahead schedule workflows
- voice message intake
- issue / deficiency workflows
- dashboard-based admin and management tools

## 1. Core idea

A user can interact with the system in 3 main ways:

1. By texting the Twilio number
2. By calling the Twilio number and leaving a recorded voice message
3. By using the web dashboard

The backend saves messages, media, voice transcripts, reports, and project activity into one workflow.

## 2. Roles

The dashboard uses role-gated views.

- `admin`: full access to dashboard, projects, team, tools, reports, approvals
- `management`: assistant, todo, lookahead, voice, labour, reports, tools
- `user` / labourer: mostly interacts by SMS and voice, not by full dashboard admin tools

If a phone number or app member is not approved or assigned to a project, some actions will be blocked.

## 3. Dashboard sections

Both dashboards include these major sections:

- `Dashboard`: high-level activity and shortcuts
- `Assistant`: send assistant messages directly from the dashboard
- `Todo`: manage home/project todo workflows
- `Lookahead`: parse lookahead schedules and generate lookahead reports
- `Voice`: review voice messages and recordings
- `Messages`: inspect inbound and outbound SMS/MMS
- `Reports`: view generated reports
- `Approvals`: review pending approval items
- `Labour`: manage labourers and labour reports
- `Projects`: manage projects
- `Team`: manage app members / access
- `Tools`: secondary admin tools

## 4. How normal users use the system

### SMS daily reporting

Users can text plain site updates, for example:

```text
daily log: formed east wall, installed embeds, crane delayed 45 minutes
```

Or use shorthand:

```text
safety icy stairs at north entrance
delay concrete truck arrived late
punch broken tile at lobby
log delivery rebar delivered to south gate
```

Typical supported log types:

- safety
- delay
- deficiency / punch
- issue
- delivery
- note
- progress
- inspection
- daily log / daily summary

### Personal journal / home-style notes

If the message is clearly a personal diary or journal update, the assistant can save it to journal instead of treating it as a question.

### Ask the assistant questions

Examples:

```text
status
help
what happened today
daily summary
daily report pdf
project docksteader
update project notes: crane access only from gate 2 this week
```

### Send photos

Users can text photos by MMS. The system stores the images and links them to the reporting workflow automatically.

### Send voice updates

Users can call the Twilio number and leave a recorded voice message. The system can transcribe and process that voice note in the same workflow.

## 5. Labour workflow

Labourers can text labour entries directly.

Examples:

```text
labour 8.0 framing cleanup
8 hours forming elevator pit
labour 4.5 demolition, 3.5 cleanup
```

Supported labour usage includes:

- daily labour entry by SMS
- corrections to previously entered hours
- labour balance queries
- labour PDF report generation

Examples:

```text
how many hours today
how many hours this week
how many hours this pay period
I made a mistake, correct the hours to 9.5
```

In the dashboard Labour page, management can:

- register labourers
- assign labourers to project slugs
- review recent labour entries
- review daily / weekly / pay-period totals
- generate labour reports

## 6. Assistant page

The Assistant page is the direct management console for composing assistant messages.

Typical use:

1. Select the user phone number
2. Confirm the active project context
3. Enter a message
4. Optionally attach photos
5. Optionally attach or transcribe a voice note
6. Send to assistant

Use this page when management wants to test or drive the same assistant workflow without using the phone directly.

## 7. Todo workflow

The Todo page manages a structured todo system.

Capabilities include:

- create todo items
- set priority
- set due dates
- add labels and tags
- add reminders
- add comments
- generate PDF / Excel todo reports

Assistant-driven todo creation is also supported. Examples:

```text
todo fix gate @site by next week
```

## 8. Lookahead workflow

The Lookahead page is used to process schedule / planning documents.

Capabilities in both projects:

- upload a 3-week lookahead Excel workbook
- parse and summarize the workbook
- generate lookahead activities report PDFs
- generate lookahead closeout report PDFs
- use active project context from a selected phone

Assistant-side lookahead usage can include:

```text
show me the activities for ALC for this week
ALC activities next week
generate the lookahead activities report
generate the lookahead closeout report
```

## 9. Voice workflow

The Voice page lets management review:

- recent voice activity
- transcripts
- linked recordings

Use this page to check what was spoken, confirm the transcript, and trace it back to project activity.

## 10. Reports workflow

The Reports page is where generated report files appear.

Typical report types:

- daily PDF reports
- labour reports
- todo reports
- lookahead activities reports
- lookahead closeout reports

The dashboard can also show report debug context such as signed URL problems and weather snapshot debug on daily reports.

## 11. Team and project access

Admins can manage:

- app members
- approved phone numbers
- project assignments
- project ownership
- note approval permissions

If a workflow fails because a user is “not approved” or “not assigned”, check the Team and Projects sections first.

## 12. Approvals

Use the Approvals page for items that require review before applying changes.

A common example is project note edit requests submitted by non-approver users.

## 13. Notifications and broadcast messages

Management can trigger notifications through the assistant.

Examples:

```text
inform management that crane delivery moved to 10am
notify all users on docksteader that gate 2 is closed
```

## 14. Common SMS commands

Examples of useful commands:

```text
help
status
project docksteader
reset
contact
daily summary
daily report pdf
update project notes: north stair closed until inspection
labour 8.0 framing cleanup
how many hours this week
start timer for concrete pour
stop timer
```

## 15. Main differences between the two projects

### `aigridline`

`aigridline` currently includes:

- activities-PDF-to-lookahead conversion workflow in the Lookahead page
- project membership backfill/admin tooling
- the newer assistant action-routing behavior

### `gridlineai`

`gridlineai` currently includes:

- the same newer assistant/correction behavior after sync
- minutes-based labour storage and correction model
- reminder / push-related functions not present in exactly the same way in `aigridline`

## 16. Best practice workflow

For site staff:

1. Text updates as they happen
2. Send MMS photos with context
3. Call in a voice note when typing is too slow
4. Use simple correction messages when a mistake was sent

For management:

1. Review messages and voice activity
2. Use Assistant for follow-up and structured requests
3. Generate daily, labour, todo, and lookahead reports
4. Keep Team and Projects assignments current
5. Review approvals and note edits promptly

## 17. Troubleshooting

If something does not work:

- If a user cannot perform an action, check Team approval and project assignment
- If a labourer cannot log hours, confirm the labourer exists and is active
- If lookahead queries fail, confirm a lookahead schedule has already been uploaded and parsed
- If a report link fails, check the Reports page and dashboard debug output
- If SMS actions behave oddly, test the same request from the Assistant page

## 18. File location

This manual is stored in:

- `gridlineai`: [USER_MANUAL.md](/home/marwan/Documents/ChatBot/USER_MANUAL.md:1)
- `aigridline`: `/home/marwan/Documents/aigridline/USER_MANUAL.md`
