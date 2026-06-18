# GridlineAI User Tutorial

This tutorial explains how to use GridlineAI for daily field reporting, journal notes, photos, voice updates, labour hours, reports, todos, lookahead schedules, and dashboard management.

Use this guide for onboarding new users. It is written for four groups:

- Field users who mainly text updates from site
- Labourers who submit hours by SMS
- Management users who review information and generate reports
- Admin users who manage access, projects, and setup

## 1. What GridlineAI Does

GridlineAI is a field reporting assistant connected to SMS, MMS, voice, and a web dashboard.

Users can:

- Text project updates
- Text personal or home journal notes
- Send photos by MMS
- Call the GridlineAI number and leave a voice message
- Log labour hours
- Ask for daily summaries and PDF reports
- Create and manage todos
- Ask lookahead schedule questions

Management can:

- Review inbound and outbound messages
- Review photos, voice notes, and transcripts
- Generate daily reports
- Generate journal reports
- Manage labourers and labour reports
- Upload lookahead schedules
- Generate lookahead reports
- Manage todos, approvals, projects, and team access

## 2. Before You Start

You need:

- The GridlineAI SMS/voice phone number
- Your phone number approved in GridlineAI
- The correct project assigned to your phone number
- Dashboard access if you are management or admin

If something is blocked, the most common cause is access setup. Ask an admin to check:

- Your approved phone number
- Your assigned project or projects
- Your role
- Whether your account is active

## 3. Main Ways To Use GridlineAI

There are three main ways to use the system.

### Text Message

Text the GridlineAI number with updates, questions, commands, labour hours, or report requests.

Example:

```text
daily log: formed the east wall and concrete truck was delayed 30 minutes
```

### Photo Message

Send photos by MMS. Add a short caption when possible.

Example:

```text
punch cracked tile at lobby entry
```

Attach the photo to the same message.

### Voice Message

Call the GridlineAI number and leave a recorded voice update. Use voice when the update is too long to type or when you are moving around site.

## 4. Basic SMS Commands

These commands are useful for most users:

```text
help
status
project docksteader
daily summary
daily report pdf
daily journal pdf
reset
contact
```

Use `help` if you forget the command format.

Use `status` to check the active context.

Use `project <project-slug>` when you need to switch the phone to a project.

Example:

```text
project docksteader
```

After that, normal site updates will be saved under that project.

## 5. Daily Site Updates By SMS

Daily site updates should be short, factual, and sent as work happens.

Good examples:

```text
daily log: ALC formed east foundation wall and installed embeds
progress duct rough-in complete at west corridor
inspection fire stopping reviewed on level 4
delivery rebar delivered to south gate
delay concrete truck arrived 45 minutes late
safety icy stairs at north entrance
punch broken tile at lobby
```

You can use either full commands or shorthand.

Full command:

```text
log safety: missing guardrail at roof edge
```

Shorthand:

```text
safety missing guardrail at roof edge
```

Supported site log categories include:

- Progress
- Notes
- Safety
- Delays
- Deficiencies
- Punch items
- Issues
- Deliveries
- Inspections
- General daily log updates

## 6. Personal Journal Or Home Notes

GridlineAI can also save personal or home-style notes as journal entries when the message clearly reads like a personal journal or home update.

Examples:

```text
home journal: boys helped make dinner and we cleaned the kitchen after
journal: today felt rushed but we got everyone to school on time
```

Plain home-style messages can also be routed to the journal when the context is clear:

```text
Boys eating dinner
Dinner being prepared by Ashley
```

Best practice for journal entries:

- Use `journal:` or `home journal:` for long personal entries
- Keep very long messages split into smaller parts
- If a long message times out, resend a shorter version
- Avoid wording that sounds like a labour report unless you are actually asking about labour

## 7. Photos And MMS

GridlineAI saves photos sent to the SMS number and links them to the message or log entry.

Use photos for:

- Progress evidence
- Deficiencies
- Safety issues
- Deliveries
- Site conditions
- Journal memories or home updates

Best practice:

- Send clear photos
- Include a short caption
- Mention location, trade, area, or issue
- Send related photos together when possible

Examples:

```text
progress waterproofing complete at north wall
```

```text
punch cracked tile at lobby entry
```

```text
safety open excavation beside west gate
```

## 8. Voice Updates

Use voice when typing is too slow.

Typical voice updates:

- End-of-day site summary
- Long site progress update
- Walkthrough notes
- Hands-free field notes

How to use:

1. Call the GridlineAI number.
2. Follow the prompt to leave a voice message.
3. Speak clearly.
4. Include the project, area, trade, and what happened.
5. Management can review the transcript in the dashboard.

Voice tips:

- Say dates and project names clearly
- Break long lists into short sentences
- Mention if something is safety, delay, delivery, deficiency, or general progress

## 9. Labour Hours By SMS

Labour hour logging is for registered labourers only. A labourer must exist in the Labour page and must be active.

Examples:

```text
labour 8.0 framing cleanup
8 hours forming elevator pit
labour 4.5 demolition, 3.5 cleanup
9 hrs
2 hrs fast fence
4 hrs housekeeping
3 hrs rough carpentry
```

Backdated labour entry example:

```text
2026-06-15 8 hours forming
```

Ask for your totals:

```text
how many hours today
how many hours this week
how many hours this pay period
my hours
```

Correct a mistake:

```text
I made a mistake, correct the hours to 9.5
```

Labour rules:

- One labour entry is allowed per labourer per date
- Corrections update an existing entry
- The labourer phone must be registered
- The project is usually taken from the labourer setup
- Very old or impossible dates may be rejected

If labour logging fails, ask management to check:

- Labourer phone number
- Labourer active status
- Assigned project slug
- Whether an entry already exists for that date

## 10. Daily Reports

Users can request summaries or PDF reports by SMS.

Examples:

```text
daily summary
daily report pdf
daily report yesterday
daily report docksteader
daily journal pdf
daily report journal home
daily report journal home 2026-06-17
```

Daily reports may include:

- Site log entries
- Photos
- Safety items
- Delays
- Deficiencies
- Deliveries
- Inspections
- Notes
- Weather snapshot, when available

Journal reports may include:

- Home or personal journal entries
- Journal photos
- Multiple contributors when more than one person posted notes

Important:

- The system sends real report links separately.
- Do not trust any manually typed or invented report link.
- If a link fails, use the dashboard Reports page.

## 11. Todo Workflows

Todos can be created by SMS or from the dashboard.

Create a todo:

```text
todo fix gate by next week
add todo replace laundry vent cover
create todo order supplies by Friday
```

List todos:

```text
show my todos
show open todos
show completed todos
todo status
```

Update todos:

```text
complete todo fix gate
close todo replace laundry vent cover
reopen todo order supplies
start todo paint garage vent
edit todo fix gate to fix west gate latch
```

Generate todo reports:

```text
todo report pdf
todo report excel
```

Dashboard todo features include:

- Create todos
- Add priority
- Add due dates
- Add reminders
- Add labels and tags
- Add comments
- Add sub-todos
- Generate PDF and Excel reports

## 12. Lookahead Schedule Workflows

Lookahead workflows are mostly used by management.

Dashboard workflow:

1. Open the Lookahead page.
2. Select or confirm the project context.
3. Upload a 3-week lookahead Excel workbook.
4. Parse and save the schedule snapshot.
5. Review extracted activities.
6. Generate lookahead activities or closeout reports.

SMS or Assistant examples:

```text
show me the activities for ALC for this week
ALC activities next week
generate the lookahead activities report
generate the lookahead closeout report
```

If lookahead queries fail, check that:

- The active project is correct
- A lookahead workbook was uploaded
- The workbook was parsed successfully
- A saved lookahead snapshot exists for the project

## 13. Notifications

Management can ask GridlineAI to send notifications.

Examples:

```text
inform management that crane delivery moved to 10am
notify all users on docksteader that gate 2 is closed
```

Use notifications for:

- Gate closures
- Schedule changes
- Safety notices
- Delivery changes
- Project-wide reminders

Use clear wording. Include who needs to know, what changed, and when.

## 14. Project Notes And Approvals

Project notes are shared context for a project. Depending on permissions, a note update may apply directly or create an approval request.

Example:

```text
update project notes: gate 2 only for crane access this week
```

If the user does not have approval rights, management can review the request in the Approvals page.

## 15. Dashboard Overview

Management and admin users use the web dashboard.

Common sections:

- Dashboard: overview and recent activity
- Assistant: send assistant messages from the dashboard
- Todo: manage todos, reminders, labels, tags, and reports
- Lookahead: upload schedules and generate lookahead reports
- Voice: review voice notes and transcripts
- Messages: inspect inbound and outbound SMS/MMS
- Reports: open generated reports
- Approvals: review pending approval items
- Labour: manage labourers and labour reports
- Projects: manage project records and project access
- Team: manage app members, phone numbers, and roles
- Tools: advanced admin utilities

## 16. Assistant Page Tutorial

Use the Assistant page when management wants to run the same workflow without texting from a phone.

Steps:

1. Open the Assistant page.
2. Select the phone context.
3. Confirm the active project.
4. Type the request.
5. Attach photos if needed.
6. Upload or transcribe a voice note if needed.
7. Send the request.
8. Review the reply and the saved output.

Good Assistant page tests:

```text
status
daily summary
daily report pdf
show me the activities for ALC for this week
update project notes: north stair closed until inspection
```

## 17. Labour Page Tutorial For Management

Use the Labour page to manage labourers and labour reports.

Common tasks:

1. Add or edit a labourer.
2. Confirm the phone number is in E.164 format, such as `+14165551234`.
3. Enter the display name.
4. Assign project slugs.
5. Confirm the labourer is active.
6. Review recent entries.
7. Check today, week, and pay-period totals.
8. Generate labour reports when needed.

When a labourer cannot log hours:

1. Check that the phone number matches the phone they text from.
2. Check that the labourer is active.
3. Check project assignment.
4. Check whether they already submitted hours for that date.
5. Check whether the date they entered is valid.

## 18. Reports Page Tutorial

Use Reports to find generated files.

Typical reports:

- Daily site reports
- Journal reports
- Labour reports
- Todo reports
- Lookahead activities reports
- Lookahead closeout reports

If a report link fails:

1. Open Reports.
2. Find the report by date, project, or type.
3. Open the stored report link.
4. Check debug or error text if present.
5. Regenerate the report if needed.

## 19. Messages And Voice Review

Use Messages to inspect SMS and MMS traffic.

Use Messages to answer:

- What did the user send?
- What did GridlineAI reply?
- What command was detected?
- Which project was used?
- Was AI used?
- Was there an error or timeout?

Use Voice to answer:

- Who called?
- What was transcribed?
- Was the transcript saved?
- Is there a linked recording?

## 20. Team And Project Access

Use Team and Projects to manage access.

Check these when a user is blocked:

- Approved phone number
- User email
- Role
- Active status
- Project slugs
- Project ownership or approval permissions

Role guide:

- Admin: full setup and management access
- Management: daily operations, reports, labour, todos, lookahead, messages, and assistant workflows
- User or labourer: mostly phone-based use, depending on setup

## 21. Best Practices

For field users:

- Text updates as the work happens
- Use simple category words like `safety`, `delay`, `delivery`, `inspection`, and `progress`
- Send photos with captions
- Use voice for long updates
- Set the project before sending project-specific updates

For labourers:

- Submit hours once per day
- Include total hours and work performed
- Use dates only when backdating
- Correct mistakes as soon as possible

For management:

- Review Messages and Voice daily
- Generate reports at the end of the day
- Keep labourers and project assignments current
- Upload lookahead schedules before asking schedule questions
- Use the Assistant page to test unclear requests

For admins:

- Keep Team records clean
- Remove stale access
- Confirm project slugs are consistent
- Review approvals regularly
- Watch for failed report links or function errors

## 22. Troubleshooting

### The assistant saved my message under the wrong project

Text:

```text
project correct-project-slug
```

Then resend the update.

### The assistant did not understand my text

Rewrite it with a clear command:

```text
log note: ...
safety ...
delay ...
daily report pdf
```

### A long journal entry timed out

Split it into shorter messages and resend. Use `journal:` or `home journal:` at the start.

### A labourer cannot log hours

Check the Labour page:

- Phone number
- Active status
- Project assignment
- Duplicate entry for the same date

### A report did not arrive

Check:

- Reports page
- Project context
- Whether there was data for that date
- Whether a report was generated but the signed URL failed

### Lookahead activities are missing

Check:

- Project context
- Uploaded workbook
- Parsed schedule snapshot
- Trade name spelling
- Date range, such as this week or next week

## 23. Quick Reference

Project:

```text
project docksteader
status
```

Daily logs:

```text
daily log: concrete pour complete at east wall
safety missing guardrail at roof edge
delay pump breakdown delayed pour 30 minutes
delivery rebar delivered to south gate
inspection fire stop reviewed level 4
```

Journal:

```text
journal: today was busy but everyone got to school on time
home journal: dinner with the boys and cleaned up kitchen
```

Reports:

```text
daily summary
daily report pdf
daily report yesterday
daily journal pdf
```

Labour:

```text
labour 8.0 framing cleanup
9 hrs
2 hrs fast fence
4 hrs housekeeping
3 hrs rough carpentry
how many hours this week
correct the hours to 9.5
```

Todos:

```text
todo fix gate by Friday
show open todos
complete todo fix gate
todo report pdf
```

Lookahead:

```text
show me the activities for ALC for this week
ALC activities next week
generate the lookahead activities report
generate the lookahead closeout report
```

Notifications:

```text
notify all users on docksteader that gate 2 is closed
inform management that crane delivery moved to 10am
```

