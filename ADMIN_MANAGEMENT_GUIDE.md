# Admin And Management Guide

This guide is for office staff, management, and admins using the dashboard.

## 1. Main dashboard areas

Use these sections most often:

- `Assistant`
- `Reports`
- `Labour`
- `Lookahead`
- `Voice`
- `Messages`
- `Projects`
- `Team`
- `Approvals`
- `Tools`

## 2. Assistant workflow

Use the `Assistant` page to:

- pick a phone context
- confirm the active project
- type a request
- attach photos
- transcribe and send a voice note

Useful requests:

```text
daily summary
daily report pdf
update project notes: gate 2 only for crane access this week
inform management that crane delivery moved to 10am
notify all users on docksteader that gate 2 is closed
show me the activities for ALC for this week
generate the lookahead activities report
generate the lookahead closeout report
```

## 3. Messages and voice review

Use `Messages` to inspect inbound and outbound traffic.

Use `Voice` to:

- review transcripts
- confirm voice content
- inspect linked recordings

## 4. Reports

Use `Reports` to review generated:

- daily PDF reports
- labour reports
- todo reports
- lookahead reports

If a report fails:

- check signed URL/debug text
- confirm the underlying project context
- confirm required inputs exist

## 5. Labour management

Use `Labour` to:

- register labourers
- assign project slugs
- verify active status
- review entries
- review weekly and pay-period totals
- generate labour reports

Common management checks:

- is the phone number correct
- is the labourer active
- is the project slug assigned

## 6. Lookahead management

Use `Lookahead` to:

- upload a 3-week lookahead workbook
- parse and save the schedule snapshot
- generate activities reports
- generate closeout reports

In `aigridline`, there is also an activities-PDF-to-lookahead conversion workflow.

Important rule:

- if users ask for lookahead activities by SMS, the project must already have a saved parsed lookahead snapshot

## 7. Todo management

Use `Todo` to:

- create and edit todos
- set due dates and reminders
- apply labels and tags
- generate PDF or Excel todo reports

## 8. Team and project access

Use `Team` and `Projects` to manage:

- approved phone numbers
- member roles
- project assignments
- project ownership
- note approval rights

If a user cannot do something, check:

1. approved phone number
2. assigned project slug
3. active member status
4. role level

## 9. Approvals

Use `Approvals` for review-driven workflows, especially project note edit requests.

## 10. Common operating routine

Daily:

1. review inbound messages and voice items
2. generate daily reports as needed
3. check labour entries and corrections
4. review approvals

Weekly:

1. update lookahead schedules
2. generate lookahead reports
3. generate labour reports
4. clean up team/project access issues

## 11. Troubleshooting

- If assistant actions fail, test the same request from the Assistant page.
- If labour corrections fail, confirm there is already a labour entry for that date.
- If lookahead report generation fails, confirm a parsed schedule exists.
- If note edits fail, confirm permission and project access.

For the full reference, see [USER_MANUAL.md](/home/marwan/Documents/ChatBot/USER_MANUAL.md:1).
