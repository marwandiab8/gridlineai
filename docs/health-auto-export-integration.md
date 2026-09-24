# Apple Health Integration (via Health Auto Export)

Apple only allows Health data to be read on-device - there is no way for this server to pull it
directly. [Health Auto Export](https://www.healthyapps.dev) is a paid App Store app that runs on
your iPhone, reads Health data with your permission, and exports it on a schedule you configure to
any URL that accepts an HTTP POST. This connects it to GridlineAI, which forwards sleep, workouts,
and daily step counts on to TimeLeftToLive.

## Endpoint

`POST https://gridlineai.web.app/api/integrations/health/export`

## Authentication

**Reuses the exact same token as the iOS Shortcuts / OwnTracks integrations** - generate/find it
in the dashboard's **Tools -> iOS Shortcuts Integration** section; do not regenerate it if other
integrations are already using it, or those will stop working too (regenerating replaces the one
active token for the whole account).

## App setup

1. Install **Health Auto Export** and grant it Health access for whatever you want tracked
   (Sleep Analysis, Workouts, Step Count, at minimum).
2. Go to **Automations** (or **Export -> Automations**, depending on app version) and create a new
   **REST API** automation.
3. **URL**: `https://gridlineai.web.app/api/integrations/health/export`
4. **Format**: JSON (not CSV - CSV is not supported by this endpoint).
5. **Add Headers**, then add: `Authorization: Bearer <token>`
6. **Metrics to include**: at minimum `Sleep Analysis` and `Step Count`; enable **Workouts**
   separately if the app's version splits it out from the metrics list.
7. **Schedule**: once or twice a day is plenty - sleep and step totals only change meaningfully
   over the course of a day, and workouts are already complete records once finished. A tighter
   schedule just re-sends data that hasn't changed (harmless - see Deduplication below - but
   pointless).
8. Run **Manual Export** once to confirm it works before relying on the schedule (see Verifying
   it works, below).

> I wrote this from Health Auto Export's own documented export/automation format, not a live look
> at your installed app's exact screen - its menus have moved around across versions. If a setting
> isn't where this doc says, look for "Automations" or "REST API" under Export/Settings; the
> URL + custom header + JSON format are the only three things that actually matter.

## What gets sent, and what this endpoint does with it

Health Auto Export posts one JSON body per export:
`{"data": {"metrics": [...], "workouts": [...] , ...other keys}}`. This endpoint only reads
`sleep_analysis` and `step_count` out of `metrics`, plus `workouts`; anything else in the payload
(state of mind, medications, ECG, etc.) is ignored, not an error.

- **Sleep** becomes a `sleep_session` event with a real start/end (the night's actual asleep
  window, not the broader in-bed window) - it shows as a genuine time block, the same as a Work or
  Home session, not a single point-in-time moment.
- **Workouts** become `completed_workout` events, each with the workout's own start/end (or
  duration, if Health Auto Export didn't send an end time), distance, calories, and average heart
  rate where available.
- **Step Count** samples for the same day are summed into one `daily_steps` total per day and
  recorded as a Moments-list entry (e.g. "8,500 steps") - steps are a running count, not a
  session, so they never invent duration on the ring.

## Deduplication

Health Auto Export's schedule typically re-exports a rolling window (e.g. "last 3 days") on every
run, so the same night's sleep or the same workout will usually be sent more than once. This is
expected and harmless: every record carries a stable ID derived from its own data (a sleep night's
exact start/end, or a workout's own Apple Health UUID), so re-sending it just resolves as a
duplicate instead of creating a second entry.

**A day's step count is kept up to date.** Steps are the one exception to "immutable once
recorded": each day is one record keyed by date, but the *total* for that day keeps changing as
Health finishes syncing more samples for it (from a phone and a Watch, reconciled over time).
TimeLeftToLive's ingestion updates a `daily_steps` record in place when a later export carries a
different total for the same day, so every export refreshes the day's count (and re-exported
past days are corrected too). Sleep and workouts stay immutable, since a finished workout or a
completed night's sleep genuinely does not change afterward - a changed payload for those is still
an `idempotency_conflict`. `stepsRevised` in the response only counts a step conflict from a
TimeLeftToLive deployment that predates in-place step updates.

## Verifying it works

Run **Manual Export** in the app, then check the dashboard's activity log / TimeLeftToLive's
activity dashboard for new Sleep, Workout, or a "N steps" Moments entry. To test the endpoint
directly:

```bash
curl -i -X POST 'https://gridlineai.web.app/api/integrations/health/export' \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "data": {
      "metrics": [
        {"name": "sleep_analysis", "data": [{"totalSleep": 7.5, "sleepStart": "2026-09-21 23:00:00 -0400", "sleepEnd": "2026-09-22 06:30:00 -0400"}]},
        {"name": "step_count", "data": [{"qty": 8500, "date": "2026-09-22 14:00:00 -0400"}]}
      ],
      "workouts": [
        {"id": "test-workout-1", "name": "Running", "start": "2026-09-22 07:00:00 -0400", "end": "2026-09-22 07:30:00 -0400"}
      ]
    }
  }'
```

A successful response is `200` with a body like
`{"ok":true,"received":{"sleep":1,"workouts":1,"steps":1},"delivered":3,"duplicates":0,"failed":0,"stepsRevised":0}`.
