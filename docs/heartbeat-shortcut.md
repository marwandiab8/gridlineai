# The heartbeat: Health Auto Export does it, the Shortcut is optional

**You already have a heartbeat.** Health Auto Export posts to GridlineAI several times a day (four to eight
on a typical day, with a gap overnight), and since 2026-11-15 every export is recorded as a heartbeat
(`heartbeats/{member}`, the `health*` fields): the phone is alive, and the day's step count says whether it
went anywhere. Thousands of steps (3,000 or more) since OwnTracks last reported, with OwnTracks quiet for
6 hours, means OwnTracks has stopped: you are texted at once, once a day at most. Under 1,500 steps while
OwnTracks is quiet reads as a day at home, so the old "OwnTracks silent" text no longer fires for that. A day
without any export is texted at the next check. Nothing to set up.

**The Shortcut below is optional.** It adds a position, which is sharper than steps: a drive to the gym and
back with the phone in a cup holder adds few steps, but a heartbeat from the gym's parking lot is proof the
phone moved. If the step-based check ever misses something you care about, set it up; otherwise skip it.

Three times a day the iPhone tells GridlineAI "I am alive, and I am here". That is all the tracking health
check needs to tell a quiet day at home (OwnTracks has nothing to report) from OwnTracks having stopped
(the phone moved, OwnTracks said nothing): the second case is texted within hours instead of a day and a
half. A heartbeat is an ordinary OwnTracks location ping tagged `t: "h"`, posted to the same endpoint with the
same token; the server stamps the time, uses it as evidence for open stays, keeps it out of the drive
detector, and records it in `heartbeats/{member}`.

## Set it up once (about five minutes)

1. **Shortcuts app → Shortcuts tab → +**. Name it `Tracking heartbeat`.
2. Add the action **Get Current Location**.
3. Add the action **Get Contents of URL** and set:
   - URL: the OwnTracks endpoint you already use in OwnTracks' Settings → Connection
     (`https://<app-domain>/api/integrations/owntracks/events`).
   - Method: **POST**.
   - Headers: `Authorization` = `Bearer <your integration token>` (the token from the dashboard Setup page,
     the same one OwnTracks uses), and `Content-Type` = `application/json`.
   - Request Body: **JSON**, with these fields:
     - `_type` (Text) `location`
     - `t` (Text) `h`
     - `tid` (Text) `HB`
     - `lat` (Number) → tap the value, choose the **Current Location** variable, then **Latitude**
     - `lon` (Number) → Current Location → **Longitude**
4. Run it once by hand. The reply is `[]`, which is the OwnTracks "received" answer. Allow location when asked.
5. **Settings → Privacy & Security → Location Services → Shortcuts → Always**, so the automation can read the
   location while the phone is locked.
6. **Shortcuts → Automation tab → +**: **Time of Day**, 7:00 AM, Daily, **Run Immediately** (not "after
   confirmation"), **Notify When Run** off, then pick `Tracking heartbeat`. Repeat for **1:00 PM** and
   **7:00 PM**.

## What you get

- "No heartbeat from the phone since …" after 14 hours without one: the automation is off, or the phone is.
- "Your phone has moved N km since OwnTracks last reported …" the moment a heartbeat arrives from somewhere
  else while OwnTracks has been quiet for 3 hours: OwnTracks stopped. Open it once; location access Always.
- No more "OwnTracks silent" text after a quiet weekend at home: the heartbeats show the phone has not moved,
  so the silence is expected.

## Checking

`scripts/tracking-status.sh` prints the last heartbeat, the last OwnTracks point, the last health check and
any stop waiting to be named. In Firestore the heartbeat is `heartbeats/{member email}` (`lastAt`, `count`,
`movedSinceOwnTracksMeters`).
