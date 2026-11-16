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

The token is the one your other Shortcuts already send, so start from one of them instead of typing it.

1. **Shortcuts app → Shortcuts tab**: press and hold **Log this place** → **Duplicate**. Rename the copy
   `Tracking heartbeat`. Delete every step except **Get Current Location** and the first **Get Contents of
   URL**; the `Authorization` header with the token stays in it.
   (Nothing to copy from? Build those two actions and add the header `Authorization` = `Bearer <token>`.
   The dashboard Setup page shows the token only when it is generated, and regenerating it means updating
   OwnTracks and every other Shortcut too.)
2. In **Get Contents of URL** set:
   - URL: `https://gridlineai.web.app/api/integrations/owntracks/events`
   - Method: **POST**
   - Request Body: **JSON**, with exactly these fields (remove any others):
     - `_type` (Text) `location`
     - `t` (Text) `h`
     - `tid` (Text) `HB`
     - `lat` (Number) → tap the value, pick the **Current Location** variable, tap the inserted variable and
       set **Get: Latitude**
     - `lon` (Number) → Current Location → **Get: Longitude**
   Shortcuts sends the JSON content type itself; a `Content-Type: application/json` header does no harm.
3. Run it once by hand (the ▶ button). Allow location when asked. The reply is `[]`, OwnTracks' "received"
   answer, and `scripts/tracking-status.sh` then shows `last heartbeat` with count 1.
4. **Settings → Privacy & Security → Location Services → Shortcuts → Always**, so the automation can read the
   location while the phone is locked.
5. **Shortcuts → Automation tab → +** (New Automation) → **Time of Day**: 7:00 AM, Daily, **Run Immediately**
   (not "Run After Confirmation"), **Notify When Run** off → **Next** → choose `Tracking heartbeat`. Repeat
   for **1:00 PM** and **7:00 PM**.

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
