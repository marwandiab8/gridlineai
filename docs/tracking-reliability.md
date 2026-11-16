# Tracking reliability: how a day gets recorded, and what keeps it right

The goal is a Time Left To Live timeline that is right on its own: where Marwan was, for how long, doing
what, with nothing to tap during the day. This page is the chain from the phone to the wheel, what each
link does when the one before it fails, and the checklist that keeps the phone side healthy. It was written
on 2026-11-15 after a darts night at the Raging Bull went missing (see "The night that went wrong").

## The chain

1. **OwnTracks on the iPhone** (HTTP mode, "significant changes" monitoring) posts to
   `ownTracksEvents`: a *transition* when a saved Region (Home, Work, Gym, or a place's name) is entered or
   left, and *location pings* as the phone moves. Every message carries coordinates and a time.
2. **GridlineAI turns messages into events** (`iosShortcutEvents`):
   - a Region crossing is `arrive_home` / `leave_home` (Work, Gym likewise), or `arrive_location` /
     `leave_location` for any other Region name, carrying the matching Known Place's activity;
   - pings drive the **drive detector** (`driveDetection.js`): `start_drive` when you set off,
     `finish_drive` when you have stopped for 7 minutes (or the pings stop for 20);
   - a `finish_drive` at one of your **Known Places** is a **visit** (`autoVisits.js`): `arrive_location`
     with the place's activity ("Darts at Raging Bull"), unless OwnTracks already reported that arrival
     as a Home/Work/Gym crossing;
   - any later evidence that you are elsewhere (the next drive starting, a ping across town, an arrival
     somewhere else) **closes the open stay** (`stayEvidence.js`) and records the `leave_location` with
     the stay's length, so Time Left counts the whole visit.
3. **Delivery to Time Left** happens twice, on purpose: inline as the event is recorded, and again from
   the `syncIosShortcutEventToTimeLeft` trigger / the 15-minute `retryIosShortcutEventsToTimeLeft` sweep.
   Time Left keys every event by its source record id, so the second delivery is a harmless "duplicate".
   The delivery state lives on the event (`timeLeftDelivery.status`).
4. **Time Left pairs boundaries into sessions** (`src/utils/lifeEventUtils.js` there): arrive + leave of
   the same place is a session; an arrival with no departure yet is "open" and keeps running (Home for
   up to 7 days, everything else 36 hours) until something later shows it ended; every minute is given
   to exactly one activity.

## What guards each link

| Failure | Guard |
| --- | --- |
| A visit to a place with no OwnTracks Region (the Raging Bull) | The drive that ended there logs the visit from the Known Place. Nothing to tap. |
| OwnTracks misses a Region exit | The next drive start, or a ping away from the place, closes the stay at the right time. |
| OwnTracks goes silent (killed, no location permission after an iOS update) | `trackingHealthCheck` (9 am and 6 pm) texts once when nothing has arrived for 36 hours, and once a day while it lasts. |
| An arrival at work or the gym with no departure | The same check texts after 18 hours. |
| A drive left open | `closeStaleDrives` every 5 minutes; the health check texts if one is stuck for 3 hours. |
| An event that did not reach Time Left | Retried every 15 minutes for 7 days; the health check texts about anything from the last day still not delivered. |
| The trigger functions running older code than the HTTP endpoints (every place visit answered "conflict" from Oct 11 to Nov 15) | Deploy the tracking functions together: `npm run deploy:tracking` in `functions/`; `scripts/check-functions-drift.sh` says when code is newer than a deploy. |
| A session that looks wrong in Time Left | Edit its times. Deleting an arrival or departure is final for the phone (a tombstone stops it being re-sent); a deleted activity can be restored from the recycle bin for 90 days. |
| A departure closed by a drive start being dropped as a "duplicate" of the drive (bug fixed 2026-11-15) | `stayClosing.stayRequest` gives the departure its own idempotency key. |
| A visit to a place you have never saved | The drive that ended there remembers the spot; when you leave, a stay of 15+ minutes is named from OpenStreetMap, saved, logged, and confirmed by one text (below). |
| OwnTracks stops while the phone keeps moving | The heartbeat Shortcut (below) shows the phone elsewhere; texted at once. |

## Phone checklist (once, and after every iOS update)

- OwnTracks → Settings: Mode **HTTP**, the GridlineAI URL and token; **Location access: Always**,
  **Precise location on**, Background App Refresh on for OwnTracks; monitoring **Significant** is enough.
- Regions named exactly `Home`, `Work`, `Gym`, plus any place you want logged the moment you arrive
  (a Region named like a Known Place, `Raging Bull`, logs that place's activity). Places you drive to
  and park at are logged without a Region, from the drive.
- The heartbeat Shortcut and its three automations (`docs/heartbeat-shortcut.md`), once; Shortcuts' location access set to Always.
- Never force-quit OwnTracks. If the health check texts "No location data since …", open the app once:
  it should show your position; if it asks about location again, choose Always.
- Known Places page in GridlineAI: each place has an activity (Darts, Gym, Grocery shopping…); set it
  once and every visit carries it. A place's radius (default 120 m, 400 m for big lots) decides what
  counts as "parked there".

## When something still looks wrong

Do not delete arrivals or departures to tidy a session. Tell Claude what happened (where, roughly when)
or edit the times in Time Left. Restoring a deleted event: Time Left → Recently deleted → Restore (the
`restoreActivityEntry` function lifts the tombstone and puts the event back). A worked repair that did it
by hand is `scripts/repair-home-2026-11-15.cjs`.

## The night that went wrong (Fri 2026-11-13)

OwnTracks recorded leaving home 6:58 pm, the drive that ended at the Raging Bull 7:34 pm (18 m from the
saved place), the drive back 9:57 pm and arriving home 10:17 pm. The pub visit itself was never logged:
"Log this place" was not run and there was no Region. Time Left then showed the Friday arrival as
"incomplete" (an open arrival older than 36 hours), the weekend as no Home at all, and the arrival was
deleted from the dashboard, which left a tombstone and made the gap permanent. Every one of those links
now has a guard above.

## Stops nobody has named yet (since 2026-11-15, evening)

A drive that ends somewhere that is not a Known Place is remembered (`pendingStops`, `unknownStops.js`). When
the next evidence shows you have left (the next drive starting, a ping across town, a Home/Work/Gym
crossing), a stay of 15 minutes or more becomes a visit:

- the spot is named from OpenStreetMap (`placeNaming.js`, Nominatim reverse geocoding, the same service the
  Known Places page uses for addresses): the business or amenity there when the map knows one ("Raging
  Bull Sports Bar"), else the street address ("25 Macdonell St, Guelph"), else the coordinates;
- it is saved as a Known Place with the activity its name suggests, and the visit is logged through the
  normal pipeline (arrive_location + leave_location with the stay) and delivered to Time Left;
- you get one text: `New place logged: Raging Bull Sports Bar (Guelph), Fri 7:34 pm to 9:57 pm (2 h 23 min),
  as "Eating out". Reply OK to keep it, or PLACE name, activity to rename (e.g. PLACE Raging Bull, Darts).`
  Replying renames the place, sets the activity, updates the two events and re-sends them to Time Left
  (within 15 minutes, by the retry sweep; Time Left accepts a revised place visit from the same source
  record). At most three such texts a day; later stops are still logged, just not texted.

Stops shorter than 15 minutes, stops OwnTracks reported as a Home/Work/Gym crossing, and stops you logged
with "Log this place" in the meantime are not named. From the second visit on, the place is known and the
drive logs it directly (autoVisits.js), no text.

## The heartbeat (since 2026-11-15, evening)

Three Shortcuts automations (7 am, 1 pm, 7 pm) post the phone's position as a location ping tagged `t: "h"`:
`docs/heartbeat-shortcut.md` has the five-minute setup. With it, the health check knows the phone is alive
and where it is: a heartbeat far from where OwnTracks last saw the phone while OwnTracks has been quiet for
3 hours is texted at once ("OwnTracks seems to have stopped"), 14 hours without a heartbeat is texted at the
next check, and OwnTracks silence while the heartbeats show the phone has not moved is no longer an alert.

## What would make it better still

- **Life Cycle as a second opinion**: it has no API, but its export can be compared with Time Left for a
  week to catch any systematic gap.
- **Google Places names**: OpenStreetMap knows most businesses but not all; if a named stop comes back as
  an address too often, the Places API (New) can be added behind the same `placeNaming.js` (it needs the
  API enabled on the project and the Routes key widened, or a second key).
