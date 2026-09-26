# OwnTracks Integration

An alternative to `docs/ios-shortcuts.md` for tracking Home/Work/Gym: [OwnTracks](https://owntracks.org)
is a dedicated location-tracking iOS app. Configured with named Regions and "HTTP mode", it posts
directly to this endpoint the moment it detects you crossing a Region boundary — no Shortcuts
automation involved, which avoids the reliability problem behind `leave_home` events going
missing (iOS can silently delay or drop a Shortcuts "Leaving <place>" automation, especially).

## Endpoint

`POST https://<app-domain>/api/integrations/owntracks/events`

## Authentication

**Reuses the exact same token as the iOS Shortcuts integration** — generate/find it in the
dashboard Setup page under "iOS Shortcuts Integration"; do not regenerate it if Shortcuts
automations are already using it, or those will break too.

Because OwnTracks' iOS app has changed which auth fields it exposes across versions, this
endpoint accepts the token **however your version of the app can send it** — set up whichever of
these your Settings screen actually has:

1. A custom HTTP header, if OwnTracks offers one: `Authorization: Bearer <token>` or
   `X-Gridline-Shortcut-Token: <token>`.
2. **HTTP Basic Auth** — the token goes in the **Password** field; the **Username** field can be
   anything (e.g. `owntracks`) since only the password is checked.
3. A `token` query parameter on the endpoint URL itself, as a last resort:
   `https://<app-domain>/api/integrations/owntracks/events?token=<token>`.

> I wrote this from OwnTracks' general documented behavior, not a live look at your installed
> app's exact screen — its Settings labels have moved around between versions. Try option 1 or 2
> first (look for "Auth", "Basic Auth", or "HTTP Headers" under Settings → Connection); fall back
> to the URL query parameter if neither is present. Whichever you use, a `curl` test (below) will
> confirm it works before you rely on it.

## OwnTracks app setup

1. Install OwnTracks and open **Settings**.
2. **Mode**: set to **HTTP** (not MQTT).
3. **Connection → URL**: `https://<app-domain>/api/integrations/owntracks/events`
4. Set up authentication using whichever option above your Settings screen exposes.
5. **Regions**: add a circular region for each place you want tracked, named exactly:
   - `Home`, `Work` (or `Office`), `Gym` (or `Fitness`) — these map to the same
     `arrive_home`/`leave_home`/`arrive_work`/`leave_work`/`arrive_gym`/`leave_gym` events
     Shortcuts already sends, so existing Home/Work/Gym session tracking keeps working unchanged.
   - Any other name (e.g. "Bells of Steel") still gets tracked, as a generic
     `arrive_location`/`leave_location` visit — same as an unrecognized Shortcuts location today.
   - Region names are matched case-insensitively, so "home", "Home", and "HOME" are equivalent.
6. Make sure **"Extended Data"**/background location permission is granted ("Always" location
   access), or region monitoring will not fire while the app is not open.

## What gets sent, and what this endpoint does with it

OwnTracks posts two kinds of message in HTTP mode; only one of them matters here:

- `_type: "location"` — a routine location ping (sent periodically or on movement). **Ignored**:
  acknowledged with `200 []` and not recorded, so these never spam the log or count as tracked time.
- `_type: "transition"` — sent the moment you enter or leave a defined Region:
  ```json
  {
    "_type": "transition",
    "event": "enter",
    "desc": "Home",
    "lat": 43.7,
    "lon": -79.7,
    "tid": "MD",
    "tst": 1758000000
  }
  ```
  This is translated into the same shape the Shortcuts endpoint already understands
  (`event_type: "arrive_home"`, `timestamp` from `tst`, `location_label` from `desc`, etc.) and
  handed to the exact same recording/dedupe/TimeLeftToLive-delivery pipeline — so once an event
  lands here, everything downstream (session pairing, the Time Left To Live ring, dashboards)
  behaves identically to a Shortcuts-sourced event, with `source: "owntracks"` recorded on it for
  anyone debugging later.

## Leaving a region also ends a "Log this place" stay

Places learned by the "Log this place" Shortcut are kept in Known Places, separately from these events.
When OwnTracks reports leaving a region, the matching open Known Places stay is closed at the exit time
OwnTracks recorded (no second event is logged - the OwnTracks leave event is the record):

- **By name first:** the region name is matched to the place name ignoring case, spaces and
  underscores, so a region called `Costco_Guelph` closes the place `Costco Guelph`.
- **Then by location:** for a region with no name match (and not Home/Work), or the generic `Gym`
  region, the nearest open stay within its radius plus 300 m of the exit point is closed - so a `Gym`
  region ends a stay saved as `GoodLife Guelph`.
- Home and Work exits never close a place by location, an *enter* never closes anything, and an exit
  older than the stay's start is ignored.
- If the place can't be closed, the OwnTracks event is still recorded normally.

A region you create while you are already standing inside it has no *enter* event for that visit, so
that visit's arrival is missing in Time Left ("Arrival was not recorded"); later visits are paired
normally.

## Verifying it works

Trigger a Region manually (walk in/out, or use OwnTracks' own "Reconnect"/simulate options if
available), then check the dashboard's activity log for a new entry sourced from `owntracks`. To
test the endpoint directly without leaving your Region:

```bash
curl -i -X POST 'https://<app-domain>/api/integrations/owntracks/events' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <token>' \
  -d '{"_type":"transition","event":"enter","desc":"Home","tst":'"$(date +%s)"',"lat":43.7,"lon":-79.7,"tid":"MD"}'
```

A successful response is `200` with an empty JSON array `[]` (OwnTracks' expected reply shape).
