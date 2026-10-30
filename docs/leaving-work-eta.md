# "Hey Siri, leaving work" — ETA text

Siri is the only trigger. The Shortcut posts where you are; GridlineAI works out the drive home in
current traffic and texts your contact from the gridlineai Twilio number:

> Marwan leaving work, will be home in 42 minutes, at 4:51 PM.

Siri then reads back: *"Told Ashley you'll be home in 42 minutes, around 4:51 PM."*

Why not the OwnTracks Work exit? It also means `leave_work`, but it fires every time you drive out for
material and come back. OwnTracks exits keep being logged; they never text on their own. The one
exception is the forgot-to-say-it fallback described at the end.

Code: `functions/leavingWorkEta.js` (endpoint, fallback, reply relay), wired in `functions/index.js`
(`leavingWorkEta`, `ownTracksEvents`, `inboundSms`) and `firebase.json`. Tests: `leavingWorkEta.test.js`
and the fallback cases at the end of `ownTracksIntegration.test.js`.

## Endpoint

`POST https://<app-domain>/api/integrations/ios-shortcuts/leaving-work`

Same token as the other Shortcut endpoints (`Authorization: Bearer <token>` or
`X-Gridline-Shortcut-Token`). Body is anything `/events` accepts; the fields that matter:

```json
{
  "latitude": 43.6532,
  "longitude": -79.7331,
  "timezone": "America/Toronto"
}
```

Send an `Idempotency-Key` header (a fresh UUID per run) so a retried request can't text twice.

Reply:

```json
{
  "ok": true, "sent": true, "duplicate": false,
  "eta_minutes": 42, "arrive_at": "4:51 PM", "arrive_at_iso": "2026-10-30T20:51:00.000Z",
  "distance_km": 63.4, "route_source": "google_routes", "origin_source": "shortcut",
  "message": "Marwan leaving work, will be home in 42 minutes, at 4:51 PM.",
  "spoken": "Told Ashley you'll be home in 42 minutes, around 4:51 PM.",
  "departure_logged": true
}
```

A second ask inside `minGapMinutes` (default 45) replies `sent: false, duplicate: true` with the earlier
ETA, and `spoken` says so ("Already told Ashley 10 minutes ago: home around 4:51 PM."). Errors carry a
`spoken` sentence too (`eta_not_configured`, `home_not_configured`, `origin_unknown`, `sms_not_configured`).

What happens on a send, in order: route lookup → text → `messages` log + `etaNotifications/{email}`
state → the departure is recorded as a `leave_work` event through the normal Shortcuts pipeline
(dashboard note "Told Ashley: home in 42 min (4:51 PM)", TimeLeft delivery), unless one is already in
the log for this departure → reply. The text never waits on the OpenAI step.

## One-time setup

### 1. Google Routes API key (traffic-aware ETA)

In the Google Cloud project behind Firebase (billing is already on for Functions):

1. APIs & Services → Enable **Routes API**.
2. Credentials → Create API key → restrict it to **Routes API** only (no HTTP referrer restriction;
   the call comes from Cloud Functions).
3. `firebase functions:secrets:set GOOGLE_MAPS_API_KEY` and paste the key.

Without the key everything still works; the ETA becomes a straight-line estimate (×1.3 road factor at
60 km/h) and the text says "in about N minutes". One lookup per departure is well inside the free
monthly calls.

### 2. Your `appMembers/{email}` document

Add an `etaNotify` map (Firestore console → `appMembers` → your email):

```json
"etaNotify": {
  "enabled": true,
  "contactName": "Ashley",
  "contactPhoneE164": "+1519XXXXXXX",
  "firstName": "Marwan",
  "home": { "latitude": 43.70, "longitude": -80.37 },
  "minGapMinutes": 45,
  "fallback": { "enabled": true, "afterLocalHour": 15, "waitMinutes": 8, "minProgressMeters": 4000, "expireMinutes": 60 }
}
```

- `contactPhoneE164` is the only required field.
- `firstName` is optional; it defaults to the first word of `displayName`.
- Home, in order of preference: `home {latitude, longitude}` → `homePlaceId` (a `knownPlaces` doc id)
  → the known place named exactly **Home** (or with activity `Home`). Drop a pin on your driveway for the
  coordinates; the ETA is only as good as the destination.
- Everything else has the defaults shown.

### 3. Deploy

Functions deploy by hand (CI deploys hosting only):

```bash
firebase deploy --only functions:leavingWorkEta,functions:ownTracksEvents,functions:inboundSms,firestore:rules,hosting
```

`ownTracksEvents` and `inboundSms` must be redeployed too: the first now runs the fallback, the second
relays your contact's replies. `leavingWorkEta` and `ownTracksEvents` list `GOOGLE_MAPS_API_KEY`, so the
secret must exist before they deploy (step 1); a placeholder value works until you have the real key —
the ETA is just an estimate until then.

### 4. Quick test from a terminal

```bash
curl -sS -X POST "https://<app-domain>/api/integrations/ios-shortcuts/leaving-work" \
  -H "Authorization: Bearer $GRIDLINE_SHORTCUT_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-$(date +%s)" \
  -d '{"latitude":43.6532,"longitude":-79.7331,"timezone":"America/Toronto"}'
```

That sends a real text, so temporarily point `contactPhoneE164` at your own phone for the first run.

## The Shortcut

Create a Shortcut named **Leaving work** (that name is the Siri phrase):

1. **Get Current Location** — Shortcuts asks for location permission once.
2. **Get Contents of URL**
   - URL: `https://<app-domain>/api/integrations/ios-shortcuts/leaving-work`
   - Method: `POST`
   - Headers: `Authorization` = `Bearer <token>`, `Idempotency-Key` = a **UUID** variable
     (add a *Generate UUID* action above, or use the *Current Date* formatted to seconds)
   - Request Body: JSON
     - `latitude` → Current Location ▸ *Latitude*
     - `longitude` → Current Location ▸ *Longitude*
     - `timezone` → `America/Toronto`
3. **Get Dictionary Value** — key `spoken` from *Contents of URL*.
4. **Show Result** — the *Dictionary Value*. When the Shortcut runs by voice, Siri reads this aloud; on
   CarPlay it is spoken, not shown.

Say "Hey Siri, leaving work". Add it to CarPlay / the Action button if you like; nothing else changes.

If the old location-based automation that texted directly is still enabled, turn it off; otherwise
Ashley gets both.

## Days you forget to say it (OwnTracks fallback)

`ownTracksEvents` arms a pending check when OwnTracks reports a **Work** region exit at or after
`afterLocalHour` (3 PM) and nothing was texted in the last `minGapMinutes`. Then, on each later ping:

| Ping shows | Result |
|---|---|
| Less than `waitMinutes` (8) since the exit | keep waiting |
| Fix accuracy worse than 150 m | ignore that ping |
| At least `minProgressMeters` (4 km) closer to home than the site was | **text sent** from the ping's position |
| Back within 400 m of the exit point, or an `arrive_work` crossing | disarmed |
| 1.5 km farther from home than the site was | disarmed (supply run the other way) |
| Within 1.5 km of home already | disarmed (too late to be useful) |
| `expireMinutes` (60) passed, or the ping itself is more than 5 minutes old | disarmed / ignored |

A fallback send logs the departure at the exit time OwnTracks reported, with the note "Told Ashley
automatically: …". Saying it to Siri after OwnTracks already fired is fine: the Siri send clears the
pending check, and the later Work exit is deduped against Siri's `leave_work` for 20 minutes instead of
logging a second departure.

Known limit: a supply run that happens to head toward home for 4 km+ still looks like the drive home.
Raise `minProgressMeters` if your usual supplier is in that direction. Turn the whole thing off with
`fallback.enabled: false`.

## Replies

If your contact replies to the Twilio number ("ok", "drive safe"), `inboundSms` relays it to your approved
phone as `Ashley: ok` and does not answer her. Nothing else about inbound SMS changes.

## Where things live

- `etaNotifications/{email}` — `lastSentAtMs`, `lastSent {trigger, minutes, arriveAtClock, body, origin, home…}`,
  `pendingFallback`, `lastFallbackOutcome`; `sends/` subcollection keeps the history. Readable by that member
  in the app (rules), written only by the server.
- `messages` — every text sent, `command: "leaving_work_eta"` (and `etaNotify.trigger` of `siri`,
  `owntracks_fallback` or `contact_reply_relay`).
- `iosShortcutEvents` — the `leave_work` departure, as with any Shortcut event.
