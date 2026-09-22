# Quick-log Shortcuts: places you name once, and "stuck in traffic"

Two hands-free Siri-triggered Shortcuts, both logged through your existing dashboard/Time Left To
Live pipeline. Neither needs a text-message round trip — naming a new place happens locally on
the phone (Shortcuts' own "Ask for Text" prompt), and traffic logging is a single one-shot call.

## 1. "Log this place" — learns a place's name once, recognizes it forever after

**Endpoint:** `POST https://<app-domain>/api/integrations/places/log`
**Auth:** same token as the other integrations (`Authorization: Bearer <token>` or
`X-Gridline-Shortcut-Token: <token>`).

### How it behaves

- POST just `{latitude, longitude}` (no `name`).
  - **Already know this spot** (within ~120m of somewhere you've named before): the visit is
    logged immediately under that name. Response: `{"ok":true,"known":true,"isNew":false,"name":"Bells of Steel","visitCount":4}`.
  - **Never seen this spot**: nothing is logged yet. Response: `{"ok":true,"known":false}`.
- POST `{latitude, longitude, name}` (name included): saves/renames the place at those
  coordinates and logs this visit under that name. Response: `{"ok":true,"known":true,"isNew":true,"name":"Bells of Steel","visitCount":1}`.
- A visit within ~120m of a place you've already named is always treated as the same place — it
  is never re-asked, and does not create a duplicate. Naming a nearby spot with a different name
  updates that place's name for next time rather than creating a second entry.
- Every visit — known or newly named — is recorded exactly like an `arrive_location` Shortcut
  event: same dashboard log note, same Time Left To Live delivery, `source: "quick_log"`.

### Shortcut setup

1. **Automation → Create Personal Automation** (or a plain Shortcut you trigger by voice/tap —
   your choice; there's nothing location-trigger-specific required, unlike the OwnTracks/Home
   automations).
2. Add **Get Current Location**.
3. Add **Get Contents of URL**:
   - Method `POST`, Headers `Authorization: Bearer <token>`, `Content-Type: application/json`.
   - Body (JSON): `{"latitude": [Latitude], "longitude": [Longitude]}` (drag the Location
     variable's Latitude/Longitude into the body — do not include `name` yet).
4. Add **Get Dictionary Value** → `known` from the response.
5. **If** `known` is `false`:
   - **Ask for Text**: "What's this place called?"
   - **Get Contents of URL** again, same URL/headers, body now
     `{"latitude": [Latitude], "longitude": [Longitude], "name": [Provided Input]}`.
   - **Show Notification**: "Saved: " + the name you just typed.
6. **Otherwise** (already known): **Show Notification** using the response's `name` field, e.g.
   "Logged: " + `name`.

A voice trigger for step 1 (Siri phrase like "Log this place") makes the whole thing hands-free
except for typing the name the one time a place is new.

## 2. "Stuck in traffic" — one-shot, no location needed

Reuses the existing Shortcuts endpoint from `docs/ios-shortcuts.md` with a new event type — no new
endpoint, no new token.

**Endpoint:** `POST https://<app-domain>/api/integrations/ios-shortcuts/events` (same as always)

```json
{
  "event_type": "traffic_jam",
  "timestamp": "<Shortcut current date ISO value>",
  "timezone": "America/Toronto",
  "project_slug": "your-project-slug",
  "source": "ios_shortcuts"
}
```

### Shortcut setup

1. **Automation → Create Personal Automation**, trigger it with a Siri phrase (e.g. "Hey Siri, log
   traffic" — "stuck in traffic" alone can be awkward as an exact Siri phrase; anything you'll
   reliably say works, the wording only matters for the phrase→Shortcut trigger, not the payload).
2. One **Get Contents of URL** action, same auth headers as your other Shortcuts, body exactly as
   above.
3. No confirmation step needed — it's a single fire-and-forget log entry, safe to trigger while
   driving.

It shows up in your dashboard log as "Stuck in traffic" and, in Time Left To Live, as a Moment
(not a timed session — there's no "traffic cleared" end event, so it doesn't track duration).
