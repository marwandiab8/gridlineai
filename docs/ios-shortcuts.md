# iOS Shortcuts Integration

Apple Shortcuts can send tracking events directly to the app without SMS.

## Endpoint

`POST https://<app-domain>/api/integrations/ios-shortcuts/events`

## Authentication

Generate a token in the dashboard Setup page under **iOS Shortcuts Integration**. The app stores only a hash of the token. The raw token is shown once when generated or regenerated.

Send the token with either header:

```http
Authorization: Bearer <token>
```

or:

```http
X-Gridline-Shortcut-Token: <token>
```

## Shortcut Action

Use **Get Contents of URL**:

- Method: `POST`
- Headers:
  - `Authorization`: `Bearer <token>`
  - `Content-Type`: `application/json`
- Body: JSON

```json
{
  "event_type": "arrive_work",
  "timestamp": "<Shortcut current date ISO value>",
  "timezone": "America/Toronto",
  "location_label": "work",
  "project_slug": "your-project-slug",
  "source": "ios_shortcuts"
}
```

`project_slug` is recommended for report accuracy. Use the same project slug you select when generating the daily report. If it is omitted, GridlineAI will use the token owner's active SMS project when available.

Supported `event_type` values:

- `arrive_work`
- `leave_work`
- `arrive_home`
- `leave_home`
- `arrive_gym`
- `leave_gym`
- `start_workout`
- `finish_workout`
- `start_spotify`
- `arrive_location`
- `leave_location`

Optional fields: `device_name`, `latitude`, `longitude`, and `notes`.

### Reliable location capture

For consistent coordinates, pass location in one of these ways:

1. Add **Get Current Location** in the shortcut.
2. In the JSON body:
   - `latitude`: the latitude value from the location action.
   - `longitude`: the longitude value from the location action.
   - `location_label`: a friendly label like "Work", "Home", or the location name.

If you want to pass the entire location object instead, you can also include it under `location` and keep
`latitude`/`longitude` as a fallback; the backend now reads coordinates from location objects too.

Example body:

```json
{
  "event_type": "start_spotify",
  "timestamp": "<Shortcut current date ISO value>",
  "timezone": "America/Toronto",
  "location_label": "Brampton",
  "location": {
    "latitude": 43.76079578096695,
    "longitude": -79.758
  },
  "project_slug": "home",
  "source": "ios_shortcuts",
  "device_name": "iPhone"
}
```

Do not send `""` or blank values for coordinates. If a value is missing, leave the key out and the event will be saved without coordinates rather than as `0`.

Timezone can be sent as `timezone` or `timeZone`.

For retry protection, set an `Idempotency-Key` header to a unique Shortcut run id when available.
