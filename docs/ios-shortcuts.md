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
- `finish_spotify`
- `start_drive`
- `finish_drive`
- `arrive_location`
- `leave_location`
- `traffic_jam`

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

## Drive tracking (CarPlay / car Bluetooth)

`start_drive` and `finish_drive` record each drive as one timed session. TimeLeftToLive shows it
under **Transportation**, the same kind of segment Donut labels "Transport". The trigger is your
phone connecting to and disconnecting from the car. Unlike Shortcuts location triggers ("Arrive"/
"Leave"), iOS runs CarPlay and Bluetooth automations immediately and without asking, so they are not
silently delayed or dropped.

Build two personal automations in the **Shortcuts** app → **Automation** tab.

### 1. Start of drive

1. Tap **+** (New Automation).
2. Choose the trigger:
   - **CarPlay** → **Connects**, if the car has CarPlay, or
   - **Bluetooth** → choose the car's Bluetooth device (e.g. the car's stereo name) → **Is Connected**.
3. Select **Run Immediately**, and turn **Notify When Run** off.
4. Tap **Next** → **New Blank Automation**, then add these actions in order:
   1. **Get Current Location**.
   2. **Current Date**, then **Format Date** → Date Format **ISO 8601**, **Include ISO 8601 Time** on.
   3. **Dictionary** with these keys (all **Text** unless noted):
      - `event_type` = `start_drive`
      - `timestamp` = *Formatted Date* (magic variable from step 2)
      - `timezone` = `America/Toronto`
      - `latitude` = *Current Location* → tap it → **Latitude**
      - `longitude` = *Current Location* → tap it → **Longitude**
      - `location_label` = *Current Location* → tap it → **City**
      - `project_slug` = `home`
      - `source` = `ios_shortcuts`
      - `device_name` = `iPhone`
   4. **Get Contents of URL**:
      - URL: `https://gridlineai.web.app/api/integrations/ios-shortcuts/events`
      - Method: **POST**
      - Headers: `Authorization` = `Bearer <your current token>`, `Content-Type` = `application/json`
      - Request Body: **File** → *Dictionary* (from step 3). Alternatively choose **JSON** and add the
        same keys directly, which lets you skip the Dictionary action.
5. Tap **Done**.

### 2. End of drive

Repeat the steps above with:
- Trigger **CarPlay → Disconnects**, or **Bluetooth → the same car → Is Disconnected**.
- `event_type` = `finish_drive`.

Everything else is identical, including the token and `project_slug`.

### Notes

- **Use the current token** from the dashboard Setup page. Shortcuts that still carry an old token get
  `401` responses and are not recorded.
- **Location on drive events.** A drive starts and ends in different places, so the TimeLeft sync sends
  drive events with coordinates only and keeps `location_label` in `metadata.driveLocationLabel`.
  That lets the start and finish pair into one session. Sending `location_label` is still useful for
  the activity log.
- **Short stops.** Turning the car off (for example at a gas station) ends the drive, and restarting it
  begins a new one. Each leg shows as its own Transportation session.
- **Test it.** Connect to the car, wait a moment, and disconnect. The dashboard activity log should show
  "Started driving" and "Finished driving", and TimeLeftToLive should show a short Transportation
  session. You can delete the test entries afterwards.
- **`traffic_jam`** still works as a separate one-tap Shortcut during a drive.
