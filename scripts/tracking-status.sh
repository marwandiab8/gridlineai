#!/usr/bin/env bash
# The state of location tracking for Marwan: last heartbeat, last OwnTracks point, the last health check,
# stops waiting to be named, and the last new-place text. Read-only (Firestore REST with gcloud credentials).
set -euo pipefail
PROJECT=gridlineai
MEMBER="${1:-marwandiab8@gmail.com}"
TOKEN="$(gcloud auth print-access-token 2>/dev/null)"
BASE="https://firestore.googleapis.com/v1/projects/$PROJECT/databases/(default)/documents"
get() { curl -s -H "Authorization: Bearer $TOKEN" "$BASE/$1"; }
python3 - "$MEMBER" "$(get "heartbeats/$MEMBER")" "$(get "driveDetection/$MEMBER")" "$(get "trackingHealth/$MEMBER")" "$(get "unknownStopState/$MEMBER")" "$(curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$BASE:runQuery" -d "{\"structuredQuery\":{\"from\":[{\"collectionId\":\"pendingStops\"}],\"where\":{\"fieldFilter\":{\"field\":{\"fieldPath\":\"memberEmail\"},\"op\":\"EQUAL\",\"value\":{\"stringValue\":\"$MEMBER\"}}},\"limit\":50}}")" <<'PY'
import json, sys, datetime
member, hb, drive, health, state, stops = sys.argv[1:7]
def v(f):
    for k in ("stringValue","timestampValue"):
        if k in f: return f[k]
    if "integerValue" in f: return int(f["integerValue"])
    if "doubleValue" in f: return f["doubleValue"]
    if "booleanValue" in f: return f["booleanValue"]
    if "nullValue" in f: return None
    if "mapValue" in f: return {k:v(x) for k,x in (f["mapValue"].get("fields") or {}).items()}
    if "arrayValue" in f: return [v(x) for x in (f["arrayValue"].get("values") or [])]
    return f
def doc(raw):
    d = json.loads(raw) if raw else {}
    return {k:v(x) for k,x in d.get("fields",{}).items()} if "fields" in d else {}
tz = datetime.timezone(datetime.timedelta(hours=-5))
when = lambda ms: datetime.datetime.fromtimestamp(ms/1000, tz).strftime("%a %b %d %-I:%M %p") if isinstance(ms,(int,float)) and ms else "never"
now = datetime.datetime.now().timestamp()*1000
h, d, t, s = doc(hb), doc(drive), doc(health), doc(state)
print(f"member            {member}")
print(f"last heartbeat    {when(h.get('lastAtMs'))}  count {h.get('count','0')}  moved since OwnTracks {h.get('movedSinceOwnTracksMeters')} m")
print(f"health export     {when(h.get('healthLastAtMs'))}  exports {h.get('healthCount','0')}  steps today {h.get('healthStepsTotal')} ({h.get('healthStepsDateKey')})  since OwnTracks {h.get('healthStepsSinceOwnTracks')}")
print(f"last OwnTracks    {when(d.get('lastPointAtMs'))}  detector {d.get('status')}  ({round((now-d['lastPointAtMs'])/3600000,1) if d.get('lastPointAtMs') else '?'} h ago)")
print(f"health check      {when(t.get('checkedAtMs'))}  ok={t.get('ok')}  last alert {when(t.get('lastAlertAtMs'))} [{t.get('lastAlertCodes','')}]")
for i in t.get("issues") or []: print(f"  issue           {i.get('code')}: {i.get('text')}")
print(f"new-place texts   {s.get('textsToday',0)} on {s.get('textsDateKey','-')} (max 3 a day)")
rows = [r.get("document") for r in json.loads(stops) if r.get("document")]
print(f"stops             {len(rows)} remembered")
for r in sorted(rows, key=lambda x: v(x['fields'].get('arrivedAtMs',{'integerValue':'0'})) or 0)[-8:]:
    f = {k:v(x) for k,x in r["fields"].items()}
    print(f"  {when(f.get('arrivedAtMs'))}  {str(f.get('status')).ljust(14)} {f.get('placeName') or ''} {str(f.get('durationMinutes'))+' min' if f.get('durationMinutes') else ''} {f.get('latitude')},{f.get('longitude')}")
PY
