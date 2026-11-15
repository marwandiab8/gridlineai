#!/usr/bin/env bash
# Says which deployed GridlineAI tracking functions are older than the last change to the tracking code.
# The Shortcut delivery triggers ran an Oct 8 mapper until Nov 15 because they were not redeployed with the
# HTTP endpoints; this makes that visible. Usage: scripts/check-functions-drift.sh   (read-only)
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT=gridlineai
REGION=northamerica-northeast1
FUNCTIONS="iosShortcutsEvents ownTracksEvents placesEvents closeStaleDrives leavingWorkEta syncIosShortcutEventToTimeLeft retryIosShortcutEventsToTimeLeft trackingHealthCheck healthExportEvents"
# The tracking code: anything these functions require, approximated by the modules below plus index.js.
FILES="functions/index.js functions/iosShortcutsIntegration.js functions/ownTracksIntegration.js functions/driveDetection.js functions/autoVisits.js functions/stayEvidence.js functions/stayClosing.js functions/placeLearning.js functions/placeActivities.js functions/placesIntegration.js functions/leavingWorkEta.js functions/timeLeftLifeEventMapper.js functions/timeLeftLifeEventDelivery.js functions/timeLeftLifeEventClient.js functions/timeLeftShortcutTriggers.js functions/timeLeftShortcutReplay.js functions/trackingHealth.js functions/healthExportIntegration.js functions/healthExportEventMapper.js"
LAST_CHANGE="$(git log -1 --format=%cI -- $FILES)"
DIRTY="$(git status --porcelain -- $FILES | wc -l)"
echo "tracking code last committed: $LAST_CHANGE$( [ "$DIRTY" -gt 0 ] && echo "  (plus $DIRTY uncommitted file(s))" )"
for f in $FUNCTIONS; do
  UPDATED="$(gcloud functions describe "$f" --region "$REGION" --project "$PROJECT" --format='value(updateTime)' 2>/dev/null || true)"
  if [ -z "$UPDATED" ]; then printf '%-36s not deployed\n' "$f"; continue; fi
  if [[ "$UPDATED" < "$LAST_CHANGE" ]]; then printf '%-36s deployed %s  STALE (code changed since)\n' "$f" "${UPDATED:0:19}"; else printf '%-36s deployed %s  current\n' "$f" "${UPDATED:0:19}"; fi
done
