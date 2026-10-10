#!/bin/sh
# Post one event to the local gateway and poll until the processor has stored it.
# Usage: scripts/smoke.sh BASE_URL   (for example http://localhost:3000)
set -eu

BASE_URL="${1:?usage: smoke.sh BASE_URL}"
EVENT='{"source":"Billing","type":"Invoice.Created","payload":{"customer":{"id":42},"amount":19.99}}'

echo "POST $BASE_URL/events"
RESPONSE=$(curl -sS -X POST -H 'Content-Type: application/json' -d "$EVENT" "$BASE_URL/events")
echo "$RESPONSE"

EVENT_ID=$(printf '%s' "$RESPONSE" | python -c 'import json,sys; print(json.load(sys.stdin)["eventId"])')
echo "eventId: $EVENT_ID"

ATTEMPT=1
while [ "$ATTEMPT" -le 30 ]; do
    ITEM=$(curl -sS "$BASE_URL/events/$EVENT_ID" || true)
    STATUS=$(printf '%s' "$ITEM" | python -c 'import json,sys
try:
    print(json.load(sys.stdin).get("status", ""))
except ValueError:
    print("")')
    if [ "$STATUS" = "processed" ]; then
        printf '%s' "$ITEM" | python -m json.tool
        exit 0
    fi
    echo "attempt $ATTEMPT: status '${STATUS:-none}', waiting"
    ATTEMPT=$((ATTEMPT + 1))
    sleep 2
done

echo "event $EVENT_ID was not processed within 60 s" >&2
exit 1
