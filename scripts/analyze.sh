#!/usr/bin/env bash
# DLMS/COSEM ingest analysis over the bridge's structured JSON logs + Redis streams.
# Usage: ./scripts/analyze.sh [bridge.log]   (defaults to newest logs/bridge.*.log)
set -euo pipefail

LOGDIR="$(cd "$(dirname "$0")/../logs" 2>/dev/null && pwd)"
LOG="${1:-$(ls -t "${LOGDIR:-.}"/bridge.*.log 2>/dev/null | head -1)}"
REDIS_CONTAINER="${REDIS_CONTAINER:-gridtokenx-redis}"

echo "== log: $LOG =="

echo "-- level counts --"
grep -o '"level":"[A-Z]*"' "$LOG" | sort | uniq -c

echo "-- ingest totals --"
echo "verified:     $(grep -c 'signature verified (REST)' "$LOG" || true)"
echo "disseminated: $(grep -c 'Disseminated SmartMeter'   "$LOG" || true)"
echo "rejected:     $(grep -ciE 'Invalid|🚫|Verification error' "$LOG" || true)"

echo "-- WARN/ERROR (deduped) --"
grep -E '"level":"(WARN|ERROR)"' "$LOG" \
  | (command -v jq >/dev/null && jq -r '.level+" "+.message' || cat) \
  | sort | uniq -c | sort -rn

echo "-- per-zone dissemination (from log) --"
grep -oE 'events:zone_[0-9]+' "$LOG" | sort | uniq -c

echo "-- live Redis stream lengths --"
for z in $(seq 0 9); do
  printf 'zone_%s=%s\n' "$z" \
    "$(docker exec "$REDIS_CONTAINER" redis-cli XLEN "gridtokenx:events:zone_$z" 2>/dev/null || echo '?')"
done
