#!/usr/bin/env bash
# Runs one load test and writes everything to loadtest/results/<mode>-<run_id>.txt:
# the k6 summary, a `docker stats` snapshot taken mid-run, and delivery
# latency per priority computed in Postgres after the backlog has drained.
#
# Usage: loadtest/run.sh capacity|fairness
# Tunables (env): HEAVY_RATE, HEAVY_CUSTOMERS, HEAVY_PLAN_RPS, EXPRESS_RATE,
#                 EXPRESS_CUSTOMERS, DURATION_S, K6_IMAGE, DRAIN_TIMEOUT_S
# Needs the compose stack running (make up).
set -euo pipefail

MODE="${1:-capacity}"
case "$MODE" in capacity|fairness) ;; *) echo "usage: $0 capacity|fairness" >&2; exit 2 ;; esac

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deployments/docker-compose.yml)
[[ -f .env ]] && COMPOSE+=(--env-file .env)
psql() { "${COMPOSE[@]}" exec -T postgres psql -U sms -d sms "$@"; }

K6_IMAGE="${K6_IMAGE:-grafana/k6:0.54.0}"
NETWORK="${NETWORK:-sms-gateway_default}"
DURATION_S="${DURATION_S:-120}"
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-600}"
RUN_ID="$(date +%s)"
CUST_MIN=$((RUN_ID * 1000))
CUST_MAX=$((RUN_ID * 1000 + 999))
SINCE="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"

mkdir -p loadtest/results
OUT="loadtest/results/${MODE}-${RUN_ID}.txt"
STATS="loadtest/results/${MODE}-${RUN_ID}.stats.txt"

{
  echo "# load test: mode=$MODE run_id=$RUN_ID started=$SINCE"
  echo "# heavy_rate=${HEAVY_RATE:-1000}/s heavy_customers=${HEAVY_CUSTOMERS:-1} heavy_plan_rps=${HEAVY_PLAN_RPS:-5000}" \
       "express_rate=${EXPRESS_RATE:-100}/s express_customers=${EXPRESS_CUSTOMERS:-100} duration=${DURATION_S}s"
  echo "# host: $(nproc) vCPU, $(free -g | awk '/Mem:/{print $2}') GB RAM"
} | tee "$OUT"

# Resource snapshot in the middle of the run, to see what is saturated.
( sleep $((DURATION_S / 2 + 10))
  docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' > "$STATS" 2>&1 ) &
STATS_PID=$!

set +e
docker run --rm --network "$NETWORK" --user "$(id -u):$(id -g)" \
  -v "$PWD/loadtest:/scripts" \
  -e MODE="$MODE" -e RUN_ID="$RUN_ID" -e DURATION_S="$DURATION_S" \
  -e HEAVY_RATE="${HEAVY_RATE:-}" -e HEAVY_CUSTOMERS="${HEAVY_CUSTOMERS:-}" -e HEAVY_PLAN_RPS="${HEAVY_PLAN_RPS:-}" \
  -e EXPRESS_RATE="${EXPRESS_RATE:-}" -e EXPRESS_CUSTOMERS="${EXPRESS_CUSTOMERS:-}" \
  -e BASE_URL="${BASE_URL:-http://api:8080}" \
  "$K6_IMAGE" run --quiet --summary-export="/scripts/results/${MODE}-${RUN_ID}.json" /scripts/k6.js 2>&1 | tee -a "$OUT"
K6_STATUS=${PIPESTATUS[0]}
set -e
wait "$STATS_PID" || true

# Wait until every message of this run reached a terminal status, so the
# latency includes the backlog, not only the messages that were quick.
echo "# waiting for the backlog to drain..." | tee -a "$OUT"
start=$(date +%s)
while :; do
  left=$(psql -tAc "SELECT count(*) FROM messages WHERE created_at >= '$SINCE'
                    AND customer_id BETWEEN $CUST_MIN AND $CUST_MAX
                    AND status IN ('queued','sending','retrying')")
  [[ "$left" -eq 0 ]] && break
  if (( $(date +%s) - start > DRAIN_TIMEOUT_S )); then
    echo "# drain timeout: $left messages still in flight" | tee -a "$OUT"; break
  fi
  sleep 5
done
echo "# drained $(( $(date +%s) - start ))s after k6 finished" | tee -a "$OUT"

echo "# delivery latency (handed_at - created_at), from Postgres:" | tee -a "$OUT"
psql -v since="$SINCE" -v cust_min="$CUST_MIN" -v cust_max="$CUST_MAX" -f - < loadtest/latency.sql | tee -a "$OUT"

echo "# docker stats mid-run:" | tee -a "$OUT"
cat "$STATS" | tee -a "$OUT"
rm -f "$STATS"

echo "# k6 exit code: $K6_STATUS (non-zero = a threshold failed)" | tee -a "$OUT"
echo "# results: $OUT"
exit "$K6_STATUS"
