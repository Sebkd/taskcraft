#!/usr/bin/env bash
# Runs the examples (E-1…E-15, examples/README.md) and prints a result for
# each: ok, FAIL or skip. Every example checks its own result and exits with
# an error when it is not what it describes; this script only collects the
# exit codes. Run before a release with --require-services: all ok.
#
#   scripts/run-examples.sh [--require-services] [name...]
#
# Kafka (E-12) and PostgreSQL (E-13, E-14): the addresses in
# TASKCRAFT_KAFKA_BROKERS / TASKCRAFT_POSTGRES_URL (or the defaults) when
# they answer; otherwise Docker (examples/docker-compose.yml), stopped again
# afterwards; otherwise their examples are skipped — a failure with
# --require-services. Logs: target/examples-run/<name>.log.
set -u

cd "$(dirname "$0")/.." || exit 1

require_services=0
selected=()
for arg in "$@"; do
  case "$arg" in
    --require-services) require_services=1 ;;
    -h | --help)
      sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    -*)
      echo "unknown option: $arg" >&2
      exit 2
      ;;
    *) selected+=("$arg") ;;
  esac
done

# id, name, what it needs, how it runs.
examples=(
  "E-1|hello|-|taskcraft"
  "E-2|outcomes|-|taskcraft"
  "E-3|long-tasks-reject|-|taskcraft"
  "E-4|resource-pools|-|taskcraft"
  "E-5|idempotent-push|-|taskcraft"
  "E-6|graceful-shutdown|-|taskcraft"
  "E-7|metadata|-|taskcraft"
  "E-8|custom-source|-|taskcraft"
  "E-9|custom-runnable|-|taskcraft"
  "E-10|observability|-|taskcraft --features metrics"
  "E-11|axum-service|-|http"
  "E-12|kafka-consumer|kafka|taskcraft-kafka"
  "E-13|durable-store|postgres|taskcraft-postgres"
  "E-14|leases-failover|postgres|taskcraft-postgres"
  "E-15|fsm-pipeline|-|taskcraft"
)
LIMIT=${TASKCRAFT_EXAMPLE_LIMIT:-300}
LOGS=target/examples-run
mkdir -p "$LOGS"

wanted() {
  [ ${#selected[@]} -eq 0 ] && return 0
  local name
  for name in "${selected[@]}"; do
    [ "$name" = "$1" ] && return 0
  done
  return 1
}

# Whether host:port accepts connections, by bash alone.
answers() {
  (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null
}

wait_port() {
  local host=$1 port=$2 seconds=$3
  for _ in $(seq 1 "$seconds"); do
    answers "$host" "$port" && return 0
    sleep 1
  done
  return 1
}

# Runs a command with a time limit (macOS has no `timeout`).
limited() {
  perl -e 'alarm shift; exec @ARGV' "$LIMIT" "$@"
}

KAFKA=${TASKCRAFT_KAFKA_BROKERS:-localhost:9092}
POSTGRES=${TASKCRAFT_POSTGRES_URL:-postgres://postgres:postgres@localhost:5432/postgres}
export TASKCRAFT_KAFKA_BROKERS=$KAFKA TASKCRAFT_POSTGRES_URL=$POSTGRES

# host and port of the first broker / of the database URL.
kafka_host=${KAFKA%%,*}
postgres_host=${POSTGRES#*@}
postgres_host=${postgres_host%%/*}

started=()
stop_started() {
  if [ ${#started[@]} -gt 0 ]; then
    docker compose -f examples/docker-compose.yml stop "${started[@]}" >/dev/null 2>&1
  fi
}
trap stop_started EXIT

# Makes a service available; REPLY is "up", or why not. Not run in a
# subshell: it records what it started.
service() {
  local name=$1 address=$2
  local host=${address%:*} port=${address##*:}
  if answers "$host" "$port"; then
    REPLY=up
  elif command -v docker >/dev/null 2>&1; then
    if docker compose -f examples/docker-compose.yml up -d "$name" >"$LOGS/docker-$name.log" 2>&1 &&
      wait_port "$host" "$port" 60; then
      started+=("$name")
      REPLY=up
    else
      REPLY="Docker did not start $name"
    fi
  else
    REPLY="no $name at $address, no Docker"
  fi
}

needs() {
  local need
  for entry in "${examples[@]}"; do
    IFS='|' read -r _ name need _ <<<"$entry"
    wanted "$name" && [ "$need" = "$1" ] && return 0
  done
  return 1
}

kafka_state="not needed"
postgres_state="not needed"
if needs kafka; then
  service kafka "$kafka_host"
  kafka_state=$REPLY
fi
if needs postgres; then
  service postgres "$postgres_host"
  postgres_state=$REPLY
fi

echo "building the examples..."
if ! cargo build -q --locked --workspace --all-features --examples >"$LOGS/build.log" 2>&1 ||
  ! cargo build -q --manifest-path examples/axum-service/Cargo.toml >>"$LOGS/build.log" 2>&1; then
  echo "build failed: $LOGS/build.log"
  tail -n 20 "$LOGS/build.log"
  exit 1
fi

# E-11: push, push again, status, cancel, then Ctrl-C; exit code 0.
http_scenario() {
  local log=$1 base=http://127.0.0.1:3000
  cargo run -q --manifest-path examples/axum-service/Cargo.toml >"$log" 2>&1 &
  local server=$!
  step() {
    echo "FAIL at: $1" >>"$log"
    kill -INT "$server" 2>/dev/null
    wait "$server" 2>/dev/null
    return 1
  }
  wait_port 127.0.0.1 3000 60 || { step "the server did not listen on port 3000"; return 1; }
  local body='{"id": "orders-2026-10", "table": "orders"}' code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$base/exports" -H 'content-type: application/json' -d "$body")
  [ "$code" = 202 ] || { step "push answered $code, not 202"; return 1; }
  code=$(curl -s -o "$LOGS/axum-again.json" -w '%{http_code}' -X POST "$base/exports" -H 'content-type: application/json' -d "$body")
  if [ "$code" != 200 ] || ! grep -q 'already running' "$LOGS/axum-again.json"; then
    step "the second push answered $code, not 200 \"already running\""
    return 1
  fi
  code=$(curl -s -o /dev/null -w '%{http_code}' "$base/exports/orders-2026-10")
  [ "$code" = 200 ] || { step "status answered $code, not 200"; return 1; }
  code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$base/exports/orders-2026-10")
  [ "$code" = 200 ] || [ "$code" = 202 ] || { step "cancel answered $code, not 200 or 202"; return 1; }
  kill -INT "$server"
  wait "$server"
}

run_one() {
  local name=$1 how=$2 log=$3 package features
  if [ "$how" = http ]; then
    http_scenario "$log"
  else
    read -r package features <<<"$how"
    # shellcheck disable=SC2086 # the features words split on purpose
    limited cargo run -q --locked -p "$package" --example "$name" $features >"$log" 2>&1
  fi
}

results=()
failed=0
skipped=0
for entry in "${examples[@]}"; do
  IFS='|' read -r id name need how <<<"$entry"
  wanted "$name" || continue
  log=$LOGS/$name.log
  state=""
  case "$need" in
    kafka) [ "$kafka_state" = up ] || state="skip ($kafka_state)" ;;
    postgres) [ "$postgres_state" = up ] || state="skip ($postgres_state)" ;;
  esac
  begin=$(date +%s)
  if [ -z "$state" ]; then
    printf '%-6s %-20s ' "$id" "$name"
    if run_one "$name" "$how" "$log"; then state=ok; else state=FAIL; failed=1; fi
    echo "$state"
  else
    skipped=$((skipped + 1))
    [ $require_services -eq 1 ] && failed=1
  fi
  results+=("$(printf '%-6s %-20s %-46s %5ss  %s' "$id" "$name" "$state" "$(($(date +%s) - begin))" "$log")")
done

echo
echo "Results:"
for line in "${results[@]}"; do
  echo "  $line"
done
if [ $failed -ne 0 ]; then
  echo "Failed: see the logs above."
  [ $require_services -eq 1 ] && echo "(--require-services: a skipped example counts as a failure)"
  exit 1
fi
if [ $skipped -gt 0 ]; then
  echo "All run ok; $skipped skipped (no service)."
else
  echo "All ok."
fi
