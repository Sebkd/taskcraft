#!/usr/bin/env bash
# Runs a command, keeping its output in a log; when it fails, repeats the
# lines that tell why as error annotations (visible without the run's logs).
# Usage: annotate.sh <log file> <command> [args...]
set -u
log=$1
shift
"$@" 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
if [ "$status" -ne 0 ]; then
  # Colour codes split words such as `error:`; strip them first.
  # A panic's message is on the line after "panicked at": kept too.
  sed -E 's/\x1b\[[0-9;]*[A-Za-z]//g' "$log" \
    | awk '/error(\[|:)|ERROR|SUMMARY|panicked|Sanitizer|failed to|could not|malformed|invalid/ {
             print; if ($0 ~ /panicked/ && (getline next_line) > 0) print next_line }' \
    | head -n 9 > "$log.why"
  # Nothing recognised: the last lines as they are.
  if [ ! -s "$log.why" ]; then
    sed -E 's/\x1b\[[0-9;]*[A-Za-z]//g' "$log" | grep -v '^[[:space:]]*$' | tail -n 8 > "$log.why"
  fi
  while IFS= read -r line; do echo "::error::${line:0:900}"; done < "$log.why"
  echo "::error::exit status $status"
fi
exit "$status"
