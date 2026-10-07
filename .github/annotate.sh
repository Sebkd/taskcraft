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
  grep -E 'error(\[|:)|ERROR|SUMMARY|panicked|Sanitizer|failed to|could not' "$log" \
    | head -n 9 \
    | while IFS= read -r line; do echo "::error::${line:0:900}"; done
  echo "::error::exit status $status"
fi
exit "$status"
