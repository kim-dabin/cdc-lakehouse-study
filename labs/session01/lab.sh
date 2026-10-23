#!/usr/bin/env bash
# Common-example execution for the live demonstration and individual review.
set -euo pipefail
cd "$(dirname "$0")/../.."
namespace="${1:?Usage: lab.sh s01-name-run action [options]}"
shift
if [[ ! "$namespace" =~ ^s01-[a-z0-9-]{3,48}$ ]]; then
  echo 'Use a namespace such as s01-example-run' >&2; exit 2
fi
action="${1:?Action is required}"
shift
case "$action" in
  payment) file=01-payment.sql ;;
  refund) file=02-refund.sql ;;
  correction) file=03-correction.sql ;;
  delete) file=04-delete-optional.sql ;;
  source) file=00-source-check.sql ;;
  source-check|source-answer) file=source-state-check.sql ;;
  refresh)
    exec ./scripts/lab.sh exec -T runner python scripts/pipeline.py ;;
  kafka|source-rows|collect-check)
    exec ./scripts/lab.sh exec -T runner python labs/session01/collection.py "$namespace" "$action" "$@" ;;
  bronze|layers|wait|check)
    exec ./scripts/lab.sh exec -T runner python labs/session01/observe.py "$namespace" "$action" "$@" ;;
  *) echo 'Actions: payment refund correction delete source source-check source-rows kafka collect-check bronze layers wait refresh check' >&2; exit 2 ;;
esac
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 -U source -d app \
  -v "namespace=$namespace" < "labs/session01/sql/$file"
