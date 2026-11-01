#!/usr/bin/env bash
# Show what an enforced dbt contract does: build fct_payment from a throwaway copy of the project
# where amount is declared as string. dbt stops before it touches the table.
set -uo pipefail
cd "$(dirname "$0")/.."
demo=.runtime/contract-demo
rm -rf "$demo" && mkdir -p "$demo" && cp -R dbt/. "$demo/" && rm -rf "$demo/target"
python3 - "$demo/models/silver/_silver__models.yml" <<'PY'
import sys
path = sys.argv[1]; text = open(path).read()
old = "      - name: amount\n        data_type: bigint\n        description: Payment amount in KRW."
assert text.count(old) == 1, "fct_payment.amount block not found"
open(path, "w").write(text.replace(old, old.replace("data_type: bigint", "data_type: string")))
PY
./scripts/lab.sh exec -T runner dbt run --project-dir "$demo" --profiles-dir dbt --select fct_payment
echo "Expected: 'This model has an enforced contract that failed' and amount BIGINT_TYPE vs STRING_TYPE."
