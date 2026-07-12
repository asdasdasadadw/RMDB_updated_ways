#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_NAME="${1:-codex_tpcc_concurrent_$$}"
WORKERS="${2:-4}"
TXNS_PER_WORKER="${3:-20}"

"$ROOT_DIR/tools/tpcc_consistency_smoke.sh" "$DB_NAME" load-only >/tmp/${DB_NAME}.load.out

"$ROOT_DIR/build/bin/rmdb" "$DB_NAME" >/tmp/${DB_NAME}.server.out 2>/tmp/${DB_NAME}.server.err &
SERVER_PID=$!

cleanup() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

for _ in $(seq 1 50); do
    if python3 - <<'PY'
import socket
try:
    s = socket.create_connection(("127.0.0.1", 8765), timeout=0.1)
    s.close()
    raise SystemExit(0)
except OSError:
    raise SystemExit(1)
PY
    then
        break
    fi
    sleep 0.1
done

CLIENT_OUT="$(python3 "$ROOT_DIR/tools/rmdb_concurrent_neworder.py" --workers "$WORKERS" --txns "$TXNS_PER_WORKER")"
echo "$CLIENT_OUT"
COMMITTED="$(printf '%s\n' "$CLIENT_OUT" | awk -F= '/committed=/{print $2}')"

cleanup
trap - EXIT

CHECK_SQL="/tmp/${DB_NAME}.check.sql"
cat >"$CHECK_SQL" <<SQL
select count(*) from orders;
select count(*) from new_orders;
select count(*) from order_line;
select count(*) from new_orders join orders on new_orders.no_o_id=orders.o_id and new_orders.no_d_id=orders.o_d_id and new_orders.no_w_id=orders.o_w_id;
select count(*) from order_line join orders on order_line.ol_o_id=orders.o_id and order_line.ol_d_id=orders.o_d_id and order_line.ol_w_id=orders.o_w_id;
SQL

"$ROOT_DIR/build/bin/rmdb" "$DB_NAME" <"$CHECK_SQL" >/tmp/${DB_NAME}.check.out 2>/tmp/${DB_NAME}.check.err

echo "Database: $DB_NAME"
echo "Committed transactions reported by client: $COMMITTED"
echo "Expected orders: $((30 + COMMITTED))"
echo "Expected new_orders: $((18 + COMMITTED))"
echo "Expected order_line: $((300 + COMMITTED * 3))"
echo "Output: $ROOT_DIR/$DB_NAME/output.txt"
tail -n 30 "$ROOT_DIR/$DB_NAME/output.txt"
