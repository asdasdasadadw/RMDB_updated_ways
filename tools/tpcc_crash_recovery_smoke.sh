#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_NAME="${1:-codex_tpcc_crash_$$}"
WORKERS="${2:-8}"
TXNS_PER_WORKER="${3:-300}"
CRASH_DELAY="${4:-0.2}"

"$ROOT_DIR/tools/tpcc_consistency_smoke.sh" "$DB_NAME" load-only >/tmp/${DB_NAME}.load.out

"$ROOT_DIR/build/bin/rmdb" "$DB_NAME" >/tmp/${DB_NAME}.server.out 2>/tmp/${DB_NAME}.server.err &
SERVER_PID=$!

cleanup() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -9 "$SERVER_PID" 2>/dev/null || true
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

set +e
python3 "$ROOT_DIR/tools/rmdb_concurrent_neworder.py" --workers "$WORKERS" --txns "$TXNS_PER_WORKER" \
    >/tmp/${DB_NAME}.client.out 2>/tmp/${DB_NAME}.client.err &
CLIENT_PID=$!
set -e

sleep "$CRASH_DELAY"
kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
wait "$CLIENT_PID" 2>/dev/null || true
trap - EXIT

CHECK_SQL="/tmp/${DB_NAME}.crash_check.sql"
cat >"$CHECK_SQL" <<'SQL'
select count(*) from orders;
select count(*) from new_orders;
select count(*) from order_line;
select count(*) from new_orders join orders on new_orders.no_o_id=orders.o_id and new_orders.no_d_id=orders.o_d_id and new_orders.no_w_id=orders.o_w_id;
select count(*) from order_line join orders on order_line.ol_o_id=orders.o_id and order_line.ol_d_id=orders.o_d_id and order_line.ol_w_id=orders.o_w_id;
select count(*) from orders where o_id>=1000;
select count(*) from new_orders where no_o_id>=1000;
select count(*) from order_line where ol_o_id>=1000;
select ol_o_id,count(*) from order_line where ol_o_id>=1000 group by ol_o_id having count(*)<>3;
select o_id,count(*) from orders where o_id>=1000 group by o_id having count(*)<>1;
select no_o_id,count(*) from new_orders where no_o_id>=1000 group by no_o_id having count(*)<>1;
SQL

"$ROOT_DIR/build/bin/rmdb" "$DB_NAME" <"$CHECK_SQL" >/tmp/${DB_NAME}.check.out 2>/tmp/${DB_NAME}.check.err

echo "Database: $DB_NAME"
echo "Crash delay: ${CRASH_DELAY}s"
echo "Client stdout:"
cat "/tmp/${DB_NAME}.client.out"
echo "Client stderr:"
cat "/tmp/${DB_NAME}.client.err"
echo "Server stderr:"
cat "/tmp/${DB_NAME}.server.err"
echo "Recovery check output: $ROOT_DIR/$DB_NAME/output.txt"
tail -n 80 "$ROOT_DIR/$DB_NAME/output.txt"
