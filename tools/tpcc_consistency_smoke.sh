#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_NAME="${1:-codex_tpcc_consistency_$$}"
MODE="${2:-mini}"
DATA_DIR="$ROOT_DIR/src/test/performance_test/table_data"
SQL_FILE="/tmp/${DB_NAME}.sql"

rm -rf "$ROOT_DIR/$DB_NAME"
rm -f "$SQL_FILE"

cat >"$SQL_FILE" <<'SQL'
create table warehouse(w_id int,w_name char(10),w_street_1 char(20),w_street_2 char(20),w_city char(20),w_state char(2),w_zip char(9),w_tax float,w_ytd float);
create table district(d_id int,d_w_id int,d_name char(10),d_street_1 char(20),d_street_2 char(20),d_city char(20),d_state char(2),d_zip char(9),d_tax float,d_ytd float,d_next_o_id int);
create table customer(c_id int,c_d_id int,c_w_id int,c_first char(16),c_middle char(2),c_last char(16),c_street_1 char(20),c_street_2 char(20),c_city char(20),c_state char(2),c_zip char(9),c_phone char(16),c_since char(19),c_credit char(2),c_credit_lim float,c_discount float,c_balance float,c_ytd_payment float,c_payment_cnt int,c_delivery_cnt int,c_data char(52));
create table history(h_c_id int,h_c_d_id int,h_c_w_id int,h_d_id int,h_w_id int,h_date char(19),h_amount float,h_data char(26));
create table item(i_id int,i_im_id int,i_name char(24),i_price float,i_data char(52));
create table stock(s_i_id int,s_w_id int,s_quantity int,s_dist_01 char(24),s_dist_02 char(24),s_dist_03 char(24),s_dist_04 char(24),s_dist_05 char(24),s_dist_06 char(24),s_dist_07 char(24),s_dist_08 char(24),s_dist_09 char(24),s_dist_10 char(24),s_ytd int,s_order_cnt int,s_remote_cnt int,s_data char(52));
create table orders(o_id int,o_d_id int,o_w_id int,o_c_id int,o_entry_d char(19),o_carrier_id int,o_ol_cnt int,o_all_local int);
create table new_orders(no_o_id int,no_d_id int,no_w_id int);
create table order_line(ol_o_id int,ol_d_id int,ol_w_id int,ol_number int,ol_i_id int,ol_supply_w_id int,ol_delivery_d char(19),ol_quantity int,ol_amount float,ol_dist_info char(26));
SQL

emit_inserts() {
    local table="$1"
    local file="$2"
    local string_cols="$3"
    awk -F, -v table="$table" -v string_cols="$string_cols" '
        BEGIN {
            split(string_cols, a, ",");
            for (i in a) {
                if (a[i] != "") {
                    is_string[a[i]] = 1;
                }
            }
        }
        NR == 1 { next }
        {
            sub(/\r$/, "");
            printf "insert into %s values(", table;
            for (i = 1; i <= NF; i++) {
                sub(/\r$/, "", $i);
                if (i > 1) {
                    printf ",";
                }
                if (is_string[i]) {
                    gsub(/\047/, "\047\047", $i);
                    printf "\047%s\047", $i;
                } else {
                    printf "%s", $i;
                }
            }
            print ");";
        }
    ' "$file" >>"$SQL_FILE"
}

emit_inserts warehouse "$DATA_DIR/warehouse.csv" "2,3,4,5,6,7"
emit_inserts district "$DATA_DIR/district.csv" "3,4,5,6,7,8"
emit_inserts customer "$DATA_DIR/customer.csv" "4,5,6,7,8,9,10,11,12,13,14,21"
emit_inserts history "$DATA_DIR/history.csv" "6,8"
emit_inserts item "$DATA_DIR/item.csv" "3,5"
emit_inserts stock "$DATA_DIR/stock.csv" "4,5,6,7,8,9,10,11,12,13,17"
emit_inserts orders "$DATA_DIR/orders.csv" "5"
emit_inserts new_orders "$DATA_DIR/new_orders.csv" ""
emit_inserts order_line "$DATA_DIR/order_line.csv" "7,10"

cat >>"$SQL_FILE" <<'SQL'
select count(*) from warehouse;
select count(*) from district;
select count(*) from customer;
select count(*) from history;
select count(*) from item;
select count(*) from stock;
select count(*) from orders;
select count(*) from new_orders;
select count(*) from order_line;

select count(*) from new_orders join orders on new_orders.no_o_id=orders.o_id and new_orders.no_d_id=orders.o_d_id and new_orders.no_w_id=orders.o_w_id;
select count(*) from order_line join orders on order_line.ol_o_id=orders.o_id and order_line.ol_d_id=orders.o_d_id and order_line.ol_w_id=orders.o_w_id;
SQL

if [[ "$MODE" == "mini" ]]; then
cat >>"$SQL_FILE" <<'SQL'
begin;
update district set d_next_o_id=d_next_o_id+1 where d_id=1 and d_w_id=1;
insert into orders values(11,1,1,1,'2026-06-25 00:00:00',0,3,1);
insert into new_orders values(11,1,1);
insert into order_line values(11,1,1,1,1,1,'2026-06-25 00:00:00',5,10,'codex-smoke');
insert into order_line values(11,1,1,2,2,1,'2026-06-25 00:00:00',5,20,'codex-smoke');
insert into order_line values(11,1,1,3,3,1,'2026-06-25 00:00:00',5,30,'codex-smoke');
commit;

select count(*) from orders;
select count(*) from new_orders;
select count(*) from order_line;
select count(*) from new_orders join orders on new_orders.no_o_id=orders.o_id and new_orders.no_d_id=orders.o_d_id and new_orders.no_w_id=orders.o_w_id;
select count(*) from order_line join orders on order_line.ol_o_id=orders.o_id and order_line.ol_d_id=orders.o_d_id and order_line.ol_w_id=orders.o_w_id;
SQL
fi

"$ROOT_DIR/build/bin/rmdb" "$DB_NAME" <"$SQL_FILE" >/tmp/${DB_NAME}.stdout 2>/tmp/${DB_NAME}.stderr

echo "Database: $DB_NAME"
echo "SQL: $SQL_FILE"
echo "Output: $ROOT_DIR/$DB_NAME/output.txt"
if grep -q "failure" "$ROOT_DIR/$DB_NAME/output.txt" || grep -q "failure" "/tmp/${DB_NAME}.stderr"; then
    echo "Result: FAILED"
    cat "/tmp/${DB_NAME}.stderr"
    exit 1
fi
echo "Result: completed"
sed -n '1,220p' "$ROOT_DIR/$DB_NAME/output.txt"
