#!/usr/bin/env python3
import argparse
import csv
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median


DISTRICTS_PER_WAREHOUSE = 10
LAST_NAMES = [
    "BARBARBAR",
    "BARBAROUGHT",
    "BARBARABLE",
    "BARBARPRI",
    "BARBARPRES",
    "BARBARESE",
    "BARBARANTI",
    "BARBARCALLY",
    "BARBARATION",
    "BARBAREING",
]

SCHEMA_SQL = [
    "create table warehouse(w_id int,w_name char(10),w_street_1 char(20),w_street_2 char(20),w_city char(20),w_state char(2),w_zip char(9),w_tax float,w_ytd float);",
    "create table district(d_id int,d_w_id int,d_name char(10),d_street_1 char(20),d_street_2 char(20),d_city char(20),d_state char(2),d_zip char(9),d_tax float,d_ytd float,d_next_o_id int);",
    "create table customer(c_id int,c_d_id int,c_w_id int,c_first char(16),c_middle char(2),c_last char(16),c_street_1 char(20),c_street_2 char(20),c_city char(20),c_state char(2),c_zip char(9),c_phone char(16),c_since char(19),c_credit char(2),c_credit_lim float,c_discount float,c_balance float,c_ytd_payment float,c_payment_cnt int,c_delivery_cnt int,c_data char(52));",
    "create table history(h_c_id int,h_c_d_id int,h_c_w_id int,h_d_id int,h_w_id int,h_date char(19),h_amount float,h_data char(26));",
    "create table item(i_id int,i_im_id int,i_name char(24),i_price float,i_data char(52));",
    "create table stock(s_i_id int,s_w_id int,s_quantity int,s_dist_01 char(24),s_dist_02 char(24),s_dist_03 char(24),s_dist_04 char(24),s_dist_05 char(24),s_dist_06 char(24),s_dist_07 char(24),s_dist_08 char(24),s_dist_09 char(24),s_dist_10 char(24),s_ytd int,s_order_cnt int,s_remote_cnt int,s_data char(52));",
    "create table orders(o_id int,o_d_id int,o_w_id int,o_c_id int,o_entry_d char(19),o_carrier_id int,o_ol_cnt int,o_all_local int);",
    "create table new_orders(no_o_id int,no_d_id int,no_w_id int);",
    "create table order_line(ol_o_id int,ol_d_id int,ol_w_id int,ol_number int,ol_i_id int,ol_supply_w_id int,ol_delivery_d char(19),ol_quantity int,ol_amount float,ol_dist_info char(26));",
]

INDEX_SQL = [
    "create index warehouse(w_id);",
    "create index item(i_id);",
    "create index district(d_w_id,d_id);",
    "create index customer(c_w_id,c_d_id,c_id);",
    "create index customer(c_w_id,c_d_id,c_last,c_first);",
    "create index stock(s_w_id,s_i_id);",
    "create index stock(s_w_id,s_quantity);",
    "create index orders(o_w_id,o_d_id,o_id);",
    "create index orders(o_w_id,o_d_id,o_c_id,o_id);",
    "create index new_orders(no_w_id,no_d_id,no_o_id);",
    "create index order_line(ol_w_id,ol_d_id,ol_o_id,ol_number);",
    "create index order_line(ol_w_id,ol_d_id,ol_i_id);",
    "create index history(h_c_w_id,h_c_d_id,h_c_id);",
]

TABLE_COLUMNS = {
    "warehouse": ["w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"],
    "district": [
        "d_id",
        "d_w_id",
        "d_name",
        "d_street_1",
        "d_street_2",
        "d_city",
        "d_state",
        "d_zip",
        "d_tax",
        "d_ytd",
        "d_next_o_id",
    ],
    "customer": [
        "c_id",
        "c_d_id",
        "c_w_id",
        "c_first",
        "c_middle",
        "c_last",
        "c_street_1",
        "c_street_2",
        "c_city",
        "c_state",
        "c_zip",
        "c_phone",
        "c_since",
        "c_credit",
        "c_credit_lim",
        "c_discount",
        "c_balance",
        "c_ytd_payment",
        "c_payment_cnt",
        "c_delivery_cnt",
        "c_data",
    ],
    "history": ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"],
    "item": ["i_id", "i_im_id", "i_name", "i_price", "i_data"],
    "stock": [
        "s_i_id",
        "s_w_id",
        "s_quantity",
        "s_dist_01",
        "s_dist_02",
        "s_dist_03",
        "s_dist_04",
        "s_dist_05",
        "s_dist_06",
        "s_dist_07",
        "s_dist_08",
        "s_dist_09",
        "s_dist_10",
        "s_ytd",
        "s_order_cnt",
        "s_remote_cnt",
        "s_data",
    ],
    "orders": ["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"],
    "new_orders": ["no_o_id", "no_d_id", "no_w_id"],
    "order_line": [
        "ol_o_id",
        "ol_d_id",
        "ol_w_id",
        "ol_number",
        "ol_i_id",
        "ol_supply_w_id",
        "ol_delivery_d",
        "ol_quantity",
        "ol_amount",
        "ol_dist_info",
    ],
}

SCALES = {
    "tiny": {"warehouses": 1, "items": 100, "customers_per_district": 30, "orders_per_district": 30},
    "small": {"warehouses": 2, "items": 1000, "customers_per_district": 300, "orders_per_district": 300},
    "medium": {"warehouses": 4, "items": 5000, "customers_per_district": 500, "orders_per_district": 500},
    "large": {"warehouses": 8, "items": 20000, "customers_per_district": 1000, "orders_per_district": 1000},
}


def write_csv(path, table, rows):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(TABLE_COLUMNS[table])
        writer.writerows(rows)


def s(prefix, value, max_len):
    text = f"{prefix}{value}"
    return text[:max_len]


def generate_data(data_dir, warehouses, items, customers_per_district, orders_per_district, new_orders_per_district, seed):
    rng = random.Random(seed)
    data_dir.mkdir(parents=True, exist_ok=True)
    initial_order_lines = 0

    write_csv(
        data_dir / "warehouse.csv",
        "warehouse",
        (
            [
                w_id,
                s("W", w_id, 10),
                s("WS1-", w_id, 20),
                s("WS2-", w_id, 20),
                s("WCITY", w_id, 20),
                "ST",
                "123456789",
                round(0.05 + (w_id % 5) * 0.01, 2),
                300000.0,
            ]
            for w_id in range(1, warehouses + 1)
        ),
    )

    write_csv(
        data_dir / "district.csv",
        "district",
        (
            [
                d_id,
                w_id,
                s("D", d_id, 10),
                s("DS1-", d_id, 20),
                s("DS2-", d_id, 20),
                s("DCITY", d_id, 20),
                "ST",
                "123456789",
                round(0.03 + (d_id % 4) * 0.01, 2),
                30000.0,
                orders_per_district + 1,
            ]
            for w_id in range(1, warehouses + 1)
            for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1)
        ),
    )

    def customer_rows():
        for w_id in range(1, warehouses + 1):
            for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
                for c_id in range(1, customers_per_district + 1):
                    yield [
                        c_id,
                        d_id,
                        w_id,
                        s("FIRST", c_id, 16),
                        "OE",
                        LAST_NAMES[c_id % len(LAST_NAMES)],
                        s("CS1-", c_id, 20),
                        s("CS2-", c_id, 20),
                        s("CCITY", c_id, 20),
                        "ST",
                        "123456789",
                        f"{1000000000000000 + c_id}"[:16],
                        "2026-01-01 00:00:00",
                        "GC",
                        50000.0,
                        round((c_id % 50) / 100.0, 2),
                        -10.0,
                        10.0,
                        1,
                        0,
                        s("CDATA", c_id, 52),
                    ]

    write_csv(data_dir / "customer.csv", "customer", customer_rows())

    write_csv(
        data_dir / "history.csv",
        "history",
        (
            [c_id, d_id, w_id, d_id, w_id, "2026-01-01 00:00:00", 10.0, s("H", c_id, 26)]
            for w_id in range(1, warehouses + 1)
            for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1)
            for c_id in range(1, customers_per_district + 1)
        ),
    )

    write_csv(
        data_dir / "item.csv",
        "item",
        ([i_id, 1000 + i_id, s("ITEM", i_id, 24), round(1.0 + (i_id % 100) * 0.5, 2), s("IDATA", i_id, 52)] for i_id in range(1, items + 1)),
    )

    def stock_rows():
        for w_id in range(1, warehouses + 1):
            for i_id in range(1, items + 1):
                yield [
                    i_id,
                    w_id,
                    50 + (i_id % 50),
                    *[s(f"D{d:02d}-", i_id, 24) for d in range(1, 11)],
                    0,
                    0,
                    0,
                    s("SDATA", i_id, 52),
                ]

    write_csv(data_dir / "stock.csv", "stock", stock_rows())

    def order_rows():
        for w_id in range(1, warehouses + 1):
            for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
                for o_id in range(1, orders_per_district + 1):
                    c_id = (o_id % customers_per_district) + 1
                    carrier = 1 if o_id <= orders_per_district - new_orders_per_district else 0
                    yield [o_id, d_id, w_id, c_id, "2026-01-01 00:00:00", carrier, 5, 1]

    write_csv(data_dir / "orders.csv", "orders", order_rows())

    first_new_order = orders_per_district - new_orders_per_district + 1
    write_csv(
        data_dir / "new_orders.csv",
        "new_orders",
        (
            [o_id, d_id, w_id]
            for w_id in range(1, warehouses + 1)
            for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1)
            for o_id in range(first_new_order, orders_per_district + 1)
        ),
    )

    def order_line_rows():
        nonlocal initial_order_lines
        for w_id in range(1, warehouses + 1):
            for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
                for o_id in range(1, orders_per_district + 1):
                    for ol_number in range(1, 6):
                        initial_order_lines += 1
                        i_id = rng.randint(1, items)
                        yield [
                            o_id,
                            d_id,
                            w_id,
                            ol_number,
                            i_id,
                            w_id,
                            "2026-01-01 00:00:00",
                            5,
                            round(1.0 + (i_id % 100) * 0.5, 2),
                            s("OLDIST", ol_number, 26),
                        ]

    write_csv(data_dir / "order_line.csv", "order_line", order_line_rows())

    return {
        "warehouse": warehouses,
        "district": warehouses * DISTRICTS_PER_WAREHOUSE,
        "customer": warehouses * DISTRICTS_PER_WAREHOUSE * customers_per_district,
        "history": warehouses * DISTRICTS_PER_WAREHOUSE * customers_per_district,
        "item": items,
        "stock": warehouses * items,
        "orders": warehouses * DISTRICTS_PER_WAREHOUSE * orders_per_district,
        "new_orders": warehouses * DISTRICTS_PER_WAREHOUSE * new_orders_per_district,
        "order_line": initial_order_lines,
        "sum_d_next_o_id": warehouses * DISTRICTS_PER_WAREHOUSE * (orders_per_district + 1),
    }


def run_command(command, cwd, input_text=None, timeout=None, stdout_path=None, stderr_path=None):
    stdout = open(stdout_path, "w", encoding="utf-8") if stdout_path else subprocess.PIPE
    stderr = open(stderr_path, "w", encoding="utf-8") if stderr_path else subprocess.PIPE
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            input=input_text,
            text=True,
            stdout=stdout,
            stderr=stderr,
            timeout=timeout,
        )
    finally:
        if stdout_path:
            stdout.close()
        if stderr_path:
            stderr.close()
    return result


def init_database(root, db_name, args):
    rmdb = root / "build" / "bin" / "rmdb"
    db_dir = root / db_name
    data_dir = root / f"{db_name}_bench_data"
    if db_dir.exists() and not args.keep_db:
        shutil.rmtree(db_dir)
    if data_dir.exists() and not args.keep_db:
        shutil.rmtree(data_dir)

    t0 = time.time()
    initial = generate_data(
        data_dir,
        args.warehouses,
        args.items,
        args.customers_per_district,
        args.orders_per_district,
        args.new_orders_per_district,
        args.seed,
    )
    generate_seconds = time.time() - t0

    sql = ["set output_file off"]
    sql.extend(SCHEMA_SQL)
    for table in [
        "warehouse",
        "item",
        "district",
        "customer",
        "stock",
        "orders",
        "new_orders",
        "order_line",
        "history",
    ]:
        sql.append(f"load {data_dir / (table + '.csv')} into {table};")
    sql.extend(INDEX_SQL)
    sql_text = "\n".join(sql) + "\n"

    t1 = time.time()
    stdout_path = Path("/tmp") / f"{db_name}.init.out"
    stderr_path = Path("/tmp") / f"{db_name}.init.err"
    result = run_command([str(rmdb), db_name], cwd=root, input_text=sql_text, timeout=args.init_timeout, stdout_path=stdout_path, stderr_path=stderr_path)
    init_seconds = time.time() - t1
    if result.returncode != 0:
        raise RuntimeError(f"init failed, see {stdout_path} and {stderr_path}")
    stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.exists() else ""
    if "failure" in stderr_text.lower():
        raise RuntimeError(f"init reported failure, see {stderr_path}")
    return initial, generate_seconds, init_seconds


def recv_response(sock):
    chunks = []
    while True:
        data = sock.recv(65536)
        if not data:
            break
        if b"\0" in data:
            before, _sep, _after = data.partition(b"\0")
            chunks.append(before)
            break
        chunks.append(data)
    return b"".join(chunks).decode("utf-8", errors="replace")


def send_sql(sock, sql):
    sock.sendall(sql.encode("utf-8") + b"\0")
    return recv_response(sock)


def response_failed(response):
    lowered = response.lower()
    return "failure" in lowered or "abort" in lowered or "error" in lowered


def parse_number(value):
    return float(value) if "." in value else int(value)


def first_table_number(response):
    number_re = re.compile(r"[-+]?\d+(?:\.\d+)?")
    for line in response.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        for cell in cells:
            if number_re.fullmatch(cell):
                return parse_number(cell)
    return None


class SharedState:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.order_lock = threading.Lock()
        self.delivery_locks = {w_id: threading.Lock() for w_id in range(1, args.warehouses + 1)}
        self.next_order = {
            (w_id, d_id): args.orders_per_district + 1
            for w_id in range(1, args.warehouses + 1)
            for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1)
        }
        self.committed = Counter()
        self.measure_committed = Counter()
        self.failed = Counter()
        self.new_order_lines = 0
        self.delivery_deletes = 0
        self.errors = []

    def allocate_order(self, w_id, d_id):
        with self.order_lock:
            key = (w_id, d_id)
            o_id = self.next_order[key]
            self.next_order[key] += 1
            return o_id

    def record_success(self, txn_type, measured, new_order_lines=0, delivery_deletes=0):
        with self.lock:
            self.committed[txn_type] += 1
            if measured:
                self.measure_committed[txn_type] += 1
            self.new_order_lines += new_order_lines
            self.delivery_deletes += delivery_deletes

    def record_failure(self, txn_type):
        with self.lock:
            self.failed[txn_type] += 1

    def record_error(self, worker_id, txn_type, message):
        with self.lock:
            self.errors.append((worker_id, txn_type, message))


def home_warehouse(args, worker_id):
    return worker_id % args.warehouses + 1


def home_district(args, worker_id):
    return (worker_id // max(1, args.warehouses)) % DISTRICTS_PER_WAREHOUSE + 1


def choose_warehouse(rng, args, worker_id):
    if args.partitioned and rng.random() < 0.95:
        return home_warehouse(args, worker_id)
    return rng.randint(1, args.warehouses)


def choose_district(rng, args, worker_id):
    if args.partitioned and rng.random() < 0.90:
        return home_district(args, worker_id)
    return rng.randint(1, DISTRICTS_PER_WAREHOUSE)


def choose_item(rng, args, worker_id):
    if not args.partitioned:
        return rng.randint(1, args.items)
    span = max(1, args.items // max(1, args.workers))
    start = (worker_id * span) % args.items + 1
    end = min(args.items, start + span - 1)
    return rng.randint(start, end)


def generate_new_order(rng, state, worker_id):
    args = state.args
    w_id = choose_warehouse(rng, args, worker_id)
    d_id = choose_district(rng, args, worker_id)
    o_id = state.allocate_order(w_id, d_id)
    c_id = rng.randint(1, args.customers_per_district)
    ol_cnt = rng.randint(5, 15)
    timestamp = "2026-07-08 00:00:00"
    statements = [
        "begin;",
        f"select w_tax from warehouse where w_id = {w_id};",
        f"select d_tax,d_next_o_id from district where d_w_id = {w_id} and d_id = {d_id};",
        f"update district set d_next_o_id=d_next_o_id+1 where d_w_id = {w_id} and d_id = {d_id};",
        f"select c_discount,c_last,c_credit from customer where c_w_id = {w_id} and c_d_id = {d_id} and c_id = {c_id};",
        f"insert into orders values({o_id},{d_id},{w_id},{c_id},'{timestamp}',0,{ol_cnt},1);",
        f"insert into new_orders values({o_id},{d_id},{w_id});",
    ]
    order_lines = []
    for ol_number in range(1, ol_cnt + 1):
        i_id = choose_item(rng, args, worker_id)
        supply_w_id = w_id if rng.random() < 0.9 else choose_warehouse(rng, args, worker_id)
        qty = rng.randint(1, 10)
        amount = round((i_id % 100 + 1) * qty * 0.5, 2)
        order_lines.append((supply_w_id, i_id, ol_number, qty, amount))
    for supply_w_id, i_id, ol_number, qty, amount in sorted(order_lines):
        statements.extend(
            [
                f"select i_price,i_name,i_data from item where i_id = {i_id};",
                f"select s_quantity,s_data,s_dist_{d_id:02d} from stock where s_w_id = {supply_w_id} and s_i_id = {i_id};",
                f"update stock set s_quantity=s_quantity-{qty},s_order_cnt=s_order_cnt+1 where s_w_id = {supply_w_id} and s_i_id = {i_id};",
                f"insert into order_line values({o_id},{d_id},{w_id},{ol_number},{i_id},{supply_w_id},'{timestamp}',{qty},{amount},'bench-new-order');",
            ]
        )
    statements.append("commit;")
    return statements, {"new_order_lines": ol_cnt, "delivery_deletes": 0}


def generate_payment(rng, state, worker_id):
    args = state.args
    w_id = choose_warehouse(rng, args, worker_id)
    d_id = choose_district(rng, args, worker_id)
    c_id = rng.randint(1, args.customers_per_district)
    amount = round(rng.uniform(1.0, 5000.0), 2)
    timestamp = "2026-07-08 00:00:00"
    return [
        "begin;",
        f"select w_street_1,w_street_2,w_city,w_state,w_zip,w_name from warehouse where w_id = {w_id};",
        f"update warehouse set w_ytd=w_ytd+{amount} where w_id = {w_id};",
        f"select d_street_1,d_street_2,d_city,d_state,d_zip,d_name from district where d_w_id = {w_id} and d_id = {d_id};",
        f"update district set d_ytd=d_ytd+{amount} where d_w_id = {w_id} and d_id = {d_id};",
        f"select c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_credit,c_credit_lim,c_discount,c_balance,c_since from customer where c_w_id = {w_id} and c_d_id = {d_id} and c_id = {c_id};",
        f"update customer set c_balance=c_balance-{amount},c_ytd_payment=c_ytd_payment+{amount},c_payment_cnt=c_payment_cnt+1 where c_w_id = {w_id} and c_d_id = {d_id} and c_id = {c_id};",
        f"insert into history values({c_id},{d_id},{w_id},{d_id},{w_id},'{timestamp}',{amount},'bench-payment');",
        "commit;",
    ], {"new_order_lines": 0, "delivery_deletes": 0}


def generate_order_status(rng, state, worker_id):
    args = state.args
    w_id = choose_warehouse(rng, args, worker_id)
    d_id = choose_district(rng, args, worker_id)
    c_id = rng.randint(1, args.customers_per_district)
    o_id = rng.randint(max(1, args.orders_per_district - 20), args.orders_per_district)
    if rng.random() < 0.25:
        last_name = LAST_NAMES[c_id % len(LAST_NAMES)]
        customer_query = f"select c_first,c_middle,c_id,c_balance from customer where c_w_id = {w_id} and c_d_id = {d_id} and c_last = '{last_name}' order by c_first limit 1;"
    else:
        customer_query = f"select c_first,c_middle,c_last,c_balance from customer where c_w_id = {w_id} and c_d_id = {d_id} and c_id = {c_id};"
    return [
        "begin;",
        customer_query,
        f"select o_id,o_entry_d,o_carrier_id from orders where o_w_id = {w_id} and o_d_id = {d_id} and o_c_id = {c_id} order by o_id desc limit 1;",
        f"select ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d from order_line where ol_w_id = {w_id} and ol_d_id = {d_id} and ol_o_id = {o_id};",
        "commit;",
    ], {"new_order_lines": 0, "delivery_deletes": 0}


def generate_stock_level(rng, state, worker_id):
    args = state.args
    w_id = choose_warehouse(rng, args, worker_id)
    d_id = choose_district(rng, args, worker_id)
    threshold = rng.randint(10, 20)
    start_o = max(1, args.orders_per_district - 20)
    end_o = args.orders_per_district
    return [
        "begin;",
        f"select d_next_o_id from district where d_w_id = {w_id} and d_id = {d_id};",
        f"select ol_i_id from order_line where ol_w_id = {w_id} and ol_d_id = {d_id} and ol_o_id >= {start_o} and ol_o_id <= {end_o};",
        f"select count(*) from stock where s_w_id = {w_id} and s_quantity < {threshold};",
        "commit;",
    ], {"new_order_lines": 0, "delivery_deletes": 0}


def execute_delivery(sock, rng, state, worker_id):
    args = state.args
    w_id = choose_warehouse(rng, args, worker_id)
    carrier_id = rng.randint(1, 10)
    timestamp = "2026-07-08 00:00:00"
    chosen = []
    lock = state.delivery_locks[w_id]
    with lock:
        response = send_sql(sock, "begin;")
        if response_failed(response):
            return False, {"new_order_lines": 0, "delivery_deletes": 0}
        for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
            response = send_sql(sock, f"select no_o_id from new_orders where no_w_id = {w_id} and no_d_id = {d_id} order by no_o_id limit 1;")
            if response_failed(response):
                send_sql(sock, "abort;")
                return False, {"new_order_lines": 0, "delivery_deletes": 0}
            no_o_id = first_table_number(response)
            if no_o_id is None:
                continue
            no_o_id = int(no_o_id)
            response = send_sql(sock, f"select o_c_id from orders where o_w_id = {w_id} and o_d_id = {d_id} and o_id = {no_o_id};")
            if response_failed(response):
                send_sql(sock, "abort;")
                return False, {"new_order_lines": 0, "delivery_deletes": 0}
            c_id = first_table_number(response)
            if c_id is None:
                continue
            c_id = int(c_id)
            total_amount = round(rng.uniform(50.0, 500.0), 2)
            statements = [
                f"delete from new_orders where no_w_id = {w_id} and no_d_id = {d_id} and no_o_id = {no_o_id};",
                f"update orders set o_carrier_id = {carrier_id} where o_w_id = {w_id} and o_d_id = {d_id} and o_id = {no_o_id};",
                f"update order_line set ol_delivery_d = '{timestamp}' where ol_w_id = {w_id} and ol_d_id = {d_id} and ol_o_id = {no_o_id};",
                f"select sum(ol_amount) from order_line where ol_w_id = {w_id} and ol_d_id = {d_id} and ol_o_id = {no_o_id};",
                f"update customer set c_balance=c_balance+{total_amount},c_delivery_cnt=c_delivery_cnt+1 where c_w_id = {w_id} and c_d_id = {d_id} and c_id = {c_id};",
            ]
            for sql in statements:
                response = send_sql(sock, sql)
                if response_failed(response):
                    send_sql(sock, "abort;")
                    return False, {"new_order_lines": 0, "delivery_deletes": 0}
            chosen.append(no_o_id)
        response = send_sql(sock, "commit;")
        if response_failed(response):
            send_sql(sock, "abort;")
            return False, {"new_order_lines": 0, "delivery_deletes": 0}
    return True, {"new_order_lines": 0, "delivery_deletes": len(chosen)}


def execute_statement_txn(sock, statements):
    for sql in statements:
        try:
            response = send_sql(sock, sql)
        except (OSError, TimeoutError) as err:
            return False, f"{type(err).__name__} while executing: {sql}"
        if response_failed(response):
            try:
                send_sql(sock, "abort;")
            except OSError:
                pass
            return False, f"server rejected: {sql} => {response[:200]}"
    return True, ""


def worker(worker_id, state, host, port, start_time, measure_start, end_time):
    rng = random.Random(state.args.seed + worker_id * 1009)
    generators = [
        ("new_order", 45, generate_new_order),
        ("payment", 43, generate_payment),
        ("order_status", 4, generate_order_status),
        ("delivery", 4, None),
        ("stock_level", 4, generate_stock_level),
    ]
    cumulative = []
    total = 0
    for name, weight, func in generators:
        total += weight
        cumulative.append((total, name, func))

    with socket.create_connection((host, port), timeout=10) as sock:
        sock.settimeout(30)
        send_sql(sock, "set output_file off")
        send_sql(sock, "set transaction isolation level snapshot isolation;")
        while time.time() < start_time:
            time.sleep(0.001)
        while time.time() < end_time:
            pick = rng.randint(1, total)
            for threshold, txn_type, func in cumulative:
                if pick <= threshold:
                    break
            if txn_type == "delivery":
                try:
                    ok, meta = execute_delivery(sock, rng, state, worker_id)
                    error = ""
                except (OSError, TimeoutError) as err:
                    ok = False
                    meta = {"new_order_lines": 0, "delivery_deletes": 0}
                    error = f"{type(err).__name__} in delivery"
            else:
                statements, meta = func(rng, state, worker_id)
                ok, error = execute_statement_txn(sock, statements)
            measured = measure_start <= time.time() < end_time
            if ok:
                state.record_success(
                    txn_type,
                    measured,
                    new_order_lines=meta["new_order_lines"] if txn_type == "new_order" else 0,
                    delivery_deletes=meta["delivery_deletes"] if txn_type == "delivery" else 0,
                )
            else:
                state.record_failure(txn_type)
                if error:
                    state.record_error(worker_id, txn_type, error)
                    break


def wait_for_server(host, port, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def query_scalar(host, port, sql):
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.settimeout(30)
        send_sql(sock, "set output_file off")
        response = send_sql(sock, sql)
    if response_failed(response):
        raise RuntimeError(f"query failed: {sql}\n{response}")
    value = first_table_number(response)
    if value is None:
        raise RuntimeError(f"query returned no scalar: {sql}\n{response}")
    return value


def run_consistency_check(host, port, initial, state):
    expected = {
        "orders": initial["orders"] + state.committed["new_order"],
        "new_orders": initial["new_orders"] + state.committed["new_order"] - state.delivery_deletes,
        "order_line": initial["order_line"] + state.new_order_lines,
        "history": initial["history"] + state.committed["payment"],
        "sum_d_next_o_id": initial["sum_d_next_o_id"] + state.committed["new_order"],
    }
    actual = {
        "orders": int(query_scalar(host, port, "select count(*) from orders;")),
        "new_orders": int(query_scalar(host, port, "select count(*) from new_orders;")),
        "order_line": int(query_scalar(host, port, "select count(*) from order_line;")),
        "history": int(query_scalar(host, port, "select count(*) from history;")),
        "sum_d_next_o_id": int(float(query_scalar(host, port, "select sum(d_next_o_id) from district;"))),
        "new_orders_join": int(query_scalar(host, port, "select count(*) from new_orders join orders on new_orders.no_o_id=orders.o_id and new_orders.no_d_id=orders.o_d_id and new_orders.no_w_id=orders.o_w_id;")),
        "order_line_join": int(query_scalar(host, port, "select count(*) from order_line join orders on order_line.ol_o_id=orders.o_id and order_line.ol_d_id=orders.o_d_id and order_line.ol_w_id=orders.o_w_id;")),
    }
    checks = {
        "orders": actual["orders"] == expected["orders"],
        "new_orders": actual["new_orders"] == expected["new_orders"],
        "order_line": actual["order_line"] == expected["order_line"],
        "history": actual["history"] == expected["history"],
        "sum_d_next_o_id": actual["sum_d_next_o_id"] == expected["sum_d_next_o_id"],
        "new_orders_join": actual["new_orders_join"] == actual["new_orders"],
        "order_line_join": actual["order_line_join"] == actual["order_line"],
    }
    return expected, actual, checks


def apply_scale(args):
    scale = SCALES[args.scale]
    for key, value in scale.items():
        if getattr(args, key) is None:
            setattr(args, key, value)
    if args.new_orders_per_district is None:
        args.new_orders_per_district = max(1, args.orders_per_district * 3 // 10)
    if args.new_orders_per_district > args.orders_per_district:
        raise ValueError("--new-orders-per-district must be <= --orders-per-district")


def parse_args():
    parser = argparse.ArgumentParser(description="Approximate local TPC-C benchmark for RMDB.")
    parser.add_argument("--db-name", default=f"codex_tpcc_large_{os.getpid()}")
    parser.add_argument("--scale", choices=sorted(SCALES), default="small")
    parser.add_argument("--warehouses", type=int)
    parser.add_argument("--items", type=int)
    parser.add_argument("--customers-per-district", type=int)
    parser.add_argument("--orders-per-district", type=int)
    parser.add_argument("--new-orders-per-district", type=int)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--warmup", type=float, default=5.0)
    parser.add_argument("--measure", type=float, default=30.0)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument(
        "--platform-profile",
        action="store_true",
        help="Use the official-style timing window: 30s warmup, 360s measure, repeat 3, median tpmC.",
    )
    parser.add_argument("--seed", type=int, default=20260708)
    parser.add_argument("--partitioned", dest="partitioned", action="store_true", default=True)
    parser.add_argument("--no-partitioned", dest="partitioned", action="store_false")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--init-timeout", type=int, default=900)
    return parser.parse_args()


def run_round(root, rmdb, args, db_name, round_no):
    print(f"== round {round_no} ==")
    initial, generate_seconds, init_seconds = init_database(root, db_name, args)
    print(f"data generation: {generate_seconds:.2f}s")
    print(f"load + index init: {init_seconds:.2f}s")
    print(f"initial rows: orders={initial['orders']} new_orders={initial['new_orders']} order_line={initial['order_line']}")

    server_stdout = Path("/tmp") / f"{db_name}.server.out"
    server_stderr = Path("/tmp") / f"{db_name}.server.err"
    server_out = open(server_stdout, "w", encoding="utf-8")
    server_err = open(server_stderr, "w", encoding="utf-8")
    server = subprocess.Popen([str(rmdb), db_name], cwd=root, stdout=server_out, stderr=server_err, text=True)
    try:
        if not wait_for_server(args.host, args.port, 10):
            raise RuntimeError(f"server did not start, see {server_stdout} and {server_stderr}")

        state = SharedState(args)
        start = time.time() + 0.5
        measure_start = start + args.warmup
        end = measure_start + args.measure
        threads = [
            threading.Thread(target=worker, args=(i, state, args.host, args.port, start, measure_start, end), daemon=True)
            for i in range(args.workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        measured_new_order = state.measure_committed["new_order"]
        tpmc = measured_new_order * 60.0 / args.measure if args.measure > 0 else 0.0
        print("== workload result ==")
        print(f"measure_new_order={measured_new_order}")
        print(f"approx_tpmC={tpmc:.2f}")
        print(f"committed={dict(state.committed)}")
        print(f"measure_committed={dict(state.measure_committed)}")
        print(f"failed={dict(state.failed)}")
        if state.errors:
            print("worker_errors:")
            for worker_id, txn_type, message in state.errors[:10]:
                print(f"  worker={worker_id} txn={txn_type} {message}")

        expected, actual, checks = run_consistency_check(args.host, args.port, initial, state)
        print("== consistency ==")
        for key in ["orders", "new_orders", "order_line", "history", "sum_d_next_o_id"]:
            print(f"{key}: actual={actual[key]} expected={expected[key]} {'OK' if checks[key] else 'BAD'}")
        print(f"new_orders_join: actual={actual['new_orders_join']} expected={actual['new_orders']} {'OK' if checks['new_orders_join'] else 'BAD'}")
        print(f"order_line_join: actual={actual['order_line_join']} expected={actual['order_line']} {'OK' if checks['order_line_join'] else 'BAD'}")
        all_ok = all(checks.values()) and not state.errors
        print(f"result={'PASS' if all_ok else 'FAIL'}")
        return all_ok, tpmc
    finally:
        if server.poll() is None:
            server.send_signal(signal.SIGINT)
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        server_out.close()
        server_err.close()


def main():
    args = parse_args()
    if args.platform_profile:
        args.warmup = 30.0
        args.measure = 360.0
        args.repeat = 3
    apply_scale(args)
    if args.repeat < 1:
        raise ValueError("--repeat must be >= 1")

    root = Path(__file__).resolve().parents[1]
    rmdb = root / "build" / "bin" / "rmdb"
    if not rmdb.exists():
        print(f"missing binary: {rmdb}", file=sys.stderr)
        return 2

    print("== RMDB approximate TPC-C local bench ==")
    print(
        f"db={args.db_name} scale={args.scale} W={args.warehouses} items={args.items} "
        f"C/D={args.customers_per_district} O/D={args.orders_per_district} workers={args.workers} "
        f"warmup={args.warmup}s measure={args.measure}s repeat={args.repeat}"
    )
    if args.platform_profile:
        print("profile=platform-like timing only; workload generator and data are local approximations")

    results = []
    for round_idx in range(1, args.repeat + 1):
        db_name = args.db_name if args.repeat == 1 else f"{args.db_name}_r{round_idx}"
        ok, tpmc = run_round(root, rmdb, args, db_name, round_idx)
        results.append((ok, tpmc))
        if not ok:
            print("benchmark stopped after failed round")
            return 1

    if len(results) > 1:
        tpmcs = [tpmc for _ok, tpmc in results]
        print("== repeated result ==")
        print("round_tpmC=" + ",".join(f"{value:.2f}" for value in tpmcs))
        print(f"median_tpmC={median(tpmcs):.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
