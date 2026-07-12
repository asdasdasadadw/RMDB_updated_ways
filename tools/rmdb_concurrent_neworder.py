#!/usr/bin/env python3
import argparse
import socket
import threading


def send_sql(sock, sql):
    sock.sendall(sql.encode("utf-8") + b"\0")
    data = sock.recv(8192)
    return data.decode("utf-8", errors="replace")


def worker(worker_id, txn_count, order_start, host, port, results, lock):
    ok = 0
    failed = 0
    with socket.create_connection((host, port), timeout=5) as sock:
        for i in range(txn_count):
            order_id = order_start + worker_id * txn_count + i
            customer_id = order_id % 10 + 1
            statements = [
                "begin;",
                "update district set d_next_o_id=d_next_o_id+1 where d_id=1 and d_w_id=1;",
                f"insert into orders values({order_id},1,1,{customer_id},'2026-06-25 00:00:00',0,3,1);",
                f"insert into new_orders values({order_id},1,1);",
                f"insert into order_line values({order_id},1,1,1,1,1,'2026-06-25 00:00:00',5,10,'codex-concurrent');",
                f"insert into order_line values({order_id},1,1,2,2,1,'2026-06-25 00:00:00',5,20,'codex-concurrent');",
                f"insert into order_line values({order_id},1,1,3,3,1,'2026-06-25 00:00:00',5,30,'codex-concurrent');",
                "commit;",
            ]
            txn_failed = False
            for sql in statements:
                response = send_sql(sock, sql)
                if "failure" in response.lower() or "abort" in response.lower() or "error" in response.lower():
                    txn_failed = True
                    break
            if txn_failed:
                failed += 1
                try:
                    send_sql(sock, "abort;")
                except OSError:
                    break
            else:
                ok += 1
    with lock:
        results["ok"] += ok
        results["failed"] += failed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--txns", type=int, default=20)
    parser.add_argument("--order-start", type=int, default=1000)
    args = parser.parse_args()

    results = {"ok": 0, "failed": 0}
    lock = threading.Lock()
    threads = []
    for worker_id in range(args.workers):
        thread = threading.Thread(
            target=worker,
            args=(worker_id, args.txns, args.order_start, args.host, args.port, results, lock),
        )
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    print(f"committed={results['ok']}")
    print(f"failed={results['failed']}")


if __name__ == "__main__":
    main()
