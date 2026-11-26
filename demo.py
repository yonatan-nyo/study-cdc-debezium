"""Run after docker compose up. Requires only the Python standard library."""
import json
import subprocess
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

ROOT = Path(__file__).resolve().parent
TOPIC = "kilatmart.public.orders"
GROUP = "kilatmart-analytics"


def compose(*args):
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT, encoding="utf-8", capture_output=True, timeout=35,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"docker compose {' '.join(args[:3])}: {detail}")
    return result.stdout.strip()


def sql(service, database, statement):
    return compose(
        "exec", "-T", service, "psql", "-U", "demo", "-d", database,
        "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", statement,
    )


def wait_for(label, read, meets, timeout=30):
    deadline = monotonic() + timeout
    last = None
    while monotonic() < deadline:
        last = read()
        if meets(last):
            return last
        sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {label}. Last observed: {last}")


def group_offsets():
    raw = compose(
        "exec", "-T", "kafka", "/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "kafka:9092", "--describe", "--group", GROUP,
    )
    for line in raw.splitlines():
        fields = line.split()
        if fields[:3] == [GROUP, TOPIC, "0"] and len(fields) >= 6:
            if not all(value.isdigit() for value in fields[3:6]):
                return None
            current, end, lag = map(int, fields[3:6])
            return {"committed_offset": current, "log_end_offset": end, "lag": lag}
    return None


def references(refs):
    # The only callers pass references generated from a hexadecimal UUID.
    return ", ".join(f"'{ref}'" for ref in refs)


def add_orders(refs):
    products = [
        ("KOPI-250G", 1, 68000),
        ("TEH-25CT", 2, 22000),
        ("GULA-1KG", 1, 18000),
    ]
    values = ", ".join(
        f"('{ref}', '{sku}', {quantity}, {price}, 'paid')"
        for ref, (sku, quantity, price) in zip(refs, products, strict=False)
    )
    return sql("orders-db", "shop", f"""
        INSERT INTO orders (order_ref, sku, quantity, unit_price_idr, status)
        VALUES {values}
        RETURNING order_ref;
    """).splitlines()


def analytics_rows(refs):
    return sql("analytics-db", "analytics", f"""
        SELECT order_ref || '@' || kafka_offset
        FROM order_changes
        WHERE operation = 'c' AND order_ref IN ({references(refs)})
        ORDER BY kafka_offset;
    """).splitlines()


def caught_up(offsets):
    return offsets is not None and offsets["lag"] == 0


def main():
    print("Waiting for the five seed orders in analytics…", flush=True)
    wait_for(
        "five seed records",
        lambda: sql(
            "analytics-db", "analytics",
            "SELECT count(*) FROM order_changes WHERE operation = 'r' "
            "AND order_ref IN ('KM-1001','KM-1002','KM-1003','KM-1004','KM-1005');"
        ),
        lambda count: count == "5",
        timeout=120,
    )
    wait_for("idle consumer group", group_offsets, caught_up)

    prefix = f"KM-DEMO-{uuid4().hex[:8].upper()}"
    live_ref = f"{prefix}-1"
    paused_refs = [f"{prefix}-2", f"{prefix}-3"]
    print(f"1. Insert {live_ref} while the consumer is running.", flush=True)
    started = monotonic()
    add_orders([live_ref])
    live_rows = wait_for("live order in analytics", lambda: analytics_rows([live_ref]), bool)
    live_seconds = round(monotonic() - started, 3)
    print(f"   Analytics: {live_rows[0]} after {live_seconds:.3f} s.", flush=True)
    before = wait_for("committed live order", group_offsets, caught_up)

    print("2. Stop the consumer, then insert two more orders.", flush=True)
    compose("stop", "consumer")
    resumed = False
    try:
        inserted = add_orders(paused_refs)
        paused = wait_for(
            "two queued changes", group_offsets,
            lambda state: state is not None and state["lag"] == len(paused_refs),
        )
        missing = analytics_rows(paused_refs)
        if set(inserted) != set(paused_refs) or missing or (
            paused["committed_offset"] != before["committed_offset"]
        ):
            raise RuntimeError(f"Unexpected state during outage: {inserted=}, {paused=}, {missing=}")
        print(f"   Source saved {len(inserted)}. Analytics has 0. Kafka lag: {paused['lag']}.",
              flush=True)

        print("3. Restart the same consumer group.", flush=True)
        resumed_at = monotonic()
        compose("start", "consumer")
        resumed = True
        recovered = wait_for(
            "both queued orders in analytics", lambda: analytics_rows(paused_refs),
            lambda rows: len(rows) == len(paused_refs),
        )
        recovery_seconds = round(monotonic() - resumed_at, 3)
        after = wait_for("lag back to zero", group_offsets, caught_up)
        recovered_refs = {row.rsplit("@", 1)[0] for row in recovered}
        recovered_offsets = [int(row.rsplit("@", 1)[1]) for row in recovered]
        expected_offsets = list(range(before["committed_offset"], paused["log_end_offset"]))
        if (
            recovered_refs != set(paused_refs) or recovered_offsets != expected_offsets
            or after["committed_offset"] != paused["log_end_offset"]
        ):
            raise RuntimeError(f"Unexpected recovery: {recovered=}, {after=}")
        print(f"   Analytics: {', '.join(recovered)} after {recovery_seconds:.3f} s. "
              f"Kafka lag: {after['lag']}.", flush=True)
    finally:
        if not resumed:
            compose("start", "consumer")

    evidence = {
        "topic": TOPIC, "group": GROUP,
        "live_rows": live_rows, "live_seconds": live_seconds,
        "before_stop": before, "while_stopped": paused, "after_restart": after,
        "paused_source_refs": inserted, "paused_analytics_rows": missing,
        "recovered_rows": recovered, "recovery_seconds": recovery_seconds,
    }
    path = ROOT / "demo-evidence.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(f"Saved {path.name}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(str(exc)) from exc
