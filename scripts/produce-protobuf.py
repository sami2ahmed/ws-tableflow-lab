#!/usr/bin/env python3
"""Produce raw protobuf events to clickstream, orders, and inventory_updates."""

from __future__ import annotations

import argparse
import random
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "proto"))

from clickstream_pb2 import ClickEvent  # noqa: E402
from inventory_pb2 import InventoryUpdate  # noqa: E402
from orders_pb2 import OrderEvent  # noqa: E402

TOPICS = ("clickstream", "orders", "inventory_updates")

EVENT_TYPES = ("page_view", "click", "scroll")
PAGES = ("/home", "/products", "/cart", "/checkout", "/account", "/search")
REFERRERS = ("", "https://google.com", "https://twitter.com", "https://newsletter.example")
STATUSES = ("created", "confirmed", "shipped", "delivered")
PAYMENT_METHODS = ("card", "paypal", "apple_pay", "bank_transfer")
UPDATE_TYPES = ("sale", "restock", "adjustment")
WAREHOUSES = ("wh-east", "wh-west", "wh-central", "wh-eu")
SKUS = [f"SKU-{i:04d}" for i in range(1, 51)]
DISCOUNT_CODES = ("SAVE10", "WELCOME20", "FLASH15", "VIP25", "FREESHIP")


def now_ms() -> int:
    return int(time.time() * 1000)


def make_click() -> bytes:
    msg = ClickEvent(
        event_id=str(uuid.uuid4()),
        user_id=f"user-{random.randint(1, 200)}",
        session_id=str(uuid.uuid4()),
        page_url=random.choice(PAGES),
        event_type=random.choice(EVENT_TYPES),
        referrer=random.choice(REFERRERS),
        timestamp_ms=now_ms(),
    )
    return msg.SerializeToString()


def make_order(*, with_discount: bool) -> bytes:
    msg = OrderEvent(
        order_id=str(uuid.uuid4()),
        # Same ID space as clickstream.user_id so attribution joins work.
        customer_id=f"user-{random.randint(1, 200)}",
        status=random.choice(STATUSES),
        total_amount=round(random.uniform(5.0, 500.0), 2),
        currency="USD",
        payment_method=random.choice(PAYMENT_METHODS),
        item_count=random.randint(1, 8),
        created_at_ms=now_ms(),
    )
    if with_discount:
        msg.discount_code = random.choice(DISCOUNT_CODES)
    return msg.SerializeToString()


def make_inventory() -> bytes:
    change = random.choice([-5, -3, -1, 1, 2, 5, 10])
    after = max(0, random.randint(0, 200) + change)
    msg = InventoryUpdate(
        sku=random.choice(SKUS),
        warehouse_id=random.choice(WAREHOUSES),
        quantity_change=change,
        quantity_after=after,
        update_type=random.choice(UPDATE_TYPES),
        updated_at_ms=now_ms(),
    )
    return msg.SerializeToString()


def delivery_report(err, _msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)


def produce(broker: str, count: int, with_discount: bool) -> None:
    producer = Producer({"bootstrap.servers": broker})
    print(f"Producing to 3 topics: {', '.join(TOPICS)}")

    builders = {
        "clickstream": lambda: make_click(),
        "orders": lambda: make_order(with_discount=with_discount),
        "inventory_updates": lambda: make_inventory(),
    }

    for topic in TOPICS:
        for _ in range(count):
            producer.produce(topic, builders[topic](), callback=delivery_report)
            producer.poll(0)
        producer.flush()
        suffix = " (with discount_code)" if topic == "orders" and with_discount else ""
        print(f"  {topic + ':':<24}{count} events sent{suffix}")

    print(f"Done. Total: {count * len(TOPICS)} protobuf messages produced.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument(
        "--with-discount",
        action="store_true",
        help="Include optional discount_code on order events (proto field 9)",
    )
    args = parser.parse_args()
    produce(args.broker, args.count, args.with_discount)


if __name__ == "__main__":
    main()
