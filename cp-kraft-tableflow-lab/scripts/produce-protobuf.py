#!/usr/bin/env python3
"""Produce raw protobuf events without requiring protoc or generated _pb2 files."""
from __future__ import annotations

import argparse
import random
import struct
import time
import uuid
from typing import Callable

from confluent_kafka import Producer

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


def varint(value: int) -> bytes:
    if value < 0:
        value &= (1 << 64) - 1
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def field_string(number: int, value: str) -> bytes:
    raw = value.encode()
    return varint((number << 3) | 2) + varint(len(raw)) + raw


def field_int(number: int, value: int) -> bytes:
    return varint(number << 3) + varint(value)


def field_double(number: int, value: float) -> bytes:
    return varint((number << 3) | 1) + struct.pack("<d", value)


def now_ms() -> int:
    return int(time.time() * 1000)


def make_click() -> bytes:
    values = (
        (1, str(uuid.uuid4())),
        (2, f"user-{random.randint(1, 200)}"),
        (3, str(uuid.uuid4())),
        (4, random.choice(PAGES)),
        (5, random.choice(EVENT_TYPES)),
        (6, random.choice(REFERRERS)),
        (7, now_ms()),
    )
    return b"".join(field_int(n, v) if n == 7 else field_string(n, v) for n, v in values)


def make_order(with_discount: bool) -> bytes:
    out = b"".join(
        (
            field_string(1, str(uuid.uuid4())),
            field_string(2, f"user-{random.randint(1, 200)}"),
            field_string(3, random.choice(STATUSES)),
            field_double(4, round(random.uniform(5.0, 500.0), 2)),
            field_string(5, "USD"),
            field_string(6, random.choice(PAYMENT_METHODS)),
            field_int(7, random.randint(1, 8)),
            field_int(8, now_ms()),
        )
    )
    if with_discount:
        out += field_string(9, random.choice(DISCOUNT_CODES))
    return out


def make_inventory() -> bytes:
    change = random.choice((-5, -3, -1, 1, 2, 5, 10))
    after = max(0, random.randint(0, 200) + change)
    return b"".join(
        (
            field_string(1, random.choice(SKUS)),
            field_string(2, random.choice(WAREHOUSES)),
            field_int(3, change),
            field_int(4, after),
            field_string(5, random.choice(UPDATE_TYPES)),
            field_int(6, now_ms()),
        )
    )


def delivery_report(err, _message) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")


def produce(broker: str, count: int, with_discount: bool) -> None:
    producer = Producer({"bootstrap.servers": broker})
    builders: dict[str, Callable[[], bytes]] = {
        "clickstream": make_click,
        "orders": lambda: make_order(with_discount),
        "inventory_updates": make_inventory,
    }
    for topic in TOPICS:
        for _ in range(count):
            producer.produce(topic, value=builders[topic](), callback=delivery_report)
            producer.poll(0)
        producer.flush()
        suffix = " with discount_code" if topic == "orders" and with_discount else ""
        print(f"{topic}: {count} events sent{suffix}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default="localhost:19090")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--with-discount", action="store_true")
    args = parser.parse_args()
    produce(args.broker, args.count, args.with_discount)


if __name__ == "__main__":
    main()
