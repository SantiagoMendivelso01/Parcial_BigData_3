"""
generate_events.py
Genera dataset sintético de eventos de comportamiento para ShopStream.
Produce >= 500,000 registros diarios con distribuciones realistas.

Uso:
    python generate_events.py --date 2024-01-15 --output ./output
    python generate_events.py --date 2024-01-15 --output ./output --records 500000
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import faker
import numpy as np

fake = faker.Faker()
random.seed(42)
np.random.seed(42)

CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "beauty", "toys", "food"]
CATEGORY_WEIGHTS = [0.25, 0.20, 0.15, 0.12, 0.10, 0.08, 0.06, 0.04]

PRODUCTS = [
    {
        "product_id": f"PROD-{i:05d}",
        "category": random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS)[0],
        "price": round(random.lognormvariate(3.5, 1.2), 2),
    }
    for i in range(1, 1001)
]

PAGE_TYPES = {
    "home":     ["/", "/home"],
    "category": [f"/categoria/{c}" for c in CATEGORIES],
    "product":  [f"/producto/PROD-{i:05d}" for i in range(1, 201)],
    "cart":     ["/carrito"],
    "checkout": ["/checkout", "/checkout/pago", "/checkout/confirmacion"],
    "search":   ["/buscar"],
    "other":    ["/about", "/contacto", "/blog", "/faq"],
}
PAGE_TYPE_WEIGHTS = [0.20, 0.25, 0.30, 0.10, 0.05, 0.07, 0.03]

DEVICE_TYPES   = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]

COUNTRIES       = ["CO", "MX", "AR", "CL", "PE", "BR", "US", "ES"]
COUNTRY_WEIGHTS = [0.30, 0.20, 0.15, 0.10, 0.08, 0.07, 0.05, 0.05]

ELEMENT_TYPES = ["button", "link", "image", "product_card", "menu", "banner", "search_result"]
ELEM_WEIGHTS  = [0.20, 0.25, 0.15, 0.25, 0.05, 0.05, 0.05]

SEARCH_QUERIES = [
    "zapatos deportivos", "camiseta hombre", "laptop gaming", "auriculares bluetooth",
    "silla oficina", "libro python", "crema hidratante", "juguetes ninos",
    "television 4k", "celular samsung", "vestido mujer", "perfume hombre",
    "mochila viaje", "tenis running", "tableta grafica",
]


def random_timestamp(date: datetime) -> str:
    hour_weights = [
        1, 0.5, 0.3, 0.3, 0.3, 0.5,
        1,   2,   3,   4,   5,   6,
        7,   5,   4,   4,   5,   6,
        8,   9,   8,   6,   4,   2,
    ]
    hour   = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    ts = date.replace(hour=hour, minute=minute, second=second, tzinfo=timezone.utc)
    return ts.isoformat()


def gen_page_view(user_id, session_id, date):
    page_type = random.choices(list(PAGE_TYPES.keys()), weights=PAGE_TYPE_WEIGHTS)[0]
    page_url  = random.choice(PAGE_TYPES[page_type])
    time_ranges = {
        "home": (5, 60), "category": (15, 120), "product": (30, 300),
        "cart": (20, 180), "checkout": (60, 600), "search": (10, 90), "other": (5, 120),
    }
    lo, hi = time_ranges[page_type]
    return {
        "event_type": "page_view",
        "user_id": user_id,
        "session_id": session_id,
        "page_url": page_url,
        "page_type": page_type,
        "timestamp": random_timestamp(date),
        "time_on_page_seconds": max(lo, int(np.random.exponential(scale=(hi - lo) / 3)) + lo),
        "referrer": random.choice(["", "google.com", "facebook.com", "instagram.com", "direct"]),
        "device_type": random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0],
        "country": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0],
    }


def gen_click(user_id, session_id, page_url, date):
    return {
        "event_type": "click",
        "user_id": user_id,
        "session_id": session_id,
        "element_id": f"elem-{uuid.uuid4().hex[:8]}",
        "element_type": random.choices(ELEMENT_TYPES, weights=ELEM_WEIGHTS)[0],
        "page_url": page_url,
        "timestamp": random_timestamp(date),
        "x_position": random.randint(0, 1920),
        "y_position": random.randint(0, 1080),
    }


def gen_search(user_id, session_id, date):
    return {
        "event_type": "search",
        "user_id": user_id,
        "session_id": session_id,
        "query": random.choice(SEARCH_QUERIES),
        "results_count": int(np.random.exponential(scale=50)) + 1,
        "timestamp": random_timestamp(date),
    }


def gen_product_view(user_id, session_id, date):
    product = random.choice(PRODUCTS)
    return {
        "event_type": "product_view",
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product["product_id"],
        "category": product["category"],
        "price": product["price"],
        "timestamp": random_timestamp(date),
        "time_on_page_seconds": random.randint(10, 300),
    }


def gen_cart_event(user_id, session_id, date):
    product = random.choice(PRODUCTS)
    return {
        "event_type": "cart_event",
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product["product_id"],
        "action": random.choices(["add", "remove"], weights=[0.75, 0.25])[0],
        "timestamp": random_timestamp(date),
    }


def generate_session(user_id, date):
    session_id = f"sess-{uuid.uuid4().hex}"
    events = []

    n_page_views = max(1, int(np.random.exponential(scale=3)))
    page_url = random.choice(PAGE_TYPES["home"])

    for _ in range(n_page_views):
        pv = gen_page_view(user_id, session_id, date)
        events.append(pv)
        page_url = pv["page_url"]

        if random.random() < 0.40:
            events.append(gen_click(user_id, session_id, page_url, date))

    if random.random() < 0.30:
        events.append(gen_search(user_id, session_id, date))

    if random.random() < 0.45:
        events.append(gen_product_view(user_id, session_id, date))

        if random.random() < 0.25:
            events.append(gen_cart_event(user_id, session_id, date))

    return events


def generate_day(date, n_records=500_000):
    all_events = []
    n_users  = 50_000
    user_ids = [f"usr-{uuid.uuid4().hex[:12]}" for _ in range(n_users)]

    while len(all_events) < n_records:
        user_id = random.choice(user_ids)
        all_events.extend(generate_session(user_id, date))

    return all_events[:n_records]


def save_partitioned(events, base_output, date):
    partition = base_output / f"year={date.year}" / f"month={date.month:02d}" / f"day={date.day:02d}"
    partition.mkdir(parents=True, exist_ok=True)

    by_type = {}
    for ev in events:
        by_type.setdefault(ev["event_type"], []).append(ev)

    saved = {}
    for etype, records in by_type.items():
        filepath = partition / f"{etype}.jsonl"
        with open(filepath, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        saved[etype] = filepath
        print(f"  OK {etype}: {len(records):,} registros -> {filepath}")

    return saved


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    parser.add_argument("--output",  default="./output")
    parser.add_argument("--records", type=int, default=500_000)
    args = parser.parse_args()

    date        = datetime.strptime(args.date, "%Y-%m-%d")
    base_output = Path(args.output)

    print(f"\nShopStream - Generando {args.records:,} eventos para {args.date}")
    events = generate_day(date, args.records)

    print(f"\nGuardando en {base_output}...")
    save_partitioned(events, base_output, date)
    print(f"\nTotal generado: {len(events):,} eventos")


if __name__ == "__main__":
    main()
