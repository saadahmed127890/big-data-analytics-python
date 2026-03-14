import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUT_DIR = Path("data/landing")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# We'll generate 3 small "daily" files to simulate batch ingestion.
# Each line is one JSON event (common pattern for logs).
start = datetime(2026, 2, 18, 8, 0, 0)

countries = ["CA", "US", "PK"]
channels = ["organic", "paid", "email"]
event_types = ["view", "add_to_cart", "purchase"]

products = [
    {"product_id": "P100", "category": "electronics", "brand": "Acme"},
    {"product_id": "P200", "category": "electronics", "brand": "Nova"},
    {"product_id": "P300", "category": "clothing", "brand": "North"},
    {"product_id": "P400", "category": "home", "brand": "Luma"},
    {"product_id": "P500", "category": "sports", "brand": "Peak"},
]

# Reference data (dimension) used for enrichment joins
ref_dir = Path("data/reference")
ref_dir.mkdir(parents=True, exist_ok=True)
with open(ref_dir / "product_catalog.json", "w", encoding="utf-8") as f:
    json.dump(products, f, indent=2)

def maybe_null(x, prob=0.03):
    return None if random.random() < prob else x

def make_event(i, ts):
    user_id = f"U{random.randint(1,120):03d}"
    session_id = f"S{random.randint(1,260):03d}"
    event_type = random.choices(event_types, weights=[70, 20, 10], k=1)[0]
    product = random.choice(products)["product_id"]
    qty = 1 if event_type != "purchase" else random.choice([1, 1, 1, 2, 3])
    price = 0.0
    if event_type == "purchase":
        price = float(random.choice([19.99, 29.99, 49.99, 79.99, 120.00]))
    ua = random.choice(["Mozilla/5.0", "Mozilla/5.0", "Mozilla/5.0", "bot/1.0"])

    # Intentional data issues for cleaning practice:
    # - some missing country/channel
    # - some duplicated event_ids
    # - some messy timestamps as strings
    event_id = f"E{i:06d}"
    if random.random() < 0.02:
        event_id = f"E{i-1:06d}"  # duplicate

    return {
        "event_id": event_id,
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product,
        "qty": qty,
        "price": price,
        "country": maybe_null(random.choice(countries), 0.05),
        "channel": maybe_null(random.choice(channels), 0.05),
        "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),  # string form
        "user_agent": ua
    }

for day in range(3):
    file_ts = (start + timedelta(days=day)).strftime("%Y-%m-%d")
    out_file = OUT_DIR / f"events_{file_ts}.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for i in range(1 + day * 500, 1 + (day + 1) * 500):
            ts = start + timedelta(days=day, seconds=i * 9)
            ev = make_event(i, ts)
            f.write(json.dumps(ev) + "\n")

print("Generated landing data in:", OUT_DIR)
print("Generated reference data in:", ref_dir)
