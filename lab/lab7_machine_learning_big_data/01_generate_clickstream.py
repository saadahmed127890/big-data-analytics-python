
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(7)

landing = Path("data/landing")
landing.mkdir(parents=True, exist_ok=True)

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

ref = Path("data/reference")
ref.mkdir(parents=True, exist_ok=True)
with open(ref / "product_catalog.json", "w", encoding="utf-8") as f:
    json.dump(products, f, indent=2)

base = datetime(2026, 2, 19, 9, 0, 0)

def maybe_null(x, prob=0.04):
    return None if random.random() < prob else x

def make_session(session_index):
    session_id = f"S{session_index:05d}"
    user_id = f"U{random.randint(1,500):04d}"
    country = maybe_null(random.choice(countries), 0.05)
    channel = maybe_null(random.choice(channels), 0.05)

    start_ts = base + timedelta(minutes=session_index * 2)

    n_events = random.randint(3, 25)
    will_purchase = 1 if random.random() < 0.10 else 0

    events = []
    purchased = False

    for i in range(n_events):
        ts = start_ts + timedelta(seconds=i * random.randint(15, 60))
        if will_purchase and i > 2 and (not purchased) and random.random() < 0.12:
            et = "purchase"
            purchased = True
        else:
            et = random.choices(["view", "add_to_cart"], weights=[75, 25], k=1)[0]

        product = random.choice(products)["product_id"]
        qty = 1 if et != "purchase" else random.choice([1,1,2,3])
        price = 0.0
        if et == "purchase":
            price = float(random.choice([19.99, 29.99, 49.99, 79.99, 120.00]))

        ua = random.choice(["Mozilla/5.0", "Mozilla/5.0", "Mozilla/5.0", "bot/1.0"])

        event_id = f"E{session_index:05d}_{i:03d}"
        if random.random() < 0.01 and i > 0:
            event_id = f"E{session_index:05d}_{i-1:03d}"

        events.append({
            "event_id": event_id,
            "user_id": user_id,
            "session_id": session_id,
            "event_type": et,
            "product_id": product,
            "qty": qty,
            "price": price,
            "country": country,
            "channel": channel,
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "user_agent": ua
        })

    return events

files = [
    landing / "events_2026-02-19.jsonl",
    landing / "events_2026-02-20.jsonl"
]

session_counter = 1
for out_file in files:
    with open(out_file, "w", encoding="utf-8") as f:
        for _ in range(600):
            session_events = make_session(session_counter)
            session_counter += 1
            for ev in session_events:
                f.write(json.dumps(ev) + "\n")

print("Generated clickstream JSONL files")
