import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

random.seed(42)
np.random.seed(42)

out = Path("data")
out.mkdir(exist_ok=True)

products = [
    ("P100", "electronics", "Acme"),
    ("P200", "electronics", "Nova"),
    ("P300", "clothing", "North"),
    ("P400", "home", "Luma"),
    ("P500", "sports", "Peak"),
]
catalog = pd.DataFrame(products, columns=["product_id", "category", "brand"])
catalog.to_csv(out / "product_catalog.csv", index=False)

base = datetime(2026, 2, 21, 10, 0, 0)
countries = ["CA", "US", "PK"]
event_types = ["view", "add_to_cart", "checkout", "purchase"]

rows = []
for i in range(400):
    user = f"U{random.randint(1,60):03d}"
    sess = f"S{random.randint(1,120):03d}"
    product_id = random.choice([p[0] for p in products])
    country = random.choice(countries)

    et = random.choices(event_types, weights=[60, 20, 12, 8], k=1)[0]
    price = 0.0
    if et == "purchase":
        price = float(random.choice([19.99, 29.99, 49.99, 79.99, 120.00]))

    ts = (base + timedelta(seconds=i * 5)).strftime("%Y-%m-%d %H:%M:%S")

    rows.append({
        "event_id": f"E{i+1:06d}",
        "user_id": user,
        "session_id": sess,
        "event_type": et,
        "product_id": product_id,
        "country": country,
        "event_ts": ts,
        "price": price
    })

events = pd.DataFrame(rows)
events.to_csv(out / "retail_events.csv", index=False)

print("Generated: data/retail_events.csv and data/product_catalog.csv")
