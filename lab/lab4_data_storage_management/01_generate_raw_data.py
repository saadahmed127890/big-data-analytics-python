import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

random.seed(10)
np.random.seed(10)

base = datetime(2026, 2, 21, 9, 0, 0)
countries = ["CA", "US", "PK"]
event_types = ["view", "add_to_cart", "checkout", "purchase"]
products = ["P100", "P200", "P300", "P400", "P500"]

out_v1 = Path("lake/raw/v1")
out_v2 = Path("lake/raw/v2")
out_v1.mkdir(parents=True, exist_ok=True)
out_v2.mkdir(parents=True, exist_ok=True)

# Reference table (dimension)
catalog = pd.DataFrame({
    "product_id": products,
    "category": ["electronics", "electronics", "clothing", "home", "sports"],
    "brand": ["Acme", "Nova", "North", "Luma", "Peak"]
})
catalog.to_csv("product_catalog.csv", index=False)

rows = []
for i in range(1200):
    et = random.choices(event_types, weights=[60, 20, 12, 8], k=1)[0]
    price = 0.0
    if et == "purchase":
        price = float(random.choice([19.99, 29.99, 49.99, 79.99, 120.00]))

    ts = (base + timedelta(seconds=i * 7)).strftime("%Y-%m-%d %H:%M:%S")
    rows.append({
        "event_id": f"E{i+1:06d}",
        "user_id": f"U{random.randint(1,150):03d}",
        "session_id": f"S{random.randint(1,400):03d}",
        "event_type": et,
        "product_id": random.choice(products),
        "country": random.choice(countries),
        "event_ts": ts,
        "price": price
    })

events = pd.DataFrame(rows)

# Raw v1 (expected schema)
events.to_csv(out_v1 / "retail_events_v1.csv", index=False)

# Raw v2 (schema drift example)
events_v2 = events.copy()
events_v2 = events_v2.rename(columns={"event_ts": "timestamp"})
events_v2["device_type"] = np.random.choice(["mobile", "web", "tablet"], size=len(events_v2))
events_v2.to_csv(out_v2 / "retail_events_v2.csv", index=False)

print("Generated raw data: lake/raw/v1/retail_events_v1.csv and lake/raw/v2/retail_events_v2.csv")
print("Generated reference: product_catalog.csv")
