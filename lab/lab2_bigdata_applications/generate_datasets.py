import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

random.seed(7)
np.random.seed(7)

# ---------- Retail events ----------
base = datetime(2026, 2, 21, 10, 0, 0)

countries = ["CA", "US", "PK"]
products = ["P100", "P200", "P300", "P400"]
event_types = ["view", "add_to_cart", "checkout", "purchase", "refund"]

rows = []
event_id = 1

def ts(i):
    return (base + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")

# Create 60 events with funnel structure + some duplicates
for i in range(60):
    user = f"U{random.randint(1,10):03d}"
    sess = f"S{random.randint(1,15):03d}"
    prod = random.choice(products)
    country = random.choice(countries)

    # bias towards views, fewer purchases
    et = random.choices(
        population=["view","add_to_cart","checkout","purchase","refund"],
        weights=[55,18,12,10,5],
        k=1
    )[0]

    price = 0.0
    if et == "purchase":
        price = float(random.choice([19.99, 29.99, 49.99, 79.99, 120.00]))
    if et == "refund":
        price = float(random.choice([19.99, 29.99, 49.99]))

    rows.append({
        "event_id": f"E{event_id:05d}",
        "user_id": user,
        "session_id": sess,
        "event_type": et,
        "product_id": prod,
        "country": country,
        "event_ts": ts(i*10),
        "price": price
    })
    event_id += 1

# Inject duplicate-like retries: same user + type within 3 seconds
dupes = []
for j in range(3):
    base_row = random.choice(rows)
    dup = base_row.copy()
    dup["event_id"] = f"E{event_id:05d}"
    event_id += 1
    # 2 seconds later
    t = datetime.strptime(base_row["event_ts"], "%Y-%m-%d %H:%M:%S") + timedelta(seconds=2)
    dup["event_ts"] = t.strftime("%Y-%m-%d %H:%M:%S")
    dupes.append(dup)

retail_df = pd.DataFrame(rows + dupes)
retail_df.to_csv("retail_events.csv", index=False)

# ---------- Bank transactions ----------
base_b = datetime(2026, 2, 21, 11, 0, 0)
merchants = ["groceries", "electronics", "travel", "fuel", "restaurant"]

bank_rows = []
for i in range(35):
    cust = f"C{random.randint(1,12):03d}"
    amt = float(random.choice([5, 12, 30, 70, 120, 250, 800, 1200, 2500]))
    intl = int(random.random() < 0.20)
    mcat = random.choice(merchants)
    label = int((amt >= 1200 and (intl == 1 or mcat in ["electronics","travel"])) or (random.random() < 0.05))

    bank_rows.append({
        "txn_id": f"T{i+1:05d}",
        "customer_id": cust,
        "txn_ts": (base_b + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amt,
        "country": random.choice(["CA","US","PK"]),
        "device_id": f"D{random.randint(1,8):03d}",
        "merchant_category": mcat,
        "is_international": intl,
        "label_is_fraud": label
    })

bank_df = pd.DataFrame(bank_rows)
bank_df.to_csv("bank_txn.csv", index=False)

# ---------- IoT air-quality ----------
base_i = datetime(2026, 2, 21, 12, 0, 0)
sensors = ["AQ01","AQ02","AQ03"]
zones = ["Downtown","Suburb"]

iot_rows = []
for i in range(120):
    pm25 = float(np.random.normal(loc=15, scale=6))
    pm25 = max(pm25, 0)

    # Inject spikes
    if i in random.sample(range(120), 8):
        pm25 = float(random.choice([90, 120, 160, 200]))

    iot_rows.append({
        "sensor_id": random.choice(sensors),
        "reading_ts": (base_i + timedelta(seconds=30*i)).strftime("%Y-%m-%d %H:%M:%S"),
        "pm25": round(pm25, 2),
        "temperature_c": round(float(np.random.normal(loc=3, scale=8)), 1),
        "zone": random.choice(zones)
    })

iot_df = pd.DataFrame(iot_rows)
iot_df.to_csv("iot_air.csv", index=False)

print("Generated: retail_events.csv, bank_txn.csv, iot_air.csv")
