
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(11)

out_dir = Path("data/raw")
out_dir.mkdir(parents=True, exist_ok=True)

countries = ["CA", "US", "PK"]
channels = ["organic", "paid", "email"]
devices = ["mobile", "web", "tablet"]
event_types = ["view", "add_to_cart", "purchase"]

products = [
    {"product_id": "P100", "category": "electronics", "brand": "Acme"},
    {"product_id": "P200", "category": "electronics", "brand": "Nova"},
    {"product_id": "P300", "category": "clothing", "brand": "North"},
    {"product_id": "P400", "category": "home", "brand": "Luma"},
    {"product_id": "P500", "category": "sports", "brand": "Peak"},
]

ref_dir = Path("data/reference")
ref_dir.mkdir(parents=True, exist_ok=True)

with open(ref_dir / "product_catalog.json", "w", encoding="utf-8") as f:
    json.dump(products, f, indent=2)

start = datetime(2026, 2, 7, 0, 0, 0)
days = 14

def make_session(session_index, day_start):
    session_id = f"S{session_index:06d}"
    user_id = f"U{random.randint(1,5000):05d}"
    country = random.choice(countries)
    channel = random.choice(channels)
    device = random.choice(devices)

    session_start = day_start + timedelta(minutes=random.randint(0, 23*60))

    n_events = random.randint(3,30)
    will_purchase = 1 if random.random() < 0.08 else 0

    events=[]
    purchased=False

    for i in range(n_events):
        ts=session_start+timedelta(seconds=i*random.randint(10,50))
        ua=random.choice(["Mozilla/5.0","Mozilla/5.0","Mozilla/5.0","bot/1.0"])

        if will_purchase and i>3 and (not purchased) and random.random()<0.10:
            et="purchase"
            purchased=True
        else:
            et=random.choices(["view","add_to_cart"],weights=[78,22],k=1)[0]

        prod=random.choice(products)["product_id"]
        qty=1 if et!="purchase" else random.choice([1,1,2,3])
        price=0.0 if et!="purchase" else float(random.choice([19.99,29.99,49.99,79.99,120.00]))

        events.append({
            "event_id":f"E{session_index:06d}_{i:03d}",
            "user_id":user_id,
            "session_id":session_id,
            "event_type":et,
            "product_id":prod,
            "qty":qty,
            "price":price,
            "country":country,
            "channel":channel,
            "device":device,
            "event_ts":ts.strftime("%Y-%m-%d %H:%M:%S"),
            "user_agent":ua
        })

    return events

session_counter=1
for d in range(days):
    day_start=start+timedelta(days=d)
    out_file=out_dir/f"events_{day_start.strftime('%Y-%m-%d')}.jsonl"

    with open(out_file,"w",encoding="utf-8") as f:
        sessions_today=900
        if d==10:
            sessions_today=1800

        for _ in range(sessions_today):
            sess_events=make_session(session_counter,day_start)
            session_counter+=1
            for ev in sess_events:
                f.write(json.dumps(ev)+"\n")

print("Generated raw JSONL files")
