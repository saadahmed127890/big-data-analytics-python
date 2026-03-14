import pandas as pd
from pathlib import Path

expected_cols = ["event_id", "user_id", "session_id", "event_type", "product_id", "country", "event_ts", "price"]

raw_v2 = Path("lake/raw/v2/retail_events_v2.csv")
quarantine_dir = Path("lake/raw/v2/quarantine")
quarantine_dir.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(raw_v2)
cols = list(df.columns)

missing = [c for c in expected_cols if c not in cols]
extra = [c for c in cols if c not in expected_cols]

if missing or extra:
    target = quarantine_dir / raw_v2.name
    raw_v2.replace(target)
    print("SCHEMA DRIFT DETECTED.")
    print("Missing:", missing)
    print("Extra:", extra)
    print("Moved raw file to quarantine:", target)
else:
    print("Schema OK. No drift detected.")
