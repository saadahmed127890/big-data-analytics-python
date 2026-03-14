import pandas as pd
from pathlib import Path

raw_path = Path("lake/raw/v1/retail_events_v1.csv")
curated_dir = Path("lake/curated/events_parquet")
curated_dir.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(raw_path)

# Minimal curation: enforce types and keep trusted columns
df["event_ts"] = pd.to_datetime(df["event_ts"])
df["price"] = df["price"].astype(float)

out_file = curated_dir / "events_clean.parquet"
df.to_parquet(out_file, index=False)

print("Wrote curated Parquet:", out_file)
print("Rows:", len(df), "| Columns:", list(df.columns))
