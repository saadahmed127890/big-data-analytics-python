import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pathlib import Path

df = pd.read_parquet("lake/curated/events_parquet/events_clean.parquet")

# Partition columns based on common filters
df["event_date"] = df["event_ts"].dt.date.astype(str)

out_dir = Path("lake/curated/events_parquet/partitioned")
out_dir.mkdir(parents=True, exist_ok=True)

table = pa.Table.from_pandas(df, preserve_index=False)

ds.write_dataset(
    table,
    base_dir=str(out_dir),
    format="parquet",
    partitioning=["event_date", "country"],
    existing_data_behavior="delete_matching"
)

print("Wrote partitioned dataset to:", out_dir)
