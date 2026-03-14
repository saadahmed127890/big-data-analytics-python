import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

df = pd.read_parquet("lake/curated/events_parquet/events_clean.parquet")

small_dir = Path("lake/curated/events_parquet/small_files")
compact_dir = Path("lake/curated/events_parquet/compacted")
small_dir.mkdir(parents=True, exist_ok=True)
compact_dir.mkdir(parents=True, exist_ok=True)

# 1) Create many small files (bad pattern)
chunks = 60
chunk_size = max(1, len(df) // chunks)

for i in range(chunks):
    chunk = df.iloc[i * chunk_size:(i + 1) * chunk_size]
    if len(chunk) == 0:
        continue
    pq.write_table(
        pa.Table.from_pandas(chunk, preserve_index=False),
        small_dir / f"part_{i:03d}.parquet"
    )

print("Small files created:", len(list(small_dir.glob("*.parquet"))))

# 2) Compaction: rewrite into fewer bigger files
target_files = 4
big_chunk = max(1, len(df) // target_files)

for j in range(target_files):
    chunk = df.iloc[j * big_chunk:(j + 1) * big_chunk]
    pq.write_table(
        pa.Table.from_pandas(chunk, preserve_index=False),
        compact_dir / f"compact_{j:02d}.parquet"
    )

print("Compacted files created:", len(list(compact_dir.glob("*.parquet"))))
