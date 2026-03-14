from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark = (SparkSession.builder
         .appName("Topic6_Clean_Transform_Enrich")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

landing_path = "data/landing/*.jsonl"

schema = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("session_id", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("qty", T.IntegerType(), True),
    T.StructField("price", T.DoubleType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("channel", T.StringType(), True),
    T.StructField("event_ts", T.StringType(), True),
    T.StructField("user_agent", T.StringType(), True),
])

raw = spark.read.schema(schema).json(landing_path)

# Data profile
print("\n=== Profile: row count ===")
print("Rows:", raw.count())

print("\n=== Profile: null rates (top fields) ===")
profile = (raw.select(
    F.count("*").alias("rows"),
    F.sum(F.col("country").isNull().cast("int")).alias("country_nulls"),
    F.sum(F.col("channel").isNull().cast("int")).alias("channel_nulls"),
    F.sum(F.col("event_id").isNull().cast("int")).alias("event_id_nulls")
).collect()[0])

rows = profile["rows"]
print("country_null_rate =", profile["country_nulls"] / rows)
print("channel_null_rate =", profile["channel_nulls"] / rows)

# Cleaning rules
df = raw.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))

allowed = ["view", "add_to_cart", "purchase"]
df = df.withColumn("is_valid_event_type", F.col("event_type").isin(allowed))
df = df.withColumn("is_valid_price", F.col("price") >= F.lit(0.0))
df = df.withColumn("is_valid_qty", F.col("qty") >= F.lit(0))
df = df.withColumn("is_bot", F.lower(F.col("user_agent")).contains("bot"))

df = df.withColumn(
    "has_required_fields",
    F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_ts").isNotNull()
)

bad = df.filter(
    ~F.col("has_required_fields") |
    ~F.col("is_valid_event_type") |
    ~F.col("is_valid_price") |
    F.col("is_bot")
)
good = df.subtract(bad)

# Deduplicate by event_id (keep latest timestamp if duplicates)
w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc_nulls_last())
good = (good.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn"))

good = good.fillna({"country": "UNKNOWN", "channel": "UNKNOWN"})

Path("data/cleaned").mkdir(parents=True, exist_ok=True)
Path("data/quarantine").mkdir(parents=True, exist_ok=True)

good.write.mode("overwrite").parquet("data/cleaned/events_cleaned.parquet")
bad.write.mode("overwrite").parquet("data/quarantine/events_quarantine.parquet")

print("\nWrote cleaned events to data/cleaned/events_cleaned.parquet")
print("Wrote quarantined events to data/quarantine/events_quarantine.parquet")

spark.stop()
