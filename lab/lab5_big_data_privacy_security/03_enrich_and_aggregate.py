from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (SparkSession.builder
         .appName("Topic6_Enrich_Aggregate")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

events = spark.read.parquet("data/cleaned/events_cleaned.parquet")

ref_schema = T.StructType([
    T.StructField("product_id", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("brand", T.StringType(), True),
])

catalog = spark.read.schema(ref_schema).json("data/reference/product_catalog.json")

# Enrichment join
enriched = events.join(catalog, on="product_id", how="left")

# Transformations
enriched = enriched.withColumn(
    "revenue",
    F.when(F.col("event_type") == "purchase", F.col("price") * F.col("qty")).otherwise(F.lit(0.0))
)
enriched = enriched.withColumn("event_date", F.to_date("event_ts"))

Path("data/curated").mkdir(parents=True, exist_ok=True)
enriched.write.mode("overwrite").parquet("data/curated/events_enriched.parquet")
print("Wrote curated enriched events to data/curated/events_enriched.parquet")

# Daily funnel metrics
daily_funnel = (enriched.groupBy("event_date", "channel")
                .agg(
                    F.countDistinct(F.when(F.col("event_type") == "view", F.col("user_id"))).alias("users_view"),
                    F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))).alias("users_add_to_cart"),
                    F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("users_purchase"),
                )
                .withColumn(
                    "view_to_cart_rate",
                    F.when(F.col("users_view") > 0, F.col("users_add_to_cart") / F.col("users_view")).otherwise(F.lit(0.0))
                )
                .withColumn(
                    "cart_to_purchase_rate",
                    F.when(F.col("users_add_to_cart") > 0, F.col("users_purchase") / F.col("users_add_to_cart")).otherwise(F.lit(0.0))
                ))

Path("data/serving").mkdir(parents=True, exist_ok=True)
daily_funnel.write.mode("overwrite").parquet("data/serving/daily_funnel.parquet")
print("Wrote daily funnel metrics to data/serving/daily_funnel.parquet")

# Product performance + AOV
purchases = enriched.filter(F.col("event_type") == "purchase")

product_perf = (purchases.groupBy("event_date", "product_id", "category", "brand")
                .agg(
                    F.count("*").alias("num_orders"),
                    F.sum("revenue").alias("total_revenue"),
                    F.avg("revenue").alias("avg_order_value"),
                )
                .orderBy(F.col("total_revenue").desc()))

product_perf.write.mode("overwrite").parquet("data/serving/product_performance.parquet")
print("Wrote product performance to data/serving/product_performance.parquet")

# Quality gates
rows = enriched.count()
null_country = enriched.filter(F.col("country").isNull()).count()
dup_event_ids = enriched.groupBy("event_id").count().filter(F.col("count") > 1).count()

print("\n=== Quality gates summary ===")
print("rows =", rows)
print("null_country =", null_country)
print("duplicate_event_ids =", dup_event_ids)

if dup_event_ids > 0:
    raise RuntimeError("Quality gate failed: duplicate event_id detected in curated data.")

spark.stop()
