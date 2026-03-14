from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("Topic6_Session_Features")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

events = spark.read.parquet("data/curated/events_enriched.parquet")

session_first = (events.groupBy("session_id")
                 .agg(
                     F.min("event_ts").alias("session_start"),
                     F.max("event_ts").alias("session_end"),
                     F.count("*").alias("events_in_session"),
                     F.countDistinct("user_id").alias("users_in_session")
                 ))

first_purchase = (events.filter(F.col("event_type") == "purchase")
                  .groupBy("session_id")
                  .agg(
                      F.min("event_ts").alias("first_purchase_ts"),
                      F.sum("revenue").alias("session_revenue")
                  ))

session_features = (session_first.join(first_purchase, on="session_id", how="left")
                    .withColumn("has_purchase", F.col("first_purchase_ts").isNotNull().cast("int"))
                    .withColumn(
                        "time_to_purchase_seconds",
                        F.when(
                            F.col("first_purchase_ts").isNotNull(),
                            F.unix_timestamp("first_purchase_ts") - F.unix_timestamp("session_start")
                        ).otherwise(F.lit(None))
                    ))

Path("data/serving").mkdir(parents=True, exist_ok=True)
session_features.write.mode("overwrite").parquet("data/serving/session_features.parquet")

print("Wrote session features to data/serving/session_features.parquet")
spark.stop()
