from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Topic3-KPIs-Batch").master("local[*]").getOrCreate()

out = Path("output")
out.mkdir(exist_ok=True)

events = (spark.read.option("header", True).option("inferSchema", True).csv("data/retail_events.csv"))

counts = events.groupBy("event_type").count()

counts_pivot = (counts.groupBy()
                .pivot("event_type", ["view", "purchase"])
                .sum("count"))

metrics = (counts_pivot
           .withColumn("conversion_rate",
                       F.when(F.col("view") > 0, F.col("purchase") / F.col("view")).otherwise(F.lit(0.0))))

purchases = events.filter(F.col("event_type") == "purchase")

rev_by_country = (purchases.groupBy("country")
                  .agg(F.round(F.sum("price"), 2).alias("revenue"),
                       F.round(F.avg("price"), 2).alias("aov"),
                       F.count("*").alias("num_purchases"))
                  .orderBy(F.desc("revenue")))

top_products = (purchases.groupBy("product_id")
                .agg(F.round(F.sum("price"), 2).alias("revenue"),
                     F.count("*").alias("num_purchases"))
                .orderBy(F.desc("revenue"))
                .limit(10))

counts.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out / "counts_by_event_type"))
metrics.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out / "funnel_metrics"))
rev_by_country.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out / "revenue_by_country"))
top_products.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out / "top_products"))

print("Wrote outputs to the output/ folder.")
spark.stop()
