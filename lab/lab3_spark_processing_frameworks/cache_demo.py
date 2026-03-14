import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Topic3-Cache").master("local[*]").getOrCreate()
events = (spark.read.option("header", True).option("inferSchema", True).csv("data/retail_events.csv"))

purchases = events.filter(F.col("event_type") == "purchase").select("country", "price")

t0 = time.time()
purchases.groupBy("country").agg(F.sum("price").alias("revenue")).collect()
purchases.groupBy("country").agg(F.avg("price").alias("aov")).collect()
t1 = time.time()
print("Time without cache (seconds):", round(t1 - t0, 3))

purchases_cached = purchases.cache()
purchases_cached.count()

t2 = time.time()
purchases_cached.groupBy("country").agg(F.sum("price").alias("revenue")).collect()
purchases_cached.groupBy("country").agg(F.avg("price").alias("aov")).collect()
t3 = time.time()
print("Time with cache (seconds):", round(t3 - t2, 3))

spark.stop()
