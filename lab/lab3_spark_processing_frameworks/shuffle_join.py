from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Topic3-Shuffle-Join").master("local[*]").getOrCreate()
print("Spark UI:", spark.sparkContext.uiWebUrl)

events = (spark.read.option("header", True).option("inferSchema", True).csv("data/retail_events.csv"))
catalog = (spark.read.option("header", True).option("inferSchema", True).csv("data/product_catalog.csv"))

enriched = events.join(catalog, on="product_id", how="left")

kpi = (enriched
       .filter(F.col("event_type") == "purchase")
       .groupBy("country", "category")
       .agg(F.round(F.sum("price"), 2).alias("revenue"))
       .orderBy(F.desc("revenue")))

kpi.show(20, truncate=False)
spark.stop()
