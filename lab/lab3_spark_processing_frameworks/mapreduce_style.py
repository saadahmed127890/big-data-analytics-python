from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Topic3-MapReduce").master("local[*]").getOrCreate()
print("Spark UI:", spark.sparkContext.uiWebUrl)

events = (spark.read.option("header", True).option("inferSchema", True).csv("data/retail_events.csv"))

counts = (events.select("country", "event_type")
          .groupBy("country", "event_type")
          .count()
          .orderBy("country", "event_type"))

counts.show(50, truncate=False)
spark.stop()
