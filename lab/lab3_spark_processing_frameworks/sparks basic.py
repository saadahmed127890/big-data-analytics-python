from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Topic3-Basics")
         .master("local[*]")
         .config("spark.ui.showConsoleProgress", "true")
         .getOrCreate())

print("Spark version:", spark.version)
print("Spark UI:", spark.sparkContext.uiWebUrl)

events = (spark.read.option("header", True).option("inferSchema", True).csv("data/retail_events.csv"))

print("Default partitions:", events.rdd.getNumPartitions())

# Job 1
print("Row count:", events.count())

# Job 2
events.orderBy("event_ts").show(5, truncate=False)

spark.stop()





