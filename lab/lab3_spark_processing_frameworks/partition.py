from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Topic3-Partitions").master("local[*]").getOrCreate()
events = (spark.read.option("header", True).option("inferSchema", True).csv("data/retail_events.csv"))

print("Original partitions:", events.rdd.getNumPartitions())

events_8 = events.repartition(8)
print("After repartition(8):", events_8.rdd.getNumPartitions())

events_2 = events_8.coalesce(2)
print("After coalesce(2):", events_2.rdd.getNumPartitions())

print("Count with 8 partitions:", events_8.count())
print("Count with 2 partitions:", events_2.count())

spark.stop()