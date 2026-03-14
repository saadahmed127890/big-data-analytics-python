
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth

spark=(SparkSession.builder
       .appName("Topic7_Association")
       .master("local[*]")
       .getOrCreate())

raw=spark.read.json("data/landing/*.jsonl")
raw=raw.filter(raw.event_type=="purchase")

baskets=(raw.groupBy("session_id")
         .agg(F.collect_set("product_id").alias("items"))
         .filter(F.size("items")>=2))

fp=FPGrowth(itemsCol="items",minSupport=0.01,minConfidence=0.2)
model=fp.fit(baskets)

rules=model.associationRules
rules.show()

spark.stop()
