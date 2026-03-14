
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pathlib import Path

spark = (SparkSession.builder
         .appName("Topic7_Build_Dataset")
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

df = (raw.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("is_bot", F.lower(F.col("user_agent")).contains("bot")))

df = df.filter(~F.col("is_bot"))
df = df.fillna({"country":"UNKNOWN","channel":"UNKNOWN"})

w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc_nulls_last())
df = (df.withColumn("rn",F.row_number().over(w))
        .filter(F.col("rn")==1)
        .drop("rn"))

session_start = df.groupBy("session_id").agg(F.min("event_ts").alias("session_start_ts"))
df = df.join(session_start,"session_id","left")

label_df = (df.groupBy("session_id")
              .agg(F.max(F.when(F.col("event_type")=="purchase",1).otherwise(0)).alias("label_purchase_24h")))

features = (df.groupBy("session_id","user_id","country","channel")
            .agg(
                F.count("*").alias("events_total"),
                F.sum(F.when(F.col("event_type")=="view",1).otherwise(0)).alias("views"),
                F.sum(F.when(F.col("event_type")=="add_to_cart",1).otherwise(0)).alias("add_to_cart"),
                F.countDistinct("product_id").alias("unique_products"),
                F.max("event_ts").alias("last_event_ts"),
                F.min("session_start_ts").alias("session_start_ts")
            ))

features = features.withColumn(
    "session_duration_seconds",
    F.unix_timestamp("last_event_ts") - F.unix_timestamp("session_start_ts")
)

dataset = features.join(label_df,"session_id","left").fillna({"label_purchase_24h":0})

Path("data/ml").mkdir(parents=True,exist_ok=True)
dataset.write.mode("overwrite").parquet("data/ml/session_dataset.parquet")

print("Saved dataset")

spark.stop()
