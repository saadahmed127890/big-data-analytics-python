from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (SparkSession.builder
         .appName("Topic6_Streaming_Demo")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

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

Path("data/stream_in").mkdir(parents=True, exist_ok=True)
Path("data/stream_out").mkdir(parents=True, exist_ok=True)
Path("data/checkpoints/topic6_stream").mkdir(parents=True, exist_ok=True)

stream = (spark.readStream
          .schema(schema)
          .json("data/stream_in"))

clean = (stream
         .withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
         .withColumn("is_bot", F.lower(F.col("user_agent")).contains("bot"))
         .filter(~F.col("is_bot"))
         .fillna({"country": "UNKNOWN", "channel": "UNKNOWN"})
         .withColumn("event_date", F.to_date("event_ts")))

agg = (clean
       .withWatermark("event_ts", "5 minutes")
       .groupBy(F.window("event_ts", "10 minutes"), F.col("channel"))
       .agg(F.count("*").alias("events")))

query = (agg.writeStream
         .outputMode("append")
         .format("parquet")
         .option("path", "data/stream_out/window_counts")
         .option("checkpointLocation", "data/checkpoints/topic6_stream")
         .start())

print("Streaming query started.")
print("Now copy one of your landing JSONL files into data/stream_in/ to simulate arriving data.")
print("Stop the stream with Ctrl+C after you see output files written.")

query.awaitTermination()
