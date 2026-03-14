from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 1) Define schema explicitly so Spark can parse JSON reliably (teaches 'structured data' concept).
schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('product_id', StringType(), True),
    StructField('country', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('event_time', StringType(), True)
])

# 2) Create Spark session in local mode (simulates small cluster on one machine).
spark = (SparkSession.builder
         .appName('RetailStreamingKPIs')
         .master('local[*]')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

# 3) Read stream from Kafka. 'value' contains the payload bytes.
raw = (spark.readStream
       .format('kafka')
       .option('kafka.bootstrap.servers', 'localhost:9092')
       .option('subscribe', 'retail-events')
       .option('startingOffsets', 'latest')
       .load())

# 4) Cast Kafka value to string and parse JSON using the schema.
json_df = raw.selectExpr('CAST(value AS STRING) as json')
events = (json_df
          .select(from_json(col('json'), schema).alias('e'))
          .select('e.*'))

# 5) Convert event_time to timestamp and compute revenue.
events_clean = (events
    .withColumn('event_ts', to_timestamp(col('event_time'), 'yyyy-MM-dd\'T\'HH:mm:ss'))
    .withColumn('revenue', col('price') * col('quantity'))
    .dropna(subset=['event_ts', 'country', 'revenue']))

# KPI 1: Revenue and orders per minute (time window).
rev_per_min = (events_clean
    .groupBy(window(col('event_ts'), '1 minute').alias('w'))
    .agg(_sum('revenue').alias('revenue_per_min'), _count('*').alias('orders_per_min'))
    .select(col('w.start').alias('window_start'), col('w.end').alias('window_end'), 'revenue_per_min', 'orders_per_min'))

# KPI 2: Running totals by country (grouped aggregation).
rev_by_country = (events_clean
    .groupBy('country')
    .agg(_sum('revenue').alias('revenue_total'), _count('*').alias('orders_total'))
    .orderBy(col('revenue_total').desc()))

# Output A: console table for quick observation (teaching feedback loop).
q1 = (rev_per_min.writeStream
      .outputMode('complete')
      .format('console')
      .option('truncate', 'false')
      .option('numRows', 50)
      .start())

# Output B: parquet for persistence (foundation for dashboards / lakehouse in later topics).
q2 = (rev_by_country.writeStream
      .outputMode('complete')
      .format('parquet')
      .option('path', 'output/rev_by_country')
      .option('checkpointLocation', 'output/checkpoints/rev_by_country')
      .start())

spark.streams.awaitAnyTermination()
