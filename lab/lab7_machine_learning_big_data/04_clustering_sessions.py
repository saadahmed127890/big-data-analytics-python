
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml.clustering import KMeans

spark=(SparkSession.builder
       .appName("Topic7_Clustering")
       .master("local[*]")
       .getOrCreate())

df=spark.read.parquet("data/ml/session_dataset.parquet")

num_cols=["events_total","views","add_to_cart","unique_products","session_duration_seconds"]

assembler=VectorAssembler(inputCols=num_cols,outputCol="vec")
vec=assembler.transform(df)

scaler=StandardScaler(inputCol="vec",outputCol="features")
scaled=scaler.fit(vec).transform(vec)

kmeans=KMeans(k=4,seed=42,featuresCol="features")
model=kmeans.fit(scaled)

pred=model.transform(scaled)
pred.show(5)

spark.stop()
