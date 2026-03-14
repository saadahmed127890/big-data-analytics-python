
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pathlib import Path

spark=(SparkSession.builder
       .appName("Topic7_Train")
       .master("local[*]")
       .getOrCreate())

df=spark.read.parquet("data/ml/session_dataset.parquet")

label="label_purchase_24h"

cat_cols=["country","channel"]
num_cols=["events_total","views","add_to_cart","unique_products","session_duration_seconds"]

indexers=[StringIndexer(inputCol=c,outputCol=f"{c}_idx",handleInvalid="keep") for c in cat_cols]
encoders=[OneHotEncoder(inputCol=f"{c}_idx",outputCol=f"{c}_oh") for c in cat_cols]

assembler=VectorAssembler(
    inputCols=[f"{c}_oh" for c in cat_cols]+num_cols,
    outputCol="features"
)

lr=LogisticRegression(featuresCol="features",labelCol=label,maxIter=30)

pipeline=Pipeline(stages=indexers+encoders+[assembler,lr])
model=pipeline.fit(df)

pred=model.transform(df)

evaluator=BinaryClassificationEvaluator(labelCol=label)
print("AUC:",evaluator.evaluate(pred))

Path("models").mkdir(parents=True,exist_ok=True)
model.write().overwrite().save("models/topic7_lr_model")

spark.stop()
