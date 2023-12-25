# -*- coding: utf-8 -*-

import sys
sys.path.append("/usr/local/spark/python/lib/py4j-0.10.7-src.zip")
sys.path.append("/usr/local/spark/python/lib/pyspark.zip")
import math
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("LoanDefaultPrediction").getOrCreate()
# spark = SparkSession.builder \
#     .appName("LoanDefaultPrediction") \
#     .config("spark.driver.bindAddress", "172.18.0.1") \
#     .config("spark.ui.port", "4040") \
#     .config("spark.port.maxRetries", "32") \
#     .getOrCreate()

data = spark.read.csv("file:///home/hadoop/lab4/data/application_data.csv", header=True, inferSchema=True)

features = [
    "NAME_EDUCATION_TYPE",
    "FLAG_CONT_MOBILE",
    "AMT_INCOME_TOTAL",
    "AMT_CREDIT", 
    "NAME_INCOME_TYPE",
    'CNT_CHILDREN',
    "FLAG_OWN_CAR",
    "FLAG_OWN_REALTY",
    "NAME_HOUSING_TYPE",
    "OBS_30_CNT_SOCIAL_CIRCLE",
]

assembler = VectorAssembler(inputCols=features, outputCol="features")
data = assembler.transform(data)

(trainingData, testData) = data.randomSplit([0.8, 0.2], seed=45)

dt = DecisionTreeClassifier(featuresCol="features", labelCol="TARGET")
dt_model = dt.fit(trainingData)
dt_predictions = dt_model.transform(testData)
dt_evaluator = MulticlassClassificationEvaluator(labelCol="TARGET", predictionCol="prediction", metricName="accuracy")
dt_accuracy = dt_evaluator.evaluate(dt_predictions)
print("DecisionTree Accuracy:", dt_accuracy)


data = data.select(features)

train_data, test_data = data.randomSplit([0.8, 0.2], seed=45)
feature_columns = [col for col in train_data.columns if col != "TARGET"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
nb = NaiveBayes(labelCol="TARGET", featuresCol="features")
pipeline = Pipeline(stages=[assembler, nb])

model = pipeline.fit(train_data)

predictions = model.transform(test_data)

evaluator = MulticlassClassificationEvaluator(labelCol="TARGET", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print("NaiveBayes Accuracy:", accuracy)

spark.stop()