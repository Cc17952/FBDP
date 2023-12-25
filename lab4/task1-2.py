# -*- coding: utf-8 -*-

import sys
sys.path.append("/usr/local/spark/python/lib/py4j-0.10.7-src.zip")
sys.path.append("/usr/local/spark/python/lib/pyspark.zip")
import math
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("LoanAmountDistribution").getOrCreate()
df = spark.read.csv("file:///home/hadoop/lab4/data/application_data.csv", header=True, inferSchema=True)

df = df.withColumn("DIFF", col("AMT_CREDIT") - col("AMT_INCOME_TOTAL"))
top_records = df.orderBy(col("DIFF").desc()).limit(10)
bottom_records = df.orderBy(col("DIFF").asc()).limit(10)

output_path = "./result/task1-2.txt"

with open(output_path, 'w') as file:
    file.write("Top 10 records with highest difference:\n")
    file.write(top_records.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "DIFF").toPandas().to_string(index=False))

    file.write("\n\nBottom 10 records with lowest difference:\n")
    file.write(bottom_records.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "DIFF").toPandas().to_string(index=False))