# -*- coding: utf-8 -*-

import sys
sys.path.append("/usr/local/spark/python/lib/py4j-0.10.7-src.zip")
sys.path.append("/usr/local/spark/python/lib/pyspark.zip")
import math
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# sc = SparkContext( 'local', 'test')
# sc.setLogLevel("INFO")
spark = SparkSession.builder.appName("LoanAmountDistribution").getOrCreate()
df = spark.read.csv("file:///home/hadoop/lab4/data/application_data.csv", header=True, inferSchema=True)
# print(df)

df = df.withColumn("AMT_CREDIT_BIN", ((col("AMT_CREDIT") / 10000).cast("int") * 10000).alias("AMT_CREDIT_BIN"))
credit_distribution = df.groupBy("AMT_CREDIT_BIN").count().sort("AMT_CREDIT_BIN")

credit_distribution.show(truncate=False)
result_str = credit_distribution.rdd.map(lambda x: "(({},{}) , {})".format(x['AMT_CREDIT_BIN'], x['AMT_CREDIT_BIN'] + 10000, x['count'])).collect()

with open('./result/task1-1.txt', 'w') as file:
    for line in result_str:
        file.write(line + '\n')