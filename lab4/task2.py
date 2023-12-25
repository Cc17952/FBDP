# -*- coding: utf-8 -*-

import sys
sys.path.append("/usr/local/spark/python/lib/py4j-0.10.7-src.zip")
sys.path.append("/usr/local/spark/python/lib/pyspark.zip")
import math
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("GenderChildIncomeAnalysis").getOrCreate()
df = spark.read.csv("file:///home/hadoop/lab4/data/application_data.csv", header=True, inferSchema=True)

# task2-1
male_customers = df.filter(col("CODE_GENDER") == "M")
child_count_stats = male_customers.groupBy("CNT_CHILDREN").count()

total_male_customers = male_customers.count()
child_count_stats = child_count_stats.withColumn("TYPE_RATIO", col("count") / total_male_customers)

# child_count_stats.select("CNT_CHILDREN", "TYPE_RATIO").show(truncate=False)
outputpath_task1 = "./result/task2-1.txt"
with open(outputpath_task1, 'w') as file:
    file.write("CNT_CHILDREN,TYPE_RATIO\n")
    for row in child_count_stats.collect():
        file.write("{},{}\n".format(row['CNT_CHILDREN'], row['TYPE_RATIO']))

# task2-2
income_stats = df.withColumn("avg_income", F.abs(col("AMT_INCOME_TOTAL") / col("DAYS_BIRTH")))
filtered_income_stats = income_stats.filter(col("avg_income") > 1).orderBy(col("avg_income").desc())

outputpath_task2 = "./result/task2-2.csv"
pandas_filtered_income_stats = filtered_income_stats.select("SK_ID_CURR", "avg_income").toPandas()
pandas_filtered_income_stats.to_csv(outputpath_task2, header=True, index=False)

spark.stop()