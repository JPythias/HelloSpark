# JawnPythias
# date:03/03/2024

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

# 1.map
# RDD里的函数

rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
result = rdd.map(lambda x : x * x)

# collect rdd -> list
print(result.collect())

# 2.flatMap
# Hello World 单词计数

