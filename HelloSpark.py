# JawnPythias
# date:02/03/2024

# 1.拿到Spark入口对象 SparkSession
# local: 在本地内存运行
from pyspark.shell import spark
from pyspark.sql import SparkSession

SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

# 2.提交大数据分析任务
rdd = spark.sparkContenxt.parallelize([('tom', 20), ('jack', 40)], ['name', 'age'])
rdd.toDF(['name','age'])