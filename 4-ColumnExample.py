# JawnPythias
# date:02/03/2024

# column / Row / expr
# 最核心的是 column

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

schema = StructType([
    StructField('Id', IntegerType(), True),
    StructField('First', StringType(), True),
    StructField('Last', StringType(), True),
    StructField('Url', StringType(), True),
    StructField('Published', StringType(), True),
    StructField('Hits', LongType(), True),
    StructField('Campaigns', ArrayType(StringType()), True),
])

df = spark.read\
    .schema(schema)\
    .json('dataset/blogs.txt')

# 1.获取所有的列
print(df.columns)

# 2.获取列的对象
print(df['Id'])
print(col('id')) # 选择这种写法

# 3.select
df.select(df['Id'], col('Id')).show()

# 4.计算
df.select(col('Hits') * 2).show()

# 5.排序
df.orderBy(col('Hits').desc()).show()

