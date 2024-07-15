from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('pyspark_join')
    .getOrCreate()
)

df1 = spark.createDataFrame([
    (0, "Kim", 1000),
]).toDF("id", "name", "asset")

df2 = spark.createDataFrame([
    ( )
]).toDF('col')

df3 = spark.createDataFrame([]).toDF('col')