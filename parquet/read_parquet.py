from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master('local[*]')
        .appName('read_parquet')
        .getOrCreate()
)

source = "/home/austin/databricks-datasets/learning-spark-v2/parquet_amazon"

df = (
    spark.read.parquet(source)
)

df2 = (df.select('*'))

df2.show()