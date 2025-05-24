from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master('local[*]')
        .appName('read_parquet')
        .getOrCreate()
)

source = "/home/austin/_dataset/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet"

df = (
    spark.read.parquet(source)
)

df2 = (df.select('*'))

df2.show(20)