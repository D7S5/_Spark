from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master('local[*]')
        .appName('read_parquet')
        .getOrCreate()
)

source = "/home/austin/sink/part-00000-3efa964b-cc46-4c6a-adbd-e03705d0b9bc-c000.snappy.parquet"

df = (
    spark.read.parquet(source)
)

df.show(10)