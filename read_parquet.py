import sys

from pyspark.sql import SparkSession

from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('flights_parquet')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('path <> :' , file= sys.stderr)
        sys.exit(-1)

df = spark.read.parquet(sys.argv[1])

df.show(20)

