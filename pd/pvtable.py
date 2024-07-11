
import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('read_parquet')
    .getOrCreate()
)

if len(sys.argv) != 2:
    print("path : <>", file= sys.stderr)
    exit(-1)
    

df = spark.read.parquet(sys.argv)

df.show(10)



