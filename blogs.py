from pyspark.sql import SparkSession
import sys
spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("blogs")
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <> :", file= sys.stderr)
    exit(-1)

df = (spark.read.format('json').load(sys.argv[1]))

df.show(d)