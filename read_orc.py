from pyspark.sql import SparkSession
import sys

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('read_orc')
    .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("path <> :" , file = sys.stderr)
        sys.exit(-1)

df = (
    spark.read.format('orc').load(sys.argv[1])
)

df.printSchema()
df.show(10)