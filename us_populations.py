import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("us_poluations")
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("Usage : source file <file>", file=sys.stderr)
    sys.exit(-1)

df = (spark.read.format('json').load(sys.argv[1]))

df.show(10, truncate=False)
