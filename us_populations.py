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

schema = StructType([
    StructField('zipcode', StringType(), nullable=False),
    StructField('city', StringType(), nullable=False),
    StructField('loc', DoubleType(), nullable=False),
    StructField('pop', IntegerType(), nullable=False),
    StructField('state', StringType(), nullable=False)
])
df = (spark.read.format('json').load(sys.argv[1]))

df.show(10)
