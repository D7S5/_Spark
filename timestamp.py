from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName('cast-timestamp')
    .getOrCreate()
    )


input_data = [(1, 1458444054093), (2, 1458444054092), (3, 1458444054000)]

schema = StructType([
    StructField('id', LongType(), True),
    StructField('unix_timestamp', LongType(), True)
])

df = (
    spark.createDataFrame(
        data = input_data, schema = schema))

(df.select('id', 'unix_timestamp')).show()

# df2 = (df.select('unix_timestamp', cast('long'))).show()

# df =(
#     .select('timestamp')
#     .from_unixtime('timestamp').alias('timestamp')
#       )
