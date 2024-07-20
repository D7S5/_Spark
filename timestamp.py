from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName('cast-timestamp')
    .getOrCreate()
    )


input_data = [(1, 1719792000), (2, 1722470399)]

schema = StructType([
    StructField('id', LongType(), True),
    StructField('unix_timestamp', LongType(), True)
])

df = (
    spark.createDataFrame(
        data = input_data, schema = schema))

(df.select('id', 
           from_unixtime('unix_timestamp').alias('timestamp'))
 ).show()

# df2 = (df.select('unix_timestamp', cast('long'))).show()

# df =(
#     .select('timestamp')
#     .from_unixtime('timestamp').alias('timestamp')
#       )
