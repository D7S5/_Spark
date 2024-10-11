from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("Smoke_Detection")
        .config("spark.jars" , jars)
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <>: " , file = sys.stderr)
    exit(-1)

df = (spark.read
      .option('header', True)
      .option('inferschema', True)
      .csv(sys.argv[1])
)

file_name = ""
currnet_time = ""

df2 = (df.select('product_id',
                  'product_name','category','discounted_price','actual_price','discount_percentage',
                  'rating', 'rating_count'))

df2.show(5)
df2.printSchema()

sink = "/home/austin/databricks-datasets/learning-spark-v2/parquet_amazon"
(df2.write
    .mode("overwrite")
    .parquet(sink))
