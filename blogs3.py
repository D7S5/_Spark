from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType, StructField, StructType, IntegerType, LongType, DateType

import sys

from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("blogs")
        .config("spark.jars" , jars)
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <> :", file= sys.stderr)
    exit(-1)

blogs_schema = StructType([
    StructField("Id", LongType(), nullable= False),
    StructField("First", StringType(), nullable= False),
    StructField("Last", StringType(), nullable= False),
    StructField("Url", StringType(), nullable= False),
    StructField("Published", DateType(), nullable= False),
    StructField("Hits", IntegerType(), nullable= False),
    StructField("Campaigns", ArrayType(StringType()), nullable= True),
])

df = (spark.read.format('json')
    #   .option('multiline', True)
    #   .schema(blogs_schema)
      .load(sys.argv[1]))


ts_pattern ="M/d/yyyy"

# df2 = (df.select(to_date("Published", "M/d/yyyy").alias("date")))
df2= (
    df.select("Id", "First", "Last", "Url",
            to_date("Published", ts_pattern).alias("Published"),
            "Hits",
            explode(df.Campaigns).alias("Campaigns"))
)
              # explode Array

df2.show()
df2.printSchema()

dbtable = 'blogs5'
db_name = 'db3'

(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())