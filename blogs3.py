from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType, StructField, StructType, IntegerType, LongType


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
    StructField("Published", StringType(), nullable= False),
    StructField("Hits", IntegerType(), nullable= False),
    StructField("Campaigns", ArrayType(StringType()), nullable= True),
])

df = (spark.read.format('json')
    #   .option('multiline', True)
      .schema(blogs_schema)
      .load(sys.argv[1]))

df2 = (
    df.select("Id", "First", "Last", "Url", "Published", "Hits", explode(df.Campaigns).alias("Campaigns")).show(10) 
    )

# df3.printSchema()
# df3.show(10)



# df = df.na.drop('all').show(10)



# df2.printSchema()
# df2.show(10)
