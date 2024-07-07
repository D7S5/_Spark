import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from schema.airbnb_schema import airbnb_schema
from config.mysql_conf import jars, url


if len(sys.argv)!=2:
    print("path: <>", file=sys.stderr)
    sys.exit(-1)

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("airbnb")
        .config("spark.jars", jars)
        .getOrCreate()
    )

schema = airbnb_schema

df = (
    spark.read.format('csv')
        .option('header', True)
        .schema(schema)
        .load(sys.argv[1])
    )


mode = 'overwrite'
table_name = 'airbnb4'

(df.write
    .format('jdbc')
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option('url' , url)
    .option('mode' , mode)
    .option('dbtable' , table_name)
    .option('user', 'austin')
    .option('password', '1234')
    .save())

