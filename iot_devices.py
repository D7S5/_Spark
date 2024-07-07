import sys

from pyspark.sql import SparkSession
from config.mysql_conf import *
from schema.iot_devices import iot_devices_schema
spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('lot_devices')
    .config("spark.jars", jars)
    .getOrCreate()
)

if len(sys.argv) != 2:
    print('path : <>', file=sys.stderr)
    sys.exit(-1)

schema = iot_devices_schema

df = (
    spark.read.format('csv')
        .option('header', True)
        .schema(schema)
        .load(sys.argv[1])
)
# df.printSchema()
# df.show(2)

table_name = 'iot_devices'
mode = 'overwrite'

(df.write
    .format('jdbc')
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option('url' , url)
    .option('mode' , mode)
    .option('dbtable' , table_name)
    .option('user', user)
    .option('password', password)
    .save())


