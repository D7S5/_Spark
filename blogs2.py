from pyspark.sql import SparkSession

import sys
from config.mysql_conf import *


spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("blogs")
        .cinfog("spark.jars" , jars)
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <> :", file= sys.stderr)
    exit(-1)

df = (spark.read.format('json').load(sys.argv[1]))

df.printSchema()


df.show()

(df.write
.format("jdbc")
.mode("overwrite")
.option("driver", driver)
.option('url', url +)
.option("dbtable", "database1")
.option("user", "austin")
.option("password", "1234")
.save())
