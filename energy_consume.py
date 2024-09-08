from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
    print("path <>: " , file = sys.stderr)
    exit(-1)


df = (spark.read
      .option('header', True)
      .option('inferschema', True)
      .csv(sys.argv[1])
)

df = (df.select('TxnDate', 'TxnTime', 'Consumption').orderBy(col('TxnDate'), col('TxnTime'), ascending=[True, True]))

df.show(10)

db_name = "db3"
table_name = "energy_consume_stage1"

(df.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())