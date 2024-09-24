from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("energy_consume_stage1")
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

# df.show(10)
# print(df.dtypes)
ts_pattern = "dd MMM yyyy"
ts_pattern2= 'HH:mm:ss'

df2 = (df.select(
            to_date('TxnDate', ts_pattern).alias('TxnDate'),
              'TxnTime', date_format('TxnTime', ts_pattern2).alias('Txn_time'),
              'Consumption')
              .drop('TxnTime')
              .withColumnRenamed('Txn_Time','TxnTime')
              .orderBy('TxnDate','TxnTime', ascending=[True, True]))

# df3 = (df2.select('*')).show(10)

db_name = "db3"
table_name = "energy_consume_stage2"

(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())
