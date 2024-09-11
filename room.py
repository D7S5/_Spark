from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("room")
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
# "08-12-2018 06:57"
ts_pattern = "MM-dd-yyyy HH:mm"

df2 = (df.select(
    'id', 'room_id/id',
    to_timestamp('noted_date', ts_pattern).alias('noted_date'),
    'temp',
    'out/in'
))
# df2.printSchema()
# df2.show(20)

db_name = "db3"
table_name = "room"

(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())


