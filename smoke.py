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

# df.printSchema()
# df.show(20)

df2 = (df.select('*'))
# ts_format = 'yyyy-MM-dd HH:mm:ss'

# df2 = (df
#        .withColumnRenamed('Fire Alarm', 'Fire_Alarm')
#        .filter(col('Fire_Alarm') == True)
#        .select(
#         '_c0',
#         from_unixtime('UTC', ts_format).alias('timestamp'),
#         'Temperature[C]',
#         'Humidity[%]',
#         'TVOC[ppb]',
#         'eCO2[ppm]',
#         'Raw H2','Raw Ethanol',
#         'Pressure[hPa]',
#         '`PM1.0`',
#         '`PM2.5`',
#         '`NC0.5`',
#         '`NC1.0`',
#         '`NC2.5`', 
#         '`CNT`',
#         'Fire_Alarm')
#         .orderBy('timestamp', ascending=[True]))

df2.printSchema()
df2.show(20, truncate = False)

db_name = "db3"
table_name = "origin_smoke_detection"

(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())

