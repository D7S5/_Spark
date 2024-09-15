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

df2 = (df
       .select('*')
       .withColumnRenamed('Fire Alarm', 'Fire_Alarm')
       .filter(col('Fire_Alarm') == True)
       )
        
df3 = (df2
       .select(
        avg(col('Humidity[%]')).alias('AVG(Humidity[%])'),
        avg(col('`PM1.0`')).alias('AVG(PM1.0)'),
        avg(col('`PM2.5`')).alias('AVG(PM2.5)'),
        avg(col('`NC0.5`')).alias('AVG(NC0.5)'),
        avg(col('`NC2.5`')).alias('AVG(NC2.5)'))
        ).show()
# ?

# db_name = "db3"
# table_name = "smoke_detection"

# (df2.write.format('jdbc')
#     .option('url', url + db_name)
#     .option('driver', 'com.mysql.cj.jdbc.Driver')
#     .option('dbtable', table_name)
#     .option('user', 'austin')
#     .option('password', password)
#     .save())

