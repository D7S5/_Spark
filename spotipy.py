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

df2 = (df.select('title',   
                 col('rank').cast('int'),
                 col('date').cast('date'),
                 'artist',
                 'url',
                 'region',
                 'chart',
                 'trend',
                 col('streams').cast('int'),
                 ))

df2.printSchema()
df2.show(30)

db_name = "db2"
table_name = "cast_spotipy"
    
(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())
