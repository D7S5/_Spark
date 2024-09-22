import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.mysql_conf import *

import re

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('flights_csv')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('path <> :' , file= sys.stderr)
        sys.exit(-1)

df = (
    spark.read
    .option('header', True)
    .option("inferschema", True)
    .csv(sys.argv[1])
    )

for each in df.schema.names:
    df = df.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','', each.replace(' ', '')))

df2 = df.select('*')

df2.printSchema()
df2.show(10, truncate = False)       

dbname = 'db3'
dbtable = 'smart_city' # table name long text  2010-2015_United_state flights count

(df2.write.format('jdbc')
    .option('url', url+dbname)
    .option('mode', 'overwrite')
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())