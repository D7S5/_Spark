import sys

from pyspark.sql import SparkSession

from config.mysql_conf import *


spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('loans_parquet')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('path <> :' , file= sys.stderr)
        sys.exit(-1)

df = spark.read.parquet(sys.argv[1])

df.show(10)

dbtable = 'loans'

(df.write.format('jdbc')
    .option('url', url_db1)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', dbtable)
    .option('user', user)
    .option('password', password)
    .save())