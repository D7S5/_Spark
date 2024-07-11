from config.mysql_conf import *

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("path : <>", file= sys.stderr)
        exit(-1)

    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('read_parquet')
        .getOrCreate()
    )
    df = spark.read.parquet(sys.argv[1])

    df.printSchema()
    df.show(10)


    table_name = "loans"

    (
        df.write.format('jdbc').option('driver', 'com.mysql.cj.jdbc.Driver')
        .option('url', url_db1)
        .option('mode', mode_overwrite)
        .option('dbtable', table_name)
        .option('user', user)
        .option('password', password)
        .save()
    )


