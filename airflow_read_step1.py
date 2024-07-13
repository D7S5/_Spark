import sys

from pyspark.sql import SparkSession
from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('read_parquet')
    .getOrCreate()
)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("path : <> :", file= sys.stderr)
        exit(-1)

    df = spark.read.parquet(sys.argv[1])

    df.printSchema()
    df.show()

  
    (
        df.write.format('jdbc').option('driver', 'com.mysql.cj.jdbc.Driver')
        .option('url', url_db1)
        .option('mode', mode_overwrite)
        .option('dbtable', 'airflowStep1')
        .option('user', 'austin')
        .option('password', password)
        .save()
    )


