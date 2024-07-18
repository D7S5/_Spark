import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('flights_parquet')
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

df2 = (
    df.filter(expr("""DEST_COUNTRY_NAME == 'United States'"""))
        .groupBy('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME')
        .agg(sum('count').alias("2010-2015 count"))
        .orderBy(desc("2010-2015 count")).show(10)
        )

