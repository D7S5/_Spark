import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from config.mysql_conf import *

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

df2 = (
    df.filter(expr("""DEST_COUNTRY_NAME == 'United States'"""))
        .groupBy('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME')
        .agg(sum('count').alias('2010-2015_count'))
        .orderBy(desc('2010-2015_count'))
        )

rank_function = (rank()
                 .over(Window.orderBy('2010-2015_count'))
                 )
rank = (df2.withColumn('2010-2015_count', rank_function))

rank.show(10)


# df2.show(10)
dbname = 'db4'
dbtable = 'flights' # table name long text  2010-2015_United_state flights count

# (df2.write.format('jdbc')
#     .option('url', url+dbname)
#     .option('mode', 'overwrite')
#     .option('driver', 'com.mysql.cj.jdbc.Driver')
#     .option('dbtable', dbtable)
#     .option('user', 'austin')
#     .option('password', password)
#     .save())