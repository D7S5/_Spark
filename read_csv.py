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

# df2 = (
#     df.filter(expr("""DEST_COUNTRY_NAME == 'United States'"""))
#         .groupBy('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME')
#         .agg(sum('count').alias('2010-2015_count'))
#         .orderBy(desc('2010-2015_count'))
#         )

df2 = (df.select('*')
       .filter("""DEST_COUNTRY_NAME == 'United States'""")
       .groupBy('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME')
       .agg(sum('count').alias('2010-2015_count'))
       .orderBy(desc('2010-2015_count'))
)

w = ((Window.partitionBy('DEST_COUNTRY_NAME').orderBy(desc('2010-2015_count'))))

df3 = (
    df2.withColumn('rank', rank().over(w))
    )

dbname = 'db4'
dbtable = 'flights_United_States_rank' # table name long text  2010-2015_United_state flights count

(df3.write.format('jdbc')
    .option('url', url+dbname)
    .option('mode', 'overwrite')
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())