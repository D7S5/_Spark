import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

df2 = (df
        .select('Id','City','Country',
                trim('Smart_Mobility ').alias('Smart_Mobility'),
                'Smart_Environment',
                trim('Smart_Government ').alias('Smart_Government'),
                trim('Smart_Economy ').alias('Smart_Econmy'),
                'Smart_People',
                'Smart_Living',
                'SmartCity_Index',
                'SmartCity_Index_relative_Edmonton'))

df2.printSchema()
df2.show(20, truncate = False)       

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