import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('origin_netflix')
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

df.printSchema()
df2 = (df.select('*'))

dbtable = 'origin_netflix'
db_name = 'db3'

(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())

# SELECT 'pk', COUNT(*)                                                                                                                                                                            
# FROM table 
# GROUP BY pk
# ORDER BY pk DESC;

# --No duplicates