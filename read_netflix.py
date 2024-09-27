from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('read_netflix')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

db_name = "db3"
dbtable = 'origin_netflix'
sql = "SELECT * FROM origin_netflix" 

df = (
    spark.read.format('jdbc')
    .option('url', url + db_name)
    .option("driver", driver)
    .option('dbtable', dbtable)
    .option('sql', sql)
    .option('user', 'austin')
    .option('password', password)
    .load()
)

df.printSchema()

df2 = (
    df.select('show_id', 'date_added'))

df2.show(20)
