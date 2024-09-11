from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *


spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('read_mysql_energy_consume_stage1')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

db_name = 'db3'
dbtable = 'energy_consume_stage1'
sql = "SELECT * FROM energy_consume_stage1" 

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
# w = Window.partitionBy('TxnDate')

# print(df.dtypes)
# .withColumnRenamed(,"avg(Consumption)")
(df.select('*')
 .groupBy('TxnDate')
 .avg()
 .orderBy(desc('TxnDate'))).show()