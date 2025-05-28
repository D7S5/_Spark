from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("energy_consume_stage1")
        .config("spark.jars" , jars)
        .getOrCreate()
)

path = "/home/austin/data/tfl_bus_safety.csv"

df = (spark.read
      .option('header', True)
      .option("inferschema", True)
      .csv(path))

df.show(10)

# db_name = "db2"
# table_name = "bus"

# (df.write.jdbc(url=url + db_name, table="bus", mode="overwrite")
#     .option('dbtable', table_name)
#     .option('user', 'austin')
#     .option('password', password)
#     .save())
# # 