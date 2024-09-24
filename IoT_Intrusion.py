from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("energy_consume_stage1")
        .config("spark.jars" , jars)
        .config("spark.debug.maxToStringFields", 50)
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <>: " , file = sys.stderr)
    exit(-1)

df = (spark.read
      .option('header', True)
      .option('inferschema', True)
      .csv(sys.argv[1])
)

col_list = df.columns
Spending_col = [i for i in col_list]
print("Spending Columns",len(Spending_col))

df.printSchema()
df2 = (df.select('*'))

df2.show(30, truncate=False)

# db_name = "db3"
# table_name = "origin_iot_intrustion"

# (df2.write.format('jdbc')
#     .option('url', url + db_name)
#     .option('driver', 'com.mysql.cj.jdbc.Driver')
#     .option('dbtable', table_name)
#     .option('user', 'austin')
#     .option('password', password)
#     .save())

