from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
import sys
from config.mysql_conf import *


spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("blogs")
        .config("spark.jars" , jars)
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <> :", file= sys.stderr)
    exit(-1)

df = (spark.read.format('json').load(sys.argv[1]))

df2 = df.select('Id','First','Last','Url','Published','Hits','Campaigns')
df2.printSchema()

df2.show()

# blogs = (
#     df.withColumn('Campaigns', expr("CAST()"))
# )



db_name = 'db3'
dbtable = "blogs2"

# (df.write
# .format("jdbc")
# .mode("overwrite")
# .option("driver", driver)
# .option('url', url+ db_name)
# .option("dbtable", dbtable)
# .option("user", "austin")
# .option("password", password)
# .save())
