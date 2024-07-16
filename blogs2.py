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

df2 = (df.select('Id','First','Last','Url','Published','Hits','Campaigns')
       .withColumn("Campaigns", expr("CAST(Campaigns as String) as New_Campaigns")))

df2.printSchema()
df2.show(10)

# blogs = (
#     df.withColumn('Campaigns', expr("CAST()"))
# )
# df3 = df2.withColumn("Campaigns", expr("CAST(Campaigns as String) as New_Campaigns"))
# df4 = df3.select(substring(df3.Campaigns, 2, -2)).alias("Camapaigns").collect()
# df4.show()

# @udf(returnType = StringType())
# def function1(a : str ) -> str:
#     return      

# Array[String] -> String remove( [ ] )

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
