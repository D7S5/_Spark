import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from config.mysql_conf import jars, url

# if len(sys.argv)!=2:
#     print("path: <>", file=sys.stderr)
#     sys.exit(-1)
    

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("sf-fire-call")    
        .config("Spark.jars", jars)
        .getOrCreate()
    )
path = "/home/austin/_dataset/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

df = (
    spark.read.format('csv')
        .option('header', True)
        .load(path)
)

df.printSchema()
df.show(10)
# mode = 'overwrite'
# table_name = 'sf-fire-call'


# (df.write
#     .format('jdbc')
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option('url' , url)
#     .option('mode' , mode)
#     .option('dbtable' , table_name)
#     .option('user', 'austin')
#     .option('password', '1234')
#     .save())

