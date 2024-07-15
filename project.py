from pyspark.sql import SparkSession
from config.mysql_conf import *
# project cost
from schema.blogs_schema import *
import sys


spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('loans_parquet')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("path <> :" , file = sys.stderr)
        sys.exit(-1)



df = (spark.read.schema(blogs_schema).json(sys.argv[1]))

df.printSchema()
df.show(10)

db_name = "db3"
dbtable = "blogs"
mode = 'overwrite'



(df.write.format('jdbc')
    .option('url' , url + db_name)
    .option("driver", driver)
    .option('mode' , mode)
    .option('dbtable' , dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())


