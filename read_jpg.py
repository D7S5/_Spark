from pyspark.ml.image import ImageSchema

from pyspark.sql import SparkSession
import sys
from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('read_jpg')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("path <> :" , file = sys.stderr)
        sys.exit(-1)

df = (
    spark.read.format('image').load(sys.argv[1])
)

df.printSchema()

# image [ origin, height, width, nChannels, mode, data ]

df2 = (df.select('image.origin'
           , 'image.height', 
           'image.width', 'image.nChannels',
            'image.mode', 'image.data'))

db_name = 'db4'

dbtable = 'cctvImages'
mode = 'append'
# default GLOBAL sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'
# Data truncation: Data too long for column 'data' at row 1 -> CREATE TABLE 

(df2.write.format('jdbc')
    .option('url' , url + db_name)
    .option("driver", driver)
    .mode(mode)
    .option('dbtable' , dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())