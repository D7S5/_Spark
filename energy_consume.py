from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
    print("path <>: " , file = sys.stderr)
    exit(-1)


df = (spark.read
      .option('header', True)
      .option('inferschema', True)
      .csv(sys.argv[1])
)

df = (df.select('*'))

df.show(30)