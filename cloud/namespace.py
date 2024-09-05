import sys

import datetime
from pyspark.sql import SparkSession
from config.mysql_conf import *
from ulid import ULID

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('flights_csv')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

namespace = "ETL"
pipeline_name = "click_steam_ingest"
source_name = "clicks"

# batch_id = ""

ulid = ULID.from_datetime(datetime.datetime.now()) # current datetime 

df = (
    spark.read.parquet(sys.argv[1])
)

