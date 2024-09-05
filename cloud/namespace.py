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

batch_id = ULID.from_datetime(datetime.datetime.now())
current_datetime = datetime.datetime.now()

in_path = f"gs://landing/{namespace}/{pipeline_name}{source_name}/{batch_id}/*"
out_path = f"gs://staging/{namespace}/{pipeline_name}{source_name}/year={current_datetime.year}/month={current_datetime.month}/day={current_datetime.day}/{batch_id}"

df = (
    spark.read.parquet(sys.argv[1])
)

df = (df.write.format('parquet')
        .save(out_path))


