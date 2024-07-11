
import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("path : <>", file = sys.stderr)
        exit(-1)

    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('df')
        .getOrCreate()
    )

    df = (
        spark.read.parquet(sys.argv[1])
    )

    df.show(10)