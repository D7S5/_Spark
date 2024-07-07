from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("csv_to_avro")
        .getOrCreate()
    )

source = "/home/austin/_dataset/tfl_bus_safety.csv"
sink = "/home/austin/sink"

df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source))
df.show(20)

(df.write
    .format("avro")
    .mode("overwrite")
    .save(sink))