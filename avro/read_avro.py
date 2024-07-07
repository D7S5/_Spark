from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("csv_to_avro")
        .getOrCreate()
)

source = "/home/austin/sink"
sink = ""

df = (spark.read.format('avro').load(source))
df.printSchema()

df.show(10)

