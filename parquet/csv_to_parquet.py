from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('csv_to_parquet')
    .getOrCreate()
    )

source = "/home/austin/_dataset/chapter3/data/sf-fire-calls.csv"
sink = "/home/austin/sink"

df = (spark
      .read
      .option('header', 'true')
      .option('inferSchema', 'true')
      .csv(source))

df.printSchema()
df.show(10)

(df.write
    .mode("overwrite")
    .format('parquet')
    .save(sink))
