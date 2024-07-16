from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master('local[*]')
        .appName("airport_join")
        .config("spark.jars", jars)
        .getOrCreate()
)

tripdelaysFilePath = '/home/austin/_dataset/databricks-datasets/learning-spark-v2/flights'
join_airportsnaFilePath = '/home/austin/_dataset/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt'

airportna = (
        spark.read
            .format('csv')
            .options(header=True, inferSchema=True, sep="\t").load(join_airportsnaFilePath)
)

airportna.createOrReplaceTempView("airports_na")

departureDelays = (
    spark.read
        .format('csv')
        .options(header=True)
        .load(tripdelaysFilePath)
)

departureDelays.printSchema()

departureDelays = (
    departureDelays
    .withColumn('delay', expr("CAST(delay as INT) as delay"))
    .withColumn('distance', expr("CAST(distance as INT) as distance"))
)

departureDelays.printSchema()

departureDelays.createOrReplaceTempView("departureDelays")

temp = (departureDelays
        .filter(expr("""origin == 'SEA' AND destination == 'SFO' and date like '01010%' and delay > 0""")))

temp.createOrReplaceTempView('temp')

spark.sql("SELECT * FROM airports_na LIMIT 10").show(10)
spark.sql("SELECT * FROM departureDelays LIMIT 10").show(10)

spark.sql("SELECT * FROM temp").show()




