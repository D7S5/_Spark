from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]") \
    .appName("SparkByExamples.com").getOrCreate()

# Create DataFrame
data = [
    ("James",None,"M"),
    ("Anna","NY","F"),
    ("Julia",None,None)
  ]

columns = ["name","state","gender"]

df = spark.createDataFrame(data,columns)
df.show()

# Using isNull()
df.filter("state is NULL").show()
df.filter(df.state.isNull()).show()

from pyspark.sql.functions import col
df.filter(col("state").isNull()).show()

df.filter("state IS NULL AND gender IS NULL").show()
df.filter(df.state.isNull() & df.gender.isNull()).show()

from pyspark.sql.functions import isnull
df.select(isnull(df.state)).show()

# Using isNotNull()
from pyspark.sql.functions import col
df.filter("state IS NOT NULL").show()
df.filter("NOT state IS NULL").show()
df.filter(df.state.isNotNull()).show()
df.filter(col("state").isNotNull()).show()

df.na.drop(subset=["state"]).show()

# Using pySpark SQL
df.createOrReplaceTempView("DATA")
spark.sql("SELECT * FROM DATA where STATE IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()