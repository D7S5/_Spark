from pyspark.sql.functions import *
from pyspark.sql.types import *
from config.mysql_conf import *
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('flights_csv')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

data = [("Alice", 25, "female"), ("Bob", 30, "male"), ("Charlie", 35, "male"), ("Lee", 30, "male")]

schema = StructType([StructField("name", StringType(), True), 
                     StructField("age", IntegerType(), True),
                     StructField("gender", StringType(), True)])
df = spark.createDataFrame(data, schema)
# validate the data frame
df2 = df.selectExpr(
    "name", "age", "gender", "CASE WHEN age > 0 THEN 'valid' ELSE 'invalid' END AS age_validation"
    ).show()

valid_df = df.filter(df.age > 0)
invalid_df = df.filter(df.age <= 0)

print("Valid data:")
valid_df.show()
print("Invalid data:")
invalid_df.show()