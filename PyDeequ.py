# install PyDeequ
# !pip install pydeequ

# import PyDeequ
from pydeequ.checks import *
from pydeequ.verification import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('flights_csv')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)
# create a data frame
data = [("Alice", 25, "female"), ("Bob", 30, "male"), ("Charlie", -35, "male")]
schema = StructType([StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True)])
df = spark.createDataFrame(data, schema)


# version

# define the data validation checks
check = Check(spark, CheckLevel.Warning, "Data Validation")
check_result = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check.hasMin("age", lambda x: x > 0)) \
    .run()
