import pandas as pd
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import LongType

from pyspark.sql import SparkSession

import pandas as pd


# @pandas_udf('long')
# def cubed(a):
#     return a * a * a

# cubed_udf = pandas_udf(cubed, returnType='long')

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('udf')
    .getOrCreate()
)


def cubed(a):
    return a * a * a 

a = 2
print(cubed(a))

spark.udf.register("cubed", cubed, LongType())
spark.range(1,9).createOrReplaceTempView('udf_test')

spark.sql('SELECT id, cubed(id) AS id_cubed FROM udf_test').show()

# df = spark.range(1,4)
# df.select("id", cubed_udf(col("id"))).show()

