import pandas as pd
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import LongType

from pyspark.sql import SparkSession

import pandas as pd


# @pandas_udf('long')
# def cubed(a):
#     return a * a * a

# cubed_udf = pandas_udf(cubed, returnType='long')


# Apache arrow
spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('udf')
    .getOrCreate()
)

def cubed(a):
    return a * a * a 

spark.udf.register("cubed", cubed, LongType())
spark.range(1,9).createOrReplaceTempView('udf_test')

spark.sql('SELECT id, cubed(id) AS id_cubed FROM udf_test').show()


def pandas_cubed(a: pd.Series) -> pd.Series:
    return a * a * a 

cubed_udf = pandas_udf(pandas_cubed, returnType=LongType())
x = pd.Series([1,2,3])
print(pandas_cubed(x))

pandas_df = spark.range(1,4)
pandas_df.select("id", cubed_udf(col("id"))).show()

