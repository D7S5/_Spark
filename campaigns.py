from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import StringType, ArrayType
import sys
from config.mysql_conf import *
from schema.campaigns_schema import campaigns_schema


spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("blogs")
        .config("spark.jars" , jars)
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <> :", file= sys.stderr)
    exit(-1)

# content_json {
# url : https://example.com/campaigns/landing?code=12342" }

def get_full_url(json_column):
    url_full= from_json(json_column, "url STRING")
    return url_full 

 # substring_index(expr, delim, count)
def extract_url(json_column):
    url_full = get_full_url(json_column)
    url = substring_index(url_full, "?", 1) # https://example.com/campagins/landing
    return url

def extract_campaigns_code(json_column):
    url_full = get_full_url(json_column)
    code = substring_index(url_full, "?", -1)
    return substring_index(code, "=", -1) # 12342

path = sys.argv[1]

df = (spark.read.schema(schema).format('json').load(path))

# df2 = (
#     df.select("timestamp", "content_json", "url", "user_id", "browser_info")
#     )

# df2.printSchema()
# df.show(10)


#        .withColumn("Campaigns", expr("CAST(Campaigns as String)")))

# df2.printSchema()
# df2.show(10)

# blogs = (
#     df.withColumn('Campaigns', expr("CAST()"))
# )
# df3 = df2.withColumn("Campaigns", expr("CAST(Campaigns as String) as New_Campaigns"))
# df4 = df3.select(substring(df3.Campaigns, 2, -2)).alias("Camapaigns").collect()
# df4.show()

# @udf(returnType = StringType())
# def function1(a : str ) -> str:
#     return      

# Array[String] -> String remove( [ ] )

db_name = 'db3'
dbtable = "blogs2"

# (df.write
# .format("jdbc")
# .mode("overwrite")
# .option("driver", driver)
# .option('url', url+ db_name)
# .option("dbtable", dbtable)
# .option("user", "austin")
# .option("password", password)
# .save())
