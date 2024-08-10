from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType


campaigns_schema = StructType([
    StructField("timestamp", LongType(), nullable= False),
    StructField("content_json", StructType([
        StructField("url", StringType(), nullable= False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("browser_info", StringType() ,nullable=True)
    ]), nullable=False),
])