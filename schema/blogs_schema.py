from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, ShortType, StringType, ArrayType


blogs_schema = StructType([
    StructField("Campaigns", ArrayType(StringType()), nullable= True),
    StructField("First", StringType(), nullable= False),
    StructField("Id", LongType(), nullable= False),
    
    StructField("Last", StringType(), nullable= False),
    StructField("Url", StringType(), nullable= False),
    StructField("Published", StringType(), nullable= False),
    StructField("Hits", IntegerType(), nullable= False),
    
])