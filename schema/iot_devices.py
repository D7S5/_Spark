from pyspark.sql.types import StructType, StructField, LongType, VarcharType, DoubleType, ShortType, StringType

    
iot_devices_schema = StructType([
        StructField("device_id", LongType(), nullable = False), 
        StructField("device_name", VarcharType(30), nullable = False), 
        StructField("ip", VarcharType(15), nullable = False), 
        StructField("cca2", VarcharType(4), nullable = False), 
        StructField("cca3", VarcharType(5), nullable = False), 
        StructField("cn", VarcharType(10), nullable = False), 
        StructField("latitude", StringType(), nullable = False), 
        StructField("longitude", StringType() , nullable = False), 
        StructField("scale", VarcharType(10), nullable = False), 
        StructField("temp", ShortType(), nullable = False), 
        StructField("humidity",ShortType(), nullable = False), 
        StructField("battery_level", ShortType(), nullable = False), 
        StructField("c02_level", ShortType(), nullable = False), 
        StructField("lcd", VarcharType(10), nullable = False), 
        StructField("timestamp", LongType(), nullable = False)])