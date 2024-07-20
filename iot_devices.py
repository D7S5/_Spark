import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.mysql_conf import *
from schema.iot_devices import iot_devices_schema
spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('lot_devices')
    .config('spark.jars', jars)
    .getOrCreate()
)

if len(sys.argv) != 2:
    print('path : <>', file=sys.stderr)
    sys.exit(-1)

schema = iot_devices_schema

df = (
    spark.read.format('json')
        .load(sys.argv[1])
)
# df.printSchema()
# df.show(2)

df2 = (
    df.select('device_id','device_name', 'ip','cca2',
               'cca3', 'cn', 'latitude', 'longitude',
                'scale', 'temp','humidity', 'battery_level',
                'c02_level', 'lcd', 'timestamp')
    .filter(expr("""cca3 == 'USA'""") )
)

db_name = 'db4'
table_name = 'US_iot_devices'

mode = 'overwrite'

(df2.write
    .format('jdbc')
    .option('driver','com.mysql.cj.jdbc.Driver')
    .option('url' , url + db_name)
    .option('mode' , mode)
    .option('dbtable' , table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())


