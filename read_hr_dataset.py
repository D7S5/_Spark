from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys

from config.mysql_conf import *

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("energy_consume_stage1")
        .config("spark.jars" , jars)
        .getOrCreate()
)

if len(sys.argv) != 2:
    print("path <>: " , file = sys.stderr)
    exit(-1)

df = (spark.read
      .option('header', True)
      .option('inferschema', True)
      .csv(sys.argv[1])
)

# df.printSchema()
df2 = (df.select('*'))

db_name = "db3"
table_name = "origin_hr_dataset"

(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())
# 
# select Department, Education, EducationField, Gender, HourlyRate, JobLevel, JobRole, MaritalStatus , MonthlyIncome 
#  from origin_hr_dataset
#  where JobLevel=3 AND MaritalStatus NOT IN Divorced ;") ;

