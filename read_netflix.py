from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('read_netflix')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

db_name = "db3"
dbtable = 'origin_netflix'
sql = "SELECT * FROM origin_netflix" 

df = (
    spark.read.format('jdbc')
    .option('url', url + db_name)
    .option("driver", driver)
    .option('dbtable', dbtable)
    .option('sql', sql)
    .option('user', 'austin')
    .option('password', password)
    .load()
)

# date_formats = ["dd/MM/yyyy", "yyyy-MM-dd", "MM/dd/yyyy"]

# for date_format in date_formats:
#      df = df.withColumn('date', to_date(col('date_added'), date_format))
dt_format = "MMMM dd, yyyy"

result_df = df.withColumn('bad_records',
                        #    when(col('FileName') == 'leaves',
                            when(col("date_added").isNull(), "False")
                            .when(to_date(col("date_added"), dt_format).isNotNull(), "True")
                            .otherwise("False"))

# java.time.format.DateTimeParseException
# If an exception occurs, add show_id to the badrecord column. 
# spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# esult_df = df.withColumn('bad_records', 
#                          when(col('FileName') == 'leaves',
#                         when(col("id").isNull(), "False")
#                         .when(to_date(col("id"), "dd MMM yyyy").isNotNull(), "True")
#                         .otherwise("False") )
                            # .when(col('FileName') == 'emp',
                            # when(col("date_added").isNull(), "False")
                            # .when(to_date(col("date_added"), "dd-MM-yyyy HH:mm").isNotNull(), "True")
                            # .otherwise("False"))

df2 = (result_df
       .filter(col("bad_records") == "True")
       .select('show_id', 'title', 
               to_date("date_added", format = dt_format).alias("date_added")
               , 'bad_records')
       )
df2.printSchema()
df2.show(20)

# w_db_name = 'db4'
# w_table_name = 'badrecord_true_netflix'

# (df2.write.format('jdbc')
#     .option('url', url + w_db_name)
#     .option('driver', 'com.mysql.cj.jdbc.Driver')
#     .option('dbtable', w_table_name)
#     .option('user', 'austin')
#     .option('password', password)
#     .save())