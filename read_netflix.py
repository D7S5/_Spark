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

# result_df.printSchema()

# df2 = (result_df
#        .filter(col("bad_records") == "True")
#        .select('*', 
#                to_date("date_added", format = dt_format).alias("date_added")
#                )
#        )

# df2 = (result_df.filter(col("bad_records") == "True")
            # .select('show_id','type', 'title', 'director', 
            #      to_date("date_added", format = dt_format).alias("date_added"),
            #     'release_year', 'rating', 'duration', 'listed_in',
            #      'country')
            #      )

# df2 = (result_df.filter(col("bad_records") == "True")
#        .select('show_id','type', 'title', 'director', 
#                  to_date("date_added", format = dt_format).alias("date_added"),
#                 'release_year', 'rating', 'duration', 'listed_in',
#                  'country')
#                  )

df2 = (result_df.filter(col("bad_records") == "False")
       .select('show_id','type', 'title', 'director', 
                 'date_added',
                'release_year', 'rating', 'duration', 'listed_in',
                 'country')
                 )

df2.printSchema()
df2.show(20)

w_db_name = 'db4'
w_table_name = 'badrecord_false_netflix_v2'

# date_added 컬럼에 country , 날짜 포맷 형식 맨 앞 공백
# 인도 india
# s2640, s4668, s5970, s5971, s5975, s5969, s6002, s7807
# show_id = 's7807'; null 체크 안들어감 -> date_added 공백 문제

# 이집트어 Egypt 바이트 문제 영어아닌 이집트어 유니코드 -> 수작업 번역
# 깨짐문제 : s8775, s8795, s2639 // * 스캔시 정상

(df2.write.format('jdbc')
    .option('url', url + w_db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', w_table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())