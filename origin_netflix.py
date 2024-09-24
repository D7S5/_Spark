import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('origin_netflix')
    .config('spark.driver.extraClassPath', jars)
    .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('path <> :' , file= sys.stderr)
        sys.exit(-1)

df = (
    spark.read
    .option('header', True)
    .option("inferschema", True)
    .csv(sys.argv[1])
    )

df.printSchema()
df2 = (df.select('*'))

dbtable = 'origin_netflix'
db_name = 'db3'

(df2.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())

# SELECT 'pk', COUNT(*)                                                                                                                                                                            
# FROM table 
# GROUP BY pk
# ORDER BY pk DESC;

# --No duplicates

# SELECT (COUNT(*)-COUNT(type)) as type_nulls,
# (COUNT(*)-COUNT(title)) as title_nulls,
# (COUNT(*)-COUNT(director)) as director_nulls,
# (COUNT(*)-COUNT(cast)) as cast_nulls,
# (COUNT(*)-COUNT(country)) as country_nulls,
# (COUNT(*)-COUNT(date_added)) as date_added_nulls,
# (COUNT(*)-COUNT(release_year)) as release_year_nulls,
# (COUNT(*)-COUNT(rating)) as rating_nulls,
# (COUNT(*)-COUNT(duration)) as duration_nulls,
# (COUNT(*)-COUNT(listed_in)) as listed_in_nulls,
# (COUNT(*)-COUNT(description)) as description_nulls,


# WITH cte AS 
# (
#  SELECT title, CONCAT(director, '---', cast) AS director_cast
# FROM origin_netflix
# )  

# SELECT director_cast, COUNT(*) AS count
# FROM cte
# GROUP BY director_cast
# HAVING COUNT(*) > 1
# ORDER BY COUNT(*) DESC;

# UPDATE origin_netflix  SET director = 'Alastair Fothergill' 
# WHERE cast = 'David Attenborough';

