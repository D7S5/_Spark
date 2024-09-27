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


# 출처
# https://www.kaggle.com/datasets/ariyoomotade/netflix-data-cleaning-analysis-and-visualization

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
# WHERE cast = 'David Attenborough'
# UPDATE origin_netflix 
# SET director = 'Not Given'
# WHERE director IS NULL;

SELECT COALESCE(nt.country,nt2.country) 
FROM origin_netflix  AS nt
JOIN origin_netflix AS nt2 
ON nt.director = nt2.director 
AND nt.show_id != nt2.show_id
WHERE nt.country IS NULL;


# SELECT director, country, date_added
# FROM origin_netflix
# WHERE country IS NULL;

# UPDATE origin_netflix 
# SET country = 'Not Given'
# WHERE country IS NULL;

# date_added 행 null

# SELECT show_id, date_added
# FROM origin_netflix
# WHERE date_added IS NULL;

# 분석에 불필요한 컬럼 제거
# ALTER TABLE origin_netflix DROP COLUMN cast
# ALTER TABLE origin_netflix DROP COLUMN description

# 유효하지 않은 컬럼 제거
# "and probably will. | NULL | NULL  | Not Given | Not Given | NULL       | NULL         | NULL   | NULL     | NULL

# DELETE FROM origin_netflix WHERE where type IS NULL AND title IS NULL;
sel