import sys

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("MnmCount")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('path <> :' , file= sys.stderr)
        sys.exit(-1)

df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(sys.argv[1]))


df2 = (df
        .select("State", "Color", "Count")
        .groupBy("State", "Color").sum("Count")
        .orderBy("sum(Count)", ascending=False))

df3 = (df
       .select('State','Count')
       .groupBy('State').sum("Count")
       .orderBy('sum(Count)', ascending=False)
       )

df2.show(n=50, truncate=False)
df3.show(n=50, truncate=False)



    
# df.withColumn('column', 
#                    when('col' == "case0", "")
#                    .when('col'=="case1", "")
#                    .when('col'.isNull(), "")
#                    .otherwise('col')
#                    ).show()```



             
             






