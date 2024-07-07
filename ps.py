from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("pyspark_mysql_connection")
        .config("spark.jars", "mysql-connector-java-8.0.13.jar")
        .getOrCreate()
)

columns = ["State","Color","Count"]
data = [("TX","Red",20),("NV","Blue",66)]

sampleDF = (spark.sparkContext.parallelize(data).toDF(columns))

sampleDF.show()

def write():
    (sampleDF.write
    .format("jdbc")
    .mode("overwrite")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option('url", "jdbc:mysql://localhost:3306/')
    .option("dbtable", "database1")
    .option("user", "austin")
    .option("password", "1234")
    .save())

def main():
    write()
    




