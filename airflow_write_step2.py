from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('airflow_step2')
        .config('spark.driver.extraClassPath', jars)
        .getOrCreate()
    )

    source_table = 'airflowStep1'
    sink_table = 'airflowStep2'

    sql = "SELECT * FROM airflowStep1" 

    df = (
        spark.read.format('jdbc')
        .option('url', url_db1)
        .option("driver","com.mysql.cj.jdbc.Driver")
        .option('dbtable', source_table)
        .option('sql', sql)
        .option('user', 'austin')
        .option('password', password)
        .load()
    )

    (df.write.format('jdbc')
        .option('url', url_db1)
        .option('driver', 'com.mysql.cj.jdbc.Driver')
        .option('dbtable', sink_table)
        .option('user', 'austin')
        .option('password', password)
        .save())