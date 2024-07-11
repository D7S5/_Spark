from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import filter, col, avg

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('mysql_to_mysql')
        .config('spark.driver.extraClassPath', jars)
        .getOrCreate()
    )

    dbtable = 'loans'
    sql = "SELECT * FROM loans" 

    df = (
        spark.read.format('jdbc')
        .option('url', url_db1)
        .option("driver","com.mysql.cj.jdbc.Driver")
        .option('dbtable', dbtable)
        .option('sql', sql)
        .option('user', user)
        .option('password', password)
        .load()
    )

    df.show(10)
    
    (df.select('*').groupBy('addr_state')
        .agg(avg('funded_amnt').alias('avg_funded_amnt'))
        .show())
    # df1.filter(df.addr_state == 'CA')
    # df.filter(df.funded_amnt >= 10000)
    # .option('url', url_db1)
    #     .option('mode', mode_overwrite)
    #     .option('dbtable', table_name)
    