from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
        .option('user', 'austin')
        .option('password', password)
        .load()
    )

    # df.printSchema()
    # df.show(50)


    #  for -> if  a.funded.amnt == b.funded_amnt : sum() -> avg() ->  
    
    # rank_function = (rank()
    #     .over(Window.partitionBy('addr_state')
    #     .orderBy('paid_amnt', ascending=False)
    #     .desc())
    #     )
    # ca

    # (df.select('*').orderBy('addr_state')
    #  .groupBy('addr_state')
    #     .agg(avg('funded_amnt').alias('avg_funded_amnt'))
    # #     .show(30))

    # df.filter(df.addr_state == 'CA')
    # df.filter(df.funded_amnt >= 10000)
    # df.filter(df.funded_amnt <= 20000).show()
    
    # .option('url', url_db1)
    #     .option('mode', mode_overwrite)
    #     .option('dbtable', table_name)
    