from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

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
        .option("driver", driver)
        .option('dbtable', dbtable)
        .option('sql', sql)
        .option('user', 'austin')
        .option('password', password)
        .load()
    )

    # df.printSchema()
    # df.show(50)

    # & 압축
    df2 = (df.filter(expr("""addr_state == 'CA' AND funded_amnt >= 10000 AND funded_amnt <= 20000 """))
           .orderBy("funded_amnt", ascending =False))

    #  AND funded_amnt >= 10000 AND funded_amnt <= 20000
    
    df2.show()
    
    
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

    # .option('url', url_db1)
    #     .option('mode', mode_overwrite)
    #     .option('dbtable', table_name)
    