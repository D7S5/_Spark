from config.mysql_conf import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master('local[*]')
        .appName('mysql_join')
        .config('spark.driver.extraClassPath', jars)
        .getOrCreate()
    )
    db_name = "db2"
    dbtable = 'departments'
    sql = "SELECT * FROM departments" 

    df_departments = (
        spark.read.format('jdbc')
        .option('url', url + db_name)
        .option("driver", driver)
        .option('dbtable', dbtable)
        .option('sql', sql)
        .option('user', 'austin')
        .option('password', password)
        .load()
    )
    
    db_name = "db2"
    dbtable2 = "employees"
    sql = "select * from employees"

    df_employees = (
        spark.read.format('jdbc')
        .option('url', url + db_name)
        .option("driver", driver)
        .option('dbtable', dbtable2)
        .option('sql', sql)
        .option('user', 'austin')
        .option('password', password)
        .load()
    )

    df_departments.show()
    df_employees.show()

    join_type = "inner" # full outer join
    join_expression = df_employees.department_id == df_departments.department_id

    df = (
        df_employees.join(df_departments, join_expression, join_type)
          .select("employee_id", "employee_name", "department_name")
          ).show()


    # df2 = (
    #     df.select("employee_id", "employee_name", "department_name").na.drop()
    #     ).show() # na.drop : remove all rows with any null values.
    
    
    table_name = "new_employees"

    (df.write.format('jdbc')
    .option('url', url + db_name)
    .option('driver', driver)
    .option('dbtable', table_name)
    .option('user', 'austin')
    .option('password', password)
    .save())



