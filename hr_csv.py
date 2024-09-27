import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.mysql_conf import *

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('flights_csv')
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

df2 = (df.select('*', expr("""CASE WHEN GenderID = '0' THEN 'male'
                            WHEN GenderID = '1' THEN 'female'
                           ELSE GenderID END as Gender
                           """)
            ).drop('GenderID'))

df3 = (df2.select('Employee_Name','EmpID','MarriedID','Gender'
                 ,'MaritalStatusID','EmpStatusID','DeptID','PerfScoreID','FromDiversityJobFairID'
                 ,'Salary','Termd','PositionID','Position','State','Zip','DOB','Sex','MaritalDesc'
                 ,'CitizenDesc','HispanicLatino','RaceDesc','DateofHire','DateofTermination'
                 ,'TermReason','EmploymentStatus','Department','ManagerName','ManagerID','RecruitmentSource'
                 ,'PerformanceScore','EngagementSurvey','EmpSatisfaction','SpecialProjectsCount'
                 ,'LastPerformanceReview_Date','DaysLateLast30','Absences'))

df3.show(20, truncate= False)
# when(df.GenderID == 0, "male")
#                 .when(df.GenderID == 1, "female")
#                 .alias('Gender')
# df3 = (df2.select('*'))
# df3.show(20, truncate = False)
       
dbname = 'db3'
dbtable = 'HR_dataset' # table name long text  2010-2015_United_state flights count

(df.write.format('jdbc')
    .option('url', url+dbname)
    .option('mode', 'overwrite')
    .option('driver', 'com.mysql.cj.jdbc.Driver')
    .option('dbtable', dbtable)
    .option('user', 'austin')
    .option('password', password)
    .save())
