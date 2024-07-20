from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("mean")
    .getOrCreate())


data = [['A', 'Guard', 11], 
        ['A', 'Guard', 8], 
        ['A', 'Forward', 22], 
        ['A', 'Forward', 22], 
        ['B', 'Guard', 14], 
        ['B', 'Guard', 14],
        ['B', 'Guard', 13],
        ['B', 'Forward', 7],
        ['C', 'Guard', 8],
        ['C', 'Forward', 5]] 

columns = ['team', 'position', 'points']

df = spark.createDataFrame(data, columns)

df.show()

df.groupBy('team').mean('points').show()

df.groupBy('team', 'position').avg('points').show()