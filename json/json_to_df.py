from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("load_json")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
)


path = "/home/austin/data/tfl_bus_safety.csv"

df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path))

select_df = df.select("year","date_of_incident",
                      "route","operator","group_name",
                      "bus_garage","borough","injury_result_description",
                      "incident_event_type","victim_category","victims_sex","victims_age")


select_df.show(n=50, truncate=False)

spark.stop()


