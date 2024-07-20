from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("load_json")
        .getOrCreate()
)

source = "/home/austin/dataset/databricks-datasets/learning-spark-v2/flights/summary-data/json"
schema = "DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count"
sink = "/home/austin/sink"


def json_to_csv():
    df = (spark.read.format("json").load(source))
    df.show()
    (df.write.format("csv")
        .option("header", True)
        .option("inferschema", True)
        .mode("overwrite")
        .save(sink))

def main():
    json_to_csv()
    
if __name__ == "__main__":
    main()


