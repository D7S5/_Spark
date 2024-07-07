import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

if len(sys.argv)!=2:
    print("path: <>", file=sys.stderr)
    sys.exit(-1)



if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("airbnb")
        .config("spark.jars", "/home/austin/jarfile/mysql-connector-java-8.0.13.jar")
        .getOrCreate()
    )

df = (
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(sys.argv[1]))

df.printSchema()
schema = StructType([
    StructField('id', StringType(), nullable=False),
    StructField('listing_url', StringType(), nullable=True),
    StructField('last_scraped', StringType(), nullable=True),
    StructField('name', StringType(), nullable=True),
    StructField('summary', StringType(), nullable=True),
    StructField('space', StringType(), nullable=True),
    StructField('description', StringType(), nullable=True),
    StructField('experiences_offered', StringType(), nullable=True),
    StructField('neighborhood_overview', StringType(), nullable=True),
    StructField('notes', StringType(), nullable=True),
    StructField('transit', StringType(), nullable=True),
    StructField('access', StringType(), nullable=True),
    StructField('interaction', StringType(), nullable=True),
    StructField('house_rules', StringType(), nullable=True),
    StructField('thumbnail_url', StringType(), nullable=True),
    StructField('medium_url', StringType(), nullable=True),
    StructField('picture_url', StringType(), nullable=True),
    StructField('xl_picture_url', StringType(), nullable=True),
    StructField('host_id', StringType(), nullable=True),
    StructField('host_url', StringType(), nullable=True),
    StructField('host_name', StringType(), nullable=True),
    StructField('host_since', StringType(), nullable=True),
    StructField('host_location', StringType(), nullable=True),
    StructField('host_about', StringType(), nullable=True),
    StructField('host_response_time', StringType(), nullable=True),
    StructField('host_response_rate', StringType(), nullable=True),
    StructField('host_acceptance_rate', StringType(), nullable=True),
    StructField('host_is_superhost', StringType(), nullable=True),
    StructField('host_thumbnail_url', StringType(), nullable=True),
    StructField('host_picture_url', StringType(), nullable=True),
    StructField('host_neighbourhood', StringType(), nullable=True),
    StructField('host_listings_count', StringType(), nullable=True),
    StructField('host_total_listings_count', StringType(), nullable=True),
    StructField('host_verifications', StringType(), nullable=True),
    StructField('host_has_profile_pic', StringType(), nullable=True),
    StructField('host_identity_verified', StringType(), rue),
    StructField('state', StringType(), nullable=True),
    StructField('ipcode', StringType(), nullable=True),
    StructField('market', StringType(), nullable=True),
    StructField('smart_location', StringType(), nullable=True),
    StructField('country_code', StringType(), nullable=True),
    StructField('country', StringType(), nullable=True),
    StructField('latitude', DoubleType(), nullable=True), 
    StructField('longitude', DoubleType(), nullable=True),
    StructField('is_location_exact', StringType(), nullable=True),
    StructField('property_type', StringType(), nullable=True),
    StructField('room_type', StringType(), nullable=True),
    StructField('accommodates', StringType(), nullable=True),
    StructField('bathrooms', StringType(), nullable=True),
    StructField('bedrooms', StringType(), nullable=True),
    StructField('beds', StringType(), nullable=True),
    StructField('bed_type', StringType(), nullable=True),
    StructField('amenities', StringType(), nullable=True),
    StructField('square_feet', StringType(), nullable=True),
    StructField('price', IntegerType(), nullable=True),
    StructField('weekly_price', IntegerType(), nullable=True),
    StructField('monthly_price', IntegerType(), nullable=True),
    StructField('security_deposit', StringType(), nullable=True),
    StructField('cleaning_fee', StringType(), nullable=True),
    StructField('guests_included', StringType(), nullable=True),
    StructField('extra_people', StringType(), nullable=True),
    StructField('minimum_nights', StringType(), nullable=True),
    StructField('maximum_nights', StringType(), nullable=True),
    StructField('minimum_minimum_nights', StringType(), nullable=True),
    StructField('maximum_minimum_nights', StringType(), nullable=True),
    StructField('minimum_maximum_nights', StringType(), nullable=True),
    StructField('maximum_maximum_nights', StringType(), nullable=True),
    StructField('minimum_nights_avg_ntm', StringType(), nullable=True),
    StructField('maximum_nights_avg_ntm', StringType(), nullable=True),
    StructField('calendar_updated', StringType(), nullable=True),
    StructField('has_availability', StringType(), nullable=True),
    StructField('availability_30', StringType(), nullable=True),
    StructField('availability_60', StringType(), nullable=True),
    StructField('availability_90', StringType(), nullable=True),
    StructField('availability_365', StringType(), nullable=True),
    StructField('calendar_last_scraped', StringType(), nullable=True),
    StructField('number_of_reviews', StringType(), nullable=True),
    StructField('number_of_reviews_ltm', StringType(), nullable=True),
    StructField('first_review', StringType(), nullable=True),
    StructField('last_review', StringType(), nullable=True),
    StructField('review_scores_rating', StringType(), nullable=True),
    StructField('review_scores_accuracy', StringType(), nullable=True),
    StructField('review_scores_cleanliness', StringType(), nullable=True),
    StructField('review_scores_checkin', StringType(), nullable=True),
    StructField('review_scores_communication', StringType(), nullable=True),
    StructField('review_scores_location', StringType(), nullable=True),
    StructField('review_scores_value', StringType(), nullable=True),
    StructField('requires_license', StringType(), nullable=True),
    StructField('license', StringType(), nullable=True),
    StructField('jurisdiction_names', StringType(), nullable=True),
    StructField('instant_bookable', StringType(), nullable=True),
    StructField('is_business_travel_ready', StringType(), nullable=True),
    StructField('cancellation_policy', StringType(), nullable=True),
    StructField('require_guest_profile_picture', StringType(), nullable=True),
    StructField('require_guest_phone_verification', StringType(), nullable=True),
    StructField('calculated_host_listings_count', StringType(), nullable=True),
    StructField('calculated_host_listings_count_entire_homes', StringType(), nullable=True),
    StructField('calculated_host_listings_count_private_rooms', StringType(), nullable=True),
    StructField('calculated_host_listings_count_shared_rooms', StringType(), nullable=True),
    StructField('reviews_per_month', StringType(), nullable=True),
])

def trim_all_string_columns(df : DataFrame) -> DataFrame:
    return (
        df.select(*[trim(col(c[0]).alias(c[0]) if c[1] == 'string' else col[0]) for c in df.dtypes])
        )


df_trimmed = (df.transform(trim_all_string_columns))
df_trimmed.show(1)

url = 'jdbc:mysql://localhost:3306/database1'
mode = 'overwrite'
table_name = 'airbnb4'

# (df_trimmed.write
#     .format('jdbc')
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option('url' , url)
#     .option('mode' , mode)
#     .option('dbtable' , table_name)
#     .option('user', 'austin')
#     .option('password', '1234')
#     .save())

    
    # airbnb_df = (
    #     df.select("*")
    #     .isNotNull())
    # airbnb_df.show(10)
