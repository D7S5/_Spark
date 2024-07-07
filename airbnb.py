import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if len(sys.argv)!=2:
    print("path: <>", file=sys.stderr)
    sys.exit(-1)

# col = "id,listing_url,scrape_id,last_scraped,name,summary,space,description,experiences_offered,neighborhood_overview,notes,transit,access,interaction,house_rules,thumbnail_url,medium_url,picture_url,xl_picture_url,host_id,host_url,host_name,host_since,host_location,host_about,host_response_time,host_response_rate,host_acceptance_rate,host_is_superhost,host_thumbnail_url,host_picture_url,host_neighbourhood,host_listings_count,host_total_listings_count,host_verifications,host_has_profile_pic,host_identity_verified,street,neighbourhood,neighbourhood_cleansed,neighbourhood_group_cleansed,city,state,zipcode,market,smart_location,country_code,country,latitude,longitude,is_location_exact,property_type,room_type,accommodates,bathrooms,bedrooms,beds,bed_type,amenities,square_feet,price,weekly_price,monthly_price,security_deposit,cleaning_fee,guests_included,extra_people,minimum_nights,maximum_nights,minimum_minimum_nights,maximum_minimum_nights,minimum_maximum_nights,maximum_maximum_nights,minimum_nights_avg_ntm,maximum_nights_avg_ntm,calendar_updated,has_availability,availability_30,availability_60,availability_90,availability_365,calendar_last_scraped,number_of_reviews,number_of_reviews_ltm,first_review,last_review,review_scores_rating,review_scores_accuracy,review_scores_cleanliness,review_scores_checkin,review_scores_communication,review_scores_location,review_scores_value,requires_license,license,jurisdiction_names,instant_bookable,is_business_travel_ready,cancellation_policy,require_guest_profile_picture,require_guest_phone_verification,calculated_host_listings_count,calculated_host_listings_count_entire_homes,calculated_host_listings_count_private_rooms,calculated_host_listings_count_shared_rooms,reviews_per_month"

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
    
    # airbnb_df = (spark.sparkContext.parallelize(df).toDF(col))
    df.printSchema()
    df.show(10)


url = 'jdbc:mysql://localhost:3306/database1'
mode = 'overwrite'
table_name = 'airbnb2'

(df.write
    .format('jdbc')
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option('url' , url)
    .option('mode' , mode)
    .option('dbtable' , table_name)
    .option('user', 'austin')
    .option('password', '1234')
    .save())

    
    # airbnb_df = (
    #     df.select("*")
    #     .isNotNull())
    # airbnb_df.show(10)
