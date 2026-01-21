from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

import json

# Load configuration from JSON file
with open("C:/Users/Jabir/Downloads/Miscellanious/Spark/Spark/configs/config.json") as f:
    config = json.load(f)

db_conf = config["sqlserver"]


#----------------------------------------
# Spark Constants
#-----------------------------------------
URL = db_conf["url"]
USER = db_conf["user"]
PASSWORD = db_conf["password"]      
dbtable_source = "dbo.BusTrips"
dbtable_target = "dbo.BusTripsTransformed"  
DRIVER = db_conf["driver"]

# -----------------------------------------
# Spark Session
# -----------------------------------------
print("Starting Spark Session...")
spark = SparkSession.builder \
    .appName("BusTripsETL") \
    .config("spark.jars", r"C:\Users\Jabir\Downloads\Miscellanious\Spark\Spark\drivers\sqljdbc_13.2\enu\jars\mssql-jdbc-13.2.1.jre8.jar")\
    .getOrCreate()

# -----------------------------------------
# Read from SQL Server
# -----------------------------------------
print("Reading data from SQL Server...")
source_df = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("dbtable", dbtable_source) \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .option("driver", DRIVER) \
    .load()

print("SOURCE DATA:")
source_df.show()

# -----------------------------------------
# Transformations
# -----------------------------------------
df_transformed = source_df \
    .withColumn("speed_kmph", round(col("distance_km") / (col("duration_min") / 60), 2)) \
    .withColumn(
        "trip_category",
        when(col("distance_km") < 10, "Short")
        .when(col("distance_km") < 15, "Medium")
        .otherwise("Long")
    ) \
    .withColumn("fare", round(col("distance_km") * 2.5, 2))

print("TRANSFORMED DATA:")
df_transformed.show()

# -----------------------------------------
# Write to SQL Server Target Table
# -----------------------------------------
# df_transformed.write.format("jdbc") \
#     .option("url", URL) \
#     .option("dbtable", dbtable_target) \
#     .option("user", USER) \
#     .option("password", PASSWORD) \
#     .option("driver", DRIVER) \
#     .mode("append") \
#     .save()

print("Data successfully loaded into BusTripsTransformed table.")
spark.stop()