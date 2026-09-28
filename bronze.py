from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

EVENTS_INPUT = RAW_DIR / "events.jsonl"
MAPS_INPUT = RAW_DIR / "maps.jsonl"

EVENTS_OUTPUT = BRONZE_DIR / "events"
MAPS_OUTPUT = BRONZE_DIR / "maps"

spark = SparkSession.builder.appName("nordeus_bronze").getOrCreate()

event_schema = StructType([StructField("id", StringType(), True), StructField("timestamp", LongType(), True), StructField("event_type", StringType(), True), StructField("user_id", StringType(), True), StructField("event_data", StructType([StructField("country", StringType(), True), StructField("device_os", StringType(), True), StructField("username", StringType(), True), StructField("state", StringType(), True), StructField("map_id", StringType(), True), StructField("opponent_id", StringType(), True), StructField("outcome", DoubleType(), True)]), True), StructField("invalid_record", StringType(), True)])
map_schema = StructType([StructField("id", StringType(), True), StructField("name", StringType(), True), StructField("invalid_record", StringType(), True)])

events_bronze = spark.read.schema(event_schema).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "invalid_record").json(str(EVENTS_INPUT))
events_bronze = events_bronze.withColumn("event_timestamp", F.to_timestamp(F.from_unixtime(F.col("timestamp")))).withColumn("event_date", F.to_date(F.col("event_timestamp")))

maps_bronze = spark.read.schema(map_schema).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "invalid_record").json(str(MAPS_INPUT))

print("\nBRONZE EVENTS")
events_bronze.show(5, truncate=False)
events_bronze.printSchema()
print("Raw event rows:", events_bronze.count())

print("\nBRONZE MAPS")
maps_bronze.show(5, truncate=False)
maps_bronze.printSchema()
print("Map rows:", maps_bronze.count())

if EVENTS_OUTPUT.exists():
    existing_events = spark.read.parquet(str(EVENTS_OUTPUT)).select("id").dropDuplicates(["id"])
    new_events = events_bronze.join(existing_events, on="id", how="left_anti")
else:
    new_events = events_bronze

print("\nINCREMENTAL CHECK")
print("New events:", new_events.count())

if new_events.limit(1).count() > 0:
    new_events.write.mode("append").partitionBy("event_date").parquet(str(EVENTS_OUTPUT))

    print("New events appended successfully.")
else:
    print("No new events to append.")

maps_bronze.write.mode("overwrite").parquet(str(MAPS_OUTPUT))

print("\nBronze saved successfully.")
print("Events:", EVENTS_OUTPUT)
print("Maps:", MAPS_OUTPUT)

spark.stop()