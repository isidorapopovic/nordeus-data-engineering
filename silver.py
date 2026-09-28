from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine"

EVENTS_SILVER_OUTPUT = SILVER_DIR / "events"
MAPS_SILVER_OUTPUT = SILVER_DIR / "maps"
EVENTS_ENRICHED_OUTPUT = SILVER_DIR / "events_enriched"
EVENTS_QUARANTINE_OUTPUT = QUARANTINE_DIR / "events"
MAPS_QUARANTINE_OUTPUT = QUARANTINE_DIR / "maps"

spark = SparkSession.builder.appName("nordeus_silver").getOrCreate()

events_bronze = spark.read.parquet(str(BRONZE_DIR / "events"))
maps_bronze = spark.read.parquet(str(BRONZE_DIR / "maps"))

if EVENTS_SILVER_OUTPUT.exists():
    existing_event_ids = spark.read.parquet(str(EVENTS_SILVER_OUTPUT)).select("event_id").dropDuplicates(["event_id"])
    new_bronze_events = events_bronze.join(existing_event_ids, events_bronze["id"] == existing_event_ids["event_id"], "left_anti")
else:
    new_bronze_events = events_bronze

print("\nINCREMENTAL SILVER CHECK")
print("New Bronze events to process:", new_bronze_events.count())

data_silver_new = (
    new_bronze_events
    .select(
        F.col("id").alias("event_id"),
        F.col("timestamp"),
        F.col("event_type"),
        F.col("user_id"),
        F.col("event_data.country").alias("country"),
        F.col("event_data.device_os").alias("device_os"),
        F.col("event_data.username").alias("username"),
        F.col("event_data.state").alias("session_state"),
        F.col("event_data.map_id").alias("map_id"),
        F.col("event_data.opponent_id").alias("opponent_id"),
        F.col("event_data.outcome").alias("outcome"),
        F.col("invalid_record")
        
    )

    .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("device_os", F.lower(F.trim(F.col("device_os"))))
    .withColumn("username", F.trim(F.col("username")))
    .withColumn("session_state", F.lower(F.trim(F.col("session_state"))))

    .withColumn("event_timestamp",F.to_timestamp(F.from_unixtime(F.col("timestamp"))))

    .withColumn("event_date", F.to_date(F.col("event_timestamp")))

    .withColumn(
        "validation_error",
        F.when(F.col("invalid_record").isNotNull(), "malformed JSON")
        .when(F.col("event_id").isNull(), "missing event_id")
        .when(F.col("user_id").isNull(), "missing user_id")
        .when(
            F.col("event_type").isNull() |
            (F.col("event_type") == ""),
            "invalid event_type"
        )
        .when(
            F.col("timestamp").isNull() |
            (F.col("timestamp") <= 0),
            "invalid timestamp"
        )
        .otherwise("valid")
    )

    .withColumn(
        "is_valid",
        F.col("validation_error") == "valid"
    )

    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)

valid_events_new = data_silver_new.filter(F.col("is_valid") == True).dropDuplicates(["event_id"])
invalid_events_new = data_silver_new.filter(F.col("is_valid") == False)

print("\nNEW EVENT VALIDATION")
print("New valid events:", valid_events_new.count())
print("New invalid events:", invalid_events_new.count())

maps_silver = (
    maps_bronze
    .select(
        F.col("id").alias("map_id"),
        F.col("name").alias("map_name"),
        F.col("invalid_record")
    )
    .withColumn("map_name", F.trim(F.col("map_name")))
    .withColumn(
        "validation_error",
        F.when(F.col("invalid_record").isNotNull(), "malformed JSON")
        .when(F.col("map_id").isNull(), "missing map_id")
        .when(
            F.col("map_name").isNull() |
            (F.col("map_name") == ""),
            "invalid map_name"
        )
        .otherwise("valid")
    )
    .withColumn("is_valid", F.col("validation_error") == "valid")
    .withColumn("processed_at", F.current_timestamp())
)

valid_maps = maps_silver.filter(F.col("is_valid") == True).dropDuplicates(["map_id"])
invalid_maps = maps_silver.filter(F.col("is_valid") == False)

print("\nMAP VALIDATION")
print("Valid maps:", valid_maps.count())
print("Invalid maps:", invalid_maps.count())

if valid_events_new.limit(1).count() > 0:
    (
        valid_events_new.write
        .mode("append")
        .partitionBy("event_date")
        .parquet(str(EVENTS_SILVER_OUTPUT))
    )

    print("New Silver events appended.")
else:
    print("No new valid Silver events.")

if invalid_events_new.limit(1).count() > 0:
    (
        invalid_events_new.write
        .mode("append")
        .partitionBy("event_date")
        .parquet(str(EVENTS_QUARANTINE_OUTPUT))
    )

    print("New quarantine events appended.")
else:
    print("No new quarantine events.")

valid_maps.write.mode("overwrite").parquet(str(MAPS_SILVER_OUTPUT))
invalid_maps.write.mode("overwrite").parquet(str(MAPS_QUARANTINE_OUTPUT))

all_valid_events = spark.read.parquet(str(EVENTS_SILVER_OUTPUT))

events_enriched = (
    all_valid_events
    .join(
        F.broadcast(
            valid_maps.select("map_id", "map_name")
        ),
        on="map_id",
        how="left"
    )
)

events_enriched.write.mode("overwrite").parquet(str(EVENTS_ENRICHED_OUTPUT))

print("\nUNKNOWN MAPS")
events_enriched.filter(
    F.col("map_id").isNotNull() &
    F.col("map_name").isNull()
).select("map_id").distinct().show(truncate=False)

print("\nSilver incremental processing completed.")
print("Silver events:", EVENTS_SILVER_OUTPUT)
print("Silver maps:", MAPS_SILVER_OUTPUT)
print("Silver enriched:", EVENTS_ENRICHED_OUTPUT)
print("Quarantine events:", EVENTS_QUARANTINE_OUTPUT)

spark.stop()