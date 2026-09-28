import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, IntegerType, DatetimeType, LongType
from pyspark.sql.window import Window



with open ("events.jsonl", "r", encoding="utf-8") as f:
    for line_num, line in enumerate(f, start=1):
        if not line.strip():
            continue
        raw=json.loads(line)
        if isinstance(raw, dict):
            print(f"type: {raw.get('event_type')}")
            for column, value in raw.items():
                print(f"column: {column}, type: {type(value).__name__}, example value: {value}")
                if isinstance(value, dict):
                    for subcolumn, subvalue in value.items():
                        print(f"  field: {column}.{subcolumn}, type: {type(subvalue).__name__}, example value: {subvalue}")
            break

        
spark = SparkSession.builder.appName("nordeus").getOrCreate()

event_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("map_id", StringType(), True),

    StructField("event_data", StructType([
        StructField("country", StringType(), True),
        StructField("device_os", StringType(), True),
        StructField("username", StringType(), True)
    ]), True),

    StructField("invalid_record", StringType(), True)
])


data_bronze = (spark.read.schema(event_schema).option("mode", "PERMISSIVE").json("events.jsonl"))

data_bronze.show(5)
data_bronze.printSchema()
print("Bronze rows:", data_bronze.count())

map_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("invalid_record", StringType(), True)
])

map_bronze = (spark.read.schema(map_schema).option("mode", "PERMISSIVE").json("maps.jsonl"))

map_bronze.show(5)
map_bronze.printSchema()
print("Bronze rows:", map_bronze.count())


data_silver = (
    data_bronze
    .select(
        F.col("id").alias("event_id"),
        F.col("timestamp"),
        F.col("event_type"),
        F.col("user_id"),
        F.col("map_id"),
        F.col("event_data.country").alias("country"),
        F.col("event_data.device_os").alias("device_os"),
        F.col("event_data.username").alias("username"),
        F.col("invalid_record")
    )

    .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("device_os", F.lower(F.trim(F.col("device_os"))))
    .withColumn("username", F.trim(F.col("username")))

    .withColumn("event_timestamp", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))

    .withColumn(
        "validation_error",
        F.when(F.col("invalid_record").isNotNull(), "malformed JSON")
        .when(F.col("event_id").isNull(), "missing event_id")
        .when(F.col("user_id").isNull(), "missing user_id")
        .when(F.col("event_type").isNull() | (F.col("event_type") == ""), "invalid event_type")
        .when(F.col("timestamp").isNull() | (F.col("timestamp") <= 0), "invalid timestamp")
        .otherwise("valid")
    )

    .withColumn("is_valid", F.col("validation_error") == "valid")

    .withColumn("processed_at", F.current_timestamp())
)

maps_silver = (
    map_bronze
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
        .when(F.col("map_name").isNull() | (F.col("map_name") == ""), "invalid map_name")
        .otherwise("valid")
    )

    .withColumn("is_valid", F.col("validation_error") == "valid")
    .withColumn("processed_at", F.current_timestamp())
)

valid_events = data_silver.filter(F.col("is_valid") == True)
invalid_events = data_silver.filter(F.col("is_valid") == False)

valid_maps = maps_silver.filter(F.col("is_valid") == True)
invalid_maps = maps_silver.filter(F.col("is_valid") == False)

print("\nEVENT VALIDATION")
print("Valid events:", valid_events.count())
print("Invalid events:", invalid_events.count())

print("\nMAP VALIDATION")
print("Valid maps:", valid_maps.count())
print("Invalid maps:", invalid_maps.count())

events_joined = valid_events.join(F.broadcast(valid_maps.select("map_id", "map_name")), on="map_id", how="left")

print("\nUNKNOWN MAPS")
events_joined.filter(F.col("map_id").isNotNull() & F.col("map_name").isNull()).select("map_id").distinct().show(truncate=False)

print("\n========== REACHED DISTINCT VALUES ==========")

print("\nMAPS")
valid_maps.select("map_name").distinct().show(100, truncate=False)

print("\nUSERNAMES")
data_silver.select("username").where(F.col("username").isNotNull()).distinct().show(1000, truncate=False)

print("\nCOUNTRIES")
data_silver.select("country").where(F.col("country").isNotNull()).distinct().show(100, truncate=False)

print("\nOPERATING SYSTEMS")
data_silver.select("device_os").where(F.col("device_os").isNotNull()).distinct().show(100, truncate=False)







# --------------------------------------------------
# GOLD - PLAYER STATS
# temporary tables only - previous code stays unchanged
# --------------------------------------------------

from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

gold_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_data", StructType([
        StructField("country", StringType(), True),
        StructField("device_os", StringType(), True),
        StructField("username", StringType(), True),
        StructField("state", StringType(), True),
        StructField("map_id", StringType(), True),
        StructField("opponent_id", StringType(), True),
        StructField("outcome", DoubleType(), True)
    ]), True)
])

gold_events_raw = spark.read.schema(gold_schema).option("mode", "PERMISSIVE").json("events.jsonl")

gold_events = (
    gold_events_raw
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
        F.col("event_data.outcome").alias("outcome")
    )
    .withColumn("event_timestamp", F.to_timestamp(F.from_unixtime("timestamp")))
)

# --------------------------------------------------
# TEMP REGISTRATION TABLE
# --------------------------------------------------

tmp_registrations = (
    gold_events
    .filter(F.col("event_type") == "registration")
    .groupBy("user_id")
    .agg(
        F.first("username", ignorenulls=True).alias("username"),
        F.first("country", ignorenulls=True).alias("country"),
        F.min("event_timestamp").alias("registration_timestamp")
    )
)

# --------------------------------------------------
# TEMP SESSION NUMBER
# each session_ping with state=started begins a new session
# --------------------------------------------------

session_window = (
    Window
    .partitionBy("user_id")
    .orderBy("timestamp", F.col("event_id").cast("long"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

tmp_events_sessions = (
    gold_events
    .withColumn(
        "new_session",
        F.when(
            (F.col("event_type") == "session_ping") &
            (F.col("session_state") == "started"),
            1
        ).otherwise(0)
    )
    .withColumn("session_number", F.sum("new_session").over(session_window))
)

# --------------------------------------------------
# TEMP MATCH STARTS
# --------------------------------------------------

start_window = Window.partitionBy("user_id", "map_id", "opponent_id").orderBy("start_timestamp")

tmp_match_starts = (
    tmp_events_sessions
    .filter(F.col("event_type") == "match_start")
    .select(
        "user_id",
        "map_id",
        "opponent_id",
        "session_number",
        F.col("timestamp").alias("start_timestamp")
    )
    .withColumn("match_number", F.row_number().over(start_window))
)

# --------------------------------------------------
# TEMP MATCH FINISHES
# --------------------------------------------------

finish_window = Window.partitionBy("user_id", "map_id", "opponent_id").orderBy("finish_timestamp")

tmp_match_finishes = (
    tmp_events_sessions
    .filter(F.col("event_type") == "match_finish")
    .select(
        "user_id",
        "map_id",
        "opponent_id",
        "outcome",
        F.col("timestamp").alias("finish_timestamp")
    )
    .withColumn("match_number", F.row_number().over(finish_window))
)

# --------------------------------------------------
# TEMP COMPLETE MATCH TABLE
# duration = finish - start
# outcome 1 = win, 0 = loss
# --------------------------------------------------

tmp_matches = (
    tmp_match_starts
    .join(
        tmp_match_finishes,
        on=["user_id", "map_id", "opponent_id", "match_number"],
        how="inner"
    )
    .withColumn("duration", F.col("finish_timestamp") - F.col("start_timestamp"))
    .withColumn("is_win", F.when(F.col("outcome") == 1.0, 1).otherwise(0))
    .filter(F.col("duration") >= 0)
)

# --------------------------------------------------
# TEMP USER + MAP STATS
# --------------------------------------------------

tmp_user_map_stats = (
    tmp_matches
    .groupBy("user_id", "map_id")
    .agg(
        F.count("*").alias("map_matches"),
        F.sum("is_win").alias("map_wins")
    )
    .withColumn("map_win_ratio", F.col("map_wins") / F.col("map_matches"))
    .join(
        F.broadcast(valid_maps.select("map_id", "map_name")),
        on="map_id",
        how="left"
    )
)

# --------------------------------------------------
# TEMP FAVORITE MAP
# highest win ratio
# more matches used as tie breaker
# --------------------------------------------------

fav_window = (
    Window
    .partitionBy("user_id")
    .orderBy(
        F.desc("map_win_ratio"),
        F.desc("map_matches"),
        F.asc("map_name")
    )
)

tmp_favorite_maps = (
    tmp_user_map_stats
    .withColumn("rank", F.row_number().over(fav_window))
    .filter(F.col("rank") == 1)
    .select(
        "user_id",
        F.col("map_name").alias("fav_map"),
        F.col("map_win_ratio").alias("fav_map_win_ratio")
    )
)

# --------------------------------------------------
# TEMP TOTAL PLAYER STATS
# --------------------------------------------------

tmp_user_totals = (
    tmp_matches
    .groupBy("user_id")
    .agg(
        F.sum("duration").alias("total_playtime"),
        F.sum("is_win").alias("total_wins"),
        F.count("*").alias("total_matches"),
        F.countDistinct("session_number").alias("total_sessions")
    )
    .withColumn("total_win_ratio", F.col("total_wins") / F.col("total_matches"))
    .withColumn("avg_matches_per_session", F.col("total_matches") / F.col("total_sessions"))
)

# --------------------------------------------------
# FINAL GOLD PLAYER TABLE
# --------------------------------------------------

gold_player_stats = (
    tmp_registrations
    .join(tmp_user_totals, on="user_id", how="left")
    .join(tmp_favorite_maps, on="user_id", how="left")
    .select(
        "user_id",
        "username",
        "country",
        "fav_map",
        F.round("fav_map_win_ratio", 3).alias("fav_map_win_ratio"),
        "total_playtime",
        F.round("total_win_ratio", 3).alias("total_win_ratio"),
        F.round("avg_matches_per_session", 2).alias("avg_matches_per_session"),
        F.to_date("registration_timestamp").alias("registration_date")
    )
    .orderBy(F.desc("total_playtime"))
)

print("\nGOLD PLAYER STATS")
gold_player_stats.show(100, truncate=False)

print("\nTOTAL GOLD PLAYERS:", gold_player_stats.count())

