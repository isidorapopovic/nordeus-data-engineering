import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path 
import numpy as np
from datetime import datetime, timezone
import json
from pyspark.sql.types import StringType,StructField,StructType, BooleanType, IntegerType, LongType,DayTimeIntervalType



with open("events.jsonl", "r", encoding="utf-8") as f:
    for line_number, line in enumerate(f, start=1):
        if not line.strip():
            continue

        raw = json.loads(line)


        if isinstance(raw, dict):
            print(f"type: {raw.get('type')}")
            for column, value in raw.items():
                print(f"column: {column}, type: {type(value).__name__}, example value: {value}")
                if isinstance(value, dict):
                    for subcolumn, subvalue in value.items():
                        print(f"  field: {column}.{subcolumn}, type: {type(subvalue).__name__}, example value: {subvalue}")
                elif isinstance(value, list):
                    for index, item in enumerate(value):
                        print(f"  field: {column}[{index}], type: {type(item).__name__}, example value: {item}")
            break


spark=SparkSession.builder.appName("nordeus").getOrCreate()

schema=StructType([StructField("id", IntegerType(), True),
                   StructField("timestamp", IntegerType(), True),
                   StructField("event_type", StringType(), True),
                   StructField("user_id", StringType(), True),
                   StructField("user", StructType([
                       StructField("country", StringType(), True),
                       StructField("device_os", StringType(), True),
                       StructField("username", StringType(), True),
                   ]), True),
                   StructField("invalid_record", StringType(), True)
        
                   ])

data_bronze = (spark.read.schema(schema).option("mode", "PERMISSIVE").json("events.jsonl"))

data_bronze.show(5)
data_bronze.printSchema()

print("Bronze rows:", data_bronze.count())



data_silver = (
    data_bronze

    # Flatten nested structure
    .select(
        F.col("id").alias("event_id"),
        F.col("timestamp"),
        F.col("event_type"),
        F.col("user_id"),
        F.col("user.country").alias("country"),
        F.col("user.device_os").alias("device_os"),
        F.col("user.username").alias("username")
    )

    # Remove rows missing important fields
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("user_id").isNotNull())
    .filter(F.col("event_type").isNotNull()))

print("Silver rows:", data_silver.count())

print("Event types:")
data_silver.groupBy("event_type").count().orderBy(F.desc("count")).show()

print("Countries:")
data_silver.groupBy("country").count().orderBy(F.desc("count")).show()

print("Operating systems:")
data_silver.groupBy("device_os").count().orderBy(F.desc("count")).show()