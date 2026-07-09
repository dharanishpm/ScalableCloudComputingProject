import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    max,
    min,
    count,
    countDistinct,
    round as spark_round,
    desc,
    col
)

# ---------------------------------------------------------
# START TIMER
# ---------------------------------------------------------

start_time = time.time()

# ---------------------------------------------------------
# CREATE SPARK SESSION
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FleetPulseBatch")
    .getOrCreate()
)

# ---------------------------------------------------------
# READ DATA FROM S3
# ---------------------------------------------------------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("s3://fleetvehicle-data/raw/")
)

print("\n====================================================")
print("             FLEETPULSE BATCH ANALYTICS")
print("====================================================")

# ---------------------------------------------------------
# SCHEMA
# ---------------------------------------------------------

print("\n========== DATASET SCHEMA ==========")
df.printSchema()

# ---------------------------------------------------------
# DATASET INFORMATION
# ---------------------------------------------------------

total_records = df.count()
total_vehicles = df.select("VehId").distinct().count()

print("\n========== DATASET INFORMATION ==========")
print(f"Total Records          : {total_records}")
print(f"Total Vehicles         : {total_vehicles}")
print(f"Spark Partitions       : {df.rdd.getNumPartitions()}")
print(f"Default Parallelism    : {spark.sparkContext.defaultParallelism}")

# ---------------------------------------------------------
# FLEET SUMMARY
# ---------------------------------------------------------

print("\n========== FLEET SUMMARY ==========")

fleet_summary = (
    df.agg(
        count("*").alias("TotalRecords"),
        countDistinct("VehId").alias("TotalVehicles"),
        spark_round(avg("Vehicle Speed[km/h]"), 2).alias("FleetAverageSpeed"),
        spark_round(avg("Engine RPM[RPM]"), 2).alias("FleetAverageRPM"),
        spark_round(avg("Absolute Load[%]"), 2).alias("FleetAverageEngineLoad"),
        max("Vehicle Speed[km/h]").alias("HighestSpeed"),
        min("Vehicle Speed[km/h]").alias("LowestSpeed"),
        max("Engine RPM[RPM]").alias("HighestRPM")
    )
)

fleet_summary.show(truncate=False)

# ---------------------------------------------------------
# PER VEHICLE ANALYTICS
# ---------------------------------------------------------

avg_speed = (
    df.groupBy("VehId")
    .agg(
        spark_round(
            avg("Vehicle Speed[km/h]"), 2
        ).alias("AverageSpeed")
    )
)

max_rpm = (
    df.groupBy("VehId")
    .agg(
        max("Engine RPM[RPM]").alias("MaximumRPM")
    )
)

avg_load = (
    df.groupBy("VehId")
    .agg(
        spark_round(
            avg("Absolute Load[%]"), 2
        ).alias("AverageEngineLoad")
    )
)

overspeed = (
    df.filter(col("Vehicle Speed[km/h]") > 120)
    .groupBy("VehId")
    .count()
    .withColumnRenamed(
        "count",
        "OverspeedEvents"
    )
)

# ---------------------------------------------------------
# CREATE SERVING DATASET
# ---------------------------------------------------------

serving = (
    avg_speed
    .join(max_rpm, "VehId")
    .join(avg_load, "VehId")
    .join(overspeed, "VehId", "left")
    .fillna({"OverspeedEvents": 0})
)

# ---------------------------------------------------------
# SAMPLE OUTPUT
# ---------------------------------------------------------

print("\n========== SERVING DATASET SAMPLE ==========")

serving.show(10, truncate=False)

# ---------------------------------------------------------
# TOP 5 FASTEST VEHICLES
# ---------------------------------------------------------

print("\n========== TOP 5 FASTEST VEHICLES ==========")

serving.orderBy(
    desc("AverageSpeed")
).show(5, truncate=False)

# ---------------------------------------------------------
# TOP 5 HIGHEST RPM
# ---------------------------------------------------------

print("\n========== TOP 5 HIGHEST RPM ==========")

serving.orderBy(
    desc("MaximumRPM")
).show(5, truncate=False)

# ---------------------------------------------------------
# TOP 5 ENGINE LOAD
# ---------------------------------------------------------

print("\n========== TOP 5 ENGINE LOAD ==========")

serving.orderBy(
    desc("AverageEngineLoad")
).show(5, truncate=False)

# ---------------------------------------------------------
# TOP 5 OVERSPEED VEHICLES
# ---------------------------------------------------------

print("\n========== TOP 5 OVERSPEED VEHICLES ==========")

serving.orderBy(
    desc("OverspeedEvents")
).show(5, truncate=False)

# ---------------------------------------------------------
# WRITE SERVING DATASET TO S3
# ---------------------------------------------------------

print("\nWriting Serving Dataset to S3...")

(
    serving.write
    .mode("overwrite")
    .parquet("s3://fleetvehicle-data/batch-results/serving/")
)

print("Serving Dataset Written Successfully")

print("Output Location:")
print("s3://fleetvehicle-data/batch-results/serving/")

# ---------------------------------------------------------
# EXECUTION TIME
# ---------------------------------------------------------

end_time = time.time()

execution_time = end_time - start_time

print("\n============================================")
print("BATCH JOB COMPLETED")
print("============================================")
print(f"Execution Time      : {execution_time:.2f} Seconds")
print(f"Records Processed   : {total_records}")
print(f"Vehicles Processed  : {total_vehicles}")
print("============================================")

spark.stop()