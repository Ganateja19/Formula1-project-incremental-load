# Databricks notebook source
# DBTITLE 1,Transform results data
# MAGIC %md
# MAGIC # Transform results data
# MAGIC
# MAGIC This notebook has been consolidated into 3 cells:
# MAGIC
# MAGIC 1. Load shared environment configuration
# MAGIC 2. Read, transform, validate, and deduplicate the bronze results data
# MAGIC 3. Write and validate the silver results table
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load environment config
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# DBTITLE 1,Transform and write results
from pyspark.sql import functions as F

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

results_selected_df = (
    spark.table(bronze_table)
    .select(
        "date",
        "raceName",
        "round",
        "season",
        "constructorId",
        "driverId",
        "grid",
        "laps",
        "number",
        "points",
        "position",
        "positionText",
        "status",
        "ingestion_date",
        "source_file"
    )
)

results_renamed_df = (
    results_selected_df
    .withColumnsRenamed({
        "driverId": "driver_id",
        "constructorId": "constructor_id",
        "raceName": "race_name",
        "positionText": "finish_position_text",
        "date": "race_date",
        "grid": "grid_position",
        "laps": "completed_laps",
        "number": "car_number",
        "position": "final_position"
    })
)

results_valid_df = (
    results_renamed_df
    .filter(
        F.col("season").isNotNull()
        & F.col("round").isNotNull()
        & F.col("constructor_id").isNotNull()
        & F.col("driver_id").isNotNull()
    )
)

results_distinct_df = results_valid_df.dropDuplicates([
    "season", "round", "constructor_id", "driver_id"
])

results_final_df = (
    results_distinct_df
    .withColumn("race_name", F.initcap(F.col("race_name")))
)

filtered_out_count = results_renamed_df.count() - results_valid_df.count()
duplicate_count = results_valid_df.count() - results_distinct_df.count()

print(f"Rows filtered out by null checks: {filtered_out_count}")
print(f"Duplicate rows removed: {duplicate_count}")

(
    results_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

display(spark.table(silver_table))