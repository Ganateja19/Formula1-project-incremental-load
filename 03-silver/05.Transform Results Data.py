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

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# DBTITLE 1,Load environment config
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

# DBTITLE 1,Transform and write results
results_selected_df = (
    spark.table(bronze_table)
    .filter(F.col("batch_id") == v_batch_id)
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


# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the transformed data to silver results table

# COMMAND ----------

# DBTITLE 1,Cell 8
if spark.catalog.tableExists(silver_table):
    existing_cols = [f.name for f in spark.table(silver_table).schema.fields]
    if "batch_id" not in existing_cols:
        spark.sql(f"ALTER TABLE {silver_table} ADD COLUMN batch_id STRING")

results_with_batch_df = results_final_df.withColumn("batch_id", F.lit(v_batch_id))

write_to_silver(
    input_df=results_with_batch_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "finish_position_text",
        "status",
        "ingestion_date",
        "source_file",
        "batch_id"
    ]
)

spark.sql(f"""
UPDATE {silver_table}
SET batch_id = regexp_extract(source_file, '/files/([^/]+)/', 1)
WHERE batch_id IS NULL
  AND regexp_extract(source_file, '/files/([^/]+)/', 1) <> ''
""")

# COMMAND ----------

display(spark.table(silver_table))