# Databricks notebook source
# DBTITLE 1,Build Results Fact
# MAGIC %md
# MAGIC # Build Results Fact
# MAGIC
# MAGIC 1. Read silver `results` table
# MAGIC 2. Read silver `sprints` table
# MAGIC 3. Add new column `session_type` with values `RACE` or `SPRINT`
# MAGIC 4. UNION `results` and `sprints`
# MAGIC 5. Derive additional columns
# MAGIC    * `is_win` -> Indicates that the driver won the race
# MAGIC    * `is_podium` -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC    * `has_points` -> Indicates that the driver has scored points
# MAGIC 6. Write the transformed data to gold `fact_session_results` table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read 1 - read source tables

# COMMAND ----------

# DBTITLE 1,Cell 8
results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .filter(F.col("source_file").contains(v_batch_id))
    .withColumn("session_type", F.lit("RACE"))
    .drop("race_name", "race_date","ingestion_date","source_file","batch_id","created_timestamp","updated_timestamp")

)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .filter(F.col("batch_id") == v_batch_id)
    .withColumn("session_type", F.lit("SPRINT"))
    .drop("race_name", "race_date","ingestion_date","source_file","batch_id","created_timestamp","updated_timestamp")

)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - UNION results and sprints

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

display(results_sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step3 Add derived columns
# MAGIC
# MAGIC * `is_win` -> Indicates that the driver won the race
# MAGIC    * `is_podium` -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC    * `has_points` -> Indicates that the driver has scored points

# COMMAND ----------

# DBTITLE 1,Cell 12
fact_session_results_df = (
    results_sprints_df
    .withColumn("is_win", F.col("final_position") == 1)
    .withColumn("is_podium", F.col("final_position").between(1,3))
    .withColumn("has_points", F.col("points") > 0)
)

display(fact_session_results_df)

# COMMAND ----------

# DBTITLE 1,Cell 15
write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_condition=(
        "t.season = s.season "
        "AND t.round = s.round "
        "AND t.constructor_id = s.constructor_id "
        "AND t.driver_id = s.driver_id "
        "AND t.session_type = s.session_type"
    ),
    columns_to_update=[
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "finish_position_text",
        "status",
        "is_win",
        "is_podium",
        "has_points"
    ]
)

# COMMAND ----------

display(
    spark
    .table('target_table'))