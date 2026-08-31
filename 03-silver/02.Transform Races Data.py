# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Transform races Data
# MAGIC
# MAGIC 1. Read bronze races table
# MAGIC 2. Keepo only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column names using snake case ( raceName-> race_name, CircuitID-> circuit_id)
# MAGIC 4. Rename coulumns to make more meaningful
# MAGIC 5. Remove Duplicate Records
# MAGIC 6. Transform values of columns circuit_name and locality to Title Case
# MAGIC 7. Write the transformed data to the silver races table 
# MAGIC
# MAGIC ### Incremental load changes required
# MAGIC
# MAGIC 1. Accept `batch_id` as a parameter to the notebook.
# MAGIC 2. Process data only for the passed `batch_id` (filter rows read from bronze using `batch_id`).
# MAGIC 3. Add `created_timestamp`, `updated_timestamp`, and `batch_id` to the silver table.
# MAGIC 4. Merge the processed data into the silver table.
# MAGIC    * `created_timestamp` should only be populated when the record is first inserted and should not be updated during merges.
# MAGIC    * Ensure older bronze data does not overwrite newer data in the silver table during re-run scenarios.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read batch parameter
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze races table

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - aread bronze races table

# COMMAND ----------

#races_df=spark.read.option('versionAsOf' , 0).table(bronze_table)


# COMMAND ----------

# DBTITLE 1,Cell 7
races_df = spark.table(bronze_table).filter(f"batch_id = '{v_batch_id}'")

# COMMAND ----------

display(races_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analytics (DROP url)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Cell 11
races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_date"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Standardise column namnes using snake case ( CircuitID-> circuit_id)
# MAGIC ###4. Rename coulumns to make more meaningful

# COMMAND ----------

# races_renamed_df = (
#     races_selected_df
#     .withColumnRenamed("circuitId", "circuit_id")
#     .withColumnRenamed("circuitName", "circuit_name")
#     .withColumnRenamed("lat", "latitude")
#     .withColumnRenamed("long", "longitude")
# )

# COMMAND ----------

races_renamed_df = (
    races_selected_df
    .withColumnsRenamed({
        "circuitId": "circuit_id",
        "raceName": "race_name",
        "date": "race_date",

    })
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicate Records

# COMMAND ----------

#races_distnict_df = races_valid_df.distinct()
#display(races_distnict_df)

# COMMAND ----------

races_distnict_df = races_renamed_df.dropDuplicates(["season", "round"])
display(races_distnict_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform values of columns race_name to Title Case

# COMMAND ----------

races_final_df = (
    races_distnict_df
    .withColumn('race_name', F.initcap(F.col('race_name')))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver races table 

# COMMAND ----------

# DBTITLE 1,Add audit timestamps
races_final_df = (
    races_final_df
    .withColumn("created_timestamp", F.current_timestamp())
    .withColumn("updated_timestamp", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Cell 23
write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "season",
        "circuit_id",
        "race_name",
        "ingestion_date",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))