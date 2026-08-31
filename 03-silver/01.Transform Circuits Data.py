# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Transform circuits Data
# MAGIC
# MAGIC 1. Read bronze circuits table
# MAGIC 2. Keepo only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column namnes using snake case ( CircuitID-> circuit_id)
# MAGIC 4. Rename coulumns to make more meaningful
# MAGIC 5. Filter out rows where circuit_id is null 
# MAGIC 6. Remove Duplicate Records
# MAGIC 7. Transform values of columns circuit_name and locality to Title Case
# MAGIC 8. Write the transformed data to silver circuits table
# MAGIC
# MAGIC ### Incremental load changes required
# MAGIC
# MAGIC 1. Accept `batch_id` as a parameter to the notebook.
# MAGIC 2. Process data only for the passed `batch_id` (filter rows read from bronze using `batch_id`).
# MAGIC 3. Add `created_timestamp`, `updated_timestamp`, and `batch_id` to the silver table.
# MAGIC 4. Merge the processed data into the silver table.
# MAGIC    * `created_timestamp` should only be populated when the record is first inserted and should not be updated during merges.
# MAGIC    * Ensure older bronze data does not overwrite newer data in the silver table during re-run scenarios.

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze circuits table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - aread bronze circuits table

# COMMAND ----------

#circuits_df=spark.read.option('versionAsOf' , 0).table(bronze_table)


# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analytics (DROP url)

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_date"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Standardise column namnes using snake case ( CircuitID-> circuit_id)
# MAGIC ###4. Rename coulumns to make more meaningful

# COMMAND ----------

# circuits_renamed_df = (
#     circuits_selected_df
#     .withColumnRenamed("circuitId", "circuit_id")
#     .withColumnRenamed("circuitName", "circuit_name")
#     .withColumnRenamed("lat", "latitude")
#     .withColumnRenamed("long", "longitude")
# )

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
    .withColumnsRenamed({
        "circuitId": "circuit_id",
        "circuitName": "circuit_name",
        "lat": "latitude",
        "long": "longitude"
    })
)

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Filter out rows where circuit_id is null 

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Remove Duplicate Records

# COMMAND ----------

#circuits_distnict_df = circuits_valid_df.distinct()
#display(circuits_distnict_df)

# COMMAND ----------

circuits_distnict_df = circuits_valid_df.dropDuplicates(["circuit_id"])
display(circuits_distnict_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Transform values of columns circuit_name and locality to Title Case

# COMMAND ----------

circuits_final_df = (
    circuits_distnict_df
    .withColumn('circuit_name', F.initcap(F.col('circuit_name')))
    .withColumn( 'locality', F.initcap(F.col('locality')))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Write the transformed data to silver circuits table 

# COMMAND ----------

circuits_final_df = (
    circuits_final_df
    .withColumn("created_timestamp", F.current_timestamp())
    .withColumn("updated_timestamp", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Cell 28
write_to_silver (
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_id = s.circuit_id",
    columns_to_update= [
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_date",
        "source_file",
        "batch_id"
    ]

)

# COMMAND ----------

display(spark.table(silver_table))