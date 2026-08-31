# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Transform constructors Data
# MAGIC
# MAGIC 1. Read bronze constructors table
# MAGIC 2. Keepo only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column names using snake case ( constructorID-> constructor_id)
# MAGIC 4. Rename columns to make them more meaningful (name -> constructor_name)
# MAGIC 5. Remove Duplicate Records
# MAGIC 6. Transform values of columns nationality to Title Case
# MAGIC 7. Write the transformed data to the silver constructors table
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

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze constructors table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read bronze constructors table

# COMMAND ----------

# DBTITLE 1,Cell 7
constructors_df = spark.table(bronze_table).filter(f"batch_id = '{v_batch_id}'")

# COMMAND ----------

display(constructors_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analytics (DROP url)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Cell 11
construtors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Standardise column namnes using snake case 
# MAGIC ###4. Rename coulumns to make more meaningful

# COMMAND ----------

# constructors_renamed_df = (
#     constructors_selected_df
#     .withColumnRenamed("circuitId", "circuit_id")
#     .withColumnRenamed("circuitName", "circuit_name")
#     .withColumnRenamed("lat", "latitude")
#     .withColumnRenamed("long", "longitude")
# )

# COMMAND ----------

# DBTITLE 1,Cell 14
constructors_renamed_df = (
    construtors_dropped_df
    .withColumnsRenamed({
        "constructorId": "constructor_id",
        "name": "constructor_name"
    })
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicate Records

# COMMAND ----------

constructors_distnict_df = constructors_renamed_df.dropDuplicates(["constructor_id"])
display(constructors_distnict_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform values of columns race_name to Title Case

# COMMAND ----------

constructors_final_df = (
    constructors_distnict_df
    .withColumn('nationality', F.initcap(F.col('nationality'))))


# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver constructors table 

# COMMAND ----------

# DBTITLE 1,Cell 22
write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "ingestion_date",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))