# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Transform drivers Data
# MAGIC
# MAGIC 1. Read bronze drivers table
# MAGIC 2. Keep only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column names using snake case ( driverId -> driver_id, dateofbirth-> date_of_birth)
# MAGIC 4. Concatenate name. givenName and namefamilyName to create a new column called driver_name and transform the value to Title Case
# MAGIC 5. Remove Duplicate Records
# MAGIC 6. Transform values of columns nationality to Title Case
# MAGIC 7. Write the transformed data to the silver drivers table
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
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze drivers table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read bronze drivers table

# COMMAND ----------

# DBTITLE 1,Cell 7
drivers_df = spark.table(bronze_table).filter(f"batch_id = '{dbutils.widgets.get('p_batch_id')}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analytics (DROP url)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Standardise column namnes using snake case 

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
    .withColumnsRenamed({
        "driverId": "driver_id",
        "dateOfBirth": "date_of_birth",})
)

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Concatenate name. givenName and namefamilyName to create a new column called driver_name and transform the value to Title Case

# COMMAND ----------

# DBTITLE 1,Cell 17
drivers_concatenated_df = (
    drivers_renamed_df
    .withColumn("driver_name",
                F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
    .drop("name")
)


# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicate Records

# COMMAND ----------

drivers_distnict_df = drivers_concatenated_df.dropDuplicates(["driver_id"])
display(drivers_distnict_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform values of columns nationality to Title Case

# COMMAND ----------

drivers_final_df = (
    drivers_distnict_df
    .withColumn('nationality', F.initcap(F.col('nationality'))))


# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver drivers table 

# COMMAND ----------

# DBTITLE 1,Cell 25
write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "ingestion_date",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))