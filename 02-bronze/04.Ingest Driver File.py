# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Drivers.json file
# MAGIC
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read the CSV file  using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
# Define the schema


name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', StringType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

## Read data from constructor file

drivers_df = (
    spark.read
    .format('json')
    .schema(drivers_schema)
    .option('mode', 'FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP2 - Add Metadata Columns

# COMMAND ----------

# DBTITLE 1,Cell 11
drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Write to bronze delta table

# COMMAND ----------

write_to_bronze (
    input_df = drivers_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))