# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Sprints.json file
# MAGIC
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC * Source File
# MAGIC * Ingestion Timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read the CSV file  using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

sprints_schema = StructType([
    StructField("date", DateType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])

# COMMAND ----------

## Read data from constructor file

sprints_df = (
    spark.read
    .format('json')
    .schema(sprints_schema)
    .option('mode', 'FAILFAST')
    .option('multiline', True)
    .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP2 - Add Metadata Columns

# COMMAND ----------

sprints_final_df= add_ingestion_metadata(sprints_df)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Write to bronze delta table

# COMMAND ----------

# DBTITLE 1,Cell 10
write_to_bronze (
    input_df = sprints_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

spark.table('formula1.bronze.sprints')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT season, Count(*)
# MAGIC FROM formula1.bronze.sprints
# MAGIC GROUP BY season
# MAGIC ORDER BY season