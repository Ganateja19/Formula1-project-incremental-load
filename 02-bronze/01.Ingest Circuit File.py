# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits. csv file
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

v_batch_id

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1 - Read the CSV file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema= StructType([
    StructField('circuitId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('circuitName', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('long', DoubleType(), True),
    StructField('locality', StringType(), True),
    StructField('country', StringType(), True)
])



# COMMAND ----------

circuits_df= (
    spark.read
    .format('csv')
    .option('header', True)
#  .option('inferSchema', True)
    .schema(circuits_schema)
    .load(source_file)
)


# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP2 - Add Metadata Columns

# COMMAND ----------

circuits_final_df= add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Write to bronze delta table

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 10
#circuits_final_df = circuits_final_df.withColumn("batch_id", F.lit(v_batch_id))


# COMMAND ----------

#(
 #   circuits_final_df
 #   .write
  ## .format('delta')
  #  .partitionBy('batch_id')
   # .option('replaceWhere', f"batch_id = '{v_batch_id}'")
    #.saveAsTable(table_name)
#)

# COMMAND ----------

write_to_bronze (
    input_df = circuits_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))