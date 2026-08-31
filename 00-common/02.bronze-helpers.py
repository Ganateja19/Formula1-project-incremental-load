# Databricks notebook source
# helper function to add metadata for ingestion (source file name and ingestion date)

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return  (
        df.withColumn('ingestion_date', F.current_timestamp())
            .withColumn('source_file', F.col('_metadata.file_path'))
    )

# COMMAND ----------

def write_to_bronze (
    input_df,
    target_table,
    batch_id
):
    final_df = input_df.withColumn("batch_id", F.lit(batch_id))
    try:
        existing_df = spark.table(target_table)
        partition_columns = (
            spark.sql(f"DESCRIBE DETAIL {target_table}")
                 .select("partitionColumns")
                 .collect()[0][0]
        )
        needs_migration = (
            existing_df.schema != final_df.schema
            or partition_columns != ["batch_id"]
        )
    except Exception:
        needs_migration = True

    writer = (
        final_df
        .write
        .mode('overwrite')
        .format('delta')
        .partitionBy('batch_id')
    )
    if needs_migration:
        writer.option('overwriteSchema', 'true').saveAsTable(target_table)
    else:
        writer.option('replaceWhere', f"batch_id = '{batch_id}'").saveAsTable(target_table)