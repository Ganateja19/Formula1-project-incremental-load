# Databricks notebook source
# DBTITLE 1,Build Constructors Dimension
# MAGIC %md
# MAGIC # Build Constructors Dimension
# MAGIC
# MAGIC 1. Read silver `constructors` table
# MAGIC 2. Read gold `ref_nationality_region` table
# MAGIC 3. Join the data from `constructors` with `ref_nationality_region` using `nationality`
# MAGIC 4. Select the required columns
# MAGIC    * `constructors.constructor_id`
# MAGIC    * `constructors.constructor_name`
# MAGIC    * `constructors.nationality`
# MAGIC    * `ref_nationality_region.region`
# MAGIC 5. Write the transformed data to gold `dim_constructors` table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1 - Read source tables

# COMMAND ----------

constructors_df = (
    spark.table(f"{catalog_name}.{silver_schema}.constructors")
    .filter(F.col("batch_id") == v_batch_id)
)

ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Join constructors with nationality_region_df using nationality

# COMMAND ----------

dim_constructors_df = (
    constructors_df
    .join(
        ref_nationality_region_df,
        constructors_df.nationality == ref_nationality_region_df.nationality,
        "left"
        )
    .select (
        constructors_df.constructor_id,
        constructors_df.constructor_name,
        constructors_df.nationality,
        ref_nationality_region_df.region.alias("nationality_region")
    )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 write the transformed data to gold dim_constructors table

# COMMAND ----------

# DBTITLE 1,Cell 13
write_to_gold(
    input_df=dim_constructors_df.dropDuplicates(["constructor_id"]),
    target_table=target_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "nationality_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))