# Databricks notebook source
# DBTITLE 1,Build Drivers Dimension
# MAGIC %md
# MAGIC # Build Drivers Dimension
# MAGIC
# MAGIC 1. Read silver `drivers` table
# MAGIC 2. Read gold `ref_nationality_region` table
# MAGIC 3. Join the data from `drivers` with `ref_nationality_region` using `nationality`
# MAGIC 4. Select the required columns
# MAGIC    * `drivers.driver_id`
# MAGIC    * `drivers.driver_name`
# MAGIC    * `drivers.date_of_birth`
# MAGIC    * `drivers.nationality`
# MAGIC    * `ref_nationality_region.region`
# MAGIC 5. Write the transformed data to gold `dim_drivers` table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md
# MAGIC #### Read 1 - read source tables

# COMMAND ----------

drivers_df = (
        spark.table(f"{catalog_name}.{silver_schema}.drivers")
        .filter(F.col("batch_id") == v_batch_id)
)
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")
display(drivers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Join drivers with nationality _region_df using nationality
# MAGIC
# MAGIC select the following columns
# MAGIC 1. drivers.drivers_id
# MAGIC 2. drivers.driver_name
# MAGIC 3. drivers.date_of_birth
# MAGIC 4. drivers.nationality
# MAGIC 5. ref_nationality_region.region

# COMMAND ----------

dim_drivers_df = (
    drivers_df
    .join(
    ref_nationality_region_df,
    drivers_df.nationality == ref_nationality_region_df.nationality,
    "left"
)
    .select(
    drivers_df["driver_id"],
    drivers_df["driver_name"],
    drivers_df["date_of_birth"],
    drivers_df["nationality"],
    ref_nationality_region_df.region.alias("nationality_region"),
    )

)



# COMMAND ----------

display(dim_drivers_df)

# COMMAND ----------

# DBTITLE 1,Cell 11

write_to_gold(
    input_df=dim_drivers_df.dropDuplicates(["driver_id"]),
    target_table=target_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "nationality_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))