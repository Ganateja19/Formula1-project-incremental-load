# Databricks notebook source
# DBTITLE 1,Build Races Dimension
# MAGIC %md
# MAGIC # Build Races Dimension
# MAGIC
# MAGIC 1. Read silver `races` table
# MAGIC 2. Read silver `circuits` table
# MAGIC 3. Join the data from `races` with `circuits` using `circuit_id`
# MAGIC 4. Select the required columns
# MAGIC    * `races.season`
# MAGIC    * `races.round`
# MAGIC    * `races.race_name`
# MAGIC    * `races.race_date`
# MAGIC    * `circuits.circuit_name`
# MAGIC    * `circuits.locality`
# MAGIC    * `circuits.country`
# MAGIC 5. Write the transformed data to gold `dim_races` table
# MAGIC
# MAGIC ## Incremental Load Changes Required
# MAGIC
# MAGIC 1. Accept `batch_id` as a parameter to the notebook.
# MAGIC 2. Process data only for the `batch_id` passed in, i.e. filter data read from silver using `batch_id`.
# MAGIC 3. Add `created_timestamp`, `updated_timestamp`, and `batch_id` columns to the gold table.
# MAGIC 4. Merge the processed data into the gold table.
# MAGIC    * `created_timestamp` should only be populated when inserting/creating the record.
# MAGIC    * `updated_timestamp` should not be updated during the merge.

# COMMAND ----------

# DBTITLE 1,Entity Relationship Flow
# MAGIC %md
# MAGIC ## Entity Relationship and Flow
# MAGIC
# MAGIC ### Entities
# MAGIC * `silver.races`
# MAGIC * `silver.circuits`
# MAGIC * `gold.dim_races`
# MAGIC
# MAGIC ### Relationship
# MAGIC * `silver.races.circuit_id` → `silver.circuits.circuit_id`
# MAGIC
# MAGIC ### Flow Chart
# MAGIC
# MAGIC ```text
# MAGIC silver.races                          silver.circuits
# MAGIC -------------                         ----------------
# MAGIC season                                circuit_id
# MAGIC round                                 circuit_name
# MAGIC race_name                             locality
# MAGIC race_date                             country
# MAGIC circuit_id          ─────────────▶    circuit_id
# MAGIC          \                             /
# MAGIC           \                           /
# MAGIC            \                         /
# MAGIC             └─────── Join on `circuit_id` ───────┘
# MAGIC                             │
# MAGIC                             ▼
# MAGIC                     Select required columns
# MAGIC                             │
# MAGIC                             ▼
# MAGIC                       gold.dim_races
# MAGIC                       --------------
# MAGIC                       season
# MAGIC                       round
# MAGIC                       race_name
# MAGIC                       race_date
# MAGIC                       circuit_name
# MAGIC                       locality
# MAGIC                       country
# MAGIC ```
# MAGIC
# MAGIC ### Output
# MAGIC * The gold `dim_races` table stores the transformed race dimension data for downstream analytics.

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1 - Read source tables

# COMMAND ----------

circuits_df = (
    spark.table(f"{catalog_name}.{silver_schema}.circuits")
    .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------


races_df = spark.table(f"{catalog_name}.{silver_schema}.races")

# COMMAND ----------

dim_races_df = races_df.join(
    circuits_df,
    races_df.circuit_id == circuits_df.circuit_id,
    "inner"
).select(
    races_df.season,
    races_df.round,
    races_df.race_name,
    races_df.race_date,
    circuits_df.circuit_name,
    circuits_df.locality,
    circuits_df.country
)

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 write transformed data to gold dime_races

# COMMAND ----------

# DBTITLE 1,Cell 10
write_to_gold(
    input_df=dim_races_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)

# COMMAND ----------

display(spark.table(target_table))