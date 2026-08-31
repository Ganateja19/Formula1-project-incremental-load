# Databricks notebook source
# MAGIC %md
# MAGIC # Transform results Data
# MAGIC
# MAGIC 1. Read bronze results table
# MAGIC 2. Keep only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column names using snake case ( driverId -> driver_id, constructorID -> constructor_id, positionText -> finish_position_text)
# MAGIC 4. Rename columns to make them more meaningful(date->race_date, grid->grid_position, laps->completed_laps, number-> car_number, postion->final_position)
# MAGIC 5. Filter out rows where season, round, constructor_id or driver_id is null
# MAGIC 6. Remove Duplicate Records
# MAGIC 7. Transform values of the nationality to Title Case
# MAGIC 8. Write the transformed data to the silver results table 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze results table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read bronze results table

# COMMAND ----------

#results_df=spark.read.option('versionAsOf' , 0).table(bronze_table)


# COMMAND ----------

results_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC {"date":"1950-05-13","raceName":"british grand prix","round":1,"season":1950,"url":"https://en.wikipedia.org/wiki/1950_British_Grand_Prix","constructorId":"alfa","driverId":"farina","grid":1,"laps":70,"number":2,"points":9.0,"position":1,"positionText":"1","status":"Finished"}

# COMMAND ----------

display(results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analytics (DROP url)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_selected_df = (
    results_df.select(
        "date",
        "raceName",
        "round",
        "season",
        "constructorId",
        "driverId",
        "grid",
        "laps",
        "number",
        "points",
        "position",
        "positionText",
        "status",
        "ingestion_date",
        "source_file"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 3. Standardise column names using snake case ( driverId -> driver_id, constructorID -> constructor_id, positionText -> finish_position_text)
# MAGIC #### 4. Rename columns to make them more meaningful(date->race_date, grid->grid_position, laps->completed_laps, number-> car_number, postion->final_position)

# COMMAND ----------

results_renamed_df = (
     results_selected_df.withColumnsRenamed({
        "driverId": "driver_id",
        "constructorId": "constructor_id",
        "racename": "race_name",
        "positionText": "finish_position_text",
        "date": "race_date",
        "grid": "grid_position",
        "laps": "completed_laps",
        "number": "car_number",
        "position": "final_position"
    })
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Filter out rows where season, round, constructor_id, or driver_id is null

# COMMAND ----------

# DBTITLE 1,Cell 17
results_valid_df = (
    results_renamed_df
    .filter(
        F.col("season").isNotNull()
        & F.col("round").isNotNull()
        & F.col("constructor_id").isNotNull()
        & F.col("driver_id").isNotNull()
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC checking quality

# COMMAND ----------

display(results_renamed_df.count() - results_valid_df.count())

# COMMAND ----------

display(results_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicate Records

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season","round","constructor_id","driver_id"])
display(results_distinct_df)

# COMMAND ----------

display(results_valid_df.count()- results_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform values of columns nationality to Title Case

# COMMAND ----------

results_final_df = (
    results_distinct_df
    .withColumn('race_name', F.initcap(F.col('race_name'))))


# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver results table 

# COMMAND ----------

(
    results_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))