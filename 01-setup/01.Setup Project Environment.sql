-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Set-Up the Project environment for Formula1 -incr
-- MAGIC
-- MAGIC 1. Create External Location databricks-course-extdl1-formula1
-- MAGIC 2. Create catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Access Cloud Storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databrickscoursextdl1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create External location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databrickscoursextdl1_formula1_incr
    URL 'abfss://formula1-incr@databrickscoursextdl1.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databricks-sc`)
    COMMENT 'External location for the formula1 incr container';

-- COMMAND ----------

ShOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create catalog

-- COMMAND ----------

 CREATE CATALOG IF NOT EXISTS formula1_incr
   MANAGED LOCATION 'abfss://formula1-incr@databrickscoursextdl1.dfs.core.windows.net/'
    COMMENT 'This is the main catalog for the formula1_incr Project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Schemas landing,bronze,silver,gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@databrickscoursextdl1.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@databrickscoursextdl1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@databrickscoursextdl1.dfs.core.windows.net/gold';

-- COMMAND ----------

USE CATALOG formula1_incr

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Volume Files

-- COMMAND ----------

-- DBTITLE 1,Cell 14
CREATE EXTERNAL VOLUME IF NOT EXISTS formula1_incr.landing.files
LOCATION 'abfss://formula1-incr@databrickscoursextdl1.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/files
-- MAGIC