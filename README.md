# Formula 1 Data Pipeline (Incremental Load)

## Overview
This repository contains a robust data engineering pipeline built using **Databricks**, **PySpark**, and **Delta Lake**. The project demonstrates an end-to-end incremental data ingestion and transformation workflow for Formula 1 data. It implements the **Medallion Architecture** (Bronze, Silver, Gold layers) to process and analyze raw data incrementally.

## Architecture

The project follows the Databricks Medallion Architecture:
- **Landing Zone**: Raw files (CSV, JSON, etc.) dropped in Azure Data Lake Storage (ADLS) in batch folders.
- **Bronze Layer**: Raw data ingested into Delta format with additional metadata (ingestion timestamp, source file).
- **Silver Layer**: Cleansed and transformed data. Schema enforcement, deduplication, and data type conversions are applied.
- **Gold Layer**: Business-level aggregates, dimensions, and facts for analytics and reporting (Star Schema).

## Project Structure
- **`00-common/`**: Shared helper functions, environment variables, and configuration details.
- **`01-setup/`**: SQL scripts to set up the Unity Catalog environment (catalogs, schemas, external locations, and volumes).
- **`02-bronze/`**: PySpark notebooks to ingest datasets (Circuits, Races, Constructors, Drivers, Results, Sprints) from the landing zone into the Bronze layer.
- **`03-silver/`**: PySpark notebooks applying transformations, cleansing, and chaining datasets to form the Silver layer.
- **`04-gold/`**: Scripts to build star schema components (Fact and Dimension tables) like Results Fact and Dimensions for Races, Drivers, and Constructors.
- **`05-analytics/`**: SQL views and queries generating business-level insights like Driver and Constructor standings.
- **`06-orchestration/`**: Orchestration logic and control tables to manage batch IDs and incremental loads.

## Technologies Used
- **Databricks** (Notebooks)
- **Apache Spark** (PySpark & Spark SQL)
- **Delta Lake**
- **Unity Catalog**
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**

## Getting Started
1. Run the scripts in `01-setup` to create the catalog `formula1_incr` and schemas (`landing`, `bronze`, `silver`, `gold`).
2. Update configuration paths in `00-common/01.environment-config.py` as needed for your storage accounts.
3. Use the orchestration notebooks in `06-orchestration` to initialize control tables and trigger the incremental load batches.
