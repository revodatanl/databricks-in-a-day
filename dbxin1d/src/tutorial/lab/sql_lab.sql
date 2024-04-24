-- Databricks notebook source
USE CATALOG databricks_in_a_day;
USE SCHEMA -- TODO

-- COMMAND ----------

-- MAGIC %md ## Raw to Bronze

-- COMMAND ----------

-- MAGIC %md Steps:
-- MAGIC - Ingest the data from `/Volumes/databricks_training/raw/baby_names` to the table `names_bronze`. 
-- MAGIC - Optionally, let's have a table comment describing what we're dealing with here.
-- MAGIC - Add a column `ingestion_timestamp` representing the time of ingestaion. It should be dynamic based on the current time of the server.
-- MAGIC - Rename `First Name` to `first_name` 

-- COMMAND ----------

-- TODO

-- COMMAND ----------

-- MAGIC %md ## Bronze to Silver

-- COMMAND ----------

-- MAGIC %md Steps:
-- MAGIC - Create a variable `current_year` that stores the current year of the server's clock.
-- MAGIC - Create a table called `names_silver` and optionally add a comment as well. The table should have three expectations. If the 2nd or 3rd are vialoated, the pipeline should fail. For the first one, it should just flag it.
-- MAGIC   - `first_name` should not be NULL.
-- MAGIC   - `count` should be larger than 0.
-- MAGIC   - `year` should be the `current_year` at maximum
-- MAGIC - In the `names_silver` table 
-- MAGIC    - rename all columns to match the [snake_case](https://en.wikipedia.org/wiki/Snake_case) pattern.
-- MAGIC    - Convert the `year` and `count` columns to `INT`.

-- COMMAND ----------

-- TODO

-- COMMAND ----------

-- MAGIC %md ## Silver to Bronze

-- COMMAND ----------

-- MAGIC %md Steps:
-- MAGIC - Create a table called `top_baby_names_2021`, and add a comment to it.
-- MAGIC - Ingest data from `baby_names_silver`. 
-- MAGIC - Filter for the year 2021
-- MAGIC - Group by `first_name` and sum up the counts. Save it as `total_counts`. 
-- MAGIC - Sort by `total_counts` and get the 10 most popular names.

-- COMMAND ----------

-- TODO
