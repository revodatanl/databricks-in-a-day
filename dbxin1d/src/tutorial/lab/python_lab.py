# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

raw_path = "/Volumes/databricks_training/raw/baby_names"

catalog = "databricks_in_a_day"
schema = # TODO
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Raw to Bronze

# COMMAND ----------

# MAGIC %md Steps:
# MAGIC - Read the data from `raw_path` to the table `names_bronze`. 
# MAGIC - Optionally, let's have a table comment describing what we're dealing with here.
# MAGIC - Add a column `ingestion_timestamp` representing the time of ingestaion. It should be dynamic based on the current time of the server.
# MAGIC - Rename `First Name` to `first_name` 

# COMMAND ----------

df_bronze = # TODO

# COMMAND ----------

# MAGIC %md ## Bronze to Silver
# MAGIC

# COMMAND ----------

# MAGIC %md Steps:
# MAGIC - Create a variable `current_year` that stores the current year of the server's clock.
# MAGIC - Create a table called `names_silver` and optionally add a comment as well. The table should have three expectations. If the 2nd or 3rd are vialoated, the pipeline should fail. For the first one, it should just flag it.
# MAGIC   - `first_name` should not be NULL.
# MAGIC   - `count` should be larger than 0.
# MAGIC   - `year` should be the `current_year` at maximum
# MAGIC - In the `baby_names_silver` table 
# MAGIC    - rename all columns to match the [snake_case](https://en.wikipedia.org/wiki/Snake_case) pattern.
# MAGIC    - Convert the `year` and `count` columns to `INT`.

# COMMAND ----------

import datetime
current_year = # TODO

df_silver = # TODO

# COMMAND ----------

# MAGIC %md ## Silver to Gold

# COMMAND ----------

# MAGIC %md Steps:
# MAGIC - Create a table called `names_gold`, and add a comment to it.
# MAGIC - Ingest data from `names_silver`. 
# MAGIC - Filter for the year 2021
# MAGIC - Group by `first_name` and sum up the counts. Save it as `total_counts`. 
# MAGIC - Sort by `total_counts` and get the 10 most popular names.

# COMMAND ----------

df_gold = # TODO
