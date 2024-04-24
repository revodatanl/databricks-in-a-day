# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG databricks_in_a_day;
# MAGIC USE SCHEMA kornelkovacs

# COMMAND ----------

# MAGIC %md ## Raw to Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS names_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMPORARY VIEW names_raw_view
# MAGIC USING csv 
# MAGIC OPTIONS (
# MAGIC   path '/Volumes/databricks_training/raw/baby_names',
# MAGIC   header true,
# MAGIC   inferschema true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE names_bronze COMMENT "Popular baby first names in New York. This data was ingested from the New York State Departement of Health." AS
# MAGIC SELECT
# MAGIC   `Year`,
# MAGIC   Count,
# MAGIC   Sex,
# MAGIC   County,
# MAGIC   current_timestamp() as ingestion_timestamp,
# MAGIC   `FIRST NAME` AS first_name
# MAGIC FROM
# MAGIC   names_raw_view

# COMMAND ----------

# MAGIC %md ## Bronze to Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS names_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE names_silver COMMENT "New York popular baby first name data cleaned and prepared for analysis." AS
# MAGIC SELECT
# MAGIC   CAST(`Year` AS INT) AS year,
# MAGIC   CAST(Count AS INT) as count,
# MAGIC   Sex as sex,
# MAGIC   County as county,
# MAGIC   first_name
# MAGIC FROM
# MAGIC   names_bronze
# MAGIC WHERE
# MAGIC   CAST(`Year` AS INT) <= 2024
# MAGIC   and CAST(Count AS INT) > 0

# COMMAND ----------

# MAGIC %md ## Silver to Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS names_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE names_gold COMMENT "A table summarizing counts of the top baby names for New York for 2021." AS
# MAGIC SELECT
# MAGIC   first_name,
# MAGIC   SUM(count) AS total_count
# MAGIC FROM
# MAGIC   names_silver
# MAGIC WHERE
# MAGIC   year = 2021
# MAGIC GROUP BY
# MAGIC   first_name
# MAGIC ORDER BY
# MAGIC   total_count DESC
# MAGIC LIMIT
# MAGIC   10;
