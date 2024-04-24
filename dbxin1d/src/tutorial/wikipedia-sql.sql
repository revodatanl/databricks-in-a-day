-- Databricks notebook source
USE CATALOG databricks_in_a_day;
USE SCHEMA kornelkovacs

-- COMMAND ----------

-- MAGIC %md ## Raw to Bronze

-- COMMAND ----------

DROP TABLE IF EXISTS wikipedia_bronze;

-- COMMAND ----------

CREATE TABLE wikipedia_bronze COMMENT "The raw wikipedia click stream dataset, ingested from /databricks-datasets." AS
SELECT
  *,
  current_timestamp() as ingestion_timestamp,
  input_file_name() as source 
FROM
json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/`

-- COMMAND ----------

-- MAGIC %md ## Bronze to Silver

-- COMMAND ----------

DROP TABLE IF EXISTS wikipedia_silver;

-- COMMAND ----------

CREATE TABLE wikipedia_silver COMMENT "Wikipedia clickstream data cleaned and prepared for analysis." AS
SELECT
  curr_title AS current_page_title,
  CAST(n AS INT) AS click_count,
  prev_title AS previous_page_title
FROM
  wikipedia_bronze
WHERE
  curr_title IS NOT NULL
  AND CAST(n AS INT) > 0

-- COMMAND ----------

-- MAGIC %md ## Silver to Gold

-- COMMAND ----------

DROP TABLE IF EXISTS wikipedia_gold;

-- COMMAND ----------

CREATE TABLE wikipedia_gold
COMMENT "A table containing the top pages linking to the Apache Spark page."
AS SELECT
  previous_page_title as referrer,
  click_count
FROM wikipedia_silver
WHERE current_page_title = 'Apache_Spark'
ORDER BY click_count DESC
LIMIT 10
