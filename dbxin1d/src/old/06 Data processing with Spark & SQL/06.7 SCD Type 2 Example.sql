-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Slowly Changing Dimension - Type 2
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A slowly changing dimension (SCD) is a dimension that contains relatively static data that can change slowly but unpredictably, rather than according to a regular schedule. Some examples of typical slowly changing dimensions are entities such as names of geographical locations, customers, or products.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Specifically, a SCD Type 2 is a common technique to preserve history in a dimension table used throughout any data warehousing/modeling architecture. When the value of a chosen attribute changes, the current record is closed and a new record is created with the changed data values and this new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's load some test data for our example, in a new `cdc_data` schema.

-- COMMAND ----------

USE CATALOG diad_catalog;

CREATE SCHEMA IF NOT EXISTS cdc_data;

CREATE TABLE
  cdc_data.users_events
AS SELECT
  col1 AS userId,
  col2 AS name,
  col3 AS city,
  col4 AS operation,
  col5 AS sequenceNum
FROM (
  VALUES
  -- Initial load.
  (124, "Raul",     "Oaxaca",      "INSERT", 1),
  (123, "Isabel",   "Monterrey",   "INSERT", 1),
  -- New users.
  (125, "Mercedes", "Tijuana",     "INSERT", 2),
  (126, "Lily",     "Cancun",      "INSERT", 2),
  -- Isabel is removed from the system and Mercedes moved to Guadalajara.
  (123, null,       null,          "DELETE", 6),
  (125, "Mercedes", "Guadalajara", "UPDATE", 6),
  -- This batch of updates arrived out of order. The above batch at sequenceNum 5 will be the final state.
  (125, "Mercedes", "Mexicali",    "UPDATE", 5),
  (123, "Isabel",   "Chihuahua",   "UPDATE", 5)
  -- Uncomment to test TRUNCATE.
  -- ,(null, null,      null,          "TRUNCATE", 3)
);

-- COMMAND ----------

SELECT * FROM diad_catalog.cdc_data.users_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delta Live Tables support for SCD type 2 is in Public Preview. In the notebook 06.7b, you can find a SQL definition of a Streaming Table that does just that.
-- MAGIC You use the same basic SQL syntax when declaring either a streaming table or a materialized view (also referred to as a LIVE TABLE).
-- MAGIC
-- MAGIC Can you create a pipeline using notebook 06.7b to apply the changes recorded in the users_events table?

-- COMMAND ----------

-- Clean up the dummy data
DROP SCHEMA cdc_data CASCADE;
