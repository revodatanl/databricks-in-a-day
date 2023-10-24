-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Attach cluster
-- MAGIC
-- MAGIC Make sure to attach a cluster to this notebook!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Exercise 1
-- MAGIC
-- MAGIC Use the cell below to create a `SELECT` statement in SQL that prints "Hello Databricks" and run it using the attached cluster.

-- COMMAND ----------

SELECT "Hello Databricks";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Exercise 2
-- MAGIC
-- MAGIC Use Markdown formatting to create a title and a list of various items.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analysis Overview
-- MAGIC
-- MAGIC - Step 1
-- MAGIC - Step 2
-- MAGIC - Step 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Exercise 3
-- MAGIC
-- MAGIC Using SQL, read the first 10 rows of the `samples.nyctaxi.trip` table.

-- COMMAND ----------

SELECT * FROM samples.nyctaxi.trips LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Show the schema of the above `samples.nyctaxi.trips` table.

-- COMMAND ----------

DESCRIBE samples.nyctaxi.trips;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Filter the `DataFrame` to include only rows for which the column `trip_distance` is greater than 10.

-- COMMAND ----------

SELECT * FROM samples.nyctaxi.trips
WHERE trip_distance > 10;