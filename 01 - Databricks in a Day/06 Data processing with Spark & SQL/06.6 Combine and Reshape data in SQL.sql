-- Databricks notebook source
-- DBTITLE 0,--i18n-8382e200-81c0-4bc3-9bdb-6aee604b0a8c
-- MAGIC %md
-- MAGIC # Combine and Reshape data with SQL
-- MAGIC Let's try and replicate similar transformations in SQL together.
-- MAGIC
-- MAGIC We will use the **`customer`** table of the tpch sample schema.

-- COMMAND ----------

USE CATALOG samples;
USE SCHEMA tpch;
DESCRIBE customer

-- COMMAND ----------

SELECT * FROM customer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Grouping data and aggregations
-- MAGIC
-- MAGIC Use SQL to implement a grouping and aggregation similar to what we have just done in Python

-- COMMAND ----------

-- DBTITLE 0,--i18n-78acde42-f2b7-4b0f-9760-65fba886ef5b
-- MAGIC %md
-- MAGIC
-- MAGIC ### 1. Find the number of customers per market segment
-- MAGIC - Group by **`c_mktsegment`**
-- MAGIC - Get count of total customers per segment. 
-- MAGIC - Get average of **`c_acctbal`** as **`avg_balance`**. Round this to an integer.

-- COMMAND ----------

-- Exercise: implement aggregation in SQL

-- COMMAND ----------

-- DBTITLE 0,--i18n-f5c20afb-2891-4fa2-8090-cca7e313354d
-- MAGIC %md
-- MAGIC
-- MAGIC ### 2. Get top three customer by account balance
-- MAGIC - Sort by **`c_acctbal`** in descending order
-- MAGIC - Limit to first three rows

-- COMMAND ----------

-- Exercise: implement aggregation in SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  
-- MAGIC ### Join Tables
-- MAGIC
-- MAGIC Spark SQL supports standard **`JOIN`** operations (inner, outer, left, right, anti, cross, semi).  
-- MAGIC Here we join the customer table with the order table.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customer_purchases AS

SELECT * 
FROM orders
INNER JOIN customer
ON orders.o_custkey = customer.c_custkey;

SELECT * FROM customer_purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customers with no orders
-- MAGIC
-- MAGIC Create a temporary view that only contains customers with no orders.

-- COMMAND ----------

-- Exercise: implement JOIN in SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pivot Tables
-- MAGIC
-- MAGIC We can use **`PIVOT`** to view data from different perspectives by rotating unique values in a specified pivot column into multiple columns based on an aggregate function.
-- MAGIC - The **`PIVOT`** clause follows the table name or subquery specified in a **`FROM`** clause, which is the input for the pivot table.
-- MAGIC - Unique values in the pivot column are grouped and aggregated using the provided aggregate expression, creating a separate column for each unique value in the resulting pivot table.
-- MAGIC
-- MAGIC The following code cell uses **`PIVOT`** to flatten out the item purchase information contained in several fields derived from the **`orders`** dataset. This flattened data format can be useful for dashboarding, but also useful for applying machine learning algorithms for inference or prediction.

-- COMMAND ----------

SELECT 
  o_custkey,
  O,
  F
FROM orders
PIVOT (
  COUNT(o_orderkey) FOR o_orderstatus IN (
    'O',
    'F')
)

-- COMMAND ----------

-- Exercise: How do we PIVOT in Python?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
