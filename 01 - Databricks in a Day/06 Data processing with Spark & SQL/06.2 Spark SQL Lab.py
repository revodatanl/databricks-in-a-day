# Databricks notebook source
# DBTITLE 0,--i18n-da4e23df-1911-4f58-9030-65da697d7b61
# MAGIC %md
# MAGIC # Spark SQL Lab
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Create a DataFrame from the **`sample.nyctaxi.trips`** table
# MAGIC 1. Display the DataFrame and inspect its schema
# MAGIC 1. Apply transformations to filter **`trip_distance`** to select only trips under 10 miles, and sort by **`tpep_pickup_datetime`**
# MAGIC 1. Count results and take the first 5 rows
# MAGIC 1. Create the same DataFrame using a SQL query
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> transformations: **`select`**, **`where`**, **`orderBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> actions: **`select`**, **`count`**, **`take`**
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# DBTITLE 0,--i18n-e0f3f405-8c97-46d1-8550-fb8ff14e5bd6
# MAGIC %md
# MAGIC
# MAGIC ### 1. Create a DataFrame from the **`events`** table
# MAGIC - Use SparkSession to create a DataFrame from the **`events`** table

# COMMAND ----------

# TODO
trips_df = FILL_IN

# COMMAND ----------

# DBTITLE 0,--i18n-fb5458a0-b475-4d77-b06b-63bb9a18d586
# MAGIC %md
# MAGIC
# MAGIC ### 2. Display DataFrame and inspect schema
# MAGIC - Use methods above to inspect DataFrame contents and schema

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-76adfcb2-f182-485c-becd-9e569d4148b6
# MAGIC %md
# MAGIC
# MAGIC ### 3. Apply transformations to filter and sort **`macOS`** events
# MAGIC - Filter for rows where **`trip_distance`** is **`less than 10`**
# MAGIC - Sort rows by **`tpep_pickup_datetime`**
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Use single and double quotes in your filter SQL expression

# COMMAND ----------

# TODO
short_df = (trips_df
          .FILL_IN
         )

# COMMAND ----------

# DBTITLE 0,--i18n-81f8748d-a154-468b-b02e-ef1a1b6b2ba8
# MAGIC %md
# MAGIC
# MAGIC ### 4. Count total number of rows and take first 5 rows
# MAGIC - Use DataFrame actions to count and take rows

# COMMAND ----------

# TODO
num_rows = short_df.FILL_IN # total number of rows
rows = short_df.FILL_IN # take the first 5 rows

# COMMAND ----------

# DBTITLE 0,--i18n-4e340689-5d23-499a-9cd2-92509a646de6
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql import Row

assert(num_rows == 20801)
assert(len(rows) == 5)
assert(type(rows[0]) == Row)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-cbb03650-db3b-42b3-96ee-54ea9b287ab5
# MAGIC %md
# MAGIC
# MAGIC ### 5. Create the same DataFrame using SQL query
# MAGIC - Use SparkSession to run a SQL query on the **`trips`** table
# MAGIC - Use SQL commands to write the same filter and sort query used earlier

# COMMAND ----------

# TODO
short_sql_df = spark.FILL_IN

display(short_sql_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1d203e4e-e835-4778-a245-daf30cc9f4bc
# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

verify_rows = short_sql_df.take(5)
assert (short_sql_df.agg({"trip_distance" : "max"}).collect()[0][0] < 10 and len(verify_rows) == 5), "Incorrect filter condition"
del verify_rows
print("All test pass")