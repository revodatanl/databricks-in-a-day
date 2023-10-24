# Databricks notebook source
# DBTITLE 0,--i18n-3fbfc7bd-6ef2-4fea-b8a2-7f949cd84044
# MAGIC %md
# MAGIC # Aggregation
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank" target="_blank">Grouped Data</a>: **`agg`**, **`avg`**, **`count`**, **`max`**, **`sum`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`approx_count_distinct`**, **`avg`**, **`sum`**

# COMMAND ----------

df = spark.table("samples.nyctaxi.trips")
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-a04aa8bd-35f0-43df-b137-6e34aebcded1
# MAGIC %md
# MAGIC
# MAGIC ### Grouping data
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# DBTITLE 0,--i18n-cd0936f7-cd8a-4277-bbaf-d3a6ca2c29ec
# MAGIC %md
# MAGIC
# MAGIC ### groupBy
# MAGIC Use the DataFrame **`groupBy`** method to create a grouped data object. 
# MAGIC
# MAGIC This grouped data object is called **`RelationalGroupedDataset`** in Scala and **`GroupedData`** in Python.

# COMMAND ----------

df.groupBy("pickup_zip")

# COMMAND ----------

df.groupBy("pickup_zip", "dropoff_zip")

# COMMAND ----------

# DBTITLE 0,--i18n-7918f032-d001-4e38-bd75-51eb68c41ffa
# MAGIC %md
# MAGIC
# MAGIC ### Grouped data methods
# MAGIC Various aggregation methods are available on the <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a> object.
# MAGIC
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | agg | Compute aggregates by specifying a series of aggregate columns |
# MAGIC | avg | Compute the mean value for each numeric columns for each group |
# MAGIC | count | Count the number of rows for each group |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | min | Compute the min value for each numeric column for each group |
# MAGIC | pivot | Pivots a column of the current DataFrame and performs the specified aggregation |
# MAGIC | sum | Compute the sum for each numeric columns for each group |

# COMMAND ----------

pickup_counts_df = df.groupBy("pickup_zip").count().orderBy('count', ascending=False)
display(pickup_counts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-bf63efea-c4f7-4ff9-9d42-4de245617d97
# MAGIC %md
# MAGIC
# MAGIC Here, we're getting the average fare amount for each starting point.

# COMMAND ----------

avg_fare_amount_df = df.groupBy("pickup_zip") \
                       .avg("fare_amount") \
                       .orderBy("avg(fare_amount)", ascending=False)
display(avg_fare_amount_df)

# COMMAND ----------

# DBTITLE 0,--i18n-62a4e852-249a-4dbf-b47a-e85a64cbc258
# MAGIC %md
# MAGIC
# MAGIC ## Built-In Functions
# MAGIC In addition to DataFrame and Column transformation methods, there are a ton of helpful functions in Spark's built-in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL functions</a> module.
# MAGIC
# MAGIC In Scala, this is <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">**`org.apache.spark.sql.functions`**</a>, and <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">**`pyspark.sql.functions`**</a> in Python. Functions from this module must be imported into your code.

# COMMAND ----------

# DBTITLE 0,--i18n-68f06736-e457-4893-8c0d-be83c818bd91
# MAGIC %md
# MAGIC
# MAGIC ### Aggregate Functions
# MAGIC
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> to apply built-in aggregate functions
# MAGIC
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>.

# COMMAND ----------

from pyspark.sql.functions import avg
avg_fare_amount_df = df.groupBy("pickup_zip") \
                       .agg(avg("fare_amount").alias("avg_fare")) \
                       .orderBy("avg_fare", ascending=False)
display(avg_fare_amount_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6bb4a15f-4f5d-4f70-bf50-4be00167d9fa
# MAGIC %md
# MAGIC
# MAGIC ### Math Functions
# MAGIC Here are some of the built-in functions for math operations.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | ceil | Computes the ceiling of the given column. |
# MAGIC | cos | Computes the cosine of the given value. |
# MAGIC | log | Computes the natural logarithm of the given value. |
# MAGIC | round | Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode. |
# MAGIC | sqrt | Computes the square root of the specified float value. |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )