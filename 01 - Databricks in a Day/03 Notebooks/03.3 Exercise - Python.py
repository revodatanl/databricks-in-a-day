# Databricks notebook source
# MAGIC %md
# MAGIC # Let's write some Python
# MAGIC
# MAGIC Make sure to attach a cluster to this notebook!

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise 1

# COMMAND ----------

print("Hello Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC Use the cell below to create a print statement in Python and run it using the attached cluster.

# COMMAND ----------

# TODO: create a print statement

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise 2
# MAGIC
# MAGIC Use Markdown formatting to create a title and a list of various items.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis Overview
# MAGIC
# MAGIC - Step 1
# MAGIC - Step 2
# MAGIC - Step 3

# COMMAND ----------

# MAGIC %md
# MAGIC Use the cell below to create a title and a list of various items. Hint: if you are lost, double click on the Markdown cell above this one!

# COMMAND ----------

# MAGIC %md
# MAGIC - [ ] TODO: create a title and a list

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise 3
# MAGIC
# MAGIC Using PySpark, read in the `samples.nyctaxi.trip` data into a `DataFrame`.

# COMMAND ----------

data = spark.read.table("samples.nyctaxi.trips")

# COMMAND ----------

# MAGIC %md
# MAGIC Print the schema of the above `DataFrame`

# COMMAND ----------

data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Display the data in the `DataFrame`.

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC Filter the `DataFrame` to include only rows for which the column `trip_distance` is greater than 10.

# COMMAND ----------

filtered_data = data.filter(data.trip_distance > 10)

# COMMAND ----------

# MAGIC %md
# MAGIC Display the filtered data to check your results.

# COMMAND ----------

display(filtered_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - [Develop code in Databricks notebooks](https://docs.databricks.com/en/notebooks/notebooks-code.html)
