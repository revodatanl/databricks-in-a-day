# Databricks notebook source
# MAGIC %md ## Notebook basics

# COMMAND ----------

print("hello Python")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 'hello SQL'

# COMMAND ----------

# MAGIC %md ## Let's take a look at the raw files

# COMMAND ----------

raw_files = dbutils.fs.ls("/Volumes/databricks_training/raw/aviation/")
raw_files

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/databricks_training/raw/aviation/2024-02-25_prices_EIN_BUD.json

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/databricks_training/raw/aviation/")

display(df)

# COMMAND ----------

df.createOrReplaceTempView("temporary_raw_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from temporary_raw_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select explode(outboundFlights) from temporary_raw_table

# COMMAND ----------


