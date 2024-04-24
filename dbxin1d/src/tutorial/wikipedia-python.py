# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ## Raw to Bronze

# COMMAND ----------

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/"
catalog = "databricks_in_a_day"
schema = "kornelkovacs"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

df_bronze = (
  spark
  .read
  .format("json")
  .load(json_path)
  .withColumn("ingestion_timestamp", F.current_timestamp())
  .withColumn("source", F.input_file_name())
  )

df_bronze.write.saveAsTable(f"{catalog}.{schema}.wikipedia_bronze", mode="overwrite")

# COMMAND ----------

# MAGIC %md ## Bronze to Silver

# COMMAND ----------

df_silver = (
    spark
    .table(f"{catalog}.{schema}.wikipedia_bronze")
    .withColumn("click_count", F.expr("CAST(n AS INT)"))
    .filter(F.col("click_count") > 0)
    .withColumnRenamed("curr_title", "current_page_title")
    .filter(F.col("current_page_title").isNotNull())
    .withColumnRenamed("prev_title", "previous_page_title")
    .select("current_page_title", "click_count", "previous_page_title")
)

df_silver.write.saveAsTable(f"{catalog}.{schema}.wikipedia_silver", mode="overwrite")

# COMMAND ----------

# MAGIC %md ## Silver to Gold

# COMMAND ----------

df_gold = (
    spark
    .table(f"{catalog}.{schema}.wikipedia_silver")
    .filter(F.col("current_page_title") == 'Apache_Spark')
    .withColumnRenamed("previous_page_title", "referrer")
    .sort(F.col("click_count").desc())
    .select("referrer", "click_count")
    .limit(10)
  )

df_gold.write.saveAsTable(f"{catalog}.{schema}.wikipedia_gold", mode="overwrite")

# COMMAND ----------


