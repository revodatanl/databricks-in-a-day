# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

raw_path = "/Volumes/databricks_training/raw/baby_names"

catalog = "databricks_in_a_day"
schema = "kornelkovacs"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Raw to Bronze

# COMMAND ----------

spark.sql(f"drop table if exists {catalog}.{schema}.names_bronze")

# COMMAND ----------

df_bronze = (
    spark
    .read
    .format("csv")
    .options(header=True, inferSchema=True)
    .load(raw_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source", F.input_file_name())
    .withColumnRenamed("First Name", "first_name") # not allowed for some reason otherwise
  )

df_bronze.write.saveAsTable(f"{catalog}.{schema}.names_bronze", mode="overwrite")

# COMMAND ----------

# MAGIC %md ## Bronze to Silver

# COMMAND ----------

spark.sql(f"drop table if exists {catalog}.{schema}.names_silver")

# COMMAND ----------

import datetime
current_year = datetime.datetime.now().year

df_silver = (
    spark
    .table(f"{catalog}.{schema}.names_bronze")
    .withColumn("year", F.col("Year").cast("int"))
    .withColumnRenamed("County", "county")
    .withColumnRenamed("Sex", "sex")
    .withColumn("count", F.col("Count").cast("int"))
    .filter("count > 0")
    .filter("first_name IS NOT NULL")
    .filter(f"year <= {current_year}")
    )

df_silver.write.saveAsTable(f"{catalog}.{schema}.names_silver", mode="overwrite")

# COMMAND ----------

# MAGIC %md ## Silver to Gold

# COMMAND ----------

spark.sql(f"drop table if exists {catalog}.{schema}.names_gold")

# COMMAND ----------

df_gold = (
    spark
    .table(f"{catalog}.{schema}.names_silver")
    .filter(F.col("year") == 2021)
    .groupBy("first_name")
    .agg(F.sum("count").alias("total_count"))
    .sort(F.col("total_count").desc())
    .limit(10)
  )

df_gold.write.saveAsTable(f"{catalog}.{schema}.names_gold", mode="overwrite")
