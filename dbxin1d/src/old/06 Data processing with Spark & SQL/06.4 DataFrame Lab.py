# Databricks notebook source
# DBTITLE 0,--i18n-5b46ceba-8f87-4062-91cc-6f02f3303258
# MAGIC %md
# MAGIC # DataFrame Lab
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Filter events where departure_time is not null
# MAGIC 2. Calculate the average per month
# MAGIC 3. Check which flights have departed early and have landed late (if any)
# MAGIC 4. Drop the `arrival_delay` column

# COMMAND ----------

# TODO: Create a DataFrame from the flights table

# COMMAND ----------

# DBTITLE 0,--i18n-412840ac-10d6-473e-a3ea-8e9e92446b80
# MAGIC %md
# MAGIC
# MAGIC ### 1. Filter events where departure_time is not null

# COMMAND ----------

# TODO: Fill in the missing code
departed_df = flights_df.FILL_IN
display(revenue_df)

# COMMAND ----------

# DBTITLE 0,--i18n-cb49af43-880a-4834-be9c-62f65581e67a
# MAGIC %md
# MAGIC
# MAGIC ### 2. Filter events where revenue is not null
# MAGIC Filter for records where **`revenue`** is not **`null`**

# COMMAND ----------

# TODO: Calculate the average delay per month. As an extra exercise, calculate the median, and group both per month and per airline


# COMMAND ----------

# DBTITLE 0,--i18n-6dd8d228-809d-4a3b-8aba-60da65c53f1c
# MAGIC %md
# MAGIC
# MAGIC ### 3. Check which flights have departed early and have landed late (if any)
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Look at the `scheduled_departure`, `departure_time` and `departure_delay` columns. What does a negative value mean?

# COMMAND ----------

# TODO: Which flights have departed early but landed late? Is there any data on the reason why?


# COMMAND ----------

# DBTITLE 0,--i18n-f0d53260-4525-4942-b901-ce351f55d4c9
# MAGIC %md
# MAGIC ### 4. Drop the arrival_delay column
# MAGIC Are you able to recreate it?
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Look at the `scheduled_arrival` and `arrival_time` time columns

# COMMAND ----------

# TODO: Drop arrival_delay

