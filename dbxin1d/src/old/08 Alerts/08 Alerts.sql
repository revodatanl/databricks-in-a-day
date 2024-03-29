-- Databricks notebook source
-- DBTITLE 0,--i18n-facded12-10b1-4c63-a075-b7790fa3cd17
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Set Up an Alert
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Use the left side bar to navigate to **Alerts**
-- MAGIC 1. Click **Create Alert** in the top right
-- MAGIC 1. Select your **User Counts** query
-- MAGIC 1. Click the field at the top left of the screen to give the alert a name **`<your_initials> Count Check`**
-- MAGIC 1. For the **Trigger when** options, configure:
-- MAGIC   * **Value column**: **`total_records`**
-- MAGIC   * **Condition**: **`>`**
-- MAGIC   * **Threshold**: **`15`**
-- MAGIC 1. For **Refresh**, select **Never**
-- MAGIC 1. Click **Create Alert**
-- MAGIC 1. On the next screen, click the blue **Refresh** in the top right to evaluate the alert

-- COMMAND ----------

-- DBTITLE 0,--i18n-f9f22ebd-4283-474a-ab45-0d7584a6b6ac
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ### Review Alert Destination Options
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. From the preview of your alert, click the blue **Add** button to the right of **Destinations** on the right side of the screen
-- MAGIC 1. At the bottom of the window that pops up, locate the and click the blue text in the message **Create new destinations in Alert Destinations**
-- MAGIC 1. Review the available alerting options
