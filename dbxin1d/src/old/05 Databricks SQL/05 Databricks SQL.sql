-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Databricks SQL
-- MAGIC
-- MAGIC Before we continue, let's do a recap of some of the things we've learned so far:
-- MAGIC 1. The Databricks workspace contains a suite of tools to simplify the data engineering development lifecycle
-- MAGIC 1. Databricks notebooks allow users to mix SQL with other programming languages to define ETL workloads
-- MAGIC 1. Delta Lake provides ACID compliant transactions and makes incremental data processing easy in the Lakehouse
-- MAGIC 1. Data Explorer simplifies managing Table ACLs, making Lakehouse data available to SQL analysts 
-- MAGIC
-- MAGIC In this section, we'll focus on exploring more Databricks SQL.
-- MAGIC Databricks SQL allows users to edit and execute SQL queries, build visualizations, and define dashboards.
-- MAGIC
-- MAGIC We'll start by focusing on leveraging Databricks SQL to configure queries. While we'll be using the Databricks SQL UI for this demo, SQL Warehouses <a href="https://docs.databricks.com/integrations/partners.html" target="_blank">integrate with a number of other tools to allow external query execution</a>, as well as having <a href="https://docs.databricks.com/sql/api/index.html" target="_blank">full API support for executing arbitrary queries programmatically</a>.
-- MAGIC
-- MAGIC From these query results, we'll generate a series of visualizations, which we'll combine into a dashboard.
-- MAGIC
-- MAGIC Finally, we'll walk through scheduling updates for queries and dashboards, and demonstrate setting alerts to help monitor the state of production datasets over time.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Databricks SQL as a tool to support production ETL tasks backing analytic workloads
-- MAGIC * Configure SQL queries and visualizations with the Databricks SQL Editor
-- MAGIC * Create dashboards in Databricks SQL
-- MAGIC * Schedule updates for queries and dashboards
-- MAGIC * Set alerts for SQL queries

-- COMMAND ----------

-- DBTITLE 0,--i18n-2396d183-ba9d-4477-a92a-690506110da6
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### Navigating Databricks SQL and Attaching to SQL Warehouses
-- MAGIC
-- MAGIC * Navigate to Databricks SQL  
-- MAGIC * Make sure a SQL warehouse is on and accessible
-- MAGIC   * Navigate to SQL Warehouses in the sidebar
-- MAGIC   * If a SQL warehouse exists and has the State **`Running`**, you'll use this SQL warehouse
-- MAGIC   * If a SQL warehouse exists but is **`Stopped`**, click the **`Start`** button if you have this option (**NOTE**: Start the smallest SQL warehouse you have available to you) 
-- MAGIC   * If no SQL warehouses exist and you have the option, click **`Create SQL Warehouse`**; name the SQL warehouse something you'll recognize and set the cluster size to 2X-Small. Leave all other options as default.
-- MAGIC   * If you have no way to create or attach to a SQL warehouse, you'll need to contact a workspace administrator and request access to compute resources in Databricks SQL to continue.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a New Query
-- MAGIC
-- MAGIC * Use the sidebar to navigate to **Queries**
-- MAGIC * Click the **`Create Query`** button
-- MAGIC * Make sure you are connected to a SQL warehouse. In the **Schema Browser**, click on the current metastore and select **`samples`**. 
-- MAGIC   * Select the **`tpch`** database
-- MAGIC   * Click on the **`partsupp`** table to get a preview of the schema
-- MAGIC   * While hovering over the **`partsupp`** table name, click the **>>** button to insert the table name into your query text

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write your first query:
-- MAGIC   * **`SELECT * FROM`** the **`partsupp`** table using the full name imported in the last step; click **Run** to preview results
-- MAGIC   * Modify this query to **`GROUP BY ps_partkey`** and return the **`ps_partkey`** and **`sum(ps_availqty)`**; click **Run** to preview results
-- MAGIC   * Update your query to alias the 2nd column to be named **`total_availqty`** and re-execute the query
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Save your query
-- MAGIC   * Click the **Save** button next to **Run** near the top right of the screen
-- MAGIC   * Give the query a name you'll remember

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Set a Query Refresh Schedule
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Locate the **Refresh Schedule** field at the bottom right of the SQL query editor box; click the blue **Never**
-- MAGIC 1. Use the drop down to change to Refresh every **1 week** at **12:00**
-- MAGIC 1. For **Ends**, click the **On** radio button
-- MAGIC 1. Select tomorrow's date
-- MAGIC 1. Click **OK**
-- MAGIC
-- MAGIC **NOTE:** Although we are using a refresh schedule of 1 week for the purposes of this training, you'll likely see shorter trigger intervals in production, such as schedules to refresh every 1 minute.
