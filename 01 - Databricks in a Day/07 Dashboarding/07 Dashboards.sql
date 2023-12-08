-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboards
-- MAGIC
-- MAGIC To access dashboards:
-- MAGIC * Navigate to home page in Databricks SQL
-- MAGIC   * Click the Databricks logo at the top of the side nav bar
-- MAGIC * Locate the **Sample dashboards** and click **`Visit gallery`**
-- MAGIC * Click **`Import`** next to the **Retail Revenue & Supply Chain** option
-- MAGIC   * Assuming you have a SQL warehouse available, this should load a dashboard and immediately display results
-- MAGIC   * Click **Refresh** in the top right (the underlying data has not changed, but this is the button that would be used to pick up changes)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Accessing Dashboards
-- MAGIC To access dashboards:
-- MAGIC
-- MAGIC * Navigate to home page in Databricks SQL
-- MAGIC   * Click the Databricks logo at the top of the side nav bar

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sample dashboards
-- MAGIC Databricks sample dashboards are pre-built visualizations that show some of the rich features of Databricks SQL for data warehousing. 
-- MAGIC
-- MAGIC You can import and use them from the Dashboard Samples Gallery, which you can access from the sidebar or by appending `/sql/dashboards/samples/` to your workspace URL. 
-- MAGIC
-- MAGIC To explore Sample dashboards:
-- MAGIC
-- MAGIC * Navigate to home page in Databricks SQL
-- MAGIC   * Click the Databricks logo at the top of the side nav bar
-- MAGIC * Locate the **Sample dashboards** and click **`Visit gallery`**
-- MAGIC
-- MAGIC Let's create one!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Retail Revenue & Supply Chain sample
-- MAGIC Click **`Import`** next to the **Retail Revenue & Supply Chain** option
-- MAGIC   * Assuming you have a SQL warehouse available, this should load a dashboard and immediately display results
-- MAGIC   * Click **Refresh** in the top right (the underlying data has not changed, but this is the button that would be used to pick up changes)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update the Dashboard
-- MAGIC Use the sidebar navigator to find the **Dashboards**
-- MAGIC   * Locate the sample dashboard you just loaded; it should be called **Retail Revenue & Supply Chain** and have your username under the **`Created By`** field. **NOTE**: the **My Dashboards** option on the right hand side can serve as a shortcut to filter out other dashboards in the workspace
-- MAGIC   * Click on the dashboard name to view it
-- MAGIC
-- MAGIC View the query behind the **Shifts in Pricing Priorities** plot
-- MAGIC   * Hover over the plot; three vertical dots should appear. Click on these
-- MAGIC   * Select **View Query** from the menu that appears
-- MAGIC
-- MAGIC Review the SQL code used to populate this plot
-- MAGIC   * Note that 3 tier namespacing is used to identify the source table; this is a preview of new functionality to be supported by Unity Catalog
-- MAGIC   * Click **`Run`** in the top right of the screen to preview the results of the query
-- MAGIC
-- MAGIC Review the visualization
-- MAGIC   * Under the query, a tab named **Table** should be selected; click **Price by Priority over Time** to switch to a preview of your plot
-- MAGIC   * Click **Edit Visualization** at the bottom of the screen to review settings
-- MAGIC   * Explore how changing settings impacts your visualization
-- MAGIC   * If you wish to apply your changes, click **Save**; otherwise, click **Cancel**
-- MAGIC
-- MAGIC Back in the query editor, click the **Add Visualization** button to the right of the visualization name
-- MAGIC   * Create a bar graph
-- MAGIC   * Set the **X Column** as **`Date`**
-- MAGIC   * Set the **Y Column** as **`Total Price`**
-- MAGIC   * **Group by** **`Priority`**
-- MAGIC   * Set **Stacking** to **`Stack`**
-- MAGIC   * Leave all other settings as defaults
-- MAGIC   * Click **Save**
-- MAGIC
-- MAGIC Back in the query editor, click the default name for this visualization to edit it; change the visualization name to **`Stacked Price`**
-- MAGIC * Add the bottom of the screen, click the three vertical dots to the left of the **`Edit Visualization`** button
-- MAGIC   * Select **Add to Dashboard** from the menu
-- MAGIC   * Select your **`Retail Revenue & Supply Chain`** dashboard
-- MAGIC * Navigate back to your dashboard to view this change

-- COMMAND ----------

-- DBTITLE 0,--i18n-c43c8f47-c8c3-4232-9a76-f82bdd204317
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Create a Bar Graph Visualization
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Click the **+** sign next to the results tab in middle of window, and select **Visualization** from the dialog box
-- MAGIC 1. Click on the name (should default to something like **`Bar 1`**) and change the name to **Total User Records**
-- MAGIC 1. Set **`user_id`** for the **X Column**
-- MAGIC 1. Set **`total_records`** for the **Y Column**
-- MAGIC 1. Click **Save**

-- COMMAND ----------

-- DBTITLE 0,--i18n-626f8b1d-51cd-47b0-8828-b35180acb40c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Review and Refresh your Dashboard
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Use the left side bar to navigate to **Dashboards**
-- MAGIC 1. Find the dashboard you've added your queries to
-- MAGIC 1. Click the blue **Refresh** button to update your dashboard
-- MAGIC 1. Click the **Schedule** button to review dashboard scheduling options
-- MAGIC   * Note that scheduling a dashboard to update will execute all queries associated with that dashboard
-- MAGIC   * Do not schedule the dashboard at this time

-- COMMAND ----------

-- DBTITLE 0,--i18n-4f69c53f-8bd9-48d5-9f6f-f97045581e49
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Share your Dashboard
-- MAGIC
-- MAGIC Steps:
-- MAGIC 1. Click the blue **Share** button
-- MAGIC 1. Select **All Users** from the top field
-- MAGIC 1. Choose **Can Run** from the right field
-- MAGIC 1. Click **Add**
-- MAGIC 1. Change the **Credentials** to **Run as viewer**
-- MAGIC
-- MAGIC **NOTE**: At present, no other users should have any permissions to run your dashboard, as they have not been granted permissions to the underlying databases and tables using Table ACLs. If you wish other users to be able to trigger updates to your dashboard, you will either need to grant them permissions to **Run as owner** or add permissions for the tables referenced in your queries.
