-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dashboard Lab
-- MAGIC
-- MAGIC The U.S. Department of Transportation's (DOT) Bureau of Transportation Statistics tracks the on-time performance of domestic flights operated by large air carriers. Summary information on the number of on-time, delayed, canceled, and diverted flights is published in DOT's monthly Air Travel Consumer Report and in this dataset of 2015 flight delays and cancellations.
-- MAGIC
-- MAGIC In this exercise, you will use this dataset, stored in the `flights_delays_cancellations` catalog, to:
-- MAGIC
-- MAGIC 1. Run some SQL queries on the table to explore the data and answer some questions
-- MAGIC 2. Create some visualizations from the query results using the Databricks display function
-- MAGIC 3. Combine the visualizations in a dashboard using the Databricks dashboard feature

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run some SQL queries on the `flights_delays_cancellations` catalog to explore the data and answer some questions. For example, you can find out the top 10 origin airports by number of flights, the average delay by carrier, and the correlation between distance and delay.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here is some sample code for a similar dataset, **not meant to be run**, to get some inspiration!
-- MAGIC
-- MAGIC ```SQL
-- MAGIC
-- MAGIC -- Top 10 origin airports by number of flights
-- MAGIC SELECT origin, COUNT(*) AS num_flights
-- MAGIC FROM flights
-- MAGIC GROUP BY origin
-- MAGIC ORDER BY num_flights DESC
-- MAGIC LIMIT 10
-- MAGIC
-- MAGIC -- Average delay by carrier
-- MAGIC SELECT carrier, AVG(arr_delay) AS avg_delay
-- MAGIC FROM flights
-- MAGIC GROUP BY carrier
-- MAGIC ORDER BY avg_delay
-- MAGIC
-- MAGIC -- Correlation between distance and delay
-- MAGIC SELECT CORR(distance, arr_delay) AS corr
-- MAGIC FROM flights
-- MAGIC
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Create some visualizations from the query results. For example, you can create a bar chart, a line chart, and a scatter plot depending on the queries you have written.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Combine the visualizations in a dashboard using the Databricks dashboard feature. You can do this by clicking on the View menu and selecting Create Dashboard. Then, you can drag and drop the visualizations to arrange them in the dashboard. You can also edit the dashboard title and add some text widgets to provide some context and explanation for the visualizations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Share with the rest of the class! Remember that dashboards are not meant to be a fully fledged visualization tool, but are great for quick prototyping and exploring of data, shareable with less technical stakeholders.
