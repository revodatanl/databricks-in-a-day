# Databricks notebook source
# DBTITLE 0,--i18n-ab2602b5-4183-4f33-8063-cfc03fcb1425
# MAGIC %md
# MAGIC # DataFrame & Column
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`select`**, **`selectExpr`**, **`drop`**, **`withColumn`**, **`withColumnRenamed`**, **`filter`**, **`distinct`**, **`limit`**, **`sort`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`isin`**, **`cast`**, **`isNotNull`**, **`desc`**, operators

# COMMAND ----------

orders_df = spark.table("samples.tpch.orders")
display(orders_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4ea9a278-1eb6-45ad-9f96-34e0fd0da553
# MAGIC %md
# MAGIC
# MAGIC ## Column Expressions
# MAGIC
# MAGIC A <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a> is a logical construction that will be computed based on the data in a DataFrame using an expression
# MAGIC
# MAGIC Construct a new Column based on existing columns in a DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

print(orders_df.o_orderstatus)
print(orders_df["o_orderstatus"])
print(col("o_orderstatus"))

# COMMAND ----------

# DBTITLE 0,--i18n-64238a77-0877-4bd4-af46-a9a8bd4763c6
# MAGIC %md
# MAGIC
# MAGIC ### Column Operators and Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | Math and comparison operators |
# MAGIC | ==, != | Equality and inequality tests (Scala operators are **`===`** and **`=!=`**) |
# MAGIC | alias | Gives the column an alias |
# MAGIC | cast, astype | Casts the column to a different data type |
# MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
# MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |

# COMMAND ----------

# DBTITLE 0,--i18n-6d68007e-3dbf-4f18-bde4-6990299ef086
# MAGIC %md
# MAGIC
# MAGIC Create complex expressions with existing columns, operators, and methods.

# COMMAND ----------

col("samples.tpch.lineitem.l_extendedprice") / col("samples.tpch.lineitem.l_quantity")

# COMMAND ----------

col("samples.tpch.lineitem.l_receiptdate").desc()

# COMMAND ----------

# DBTITLE 0,--i18n-7c1c0688-8f9f-4247-b8b8-bb869414b276
# MAGIC %md
# MAGIC Here's an example of using these column expressions in the context of a DataFrame

# COMMAND ----------

rev_df = (orders_df
         .filter(col("o_orderpriority") == '4-NOT SPECIFIED')
         .withColumn("o_totalprice_k", col("o_totalprice") / 1000)
         .sort(col("o_orderdate").desc())
        )

# to see the new column, extend rev_df
display(rev_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7ba60230-ecd3-49dd-a4c8-d964addc6692
# MAGIC %md
# MAGIC
# MAGIC ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

# DBTITLE 0,--i18n-3e95eb92-30e4-44aa-8ee0-46de94c2855e
# MAGIC %md
# MAGIC
# MAGIC ### Subset columns
# MAGIC Use DataFrame transformations to subset columns

# COMMAND ----------

# DBTITLE 0,--i18n-987cfd99-8e06-447f-b1c7-5f104cd5ed2f
# MAGIC %md
# MAGIC
# MAGIC #### **`select()`**
# MAGIC Selects a list of columns or column based expressions

# COMMAND ----------

orders_subset_df = orders_df.select("o_orderkey", "o_orderdate", "o_totalprice")
display(orders_subset_df)

# COMMAND ----------

from pyspark.sql.functions import col

orders_renamed_subset_df = orders_subset_df.select(
    col("o_orderkey").alias("order_id"), 
    col("o_orderdate").alias("order_date"), 
    col("o_totalprice").alias("total_price")
)
display(orders_renamed_subset_df)
