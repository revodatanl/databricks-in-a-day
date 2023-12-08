-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog (SQL)
-- MAGIC
-- MAGIC This notebook provides an example workflow for getting started with Unity Catalog by showing how to do the following:
-- MAGIC
-- MAGIC - Choose a catalog and create a new schema.
-- MAGIC - Create a managed table and add it to the schema.
-- MAGIC - Query the table using the three-level namespace.
-- MAGIC - Manage data access permissions on the table.
-- MAGIC - Explore grants on various objects in Unity Catalog.
-- MAGIC
-- MAGIC ## Requirements
-- MAGIC
-- MAGIC - The workspace must be attached to a Unity Catalog metastore.
-- MAGIC - Notebook is attached to a cluster that uses DBR 11.1 or higher and uses the single user or shared cluster access mode.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Three-level namespace
-- MAGIC
-- MAGIC Unity Catalog introduces the concept of a **catalog** into the hierarchy. As a container for schemas, the catalog provides a new way for organizations to segragate their data. This can be handy in many use cases. For example:
-- MAGIC
-- MAGIC * Separating data relating to business units within your organization (sales, marketing, human resources, etc)
-- MAGIC * Satisfying SDLC requirements (dev, staging, prod, etc)
-- MAGIC * Establishing sandboxes containing temporary datasets for internal use
-- MAGIC
-- MAGIC There can be as many catalogs as you like, which in turn can contain as many schemas as you like. To deal with this additional level, complete table/view references in Unity Catalog use a three-level namespace. 
-- MAGIC
-- MAGIC     <catalog>.<schema>.<table>`
-- MAGIC
-- MAGIC Here's an example:
-- MAGIC
-- MAGIC     SELECT * FROM mycatalog.myschema.mytable;
-- MAGIC
-- MAGIC If you already have data in a Databricks workspace's local Hive metastore or an external Hive metastore, Unity Catalog is **additive**: the workspace’s Hive metastore becomes one catalog within the 3-layer namespace (called `hive_metastore`) and tables in the Hive metastore can be accessed using three-level namespace notation: `hive_metastore.<schema>.<table>`.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a new catalog
-- MAGIC
-- MAGIC Each Unity Catalog metastore contains a default catalog named `main` with an empty schema called `default`.
-- MAGIC
-- MAGIC To create a new catalog, use the `CREATE CATALOG` command. You must be a metastore admin to create a new catalog.
-- MAGIC
-- MAGIC If using your own private environment you are free to choose your own catalog name, but if performing this exercise in a shared training environment, please use a unique one to avoid conflicts in the namespace.
-- MAGIC
-- MAGIC The following commands show how to:
-- MAGIC
-- MAGIC - Create a new catalog.
-- MAGIC - Select a catalog.
-- MAGIC - Show all catalogs.
-- MAGIC - Grant permissions on a catalog.
-- MAGIC - Show all grants on a catalog.

-- COMMAND ----------

-- TODO: create a new catalog named 'quick_start'
-- HINT: CREATE CATALOG IF NOT EXISTS your_catalog_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Following this **`CREATE`** statement, let's observe the effect by clicking the **Data** icon in the left sidebar. We see that there is a new catalog present, in accordance with the one we just created.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SQL developers will probably also be familiar with the **`USE`** statement to select a default schema, shortening queries by not having to specify it all the time. 
-- MAGIC Unity Catalog adds two additional statements to the language:
-- MAGIC
-- MAGIC     USE CATALOG mycatalog;
-- MAGIC     USE SCHEMA myschema;

-- COMMAND ----------

-- TODO: Set the current catalog

-- COMMAND ----------

-- Show all catalogs in a metastore
SHOW CATALOGS;

-- COMMAND ----------

-- Grant create schema, create table, & use catalog permissions to all users on the account
-- This also works for other account-level groups and individual users
GRANT CREATE SCHEMA, CREATE TABLE, USE CATALOG
ON CATALOG quickstart_catalog
TO `account users`;

-- COMMAND ----------

-- Check grants on the quickstart catalog
SHOW GRANT ON CATALOG quickstart_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and manage schemas
-- MAGIC Schemas, also referred to as databases, are the second layer of the Unity Catalog namespace. They logically organize tables and views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's create a new schema. For the name, we don't need to go to any effort to generate a unique name like we did for the catalog, since we are now in a brand new catalog that is isolated from everything else in the metastore.

-- COMMAND ----------

--- Create a new schema in the quick_start catalog
CREATE SCHEMA IF NOT EXISTS quickstart_schema
COMMENT "A new Unity Catalog schema called quickstart_schema";

-- COMMAND ----------

-- TODO: Show schemas in the selected catalog


-- COMMAND ----------

-- Describe a schema
DESCRIBE SCHEMA EXTENDED quickstart_schema;

-- COMMAND ----------

-- Drop a schema (uncomment the following line to try it. Be sure to re-create the schema before continuing with the rest of the notebook.)
-- DROP SCHEMA quickstart_schema CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a managed table
-- MAGIC
-- MAGIC *Managed tables* are the default way to create table with Unity Catalog. If no `LOCATION` is included, a table is created in the managed storage location configured for the metastore.
-- MAGIC
-- MAGIC The following commands show how to:
-- MAGIC - Select a schema.
-- MAGIC - Create a managed table and insert records into it.
-- MAGIC - Show all tables in a schema.
-- MAGIC - Describe a table.

-- COMMAND ----------

-- TODO: Use the current schema


-- COMMAND ----------

-- Create a managed Delta table and insert some records
CREATE TABLE IF NOT EXISTS quickstart_table AS (SELECT * FROM samples.nyctaxi.trips LIMIT 1000)

-- COMMAND ----------

-- View all tables in the schema
SHOW TABLES IN quickstart_schema;

-- COMMAND ----------

-- Describe this table
DESCRIBE TABLE EXTENDED quickstart_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC With the three level namespaces you can access tables in several different ways:
-- MAGIC - Access the table with a fully qualified name.
-- MAGIC - Select a default catalog and access the table using the schema and table name.
-- MAGIC - Select a default schema and use the table name.
-- MAGIC
-- MAGIC The following three commands are functionally equivalent.

-- COMMAND ----------

-- Query the table using the three-level namespace
SELECT
  *
FROM
  quickstart_catalog.quickstart_schema.quickstart_table;

-- COMMAND ----------

-- Set the default catalog and query the table using the schema and table name
USE CATALOG quickstart_catalog;
SELECT *
FROM quickstart_schema.quickstart_table;

-- COMMAND ----------

-- Set the default catalog and default schema and query the table using the table name
USE CATALOG quickstart_catalog;
USE quickstart_schema;
SELECT *
FROM quickstart_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drop Table
-- MAGIC If a *managed table* is dropped with the `DROP TABLE` command, the underlying data files are removed as well. 
-- MAGIC
-- MAGIC If an *external table* is dropped with the `DROP TABLE` command, the metadata about the table is removed from the catalog but the underlying data files are not deleted. 

-- COMMAND ----------

-- Drop the managed table. Uncomment the following line to try it out. Be sure to re-create the table before continuing.
-- DROP TABLE quickstart_catalog.quickstart_schema.quickstart_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manage permissions on data
-- MAGIC
-- MAGIC You use `GRANT` and `REVOKE` statements to manage access to your data. Unity Catalog is secure by default, and access to data is not automatically granted. Initially, all users have no access to data. Metastore admins and data object owners can grant and revoke access to users and groups. Grants are inherited on child securable objects.
-- MAGIC
-- MAGIC #### Ownership
-- MAGIC Each object in Unity Catalog has an owner. The owner can be any user, service principal, or group, all referred to in this notebook by the general term *principals*. A principal becomes the owner of a securable object when they create it or when ownership is transferred by using an `ALTER` statement.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Manage privileges
-- MAGIC The following privileges can be granted on Unity Catalog objects:
-- MAGIC - `CREATE CATALOG`: Allows the grantee to create catalogs within a metastore.
-- MAGIC - `CREATE EXTERNAL LOCATION`: When applied to a storage credential, allows the grantee to create an external location using the storage credential. When applied to a metastore, allows the grantee to create an external location.
-- MAGIC - `CREATE FUNCTION`: Allows the grantee to create functions within a catalog.
-- MAGIC - `CREATE SCHEMA`: Allows the grantee to create schemas within a catalog.
-- MAGIC - `CREATE TABLE`: Allows the grantee to create tables within a schema.
-- MAGIC - `EXECUTE`: Allows the grantee to to invoke a user defined function.
-- MAGIC - `MODIFY`: Allows the grantee to insert, update and delete data to or from a table.
-- MAGIC - `SELECT`: Allows the grantee to read data from a table or view.
-- MAGIC - `USE CATALOG`: This privilege does not grant access to the securable object itself, but allow the grantee to traverse the catalog in order to access its child objects. 
-- MAGIC - `USE SCHEMA`: This privilege does not grant access to the securable object itself, but allow the grantee to traverse the schema in order to access its child objects. For example, to select data from a table, a user needs the `SELECT` privilege on that table, the `USE SCHMEA` privilege on its parent schema, and the `USE CATALOG` privilege on its parent catalog. You can use this privilege to restrict access to sections of your data namespace to specific groups.
-- MAGIC
-- MAGIC Three additional privileges are relevant only to external tables and external storage locations that contain data files.
-- MAGIC
-- MAGIC - `CREATE EXTERNAL TABLE`: Allows the grantee to create an external table at a given external location.
-- MAGIC - `READ FILES`: Allows the grantee to read data files from a given external location.
-- MAGIC - `WRITE FILES`: Allows the grantee to write data files to a given external location.
-- MAGIC
-- MAGIC Privileges are inherited on child securable objects. Granting a privilege on a securable grants the privilege on its child securables.
-- MAGIC
-- MAGIC The following commands show how to:
-- MAGIC - Grant privileges.
-- MAGIC - Show grants on a securable object.
-- MAGIC - Revoke a privilege.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Grant a privilege**: Grants a privilege on securable objects to a principal. Only metastore admins and owners of the securable object can perform privilege granting.

-- COMMAND ----------

-- TODO: Grant USE SCHEMA on a schema to `account users`


-- COMMAND ----------

-- TODO: Grant SELECT privilege on a table to `account users`


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Show grants**: Lists all privileges that are granted on a securable object.

-- COMMAND ----------

-- Show grants on quickstart_table
SHOW GRANTS
ON TABLE quickstart_catalog.quickstart_schema.quickstart_table;

-- COMMAND ----------

-- TODO: Show grants on quickstart_schema


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Revoke a privilege**: Revokes a previously granted privilege on a securable object from a principal.

-- COMMAND ----------

-- Revoke select privilege on quickstart_table
REVOKE SELECT
ON TABLE quickstart_schema.quickstart_table
FROM `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a view
-- MAGIC Let's create a view that presents a processed view of the source data by filtering out rows with a distance > 5.

-- COMMAND ----------

CREATE OR REPLACE VIEW short_trips AS (
  SELECT *
  FROM quickstart_table
  WHERE trip_distance <= 5
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's query the view to observe the results of the filtering.

-- COMMAND ----------

SELECT * FROM short_trips

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your turn! Create a view by filtering the quickstart_table, using a criterion of your choice.

-- COMMAND ----------

-- TODO: Create a view from quickstart_table


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Granting privileges to a view
-- MAGIC Just like with tables, we can use use GRANT and REVOKE statements to manage access to views.
-- MAGIC
-- MAGIC For example, let's allow members of the *account users* group to query the *short_trips* view. In order to do this, they need **SELECT**, in addition to the **USAGE** grants on the containing schema and catalog that we set up earlier.

-- COMMAND ----------

-- TODO: Grant SELECT on short_trips to `account users`


-- COMMAND ----------

-- TODO: Show grants on short_trips


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating and managing user-defined functions
-- MAGIC
-- MAGIC Unity Catalog is capable of managing user-defined functions within schemas as well. For this example, we'll set up simple function that masks all but the last two characters of a string.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cc_mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", 4), RIGHT(x, 4))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's use the **`DESCRIBE`** command to get information about this function we just created.

-- COMMAND ----------

DESCRIBE FUNCTION cc_mask

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's try it out. Once again, we are the data owner so no grants are required.

-- COMMAND ----------

-- running the function with a fake, invalid cc card number
SELECT cc_mask('4916-4811-5814-8111') AS data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Let's uncomment and run the following cell to remove the catalog that we have created. The **`CASCADE`** qualifier will remove the catalog along with any contained elements.

-- COMMAND ----------

-- Let's clean-up our workspace by deleting our catalog
-- DROP CATALOG IF EXISTS quickstart_catalog CASCADE;
