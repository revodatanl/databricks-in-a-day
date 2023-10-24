-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Managing identities in the workspace
-- MAGIC
-- MAGIC In this lab, you will learn how to:
-- MAGIC * Assign users, service principals and groups to a workspace
-- MAGIC * Distribute administrative tasks by reassigning administration capabilities to others

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview
-- MAGIC
-- MAGIC We have seen that Databricks has identities defined in the account which are distinct from those defined in the workspace, though they share many properties. In this lab, we will take a look at how these relate to workspace identities, and the various tasks associated with administering workspace capabilities and access control.
-- MAGIC
-- MAGIC Please note that the material covered in this lab relates to workspaces that have Unity Catalog enabled, which is now a recommended best practice. Also, this activity relates to Azure Databricks only.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC
-- MAGIC If you would like to follow along with this demo, you will need:
-- MAGIC * A workspace with the Unity Catalog enabled
-- MAGIC * Workspace admin capabilities
-- MAGIC * Additional group named *analysts* created at the account level with one or two secondary users as members, as covered in the *"Managing account-level principals"* lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Users and service principals
-- MAGIC
-- MAGIC Prior to Unity Catalog, workspace users were created (and deleted) through the workspace admin console, and they were local to the workspace in which they were created.
-- MAGIC
-- MAGIC With the arrival of Unity Catalog, its use of account-level identities requires the creation of users and service principals at the account level. But, adding these at the account level alone will not provide them with the ability to develop Databricks assets like notebooks, jobs and dashboards. In order to access the development environment, a user needs an identity at the workspace level.
-- MAGIC
-- MAGIC To avoid the need to duplicate and manage identities at both levels, identity federation allows administrators to assign account-level identities from the account to one or more workspaces as needed.
-- MAGIC
-- MAGIC This assignment can be done in two ways:
-- MAGIC * The account administrator can "push" user or service principals to one or more workspaces from the account console
-- MAGIC * A workspace administrator can "pull" users or service principals from the account to the workspace

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Adding users from the workspace admin console
-- MAGIC
-- MAGIC Workspace administrators can assign account-level users to the workspace they manage, or create new users.
-- MAGIC
-- MAGIC Let's assign an existing user now.
-- MAGIC 1. Open the workspace admin console. In the left sidebar, select **Settings > Admin Console**.
-- MAGIC 1. Let's click the **Users** tab.
-- MAGIC 1. Click **Add user**.
-- MAGIC 1. Select the desired user from the dropdown.
-- MAGIC 1. Click **Add**.
-- MAGIC The new user will be issued an email inviting them to join the workspace. They will be able to log in using their established email and password.
-- MAGIC
-- MAGIC Now let's create a new user.
-- MAGIC 1. Still in the **Users** tab, click **Add user** again.
-- MAGIC 1. Open the dropdown, and click **Add new user**.
-- MAGIC 1. Supply the email address for the new user. For the purposes of this training exercise, I am using a temporary email address courtesy of <a href="https://www.dispostable.com/" target="_blank">dispostable.com</a>.
-- MAGIC The new user will be issued an email inviting them to set up their password and join the workspace. Note that in this case, a corresponding account-level identity will be created for this new user automatically.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Removing users from the workspace admin console
-- MAGIC Workspace administrators can similarly remove users from the workspace they manage. This does not affect the corresponding account-level identity or its presence in other workspaces; it merely removes access to the workspace in question.
-- MAGIC 1. In the admin console, click the **Users** tab.
-- MAGIC 1. Locate the desired user.
-- MAGIC 1. Click the **X** at the right side.
-- MAGIC 1. Confirm the removal.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing service principals from the workspace admin console
-- MAGIC
-- MAGIC The workflow for assigning or creating service principals from the workspace admin console is very similar to that for users. Let's try that now.
-- MAGIC 1. In the admin console, let's click the **Service principals** tab.
-- MAGIC 1. Click **Add service principal**.
-- MAGIC 1. Select the desired service principal from the dropdown.
-- MAGIC 1. Click **Add**.
-- MAGIC
-- MAGIC If we chose to create a new service principal, this would automatially create a corresponding account-level identity - just like it does for newly created users.
-- MAGIC
-- MAGIC To remove a service principal:
-- MAGIC 1. In the admin console, click the **Service principals** tab.
-- MAGIC 1. Locate the desired service principal.
-- MAGIC 1. Click the **X** at the right side.
-- MAGIC 1. Confirm the removal.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Groups
-- MAGIC
-- MAGIC Groups can be assigned and removed from workspaces in the same manner as users and service principals, however note the following:
-- MAGIC * We cannot create new groups from the workspace admin console like we can for users and service principals
-- MAGIC * When assigned to a workspace, groups are managed as atomic units. You cannot assign, remove, or entitle group members individually. Group membership can only be modified at the account level by an account administrator.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Adding a group
-- MAGIC
-- MAGIC Let's assign a group to our workspace.
-- MAGIC
-- MAGIC 1. In the admin console, click the **Groups** tab.
-- MAGIC 1. Let's click **Add Group**.
-- MAGIC 1. Select the desired group from the dropdown.
-- MAGIC 1. Click **Add**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Removing a group
-- MAGIC
-- MAGIC To remove a group:
-- MAGIC 1. In the admin console, click the **Groups** tab.
-- MAGIC 1. Locate the desired group.
-- MAGIC 1. Click the **X** at the right side.
-- MAGIC 1. Confirm the removal.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Distributing administrative responsibilities
-- MAGIC
-- MAGIC Workspace admins can perform these workspace management tasks:
-- MAGIC * Assign, create and remove users and service principals
-- MAGIC * Assign and remove groups
-- MAGIC * Configure workspace access control to provide a more secure environment that restricts who has access to which assets.
-- MAGIC * Control who has access to which services (Data Science and Engineering/Machine Learning workspace, Databricks SQL, cluster management)
-- MAGIC * Control other aspects of the environment, such as global cluster initialization scripts, cluster settings, and configuring other workspace and Databricks SQL settings 
-- MAGIC * Create and control access to compute resources through entitlements and policies
-- MAGIC
-- MAGIC Let's temporarily grant administrative capabilities to a user in our workspace.
-- MAGIC 1. In the admin console, click the **Users** tab.
-- MAGIC 1. Locate the desired user.
-- MAGIC 1. Enable the **Admin** entitlement.
-- MAGIC
-- MAGIC Revocation can be accomplished by repeating the steps and clearing the **Admin** entitlement.
-- MAGIC
-- MAGIC Though it's presented as an entitlement in the user interface, **Admin** is not a real entitlement like the others. There actually exists a special, local group call *admins* that is built in to each workspace. Members of this group have administrative capabilities. Adding or removing users from this group is accomplished through this entitlement.