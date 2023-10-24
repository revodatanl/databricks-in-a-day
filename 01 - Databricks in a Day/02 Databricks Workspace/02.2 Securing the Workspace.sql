-- Databricks notebook source
-- INCLUDE_HEADER_TRUE
-- INCLUDE_FOOTER_TRUE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Securing the workspace
-- MAGIC
-- MAGIC In this lab you will learn how to:
-- MAGIC * Use entitlements to control access to Databricks services
-- MAGIC * Secure access to workspace assets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview
-- MAGIC
-- MAGIC In Databricks, you can use access control lists (ACLs) to configure permission to access workspace objects like folders, notebooks, clusters, and more. Employing the use of such ACLs allows you to secure the workspace providing access to resources to only those who need it.
-- MAGIC
-- MAGIC All workspace administrators can manage access control lists, as can users who have been given delegated permissions to manage access control lists.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC
-- MAGIC If you would like to follow along with this lab, you will need:
-- MAGIC * Workspace administrator capabilities
-- MAGIC * Additional group named *analysts* created at the account level with a secondary users as a member, as covered in the *"Managing account identities"* lab
-- MAGIC * *analysts* group must be assigned to the workspace, as covered in the *Managing identities in the workspace* lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Entitlements
-- MAGIC
-- MAGIC The workspace defines a small set of high-level capabilities referred to as **entitlements**. Currently defined entitlements include:
-- MAGIC * **Admin**: provides workspace administrator capabilities
-- MAGIC * **Workspace access**: allow access the the *Data Science & Engineering Workspace* and *Databricks Machine Learning* services.
-- MAGIC * **Databricks SQL access**: allow access to the *Databricks SQL* service.
-- MAGIC * **Allow unrestricted cluster creation**: unrestricted ability to create all-purpose clusters or SQL warehouses, bypassing any policies that might be in effect.
-- MAGIC
-- MAGIC Entitlements can be granted to individual users or service principals, or they can be granted to groups in which case they are inherited by all members of the group. As an example of this principle in motion, the **Workspace access** and **Databricks SQL access** are granted to all users by default, through entitlements on the special workspace local *users* group. You can disable these if desired by editing the entitlements for that group.
-- MAGIC
-- MAGIC Security best practices suggest we should manage such entitlements at the group level, but in an identity federated workspace we must work on the principals as they are assigned. If users and service principals are assigned individually, then entitlements must likewise be assigned individually. If the group structure you have doesn't work well for administering the workspace, then consider collaborating with your account aministrator to define a group structure to better address the needs of the workspace environment. There's no restriction on how many groups users can belong to, so the situation might call for defining groups that meet data governance needs precisely, along with a different set of groups to address the workspace requirements adequately.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Adding group entitlements
-- MAGIC
-- MAGIC Let's grant the ability to perform unrestricted cluster creation to all users.
-- MAGIC 1. Open the workspace admin console. In the left sidebar, select **Settings > Admin Console**.
-- MAGIC 1. In the admin console, click the **Groups** tab.
-- MAGIC 1. Select the *users* group.
-- MAGIC 1. Click the **Entitlements** tab.
-- MAGIC 1. Enable **Allow unrestricted cluster creation**.
-- MAGIC
-- MAGIC Note: in general we don't recommend leaving this setting enabled for all users as it provides them considerable leeway in creating their own compute resources which could lead to a costly bill.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Removing group entitlements
-- MAGIC
-- MAGIC Let's restrict members of the **analysts** group to Databricks SQL only; that is, disable **Workspace access**.
-- MAGIC 1. In the admin console, click the **Groups** tab.
-- MAGIC 1. Select the *analysts* group.
-- MAGIC 1. Click the **Entitlements** tab.
-- MAGIC 1. Clear the **Workspace access** checkbox.
-- MAGIC
-- MAGIC However, it isn't as simple as that! Entitlements are additive across all the groups a user belongs to, and recall we mentioned earlier that **Workspace access** is granted by default to the *users* group, which spans all users in the workspace. Therefore, in order to really revoke workspace access, we must disable this entitlement on that group as well.
-- MAGIC
-- MAGIC 1. From where we left off, let's click **Groups**.
-- MAGIC 1. Select the *users* group
-- MAGIC 1. Click the **Entitlements** tab.
-- MAGIC 1. Clear the **Workspace access** checkbox.
-- MAGIC 1. While we're at it, let's clear **Allow unrestricted cluster creation** which we just set a moment ago.
-- MAGIC
-- MAGIC One important thing to note about what we've done here: given the current group structure, this was the only way to achieve the desired result. But it means that as of now, all users will not be able to access the workspace, which might not be a desireable side-effect. To properly solve this, we need a better group structure. In particular, we need another group to catch the subset of users that should have access to the workspace.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing user entitlements
-- MAGIC
-- MAGIC For users or service principals that were assigned to the workspace individually, we likewise manage entitlements individually.
-- MAGIC 1. In the admin console, click the **Users** tab.
-- MAGIC 1. Locate the desired user.
-- MAGIC 1. Enable or disable the entitlements as needed. Note the following:
-- MAGIC    * If the checkbox is noninteractive (that is, it can't be enabled/disabled) this is an indicator that the entitlement is inherited from a group. If this is the case, then we need to look at the user's group membership and consider making the entitlement change there. Hovering over the checkbox will tell us which group the entitlement is inherited from.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Workspace access control
-- MAGIC
-- MAGIC To facilitate a more secure workspace environment with the ability to exercise control over workspace assets, we must ensure that workspace access control is enabled (although that is the default for new workspaces).
-- MAGIC
-- MAGIC Databricks workspaces provide a fine-grained security model that allows you to exercise access control over:
-- MAGIC * Workspace assets (Notebooks)
-- MAGIC * Compute resources (clusters, pools and jobs)
-- MAGIC * Personal access tokens (the ability to generate secondary credentials for use with command-line tools or APIs)
-- MAGIC
-- MAGIC All access control is enabled by default. Let's see how we can view and reconfigure these settings.
-- MAGIC 1. In the admin console, click the **Workspace settings** tab.
-- MAGIC 1. Review the settings in the **Access Control** group.
-- MAGIC
-- MAGIC Notice that there is a **Table Access Control** item. This setting pertains to tables in the legacy metastore and is independent of the security that Unity Catalog brings.
-- MAGIC
-- MAGIC With workspace access control enabled, administrators can then control access to assets in the workspace, and users can control access to their own assets.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Controlling access to workspace objects and Repos
-- MAGIC
-- MAGIC Let's view the ACL for our own folder in the workspace.
-- MAGIC
-- MAGIC 1. In the *Data Science & Engineering Workspace*, click **Workspace** in the left sidebar.
-- MAGIC 1. Navigate to your user folder in the *Users* folder.
-- MAGIC 1. Click the chevron to the right of your folder's menu item and select **Permissions**.
-- MAGIC
-- MAGIC From here we can review the existing permissions, which are:
-- MAGIC * Our own id has **Can Manage** permissions by virtue of ownership; however we can change or revoke this grant.
-- MAGIC * Workspace administrators (that is, the special workspace local *admins* group) also has **Can Manage**, and this cannot be changed or revoked.
-- MAGIC
-- MAGIC Let's allow users in the workspace read access to our folder.
-- MAGIC 1. Open the dropdown menu in the **NAME** column. Select the *all users* entry in the **Groups** section
-- MAGIC 1. In the **PERMISSION** column, select *Can Read*.
-- MAGIC 1. Click **Add**.
-- MAGIC 1. Click **Save**.
-- MAGIC
-- MAGIC Note the following:
-- MAGIC * Revoking permissions can be done in the same manner, by selecting the **X** in the appropriate row.
-- MAGIC * Access control can also be done on individual objects within folders, following a similar workflow.
-- MAGIC * Access control can also be done on Repos following a similar workflow. In this case access control applies to the entire Repo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Controlling access to clusters
-- MAGIC
-- MAGIC Let's view the ACL for our a cluster that we have access to.
-- MAGIC
-- MAGIC 1. In the *Data Science & Engineering Workspace*, click **Compute** in the left sidebar.
-- MAGIC 1. The clusters to which you have access are shown. Select one.
-- MAGIC 1. Select **More > Permissions**.
-- MAGIC 1. From here, the UI functionality is similar to that used for notebook and folder access control. Let's allow everyone access to the cluster by selecting *all users* and *Can Restart* for **NAME** and **PERMISSION** respectively.
-- MAGIC 1. Click **Add** then **Save**.
-- MAGIC
-- MAGIC Please note that *Single user* clusters have a **Single user access** setting that only allows a dedicated user to connect, irrespective of the cluster permissions. Cluster permissions are most effective on the other cluster types.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Controlling access to workflows
-- MAGIC Databricks workflows allow you to orchestrate the execution of jobs and pipelines on a schedule or on demand. Each of these constructs supports access control.
-- MAGIC 1. In the *Data Science & Engineering Workspace*, click **Workflows** in the left sidebar.
-- MAGIC 1. Select **Jobs** or **Delta Live Tables**.
-- MAGIC 1. Select the desired entity.
-- MAGIC 1. For jobs, select **Edit permissions** in the column on the right. For Delta Live Tables, select **Permissions** from the top.
-- MAGIC 1. From here, the UI functionality is similar to that used for the others. One unique property of jobs and pipelines is that both include an *Is Owner* privilege that designates ownership and allows you to change ownership.