# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate secret for application
# MAGIC 3. Set Spark Config with AppId, ClientId, Directory, TenantId, Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-clientid')
tenant_id = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-tenantid')
client_secret = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-clientsecret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1winterdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1winterdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1winterdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1winterdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1winterdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@f1winterdl.dfs.core.windows.net/"))

# COMMAND ----------


