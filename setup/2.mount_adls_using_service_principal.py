# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id, client_secret from key valut
# MAGIC 2. Set Spark Config with AppId, ClientId, Directory, TenantId, Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC
# MAGIC Note: Mounts are a legacy access pattern. Databricks recommends using Unity Catalog for managing all data access, but it's only available for Premium tier!

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-clientid')
tenant_id = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-tenantid')
client_secret = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-clientsecret')

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://raw@f1winterdl.dfs.core.windows.net/",
    mount_point = "/mnt/f1winterdl/raw",
    extra_configs = configs
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1winterdl/raw"))

# COMMAND ----------

display(spark.read.csv("/mnt/f1winterdl/raw/circuits.csv"))

# COMMAND ----------

# get all the current mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# unmount
dbutils.fs.unmount("/mnt/f1winterdl/raw")

# COMMAND ----------

# verify that it's deleted
display(dbutils.fs.mounts())

# COMMAND ----------

# create function to mount for all containers
def mount_adls(storage_acc_name, container_name):
    # get secrets from key vault
    client_id = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-clientid')
    tenant_id = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-tenantid')
    client_secret = dbutils.secrets.get(scope = 'f1-winter-scope', key = 'f1winterdl-clientsecret')

    # set spark config
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    # unmount the mount if it exists
    if any(mount.mountPoint == f"/mnt/{storage_acc_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_acc_name}/{container_name}")
    
    # mount storage account container
    return dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_acc_name}/{container_name}",
        extra_configs = configs
    )


# COMMAND ----------

mount_adls("f1winterdl", "raw")
mount_adls("f1winterdl", "processed")
mount_adls("f1winterdl", "presentation")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


