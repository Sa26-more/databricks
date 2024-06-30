# Databricks notebook source
print("Hello World!")

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list('rk-scope')

# COMMAND ----------

application_id = dbutils.secrets.get('rk-scope','rk-application-id')
directory_id = dbutils.secrets.get('rk-scope','rk-directory-id')
service_credential = dbutils.secrets.get('rk-scope','rk-service-credential')

# COMMAND ----------

storage_account_name = "storagepracticepune"
container_name = "databricks-basic"
directory_name = "input"

# COMMAND ----------

mount_point = "/mnt/spractice"
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": service_credential,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
    "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}
# Check if mount point already exists
if not any(mount_point in mp for mp in dbutils.fs.mounts()):
    # Mount Point with Service Principal
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_name}",
        mount_point=mount_point,
        extra_configs=configs
    )

# COMMAND ----------

dbutils.fs.ls("/mnt/spractice")

# COMMAND ----------

df = spark.read.option("header", "true").csv("/mnt/spractice/vgsales.csv")
df.display()

# COMMAND ----------

from pyspark.sql.functions import countDistinct

display(df.agg(countDistinct("Platform")))

# COMMAND ----------

from pyspark.sql.functions import col
df = df.repartition(31, col("Platform"))

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.write.mode("overwrite").csv("/mnt/spractice/repartition")

# COMMAND ----------

df.write.mode("overwrite").partitionBy("Platform").parquet("/mnt/spractice/partition")
