# Databricks notebook source
# MAGIC %md
# MAGIC ## this is syntax
# MAGIC service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")
# MAGIC spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")
# MAGIC

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("appSecretScope")

# COMMAND ----------

dbutils.secrets.help()


# COMMAND ----------

service_credential = dbutils.secrets.get(scope="appSecretScope",key="appSecret")
display(service_credential)

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope="appSecretScope", key= "dirid")
spark.conf.set("fs.azure.account.auth.type.storagepra124.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storagepra124.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storagepra124.dfs.core.windows.net", dbutils.secrets.get(scope="appSecretScope", key="appId"))
spark.conf.set("fs.azure.account.oauth2.client.secret.storagepra124.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storagepra124.dfs.core.windows.net", "https://login.microsoftonline.com/" + tenant_id +"/oauth2/token")

# COMMAND ----------

tenant_id = dbutils.secrets.get(scope="appSecretScope", key= "dirid")
spark.conf.set("fs.azure.account.auth.type.prastorage1234.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.prastorage1234.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.prastorage1234.dfs.core.windows.net", dbutils.secrets.get(scope="appSecretScope", key="appId"))
spark.conf.set("fs.azure.account.oauth2.client.secret.prastorage1234.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.prastorage1234.dfs.core.windows.net", "https://login.microsoftonline.com/" + tenant_id +"/oauth2/token")

# COMMAND ----------

source_path = "abfss://source@prastorage1234.dfs.core.windows.net/sales/"
df= spark.read.csv(source_path, header=True, inferSchema=True, sep=",")
display(df)

# COMMAND ----------

target_path = "abfss://target@prastorage1234.dfs.core.windows.net/output/"
df.write.format("delta").partitionBy("REGION", "COUNTRY").mode("overwrite").save(target_path)

