# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="prasScope0505")

# COMMAND ----------

storageAccountAccessKey = dbutils.secrets.get(scope="prasScope0505", key="keyblob")
print(storageAccountAccessKey)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.storacc0505.blob.core.windows.net",  "ldip/Ln4RrzNGbzjWR68D/VKVbiEuSB96nmifB9m96/dyg91znKo1dXoXXgqZgpXNN33ZQVtmLf++AStkqm9lw==" )


# COMMAND ----------

# MAGIC %md
# MAGIC ### this above step or below step. Key is exposed above. Key is redacted below.

# COMMAND ----------

spark.conf.set("fs.azure.account.key.storacc0505.blob.core.windows.net", storageAccountAccessKey)


# COMMAND ----------

sourcePath = "wasbs://source@storacc0505.blob.core.windows.net"
df = spark.read.format("csv").option("header", True).option("delimiter", ",").option("inferSchema", True).load(sourcePath)

# COMMAND ----------

df_select = df.select("Region","Country","OrderDate","UnitPrice","UnitCost","TotalRevenue","TotalCost","TotalProfit")

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df_select.write.format("parquet").partitionBy("country").mode("overwrite").saveAsTable("Sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended   Sales

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/sales
