# Databricks notebook source
df = spark.read.format("csv").option("header", True).option("inferSchema", True).option("sep", ",").load("dbfs:/FileStore/tables/CountrySales.csv")

# COMMAND ----------

df1 = df.select("Region", "Country", "ItemType" , "SalesChannel", "UnitsSold", "TotalRevenue")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/CountrySales/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/CountrySales/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/CountrySales/_delta_log/
# MAGIC

# COMMAND ----------

#dbutils.fs.rm("dbfs:/FileStore/tables/CountrySales/", True)


# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/FileStore/tables/CountrySales/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/tables/CountrySales/`

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`dbfs:/FileStore/tables/CountrySales/` set unitsSold = 1000 where country = 'Libya'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/tables/CountrySales/` where country = 'Libya'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/FileStore/tables/CountrySales/`

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.`dbfs:/FileStore/tables/CountrySales/` where country = 'Armenia'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/tables/CountrySales/` where country = 'Armenia'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/FileStore/tables/CountrySales/`

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/CountrySales/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC select count( *) from delta.`dbfs:/FileStore/tables/CountrySales/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`dbfs:/FileStore/tables/CountrySales/` version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`dbfs:/FileStore/tables/CountrySales/` version as of 2

# COMMAND ----------

# MAGIC %md
# MAGIC Now rollback delete operation

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table delta.`dbfs:/FileStore/tables/CountrySales/` to version as of 1

# COMMAND ----------

# MAGIC %md
# MAGIC Now get the count (*) so that we have come to the previous state

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`dbfs:/FileStore/tables/CountrySales/` 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/FileStore/tables/CountrySales/` 
