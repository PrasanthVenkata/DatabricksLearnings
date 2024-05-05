# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

# Read the data from CSV file
df = spark.read.csv("dbfs:/FileStore/tables/CountrySales.csv", header=True, inferSchema=True, sep=",")
display(df)

# COMMAND ----------

# Get the number of partitions in the data frame
df.rdd.getNumPartitions()

# COMMAND ----------

df2 = df.repartition(4)

# COMMAND ----------

# Get the number of partitions in the data frame
df2.rdd.getNumPartitions()

# COMMAND ----------

df2.write.format("delta").mode("overwrite").option("path", "dbfs:/FileStore/tables/delta").saveAsTable("Sales")

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

spark.sql("select * from sales")

# COMMAND ----------

display(spark.sql("select * from sales"))

# COMMAND ----------

spark.catalog.isCached("sales")

# COMMAND ----------

#spark.catalog.cacheTable("sales", "MEMORY_AND_DISK")

# COMMAND ----------

spark.catalog.isCached("sales")

# COMMAND ----------

spark.catalog.cacheTable("sales")

# COMMAND ----------

spark.catalog.isCached("sales")

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

spark.catalog.isCached("sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE EXTERNAL TABLE 
# MAGIC -- CTAS - CREATE TABLE AS SELECT STATEMENT
# MAGIC
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS Sales_Ext
# MAGIC using DELTA
# MAGIC LOCATION "dbfs:/FileStore/tables/prasanth/delta"
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended Sales_Ext

# COMMAND ----------

spark.catalog.listTables() 

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/prasanth/delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS Sales_Ext_PARQUET
# MAGIC using PARQUET
# MAGIC LOCATION "dbfs:/FileStore/tables/prasanth/PARQUET"
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended Sales_Ext_PARQUET

# COMMAND ----------

spark.catalog.listTables() 

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/prasanth/PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS Sales_Ext_CSV
# MAGIC using CSV
# MAGIC LOCATION "dbfs:/FileStore/tables/prasanth/CSV"
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended Sales_Ext_CSV

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/prasanth/CSV
