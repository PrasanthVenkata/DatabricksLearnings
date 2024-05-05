# Databricks notebook source
""" Upload CountrySales.csv into dbfs:/FileStore/tables/ location
"""
df = spark.read.csv("dbfs:/FileStore/tables/CountrySales.csv", header=True, inferSchema=True)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("Sales")

# COMMAND ----------

# Check the table type
spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended Sales

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/sales

# COMMAND ----------

# MAGIC %fs head dbfs:/user/hive/warehouse/sales/part-00000-d3614a36-e334-4739-9ada-2ff6d61dfe0c-c000.snappy.parquet

# COMMAND ----------

df_parquet = spark.read.format("delta").load("dbfs:/user/hive/warehouse/sales/")
display(df_parquet)

# COMMAND ----------

# how to create a local temporary view
df.createOrReplaceTempView("v_Sales")

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS msALES AS SELECT * FROM v_Sales")

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended msales

# COMMAND ----------

df.write.format("csv").mode("overwrite").saveAsTable("Sales_CSV")

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

df.write.format("json").mode("overwrite").saveAsTable("Sales_JSON")

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

df.write.format("parquet").mode("overwrite").saveAsTable("Sales_Parquet")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/sales
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %fs ls dbfs:/user/hive/warehouse/sales_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC /* meaning 1 =2 returns false, meaning create an empty table */
# MAGIC create table if not exists m_salesInfo using delta as select * from v_sales where 1=2

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into m_salesInfo select * from v_sales;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended m_salesInfo
