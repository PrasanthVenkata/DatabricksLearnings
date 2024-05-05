# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs 

# COMMAND ----------

def power (m,n): return m**n

# COMMAND ----------

x = power(3, 2)
print (x)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists trainingDB

# COMMAND ----------

# MAGIC %sql
# MAGIC use trainingDB

# COMMAND ----------

# MAGIC %sql
# MAGIC create table  if not exists customer
# MAGIC (customerid integer,
# MAGIC firstname varchar(20),
# MAGIC lastname varchar(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC describe customer

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended customer

# COMMAND ----------

# MAGIC %scala
# MAGIC println("This is my statement in Scala")

# COMMAND ----------

x=100
print(type(x))

y = 100.1
print( type(y))

# COMMAND ----------

# MAGIC %r
# MAGIC print("This is my first notebook", quote=FALSE)

# COMMAND ----------

# MAGIC %fs mkdirs /dbfs:/FileStore/Createdon23

# COMMAND ----------

# MAGIC %fs ls /dbfs:/FileStore

# COMMAND ----------

# MAGIC %fs ls
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh ls /usr

# COMMAND ----------

# MAGIC %md
# MAGIC #types of clusters in Azure databricks
# MAGIC ## Second level
# MAGIC ### third level
# MAGIC ## *This is in italivs*
# MAGIC ## **This is in bold**

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %pip install xlrd
