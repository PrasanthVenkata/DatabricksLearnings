# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists TrainingDB

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS SALES")

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

spark.catalog.listDatabases()

# COMMAND ----------

display(spark.catalog.listDatabases())

# COMMAND ----------

spark.catalog.currentCatalog()

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.databaseExists('hr')

# COMMAND ----------

# MAGIC %sql 
# MAGIC use database trainingdb

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists Customers;
# MAGIC
# MAGIC create table if not exists Customers (CUSTOMERNUMBER INT, CUSTOMERNAME STRING,
# MAGIC address string,
# MAGIC city string);

# COMMAND ----------

spark.catalog.listColumns('trainingDB.Customers')

# COMMAND ----------

spark.catalog.getDatabase('trainingDB')

# COMMAND ----------

spark.catalog.listTables('trainingDB')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe customers

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended customers

# COMMAND ----------

# Reading data from CSV file
df = spark.read.csv("dbfs:/FileStore/tables/CountrySales.csv", inferSchema=True, header=True, sep=",")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table Sales

# COMMAND ----------



# COMMAND ----------

df.createOrReplaceTempView('view1')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Sales as select * from view1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended Sales
