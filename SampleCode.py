# Databricks notebook source
x=100
y=300
print("Sum of two numbers =", x+y)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database SampleDB

# COMMAND ----------

# MAGIC %sql
# MAGIC use SampleDB
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists  Customer1
# MAGIC (empID varchar(10),
# MAGIC empName varchar(300))
