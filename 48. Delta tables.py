# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS spark_catalog.default.EMPLOYEES;
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/employees", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists EMPLOYEES
# MAGIC (
# MAGIC   ID INT,
# MAGIC   NAME STRING,
# MAGIC   AGE INT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED EMPLOYEES

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history employees

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/user/hive/warehouse/employees`

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees/_delta_log/
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employees values (2,'name1', 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employees values (3,'name2', 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/user/hive/warehouse/employees`

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees/_delta_log/
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`dbfs:/user/hive/warehouse/employees` set age = 10 where id =2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/user/hive/warehouse/employees`

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`dbfs:/user/hive/warehouse/employees` set age = 11 where id =3

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/user/hive/warehouse/employees`

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees/ 

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.`dbfs:/user/hive/warehouse/employees` where id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/user/hive/warehouse/employees/`

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark style history

# COMMAND ----------

from delta.tables import DeltaTable
employees = DeltaTable.forName(spark, "Employees")
df =  employees.history().select("version", "operationParameters", "operation", "operationMetrics")
display(df)

# COMMAND ----------

df = spark.sql("describe history employees")
df1 = df.select("version", "timeStamp", "operationMetrics")
display (df1)

# COMMAND ----------

df1 = df.selectExpr("max(version)")
display (df.selectExpr("max(version)"))

# COMMAND ----------

display(df1.collect()[0][0])
maxVersionNum = df1.collect()[0][0]

# COMMAND ----------

dfLatestVersion = spark.read.format("delta").option("versionAsOf", maxVersionNum).load("dbfs:/user/hive/warehouse/employees/")
display(dfLatestVersion)

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table Employees to version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/user/hive/warehouse/employees/`

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table Employees to version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/user/hive/warehouse/employees/`
