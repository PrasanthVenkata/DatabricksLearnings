# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/prasanth/

# COMMAND ----------


# %fs ls dbfs:/FileStore/tables/prasanth/csv
df_employee = spark.read.csv("dbfs:/FileStore/tables/prasanth/csv/Employees_South.csv", header=True, inferSchema=True, sep=",")
display(df_employee)


# COMMAND ----------

df_employee.count()

# COMMAND ----------

employee_schema =  df_employee.schema
display(employee_schema)

# COMMAND ----------

source_filePath = "dbfs:/FileStore/tables/prasanth/csv/*.csv"
df_frame =  spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", "-").load(source_filePath)
display(df_frame)

# COMMAND ----------

from pyspark.sql.types import * 
schema_manually = StructType([StructField('empno', IntegerType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('city', StringType(), True), StructField('email', StringType(), True), StructField('hiredate', StringType(), True), StructField('phone', LongType(), True), StructField('salary', IntegerType(), True)])
df_manually = spark.createDataFrame(schema = schema_manually, data = [])
display(df_manually)


# COMMAND ----------

file_List = dbutils.fs.ls("dbfs:/FileStore/tables/prasanth/csv")
display("File list", file_List)
display("Type of file list is ", type(file_List))

# COMMAND ----------

file_List1 =  dbutils.fs.ls ('dbfs:/FileStore/tables/prasanth/csv')
display(file_List1)
for fileInfo in file_List1:
  print(fileInfo.path)
  fileContent= spark.read.csv(fileInfo.path, header=True, schema = schema_manually, sep=",")
  df_manually =df_manually.unionAll(fileContent)
  display(df_manually)


# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/tables/prasanth/csv/username_password_recovery_code.csv

# COMMAND ----------

from pyspark.sql.functions import to_date
df_main_new = df_manually.withColumn("hiredate", to_date("hiredate", "M/dd/yyyy") )
display(df_main_new)
df_main_renamed = df_main_new.withColumnRenamed("hiredate", "Hire Date")
display(df_main_renamed)
