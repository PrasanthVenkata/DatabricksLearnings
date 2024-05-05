# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/XML

# COMMAND ----------

# Install spark-xml Maven package as a library - this is the pre-requisite
df =  spark.read.format("xml").option("rootTag", "cloudmonks").option("rowTag", "Employee").option("inferSchema", True).load("dbfs:/FileStore/tables/XML/Employees.xml")
display(df)


# COMMAND ----------

df.schema

# COMMAND ----------

# Reading data from simple XML file format
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

employee_schema = StructType([StructField('Employee_ID', IntegerType(), True), StructField('Name', StringType(), True), StructField('Job', StringType(), True), StructField('Salary', LongType(), True), StructField('Department_ID', IntegerType(), True)])
df =  spark.read.format("xml").option("rootTag", "cloudmonks").option("rowTag", "Employee").schema(employee_schema).load("dbfs:/FileStore/tables/XML/Employees.xml")
display(df)


# COMMAND ----------

# Working with Nested XML files
# Install spark-xml Maven package as a library - this is the pre-requisite
df_Nested =  spark.read.format("xml").option("rootTag", "cloudmonks").option("rowTag", "Employee").option("inferSchema", True).load("dbfs:/FileStore/tables/XML/Employees_Nested.xml")
display(df_Nested)

# here skillset is StructType
df_Nested.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_employee_new = df_Nested.select("Employee_ID","Name", "Job","Salary", col("SkillSet.*"), "Department_ID")
display(df_employee_new)

# COMMAND ----------

from pyspark.sql.functions import col
df_employee_new = df_Nested.select("Employee_ID","Name", "Job","Salary", col("Skillset.PrimarySkill").alias("Primary skill"),col("SkillSet.SecondarySkill").alias('Secondary skill'), "Department_ID")
display(df_employee_new)
