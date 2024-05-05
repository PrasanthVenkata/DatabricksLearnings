# Databricks notebook source
df_employee = spark.read.format("csv").option("inferSchema", True).option("delimiter", ",").option("header", True).load("dbfs:/FileStore/tables/employee.csv")
display(df_employee)

# COMMAND ----------

from pyspark.sql.functions import col
df_employee_alias = df_employee.alias("emp")
df_employee.join(df_employee_alias)

df_selfJoin = df_employee.alias("employee").join(df_employee.alias("manager"), on=col("employee.MANAGER_ID")== col("manager.EMPLOYEE_ID"), how = "inner").selectExpr("EMPLOYEE.EMPLOYEE_ID", "EMPLOYEE.FIRST_NAME", "EMPLOYEE.LAST_NAME", "MANAGER.FIRST_NAME", "MANAGER.LAST_NAME")

display(df_selfJoin) 


# COMMAND ----------

#Union
df_employee_East = spark.read.format("csv").option("inferSchema", True).option("delimiter", ",").option("header", True).load("dbfs:/FileStore/tables/Employees_East_lyst1432.csv")
display(df_employee_East)

df_employee_North = spark.read.format("csv").option("inferSchema", True).option("delimiter", ",").option("header", True).load("dbfs:/FileStore/tables/Employees_North_lyst7903.csv")
display(df_employee_North)

df_union = df_employee_East.union(df_employee_North)
display(df_union)


# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

df_unionAll = df_employee_East.unionAll(df_employee_North)
display(df_unionAll)


# COMMAND ----------

# Define some data
data1 = [("John", 25), ("Alice", 30), ("Bob", 35)]

# Define the schema for the DataFrame
schema1 = ["Name", "Age"]

df1 = spark.createDataFrame(data=data1, schema = schema1)

# Define some data
data2 = [("Kanth", 15), ("Man", 30), ("hello", 35)]

# Define the schema for the DataFrame
schema2 = ["Name", "Age"]

df2 = spark.createDataFrame(data=data2, schema = schema2)

df_union = df1.union(df2)

display(df_union)

# COMMAND ----------

# Order of the coumns changed - so wrong data

# Define some data


data1 = [("John", 25), ("Alice", 30), ("Bob", 35)]

# Define the schema for the DataFrame
schema1 = ["Name", "Age"]

df1 = spark.createDataFrame(data=data1, schema = schema1)

# Define some data
data2 = [( 15, "Kanth"), ( 30, "Man"), ( 35, "hello")]

# Define the schema for the DataFrame
schema2 = ["Age", "Name"]

df2 = spark.createDataFrame(data=data2, schema = schema2)

df_union = df1.union(df2)

display(df_union)

# if the column order does not match, use a transformation called UnionByName
df_unionByName = df1.unionByName(df2)

display(df_unionByName)



# COMMAND ----------

# Order of the columns changed and number of columns are not matching - how to handle

# Define some data


data1 = [("John", 25), ("Alice", 30), ("Bob", 35)]

# Define the schema for the DataFrame
schema1 = ["Name", "Age"]

df1 = spark.createDataFrame(data=data1, schema = schema1)

# Define some data
data2 = [( 15, "Kanth", "Cricket"), ( 30, "Man" , "basketball"), ( 35, "hello", "tennis")]

# Define the schema for the DataFrame
schema2 = ["Age", "Name", "Hobby"]

df2 = spark.createDataFrame(data=data2, schema = schema2)

# if the column order does not match, use a transformation called UnionByName and 
# if the number of columns not matching, use a argument called allowMissingColumns = True
df_unionByName = df1.unionByName(df2, allowMissingColumns = True)

display(df_unionByName)
