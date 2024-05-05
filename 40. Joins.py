# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

#%fs rm dbfs:/FileStore/tables/employee.csv


# COMMAND ----------

#%fs rm dbfs:/FileStore/tables/department.csv


# COMMAND ----------

df_employee = spark.read.format("csv").option("inferSchema", True).option("delimiter", ",").option("header", True).load("dbfs:/FileStore/tables/employee.csv")
display(df_employee)

# COMMAND ----------

df_department = spark.read.format("csv").option("inferSchema", True).option("delimiter", ",").option("header", True).load("dbfs:/FileStore/tables/department.csv")
display(df_department)


# COMMAND ----------

df_employee.columns


# COMMAND ----------

df_department.columns


# COMMAND ----------

df_inner = df_employee.join(df_department, on=df_department.DEPARTMENT_ID == df_employee.DEPARTMENT_ID, how = "inner").drop(df_department.DEPARTMENT_ID)
display(df_inner)
df_inner.count()

# COMMAND ----------

df_left = df_employee.join(df_department, on=df_department.DEPARTMENT_ID == df_employee.DEPARTMENT_ID, how = "leftouter").drop(df_department.DEPARTMENT_ID)
display(df_left)
df_left.count()

# COMMAND ----------

df_right = df_employee.join(df_department, on=df_department.DEPARTMENT_ID == df_employee.DEPARTMENT_ID, how = "rightouter").drop(df_department.DEPARTMENT_ID)
display(df_right)
df_right.count()

# COMMAND ----------

df_fullouter = df_employee.join(df_department, on=df_department.DEPARTMENT_ID == df_employee.DEPARTMENT_ID, how = "fullouter").drop(df_department.DEPARTMENT_ID)
display(df_fullouter)
df_fullouter.count()

# COMMAND ----------

# DO NOT SPECIFY ANY JOIN CONDITION
df_cross = df_employee.join(df_department,  how = "cross").drop(df_department.DEPARTMENT_ID)
display(df_cross)
df_cross.count()

# COMMAND ----------

# Another syntax for cross join
df_crossJoin =  df_employee.crossJoin(df_department)
display(df_crossJoin)
df_crossJoin.count()

# COMMAND ----------

df_leftSemi = df_employee.join(df_department, on=df_department.DEPARTMENT_ID == df_employee.DEPARTMENT_ID, how = "semi").drop(df_department.DEPARTMENT_ID)
display(df_leftSemi)
df_leftSemi.count()

# COMMAND ----------

df_leftAnti = df_employee.join(df_department, on=df_department.DEPARTMENT_ID == df_employee.DEPARTMENT_ID, how = "anti").drop(df_department.DEPARTMENT_ID)
display(df_leftAnti)
df_leftAnti.count()

# COMMAND ----------

df_selfJoin =  df_employee.alias("df1_alias").join( df_employee.alias("df2_alias"), on=df_employee['MANAGER_ID'] == df_employee['EMPLOYEE_ID'], how="inner").select('df1_alias.EMPLOYEE_ID', 'df1_alias.FIRST_NAME', 'df1_alias.MANAGER_ID')
df_selfJoin.show()

# COMMAND ----------

# Assuming df is your DataFrame

# Import necessary libraries
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SelfJoinExample") \
    .getOrCreate()

# Create a sample DataFrame
data = [(1, 'Alice', 100),
        (2, 'Bob', 200),
        (3, 'Charlie', 300),
        (4, 'David', 400)]

df = spark.createDataFrame(data, ['id', 'name', 'manager_id'])

# Perform self-join
self_joined_df = df.alias('emp1').join(df.alias('emp2'), df['id'] == df['manager_id'], how='inner') \
                                  .select('emp1.name', 'emp1.manager_id', 'emp2.name')

# Show the result
self_joined_df.show()

