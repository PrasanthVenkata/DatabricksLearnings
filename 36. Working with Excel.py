# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/Excel

# COMMAND ----------

# reading from single sheet
df_hyd = spark.read.format("com.crealytics.spark.excel").option("inferSchema",True).option("header", True).option("dataAddress", "EmployeeHYD!").load("dbfs:/FileStore/tables/Excel/Employee.xlsx")
display(df_hyd)

# COMMAND ----------

#reading data from multiple excel sheets using list preparation (Manually)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from pyspark.sql.functions import input_file_name
employee_List = ["EMPLOYEEHYD", "EMPLOYEEKERALA", "EMPLOYEEBAN"]
display(type(employee_List))
emp_schema = StructType([
                        StructField("EMPNO", StringType(), False),
                         StructField("ENAME", StringType(), True),
                         StructField("JOB", StringType(), True),
                         StructField("SAL", LongType(), True),
                         StructField("DEPTNO", StringType(), True)
                          ])

# Create an empty data frame
df_main = spark.createDataFrame(data = [], schema = emp_schema)
df_main.printSchema()
df_main.count()


for workSheetName in employee_List:
    df_Individual = spark.read.format("com.crealytics.spark.excel").option("inferSchema",True).option("header", True).option("dataAddress", workSheetName+"!").load("dbfs:/FileStore/tables/Excel/Employee.xlsx")
   # display(df_Individual)
   # df_Individual = df_Individual.withColumn("Input file name", input_file_name())
    df_main = df_main.unionAll(df_Individual)

display(df_main)
#df_main = df_main.withColumn("Input file name", input_file_name())
df_main.count()
display(df_main)

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

#%pip install pandas openpyxl xlrd

# how to get the sheets dynamically and  create a excel object reference
import pandas as pd
xlObjRef = pd.ExcelFile("dbfs/FileStore/tables/Excel/Employee.xlsx")
print(xlObjRef)

# COMMAND ----------

# MAGIC %pip install pandas
