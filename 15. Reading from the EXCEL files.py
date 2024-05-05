# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/tables/Excel")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/Employee.xlsx")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/Excel/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

df_employee = spark.read.format("com.crealytics.spark.excel").\
    option("inferSchema", True)\
        .option("header", True)\
        .option("dataAddress", "EmployeeHYD!").\
                load("dbfs:/FileStore/tables/Excel/Employee.xlsx")
df_employee.printSchema()
display(df_employee)

# COMMAND ----------

df_employee = spark.read.format("com.crealytics.spark.excel").\
    option("inferSchema", True)\
        .option("header", True)\
        .option("dataAddress", "'EmployeeHYD'!A1:D3").\
                load("dbfs:/FileStore/tables/Excel/Employee.xlsx")
df_employee.printSchema()
display(df_employee)

# COMMAND ----------

# Create a schema programatically
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, StructType, StructField
df_schema = StructType([StructField("EMPNO", StringType(), False), 
                        StructField("ENAME", StringType(), False), 
                        StructField("JOB", StringType(), False), 
                        StructField("SAL", DoubleType(), False), 
                        StructField("DEPTNO", StringType(), False) ])
df_dataframe1 = spark.createDataFrame(data=[], schema=df_schema)

df_dataframe1.count()



# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------



# COMMAND ----------

#Create Excel object reference to read the worksheet names
#/FileStore/tables/Excel/Employee.xls
import pandas as pd
objectRef = pd.ExcelFile("/dbfs/FileStore/Employee.xls")


# COMMAND ----------

import pandas as pd
objectRef = pd.ExcelFile("/dbfs/FileStore/tables/Excel/Employee.xlsx")



# COMMAND ----------

excel_worksheet_names = objectRef.sheet_names
print(excel_worksheet_names)

# COMMAND ----------

# MAGIC %sh ls -r Excel

# COMMAND ----------

# MAGIC %sh ls *

# COMMAND ----------

#import openpyxl

# Load Excel file
#wb = openpyxl.load_workbook("/dbfs/FileStore/tables/Excel/Employee.xlsx")

# Get worksheet names
#sheet_names = wb.sheetnames

# Print worksheet names
#print(sheet_names)


# COMMAND ----------

excel_worksheet_names = ['EmployeeHYD','EmployeeKerala', 'EmployeeBAN']
display(type(excel_worksheet_names))
print(excel_worksheet_names)

# COMMAND ----------


df_main = spark.createDataFrame(data=[], schema=df_schema)
print(excel_worksheet_names)
for sheet_name in excel_worksheet_names :
    df_individualSheet = spark.read.format("com.crealytics.spark.excel").\
        option("header", True).\
            schema(df_schema).\
            option("dataAddress", sheet_name+"!").\
            load("dbfs:/FileStore/tables/Excel/Employee.xlsx")
    df_main = df_main.unionAll(df_individualSheet)
    display(df_individualSheet)
    print(sheet_name + "!")



# COMMAND ----------

display(df_main)
