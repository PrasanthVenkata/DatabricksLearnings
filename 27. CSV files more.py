# Databricks notebook source
list_files = dbutils.fs.ls("dbfs:/FileStore/tables/prasanth/csv")
display(list_files)

# COMMAND ----------

for x in dbutils.fs.ls("dbfs:/FileStore/tables"):
    print(x.path)

# COMMAND ----------

# Create empty data frame with user defined schema
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
cols = StructType([StructField("empno", IntegerType(), False),
                  StructField("First_Name", StringType(), False),
                  StructField("Last_Name", StringType(), False),
                  StructField("city", StringType(), False),
                  StructField("email", StringType(), False),
                  StructField("hiredate", StringType(), False),
                  StructField("phone", StringType(), False),
                  StructField("salary", IntegerType(), False),
                  StructField("input_file_name", StringType(), False)
                  ])

df = spark.createDataFrame(data=[], schema=cols) 
df.count()

# COMMAND ----------

#iterate over the list of files
"""
for input_file in list_files:
    filePath = input_file.path
    display(filePath)
    df_inputFile = spark.read.csv(filePath, schema= cols, header=True, sep=",")
    df = df.unionAll(df_inputFile)

display(df)
"""

# COMMAND ----------

from pyspark.sql.functions import input_file_name
for input_file in list_files    filePath = input_file.path
    display(filePath)
    df_inputFile = spark.read.csv(filePath, schema= cols, header=True, sep=",", mode='DROPMALFORMED') #PERMISSIVE
    df_inputFile = df_inputFile.withColumn("input_file_name", input_file_name())
    df = df.unionAll(df_inputFile)

display(df)
