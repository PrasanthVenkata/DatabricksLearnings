# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/multiline

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/multiline/Employees_Kerala.json

# COMMAND ----------

df_hyd = spark.read.format("json").option("multiLine", True).load("dbfs:/FileStore/tables/multiline/Employees_Hyd.json")
display(df_hyd)

# COMMAND ----------

df_hyd_Reorder = df_hyd.select("EMPNO", "NAME", "JOB","SAL", "DEPTNO")
display(df_hyd_Reorder)

# COMMAND ----------

df_hyd_Reorder = df_hyd.select(df_hyd[ "EMPNO"] , df_hyd["NAME"],df_hyd[ "JOB"],df_hyd["SAL"], df_hyd["DEPTNO"])
display(df_hyd_Reorder)

# COMMAND ----------

# here case is important
df_hyd_Reorder=df_hyd.select(df_hyd.EMPNO,df_hyd.NAME,df_hyd.JOB,df_hyd.SAL,df_hyd.DEPTNO)
display(df_hyd_Reorder)

# COMMAND ----------

df_hyd.printSchema()

# COMMAND ----------

schemac_cols = "EMPNO INT, DEPTNO INT, NAME STRING, SALARY INT, JOB STRING"
df_CS = spark.read.json(schema= schemac_cols, multiLine=True,path="dbfs:/FileStore/tables/multiline/Employees_Hyd.json" )
display(df_CS)

# COMMAND ----------

# Iterate over the multiple JSON files
from pyspark.sql.functions import input_file_name
src_file_Path = dbutils.fs.ls("dbfs:/FileStore/tables/multiline")
df_Total = spark.createDataFrame(data=[], schema=schemac_cols)
for filePath in src_file_Path:
    print(filePath.path)
    df_Individial = spark.read.json(schema=schemac_cols, path=filePath.path, multiLine=True)
    df_Total = df_Total.unionAll(df_Individial)
    print("Number of records in the file  " + filePath.path, "is :" , df_Individial.count())

print("Total records in all the files read are " , "  :   " ,df_Total.count())

df_Total = df_Total.select("*", input_file_name().alias("Source file name"))
display(df_Total)

# COMMAND ----------

df_Total = df_Total.drop("Input file name")
display(df_Total)
df_Total = df_Total.withColumn("SOURCE FILE COLUMN", input_file_name())
display(df_Total)


# COMMAND ----------

from pyspark.sql.functions import input_file_name
json_file_path = "dbfs:/FileStore/tables/MixedFileDir/*.json"
from pyspark.sql.functions import input_file_name

df_Total = spark.createDataFrame(data=[], schema=schemac_cols)
df_Total = spark.read.json(schema=schemac_cols, path=json_file_path, multiLine=True)
print("Number of records in the files  "  ,  df_Total.count())

display(df_Total) 
df_Total = df_Total.withColumn("SOURCE FILE COLUMN", input_file_name())
display(df_Total)

# COMMAND ----------

# reading from specific files
from pyspark.sql.functions import input_file_name
list_of_two_files = ["dbfs:/FileStore/tables/MixedFileDir/Employees_Kerala.json", "dbfs:/FileStore/tables/MixedFileDir/Employees_Hyd.json"]



df_Total = spark.createDataFrame(data=[], schema=schemac_cols)
df_Total = spark.read.json(schema=schemac_cols, path=list_of_two_files, multiLine=True)
print("Number of records in the files  "  ,  df_Total.count())

display(df_Total) 
df_Total = df_Total.withColumn("SOURCE FILE COLUMN", input_file_name())
display(df_Total)

# COMMAND ----------

#%fs ls dbfs:/FileStore/tables/multiline/
#dbutils.fs.rm("dbfs:/FileStore/tables/multiline/COURSE.JSON")


# COMMAND ----------

df_hyd = spark.read.json("dbfs:/FileStore/tables/multiline/COURSE.JSON", multiLine=True, dateFormat="dd-MM-yyyy").cache()
display(df_hyd) 
