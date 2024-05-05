# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/tables/multiline")

# COMMAND ----------

df_ML = spark.read.json("dbfs:/FileStore/tables/multiline/Employees_Kerala.json",multiLine=True)
display(df_ML)
df_new = df_ML.select("EMPNO", "DEPTNO", "JOB", "NAME", "SAl")
display(df_new)

df_new = df_ML.select(df_ML.DEPTNO, df_ML.EMPNO)
display(df_new)

# COMMAND ----------

df_new_2=  df_ML.select(df_ML["EMPNO"],df_ML["DEPTNO"], df_ML["SAL"])
display(df_new_2)

# COMMAND ----------

from pyspark.sql.functions import col
df_new_3 = df_ML.select(col("EMPNO"), col("SAL"))
display(df_new_3)

# COMMAND ----------

from pyspark.sql.functions import col
df_new_4 = df_ML.select("*")
display(df_new_4)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType, TimestampType, LongType
emp_cols = StructType([StructField("EMPNO", IntegerType(), False),
                      StructField("NAME", StringType(), False),
                      StructField("DEPTNO", StringType(), False),
                      StructField("SAL", LongType(), False),
                      StructField("JOB", StringType(), False)
                      ])

df_data =  spark.read.json("dbfs:/FileStore/tables/multiline/Employees_Kerala.json",multiLine=True, schema=emp_cols)
display(df_data)
#df_emp =  spark.createDataFrame(data=df_data, schema=emp_cols)

# COMMAND ----------

df_new_5 = spark.read.format("json").option("multiLine", True).schema(emp_cols).load("dbfs:/FileStore/tables/multiline/Employees_Kerala.json")
display(df_new_5)

# COMMAND ----------

s_path = "dbfs:/FileStore/tables/multiline/"
src_path = "dbfs:/FileStore/tables/multiline/*.json"
listofFiles = dbutils.fs.ls(s_path)
display(listofFiles)
df_all = spark.read.format("json").option("multiLine", True).schema(emp_cols).load(src_path)
print("first method to read all files",)

display(df_all)

df_1 =  spark.createDataFrame(data=[], schema=emp_cols)
for fileInfo in listofFiles:
    df_indiv = spark.read.format("json").option("multiLine", True).schema(emp_cols).load(fileInfo.path)
    df_1 = df_1.unionAll(df_indiv)

print("Second method to read all files",)
display(df_1)

fileds_list =  df_1.schema.fields
print(fileds_list)

for filed_Item in fileds_list :
    print(filed_Item.name)

# COMMAND ----------



