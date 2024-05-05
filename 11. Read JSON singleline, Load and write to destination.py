# Databricks notebook source
dbutils.fs.ls ("dbfs:/FileStore/tables/prasanth/csv/regions/")
data_frame_emp_east = spark.read.csv("dbfs:/FileStore/tables/prasanth/csv/regions/Employees_East.csv", header=True, inferSchema=True,sep=",")
display(data_frame_emp_east)



#File uploaded to /FileStore/tables/prasanth/csv/regions/Airlines1.json
#File uploaded to /FileStore/tables/prasanth/csv/regions/Airlines2.json
#File uploaded to /FileStore/tables/prasanth/csv/regions/Employees_Hyd.json
#File uploaded to /FileStore/tables/prasanth/csv/regions/Airlines3.json
#File uploaded to /FileStore/tables/prasanth/csv/regions/Employees_Kerala.json
#File uploaded to /FileStore/tables/prasanth/csv/regions/MultiLine_JSON.dbc


dbutils.fs.ls("dbfs:/FileStore/tables/prasanth/csv/regions")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

dbutils.fs.mkdirs("dbfs:/FileStore/tables/singleline")
dbutils.fs.ls("dbfs:/FileStore/tables/singleline")

#reading data from single line json.
df_json = spark.read.json("dbfs:/FileStore/tables/singleline/Airlines1.json", multiLine=False)
display(df_json)
display(df_json.printSchema)
df_json_new  = df_json.select(df_json.ID.cast("int").alias("ID_INTEGER"), "Name", "Code", "country" )
display(df_json_new)
display(df_json_new.printSchema)






# COMMAND ----------

# MAGIC
# MAGIC %fs ls dbfs:/FileStore/tables/singleline/
# MAGIC
# MAGIC

# COMMAND ----------

# reading from multiple file
src_path = "dbfs:/FileStore/tables/singleline/*"
df_all =  spark.read.format("json").option("multiLine", False).load(src_path)
display(df_all)
display(df_all.count())



# COMMAND ----------

from pyspark.sql.functions import input_file_name, col

df_all_new = df_all.withColumn("Input file name",input_file_name())\
    .withColumn("AIRLINES_ID_ID",col("ID").cast("int"))\
        .drop("ID")

display(df_all_new)



# COMMAND ----------

from pyspark.sql.functions import count
df_row_count = df_all_new.groupBy("Input file name")\
    .agg(count("*").alias("row count"))
display(df_row_count)

# COMMAND ----------

schema1 = "Code STRING, Country STRING, Name STRING ,  AIRLINES_ID_ID INTEGER"
df_1 = spark.createDataFrame(data=[], schema = schema1)
listFile = dbutils.fs.ls("dbfs:/FileStore/tables/singleline/")
for file1 in listFile:
    display(file1.path)
    df_indiv = spark.read.json(file1.path, multiLine=False)
    display( file1.path , "-", df_indiv.count())
    df_1 = df_1.unionAll(df_indiv)


# COMMAND ----------

display(df_1.count())

# COMMAND ----------

# MAGIC %fs rm /FileStore/tables/prasanth/csv/regions/Airlines2.json /FileStore/tables/prasanth/csv/regions/Airlines1.json /FileStore/tables/prasanth/csv/regions/Airlines3.json

# COMMAND ----------


