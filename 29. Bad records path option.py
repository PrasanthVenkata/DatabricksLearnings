# Databricks notebook source
dbutils.fs.ls("dbfs:/FileStore/tables/prasanth/csv")

# COMMAND ----------

# Create the schema as per the uploaded file.
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DateType
schema1 = StructType([StructField("id", IntegerType(), True),
                     StructField("Name", StringType(), True) ,
                     StructField("DOJ", DateType(), True) ,
                     StructField("Salary", IntegerType(), True) ]
)

# COMMAND ----------

# Upload a tampered file with four  columns ID, NAME, DOJ & SALARY to the location dbfs:/FileStore/tables/prasanth/csv
dbutils.fs.head("dbfs:/FileStore/tables/prasanth/csv/employeeBadrecords.csv")


#dbutils.fs.rm("dbfs:/FileStore/tables/prasanth/employeeBadrecords.csv")


# COMMAND ----------

# Read the file using the permissive mode (Default mode), no need to specify the mode. Use the BadRecordsPath option to redirect the bad rows to that folder location
df_valid  = spark.read.format("csv").option("header", True).option("delimiter", ",").schema(schema1).option("dateformat","dd.MM.yyyy").option("badRecordsPath", "dbfs:/FileStore/tables/prasanth/csv/").\
    load("dbfs:/FileStore/tables/prasanth/csv/employeeBadrecords.csv")
df_valid.count()
display(df_valid)

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/prasanth/csv/20240308T214645/bad_records/part-00000-354e66d2-5d3e-4aff-9b5a-b342b489d970
