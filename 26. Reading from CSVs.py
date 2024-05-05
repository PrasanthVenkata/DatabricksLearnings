# Databricks notebook source
# Databricks notebook source
#dbutils.fs.rm("dbfs:/FileStore/tables/prasanth/csv/regions", True)
dbutils.fs.ls("dbfs:/FileStore/tables/prasanth/csv/")


# COMMAND ----------

df = spark.read.csv(["dbfs:/FileStore/tables/prasanth/csv/Employees_East.csv", "dbfs:/FileStore/tables/prasanth/csv/Employees_North.csv"] , inferSchema=True, sep= ",", header=True)
display(df)

# COMMAND ----------

from pyspark.sql.types import * 
schema_manually = StructType([StructField('empno', IntegerType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('city', StringType(), True), StructField('email', StringType(), True), StructField('hiredate', StringType(), True), StructField('phone', LongType(), True), StructField('salary', IntegerType(), True)])


# COMMAND ----------

df1 = spark.read.csv(["dbfs:/FileStore/tables/prasanth/csv/Employees_East.csv", "dbfs:/FileStore/tables/prasanth/csv/Employees_North.csv"] , schema=schema_manually, sep= ",", header=True)
display(df1)

# COMMAND ----------

df2= df1.select("empno", "First_name", "Last_Name")
display(df2)


df2.show(5)
df1.explain(True)

# COMMAND ----------

df_rec= spark.read.format("csv").option("recursiveFileLookup", True).schema(schema_manually).option("header", True).option("delimiter", ",").load("dbfs:/FileStore/tables/prasanth/csv/")
display(df_rec )

# COMMAND ----------

df2 =  df_rec.distinct()
display(df2)
