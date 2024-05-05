# Databricks notebook source
'''
Upload the employee csv file and check that exists
'''
dbutils.fs.ls('dbfs:/FileStore/tables')

# COMMAND ----------

# Cleanse the data using the filter and sort the data by OrderBy transformation
from pyspark.sql.functions import col
df = spark.read.format("csv").option("header", True).option("delimiter", ",").option("inferSchema", True).load("dbfs:/FileStore/tables/employee.csv")
display(df)
df_cleanse = df.filter(df.SALARY > 2500).orderBy(df.SALARY.desc())
df_cleanse.count()
display(df_cleanse)

# Other notation for the orderBy transformation
df_cleanse_orderBy = df_cleanse.orderBy(df_cleanse["EMPLOYEE_ID"].desc()   )
display(df_cleanse_orderBy)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/write", recurse=True )

# COMMAND ----------

# Write the cleansed data into a CSV - persistent data storage
df_cleanse.write.format("csv").option("overwrite", True).option("header", True).save("dbfs:/FileStore/tables/write")

# COMMAND ----------

# Check the files in the directory where csv file is created
dbutils.fs.ls("dbfs:/FileStore/tables/write")

# COMMAND ----------

# Get the file name of the 4 file (Index =3) dynamically
listoffiles = dbutils.fs.ls("dbfs:/FileStore/tables/write")
print(listoffiles[3].path)

# COMMAND ----------

# Read the contents of the written csv file by getting the file name of the 4 file (Index =3) dynamically
#dbutils.fs.head("dbfs:/FileStore/tables/write/part-00000-tid-758329941664393177-2f39390c-5c61-455a-85fa-e072276941fe-127-1-c000.csv")
dbutils.fs.head(listoffiles[3].path)

# COMMAND ----------

# Write the cleansed data in the PARQUET format (Pronounced as PARQUE)
df_cleanse.write.format("parquet").mode("append").save("dbfs:/FileStore/tables/write/parquet")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/write/parquet")

# COMMAND ----------

listoffiles =  dbutils.fs.ls("dbfs:/FileStore/tables/write/parquet")
#dbutils.fs.head("dbfs:/FileStore/tables/write/parquet/part-00000-tid-3638639885198160209-fc8d19f0-c5e9-498e-9338-5df13e2856e0-139-1-c000.snappy.parquet")
dbutils.fs.head(listoffiles[3].path)

# COMMAND ----------

df_cleanse.write.format("parquet").mode("overwrite").partitionBy("DEPARTMENT_ID").save("dbfs:/FileStore/tables/write/parquet")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/write/parquet")

# COMMAND ----------

# Check the list of files in department = 10 partition
dbutils.fs.ls("dbfs:/FileStore/tables/write/parquet/DEPARTMENT_ID=10/")


# COMMAND ----------

# Check the list of files in department = 10 partition , dynamically read the list of files and get the path of the fourth file (index = 3). This is the file where Parquet data is present
#dbutils.fs.head("dbfs:/FileStore/tables/write/parquet/DEPARTMENT_ID=10/part-00000-tid-6663398109785506263-955b5d69-e271-4481-aac8-cb651112dad0-142-1.c000.snappy.parquet")
listoffiles =  dbutils.fs.ls("dbfs:/FileStore/tables/write/parquet/DEPARTMENT_ID=10")
#dbutils.fs.head("dbfs:/FileStore/tables/write/parquet/part-00000-tid-3638639885198160209-fc8d19f0-c5e9-498e-9338-5df13e2856e0-139-1-c000.snappy.parquet")
dbutils.fs.head(listoffiles[3].path)

# COMMAND ----------

#Writing the data into a permanent table
df_cleanse.write.format("parquet").mode("overwrite").partitionBy("DEPARTMENT_ID").saveAsTable("EMPLOYEE")


# COMMAND ----------

# MAGIC %sql
# MAGIC /* Show all the database */
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Show the list of tables in the default database */
# MAGIC
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %sql 
# MAGIC /* Show the list of records  in the employee table */
# MAGIC
# MAGIC select * from default.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Read the parquet folder (where the parquet files are present) as a table */
# MAGIC select * from parquet.`dbfs:/FileStore/tables/write/parquet`
# MAGIC /*
# MAGIC this is apos symbol - present on the below the escape character of keyboard
# MAGIC */
# MAGIC
