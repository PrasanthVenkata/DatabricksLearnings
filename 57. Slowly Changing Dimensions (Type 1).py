# Databricks notebook source
# MAGIC %md
# MAGIC # delete the source table if it exists

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists v_customers

# COMMAND ----------

# MAGIC %md
# MAGIC # delete the target table if it exists

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dim_customers

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a data frame. first schema and then data

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType
cust_schema = StructType([StructField("Customer_id", IntegerType(), False),StructField("FirstName", StringType(), True),StructField("LastName", StringType(), True),StructField("Address", StringType(), True)    ])

# COMMAND ----------

cust_data = [(100, "Prasanth", "Venkata", "hyderabad"), (101, "Amandeep", "Singh", "Noida"), (102, "Mathukutty", "Mysen", "Bangalore")]
df_cus = spark.createDataFrame(data=cust_data, schema = cust_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a temp table (Source) - local table

# COMMAND ----------

df_cus.createOrReplaceTempView("v_customers")

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc extended dim_customers

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/Data/Delta", True)

# COMMAND ----------

# MAGIC %md 
# MAGIC # create a table where we want to insert data - this is the target table (Database table .. in realtime also)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dim_customers
# MAGIC (Customer_id int,
# MAGIC  FirstName string,
# MAGIC  LastName string,
# MAGIC  Address string
# MAGIC )
# MAGIC using DELTA
# MAGIC LOCATION 'dbfs:/FileStore/tables/Data/Delta'

# COMMAND ----------

# MAGIC %md
# MAGIC #Initially the target table is empty

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_customers

# COMMAND ----------

# MAGIC %md
# MAGIC #this returns 0 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use the merge operation to insert or update the data from source to target
# MAGIC ## MErge is like Upsert meaning update or insert
# MAGIC ## if the record exists, then update
# MAGIC ## use insert

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_customers as tar
# MAGIC using v_customers as src
# MAGIC on tar.Customer_id = src.customer_id
# MAGIC when matched and (src.firstname <> tar.firstname or src.lastname <> tar.lastname or src.address <> tar.address) then 
# MAGIC update set tar.firstname = src.firstname , tar.lastname = src.lastname, tar.address = src.address
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a new source (same name but with different data). Now merge into the target

# COMMAND ----------

cust_data = [(103, "new", "record", "Chennai"), (101, "Amandeep", "Singh", "Bangalore"), (102, "Mathukutty", "Mysen", "Bangalore")]
df_cus = spark.createDataFrame(data=cust_data, schema = cust_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### in this, one row is new (first name as new is new record), one row is updated (Amandeep) and one is unchanged (Mysen).

# COMMAND ----------

df_cus.createOrReplaceTempView("v_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_customers as tar
# MAGIC using v_customers as src
# MAGIC on tar.Customer_id = src.customer_id
# MAGIC when matched and (src.firstname <> tar.firstname or src.lastname <> tar.lastname or src.address <> tar.address) then 
# MAGIC update set tar.firstname = src.firstname , tar.lastname = src.lastname, tar.address = src.address
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/FileStore/tables/Data/Delta`
