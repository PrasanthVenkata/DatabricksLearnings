# Databricks notebook source
# MAGIC %sql
# MAGIC describe history delta.`dbfs:/FileStore/tables/Data/DeltaType2`

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/SCD/Type2")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/SCD/Type2

# COMMAND ----------

# MAGIC %md
# MAGIC ### uploaded files
# MAGIC ### File uploaded to /FileStore/tables/SCD/Type2/InitialCustomers.csv
# MAGIC ### This table has 3 records 101, 102, 103
# MAGIC
# MAGIC ### File uploaded to /FileStore/tables/SCD/Type2/InitialCustomers1_1.csv
# MAGIC ### This table has 2 records.
# MAGIC ### first record is update record for 101
# MAGIC ### second record is a new record 104
# MAGIC
# MAGIC ### Please note Id column is a Surrogate key. Unique across the Database. meaning only one value for the total database.
# MAGIC
# MAGIC ## Please note that file InitialCustomers1_1.csv is assumed to have only updates and additions. 

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS default.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS default.customers_update

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/customers_update", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/customers", True)

# COMMAND ----------

"""
fs rm dbfs:/FileStore/tables/SCD/Type2/InitialCustomers.csv
"""

# COMMAND ----------

"""
%fs rm dbfs:/FileStore/tables/SCD/Type2/InitialCustomers1_1.csv
"""


# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
 

# Read data from CSV into a DataFrame
csv_file_path = "dbfs:/FileStore/tables/SCD/Type2/InitialCustomers.csv"
df = spark.read.option("header", "true").csv(csv_file_path)

# Write DataFrame to Delta table
delta_table_name = "customers"
df.write.format("delta").saveAsTable(delta_table_name)

# Optionally, you can also save the data directly to a Delta table without creating an intermediate DataFrame
# spark.read.option("header", "true").csv(csv_file_path).write.format("delta").saveAsTable(delta_table_name)

 


# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`dbfs:/user/hive/warehouse/customers`

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize SparkSession
#spark = SparkSession.builder \
#   .appName("CSV to Delta") \
#    .getOrCreate()

# Read data from CSV into a DataFrame
csv_file_path = "dbfs:/FileStore/tables/SCD/Type2/InitialCustomers1_1.csv"
df = spark.read.option("header", "true").csv(csv_file_path)

# Write DataFrame to Delta table
delta_table_name = "customers_update"
df.write.format("delta").saveAsTable(delta_table_name)

# Optionally, you can also save the data directly to a Delta table without creating an intermediate DataFrame
# spark.read.option("header", "true").csv(csv_file_path).write.format("delta").saveAsTable(delta_table_name)

# Stop SparkSession (optional)
#spark.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customers
# MAGIC USING
# MAGIC  
# MAGIC -- Update table with rows to match (new and old referenced as seen in example above)
# MAGIC ( SELECT
# MAGIC null AS id, Customer_ID, FirstName, LastName, Address, current_date AS start_date, null AS end_date
# MAGIC FROM customers_update
# MAGIC UNION ALL
# MAGIC SELECT id,
# MAGIC  Customer_ID, FirstName, LastName, Address, start_date, End_Date
# MAGIC FROM customers
# MAGIC WHERE Customer_ID IN
# MAGIC (SELECT Customer_ID FROM customers_update)
# MAGIC ) scdChangeRows
# MAGIC  
# MAGIC -- based on the following column(s)
# MAGIC ON customers.id = scdChangeRows.id
# MAGIC  
# MAGIC -- if there is a match do this…
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET customers.end_date = current_date()
# MAGIC  
# MAGIC -- if there is no match insert new row
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ### https://www.advancinganalytics.co.uk/blog/2021/9/27/slowly-changing-dimensions-scd-type-2-with-delta-and-databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_update

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE customers
# MAGIC SELECT ROW_NUMBER() OVER (ORDER BY customer_id NULLS LAST) - 1 AS id, Customer_ID, 
# MAGIC  firstname, lastname,  address, 
# MAGIC  start_date, end_date
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE table customers to version as of  1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers
