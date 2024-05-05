# Databricks notebook source
from pyspark.sql.functions import col

df_permissive = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", True).option("delimiter", ",").schema("EMPNO INT, FIRST_NAME STRING, LAST_NAME STRING, CITY STRING,EMAIL STRING, HIREDATE DATE, PHONE STRING, SALARY INT,CorruptRecord STRING" ).option("columnNameOfCorruptRecord", "CorruptRecord").option("dateFormat", "M/dd/yyyy").load("dbfs:/FileStore/tables/Employees_WestTampered.csv")
display(df_permissive)


# COMMAND ----------

df_valid = df_permissive.filter(col("CorruptRecord").isNull() == True).drop("CorruptRecord")
df_Invalid = df_permissive.filter(col("CorruptRecord").isNull() == False).drop("CorruptRecord")
display(df_valid)
display(df_Invalid)



# COMMAND ----------

df_valid.write.format("csv").options(header=True, delimiter="|").mode("overwrite").save("dbfs:/FileStore/tables/Valid/")


# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Valid

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/Valid/part-00000-tid-8105877462023120666-e0972952-b050-4b6a-9f79-8aff2ba5c12c-36-1-c000.csv

# COMMAND ----------

df_valid.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
df_valid.select(spark_partition_id().alias("Partition_ID")).groupBy("Partition_ID").count().show()

# COMMAND ----------

df_valid = df_valid.repartition(2)
df_valid.select(spark_partition_id().alias("Partition_ID")).groupBy("Partition_ID").count().show()

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df_permissive = df_permissive.withColumn("SRC_FILENAME", input_file_name())
display(df_permissive) 

# COMMAND ----------

df_permissive.select("*").show(truncate=False)

# COMMAND ----------

df_permissive.select("empno", "salary", "phone").show(truncate=False)

# COMMAND ----------

df_permissive.select("empno", "salary", "phone").filter(col("salary").isNull()==True).show(truncate=False)

# COMMAND ----------

df_permissive.select("empno", "salary", "phone").where(col("salary").isNull()==True).show(truncate=False)
