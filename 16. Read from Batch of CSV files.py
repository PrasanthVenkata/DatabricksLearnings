# Databricks notebook source
dbutils.fs.ls("dbfs:/databricks-datasets/asa/airlines")

# COMMAND ----------

dataframe_Airlines = spark.read.format("csv").option("header", True).option("sep", ",").load("dbfs:/databricks-datasets/asa/airlines") 
display(dataframe_Airlines)
dataframe_Airlines.count()
dataframe_Airlines.printSchema()

# COMMAND ----------

# Get the number of rows
dataframe_Airlines.count()

# COMMAND ----------

#limit the number of rows to 100
display(dataframe_Airlines.limit(100))

# COMMAND ----------

# Get the number of columns in the dataframe
display(len(dataframe_Airlines.columns) )

# COMMAND ----------

# Get the distinct values for two colums UniqueCarrier and Flight Number
new_DF = dataframe_Airlines.select("UniqueCarrier", "FlightNum").distinct()
display(new_DF)

# COMMAND ----------


