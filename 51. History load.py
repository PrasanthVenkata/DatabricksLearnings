# Databricks notebook source

#dbutils.fs.mkdirs("dbfs:/FileStore/tables/Data/20240414")
#dbutils.fs.mkdirs("dbfs:/FileStore/tables/Data/20240415")
#dbutils.fs.mkdirs("dbfs:/FileStore/tables/Data/20240416")

# COMMAND ----------

# MAGIC %md
# MAGIC Create only once so comment the above code

# COMMAND ----------

dbutils.widgets.text("LoadType","")
dbutils.widgets.text("LoadDate","")

# COMMAND ----------

#dbutils.fs.rm("dbfs:/FileStore/tables/Data/DeltaLake/Employee", True)

# COMMAND ----------

str_LoadType = dbutils.widgets.get("LoadType")
str_LoadDate = dbutils.widgets.get("LoadDate")

# COMMAND ----------

print(str_LoadType)
print(str_LoadDate)


# COMMAND ----------

# MAGIC %md
# MAGIC Common path for the data files.

# COMMAND ----------

str_filePath = "dbfs:/FileStore/tables/Data/" +str_LoadDate
print("Full path", str_filePath)

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).option("sep", ",").load(str_filePath)

# COMMAND ----------

# MAGIC %md 
# MAGIC Implement history load (Full) and delta load.

# COMMAND ----------

if str_LoadType.upper() == "HISTORY": 
    df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/Data/DeltaLake/Employee")
    display(df)
    dbutils.notebook.exit("Full load completed successfully " + str(df.count()))
else:
    df.write.format("delta").mode("append").save("dbfs:/FileStore/tables/Data/DeltaLake/Employee")
    df_after = spark.read.format("delta").load("dbfs:/FileStore/tables/Data/DeltaLake/Employee")
    display(df_after)
    dbutils.notebook.exit("Delta load completed successfully " + str( df_after.count()))

# COMMAND ----------

display(df_after)

# COMMAND ----------


