# Databricks notebook source
# list of files from the given directory
dbutils.fs.ls("dbfs:/FileStore/tables")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/"))

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/PrasanthFiles")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/username-1.csv"))

dbutils.fs.cp("dbfs:/FileStore/tables/username-1.csv", "dbfs:/FileStore/PrasanthFiles/")

display(dbutils.fs.ls("dbfs:/FileStore/PrasanthFiles/"))

#dbutils.fs.rm("dbfs:/FileStore/PrasanthFiles/username-1.csv")



# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/PrasanthFiles/username-1.csv")
