# Databricks notebook source
# check the list of folders in the dbfs folder.
%fs ls dbfs:/

# COMMAND ----------

# check the list of folders in the sample datasets
%fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines

# COMMAND ----------

# this takes looonng time to complete
listOfFiles = dbutils.fs.ls("dbfs:/databricks-datasets/asa/airlines/")
for fileInfo in listOfFiles:
    print(fileInfo.path)
    df_airlines = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load(fileInfo.path)
    display(df_airlines)

# COMMAND ----------

# this takes 14 minutes to complete
df_airlines = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("dbfs:/databricks-datasets/asa/airlines/*")
#display(df_airlines)
df_airlines.count()

# COMMAND ----------

df_airlines.count()

# COMMAND ----------

 
    df_airlines = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("dbfs:/databricks-datasets/asa/airlines/2001.csv")
    display(df_airlines)

# COMMAND ----------

df_select = df_airlines.select("origin", "dest", "distance").distinct()
display(df_select)
df_select.count()

# COMMAND ----------

df_filter = df_airlines.filter("month = 2")
display(df_filter)
df_filter.count()
# 477824
#df_filter = df_airlines.filter("month == 2")
#display(df_filter)
#df_filter.count()

# COMMAND ----------

from pyspark.sql.functions import col
df_filter = df_airlines.filter(col("month") == 2)
#display(df_filter)
df_filter.count()

# COMMAND ----------

df_drop =  df_airlines.drop("DayofMonth")
display(df_drop)

# COMMAND ----------

df_drop =  df_airlines.drop("DayofMonth", "CRSDEPTIME")
display(df_drop)

# COMMAND ----------

from pyspark.sql.functions import col
df_filter = df_airlines.where(col("month") == 2)
#display(df_filter)
df_filter.count()

# COMMAND ----------

df_weekend = df_airlines.select("Year", "Month", "DayOfWeek").withColumn("Weekend", col("DayOfWeek").isin(6,7)).filter("WEekend == true")                                                        
display(df_weekend)

# COMMAND ----------

df_airlines.printSchema()

# COMMAND ----------

# Type casting is a process of converting from one datatype to another datatype
df_airlines_cast = df_airlines.withColumn("ActualElapsedTime", col("ActualElapsedTime").cast("int"))
display(df_airlines_cast)
df_airlines_cast.printSchema()

# COMMAND ----------

# Type casting is a process of converting from one datatype to another datatype
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
df_airlines_cast = df_airlines.withColumn("ActualElapsedTime", col("ActualElapsedTime").cast(IntegerType()))
display(df_airlines_cast)
df_airlines_cast.printSchema()
