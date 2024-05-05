# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/singleline

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/singleline/Airlines1.json

# COMMAND ----------

# reading data from single line json
df_airlines = spark.read.json("dbfs:/FileStore/tables/singleline/Airlines1.json", multiLine=False )
display(df_airlines)
df_orderColumns = df_airlines.select("id", "code", "name", "country")
display(df_orderColumns)

# COMMAND ----------

df_airlines.show()

# COMMAND ----------

df_airlines.show(truncate=False)

# COMMAND ----------

df_airlines.printSchema()

# COMMAND ----------

#type casting is a process of converting one datatype to another data type
from pyspark.sql.functions import col
df_IDasInt =  df_airlines.select( col("id").cast("int").alias("Airlines_ID") , col("name"), col("code").alias("Flight Code "), "country")
df_IDasInt.printSchema()
display(df_IDasInt)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

df_schema = StructType([StructField("ID", StringType(), True), 
                         StructField("Name", StringType(), True),
                         StructField("Code", StringType(), True),                                    
                          StructField("Country", StringType(), True)])
df = spark.read.format("json").schema(df_schema).option("multiLine", False).load("dbfs:/FileStore/tables/singleline/Airlines1.json")
display(df)


# COMMAND ----------

df.count()

# COMMAND ----------

fileList = ["dbfs:/FileStore/tables/singleline/Airlines1.json","dbfs:/FileStore/tables/singleline/Airlines2.json"]
df_allFiles = spark.read.json(fileList, multiLine=False, schema=df_schema)
display(df_allFiles)
df_allFiles.count()

# COMMAND ----------

#%fs ls dbfs:/FileStore/tables/singleline/

listOfFiles = dbutils.fs.ls("dbfs:/FileStore/tables/singleline/")
display(listOfFiles)
# create empty data frame 
df = spark.createDataFrame(data=[], schema=df_schema)
for fileInfo in listOfFiles:
    print(fileInfo.path)
    df = df.unionAll( spark.read.json( fileInfo.path, multiLine=False ))

display(df)   
df.count()
