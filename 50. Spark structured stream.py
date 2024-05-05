# Databricks notebook source
schema1 = "time STRING, action STRING" 

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/structured-streaming/events

# COMMAND ----------

df_stream_read = spark.readStream.format("json").schema(schema1).option("maxFilesPerTrigger", 1).load("dbfs:/databricks-datasets/structured-streaming/events")

# COMMAND ----------

 from pyspark.sql.functions import from_unixtime, col
 df_stream_read =df_stream_read.withColumn("time", from_unixtime(col("time")))

# COMMAND ----------

df_stream_read.writeStream.format("delta").option("checkPointLocation", "dbfs:/FileStore/tables/prasanth/Checkpoint").\
    start("dbfs:/FileStore/tables/json/streamWrite")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/json/streamWrite

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS EVENTS
# MAGIC (
# MAGIC   time STRING, action STRING
# MAGIC ) USING DELTA 
# MAGIC LOCATION 'dbfs:/FileStore/tables/json/streamWrite'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM DELTA.`dbfs:/FileStore/tables/json/streamWrite`
