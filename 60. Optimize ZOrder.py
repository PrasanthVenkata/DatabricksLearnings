# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/asa/airlines/1991.csv
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType
airlines_schema = StructType([StructField('Year',StringType(), False),	StructField('Month',StringType(), False),	StructField('DayofMonth',StringType(), False),	StructField('DayOfWeek',StringType(), False),	StructField('DepTime',StringType(), False),	StructField('CRSDepTime',StringType(), False),	StructField('ArrTime',StringType(), False),	StructField('CRSArrTime',StringType(), False),	StructField('UniqueCarrier',StringType(), False),	StructField('FlightNum',StringType(), False),	StructField('TailNum',StringType(), False),	StructField('ActualElapsedTime',StringType(), False),	StructField('CRSElapsedTime',StringType(), False),	StructField('AirTime',StringType(), False),	StructField('ArrDelay',StringType(), False),	StructField('DepDelay',StringType(), False),	StructField('Origin',StringType(), False),	StructField('Dest',StringType(), False),	StructField('Distance',StringType(), False),	StructField('TaxiIn',StringType(), False),	StructField('TaxiOut',StringType(), False),	StructField('Cancelled',StringType(), False),	StructField('CancellationCode',StringType(), False),	StructField('Diverted',StringType(), False),	StructField('CarrierDelay',StringType(), False),	StructField('WeatherDelay',StringType(), False),	StructField('NASDelay',StringType(), False),	StructField('SecurityDelay',StringType(), False),	StructField('LateAircraftDelay',StringType(), False)])



# COMMAND ----------

df =  spark.read.format("csv").option("header", True).option("delimiter", ",").schema(airlines_schema).load("dbfs:/databricks-datasets/asa/airlines/*")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/OptimizeZorder")
df.write.format('delta').mode('overwrite').save("dbfs:/FileStore/tables/OptimizeZorder")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/OptimizeZorder

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize and ZOrder lets you speed up the data reading and writing by changing the layout of the data in the cloud storage
# MAGIC ### The algorithms that support changing the layout of data are 
# MAGIC ### 1. Bin-packing
# MAGIC ### 2. ZOrdering
# MAGIC
# MAGIC ## Bin packing algorithm  - uses Optimize command - helps to coalesce small files into larger files. In the above exercise, there are many many small files written to the delta folder. 
# MAGIC ### When I run the write statement here, it is writing 99 smaller files. 
# MAGIC ### it would be hard to read from 99 file if required.
# MAGIC ## to speed up the process, we will use Optimize
# MAGIC ## Zorder algorithm  - uses ZOrder command. This command needs to be used against the columns that are used in "WHERE" CLAUSE.
# MAGIC ## On the high cardinality columns - Zorder shall be used.  Meaning the columns that have high distinct values.
# MAGIC ## Zorder helps you to co-locate the data into a single file or small set of files
# MAGIC ### This reduces the amount of data reads from Spark.

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now create a delta table on top of delta path
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table airlines
# MAGIC using delta
# MAGIC location 'dbfs:/FileStore/tables/OptimizeZorder'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize airlines
# MAGIC zorder by (Year, Origin)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/OptimizeZorder

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is before optimization (Version 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from airlines version as of  0 where year like '19%' and origin in ('SYR', 'JFK', 'LAX')
# MAGIC -- this is slower as more files to refer

# COMMAND ----------

# MAGIC %md
# MAGIC ## This is after optimization (Version 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from airlines version as of  1 where year like '19%' and origin in ('SYR', 'JFK', 'LAX')
# MAGIC -- this is faster as less files to refer

# COMMAND ----------

# MAGIC %md
# MAGIC ### The number of records before optimization and after optimization are same.
# MAGIC ### however, 98 files are written as 5 files now.
# MAGIC ### but as the history is maintained in the Delta tables, the old files  (98) are added to the new 5 files. Making total 113 files (98 + 5 + extra files)
# MAGIC
