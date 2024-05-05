# Databricks notebook source
df_airlines = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("dbfs:/databricks-datasets/asa/airlines/2001.csv")
display(df_airlines)

# COMMAND ----------

df_airlines = df_airlines.withColumnRenamed("DepTime", "Departure Time")
df_airlines.columns

# COMMAND ----------

df_select = df_airlines.select("year", "CRSArrTime", "UniqueCarrier")
display(df_select)
df_select.count()


# COMMAND ----------

df_distinct = df_select.distinct()
display(df_distinct)
df_distinct.count()

# COMMAND ----------

df_dropDuplicates = df_airlines.dropDuplicates(["Year", "Month"])
display(df_dropDuplicates)
df_dropDuplicates.count()

# COMMAND ----------

df_dropDuplicates = df_airlines.select("Month", "Year").dropDuplicates(["Year", "Month"])
display(df_dropDuplicates)
df_dropDuplicates.count()

# COMMAND ----------

df_sorted = df_dropDuplicates.sort(df_dropDuplicates["Month"].desc())
display(df_sorted )

# COMMAND ----------

df_sorted = df_dropDuplicates.sort(df_dropDuplicates["Month"].asc())
display(df_sorted )

# COMMAND ----------

df_sorted = df_dropDuplicates.sort(df_dropDuplicates["Month"], ascending=True)
display(df_sorted )

# COMMAND ----------

from pyspark.sql.functions import asc, desc, asc_nulls_first, asc_nulls_last
df_sortedAsc = df_dropDuplicates.sort(asc("Month"))
display(df_sortedAsc)

# COMMAND ----------

df_groupBy = df_airlines.groupBy("Origin").count()
display(df_groupBy)
