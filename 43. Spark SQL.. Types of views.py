# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables
# MAGIC

# COMMAND ----------

""" Upload CountrySales.csv into dbfs:/FileStore/tables/ location
"""
df = spark.read.csv("dbfs:/FileStore/tables/CountrySales.csv", header=True, inferSchema=True)

# COMMAND ----------

# Create a temporary view . This is the local temporary view.
df.createOrReplaceTempView('view1')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from view1
# MAGIC

# COMMAND ----------

# Use view with the select statement 
df_selectedColumns =  spark.sql("select region, country, saleschannel, unitssold, totalrevenue from view1")
display(df_selectedColumns)

# COMMAND ----------

# Use view with the where clause
df_selectedWhere =  spark.sql("select * from view1 where saleschannel = 'Offline'")
display(df_selectedWhere)

# COMMAND ----------

df_selectedWhereOrder =  spark.sql("select * from view1 where saleschannel = 'Offline' order by region")
display(df_selectedWhereOrder)

# COMMAND ----------

# Writing the aggregate - use triple quotes when writing in multiple lines

df_aggregate = spark.sql(""" select region, country , count(unitsSold) from view1 
          group by region, country 
          having count(unitsSOld) >= 10 order by 3 desc""")
display(df_aggregate)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # This view (view1 as this is local temporary view) cannot (CANNOT ) be accessed from other notebook

# COMMAND ----------

# Use global temporary views
df.createOrReplaceGlobalTempView('vw_global')
df_india = spark.sql("select * from global_temp.vw_global where country = 'India'")
display(df_india)

# COMMAND ----------

# MAGIC %sql
# MAGIC show  tables in default

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view view1

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases  

# COMMAND ----------

# List tables or views in the global_temp database
tables = spark.catalog.listTables('global_temp')

# Print the list of tables or views
for table in tables:
    print("Table name :" , table.name)
    print("Table details :", table)



# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- /* Dropping the global temporary view */
# MAGIC drop view global_temp.vw_global

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This cannot be found as the view is dropped already
# MAGIC select * from global_temp.vw_global

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This cannot be found as the view is dropped already
# MAGIC select * from  view1
