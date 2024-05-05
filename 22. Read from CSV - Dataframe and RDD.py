# Databricks notebook source
list1  = [('John','a71becb3-536c-ed11-81ac-0022489337d7'),
('Emily','eaace215-d771-ed11-81ab-00224897c147'),
('Emily','465d9e11-a26e-ed11-81ab-002248950d70'),
('Emily','ac957e9f-0671-ed11-81ac-00224810b565'),
('Legal rep for','4abaab35-ca71-ed11-81ac-0022489337d7'),
('Plantiff','f29aae2f-ca71-ed11-81ac-0022489337d7'),
('Legal rep ','16baab35-ca71-ed11-81ac-0022489337d7'),
('Dodgey','240079e3-c871-ed11-81ac-0022489336e2'),
('Elli','eda3278c-d071-ed11-81ab-002248950d70'),
('Florence','5bb839bd-1971-ed11-81ac-00224810b565')]

# COMMAND ----------

rdd = sc.parallelize(list1)

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

rdd1 = rdd.repartition(3)

# COMMAND ----------

rdd1.getNumPartitions()

# COMMAND ----------

rdd2= rdd.coalesce(1)

# COMMAND ----------

rdd2.getNumPartitions()

# COMMAND ----------

# Databricks notebook source
dbutils.fs.ls("dbfs:/databricks-datasets/asa/airlines")

# COMMAND ----------

dataframe_Airlines = spark.read.format("csv").option("header", True).option("sep", ",").load("dbfs:/databricks-datasets/asa/airlines/1988.csv") 

# COMMAND ----------

dataframe_Airlines.show()

# COMMAND ----------

dataframe_Airlines.rdd.getNumPartitions()

# COMMAND ----------

rdd1 = dataframe_Airlines.rdd.coalesce(2)

# COMMAND ----------

rdd1.getNumPartitions()
