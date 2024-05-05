# Databricks notebook source
list1 = [('John','a71becb3-536c-ed11-81ac-0022489337d7'),
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

rdd1 = sc.parallelize(list1)
rdd1.collect()

# COMMAND ----------

display(type(rdd1))

# COMMAND ----------

rdd1.count()

# COMMAND ----------

rdd1.getNumPartitions()

# COMMAND ----------

rdd2= rdd1.repartition(6)

# COMMAND ----------

rdd2.getNumPartitions()

# COMMAND ----------

rdd3 = rdd2.coalesce(2)

# COMMAND ----------

rdd3.getNumPartitions()

# COMMAND ----------

from pyspark.sql import Row
rdd_row = rdd1.map(lambda x:Row(Name=x[0], Age=x[1]))
rdd_row.collect()

# COMMAND ----------

df = spark.createDataFrame(rdd_row)
display(df)

# COMMAND ----------

from pyspark.sql import Row
rdd_prasanth = rdd1.map(lambda i: Row(i[0], i[1]))
rdd_prasanth.collect()


# COMMAND ----------

df1 = rdd_prasanth.toDF()
display(df1)

# COMMAND ----------

df1 = rdd_prasanth.toDF(["Name", "Age"])
display(df1)

# COMMAND ----------

df = spark.createDataFrame(rdd_prasanth).toDF("Name", "Age")
display(df)

# COMMAND ----------


