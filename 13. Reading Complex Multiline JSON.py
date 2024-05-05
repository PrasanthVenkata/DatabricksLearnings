# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/tables/ComplexJSON")


# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/ComplexJSON")

# COMMAND ----------

df_stuDetails = spark.read.format("json").option("multiLine",True).load("dbfs:/FileStore/tables/ComplexJSON/StudentsDetails.json")
display(df_stuDetails)
df_stuDetails.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col
df_sNew= df_stuDetails.select("sname", "result", explode(col("marks")).alias("FlattenedMarks"))
display(df_sNew)
df_sNew.printSchema()
df_flat =  df_sNew.select("FlattenedMarks.*", "FlattenedMarks.city")
df_flat.printSchema()
display(df_flat)
df2  =  df_flat.select("email", col("hallticket").alias("Roll number"))
display(df2)


# COMMAND ----------

df_exploded =  df_stuDetails.withColumn("Exploded marks", explode("marks")).drop("marks")
df_exploded.printSchema()

# COMMAND ----------

#use object reference technique to flatten the elements of struct data type
df_objRef = df_exploded.select("Exploded marks.*", col("city").alias("New city"))
display(df_objRef)


# COMMAND ----------



# COMMAND ----------

df_employee = spark.read.json("dbfs:/FileStore/tables/ComplexJSON/Employee.json", multiLine=True)
display(df_employee)
df_employee.printSchema()

# COMMAND ----------

df_e1 = df_employee.select("address.*", "address.city")
display(df_e1)

df_employee_exploded = df_employee.select("address", "age", "firstName", "gender", "lastName", explode("phoneNumbers").alias("PhonenumbersExploded"))
df_employee_exploded.printSchema()
display(df_employee_exploded)

# now flatten the struct types using the object reference
df_flattenedStru = df_employee_exploded.select("address.city", "address.postalCode", "PhonenumbersExploded.number", "PhonenumbersExploded.type")
df_flattenedStru.printSchema()
display(df_flattenedStru)

