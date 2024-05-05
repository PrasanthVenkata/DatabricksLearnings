# Databricks notebook source
from pyspark.sql.functions import lit
df1 =  spark.range(3).withColumn("CustomerId", lit("1"))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark.range generates sequence numbers from 0 - (n-1). Where n is the argument. 0 inclusive and n exclusive. 3 means 0 inclusive and 3 exclusive.
# MAGIC 1 in double quotes represents string.

# COMMAND ----------

df1.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/DeltaSchema")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/DeltaSchema

# COMMAND ----------

from pyspark.sql.functions import lit
df2 =  spark.range(3).withColumn("CustomerName", lit(2))
display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark.range generates sequence numbers from 0 - (n-1). Where n is the argument. 0 inclusive and n exclusive. 3 means 0 inclusive and 3 exclusive.
# MAGIC 2 without double quotes represents integer.

# COMMAND ----------

# MAGIC %md
# MAGIC try to append an integer value to string value

# COMMAND ----------

try:
    df2.write.format("delta").mode("append").save("dbfs:/FileStore/tables/DeltaSchema")
except Exception as e:
    print("An error occuured", str(e) )

# COMMAND ----------

from pyspark.sql.functions import lit
dbutils.fs.rm("dbfs:/FileStore/tables/DeltaSchema2", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Now create a new data frame with one column.
# MAGIC create a second data frame with one column.
# MAGIC try to append both of them

# COMMAND ----------

from pyspark.sql.functions import lit
df1 = spark.range(4)
df1.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/DeltaSchema2")


# COMMAND ----------

display(df1)

# COMMAND ----------



# COMMAND ----------


try:
    df2 = spark.range(4).withColumn("CustomerID", lit(4))
    display(df2)
    df2.write.format("delta").mode("append").save("dbfs:/FileStore/tables/DeltaSchema2")
except Exception as e:
    print("Exception occurred", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC If you use mergeSchema is true, then no error occurs.

# COMMAND ----------

try:
    df2 = spark.range(4).withColumn("CustomerID", lit(4))
    display(df2)
    df2.write.format("delta").mode("append").option("mergeSchema", True).save("dbfs:/FileStore/tables/DeltaSchema2")
    display(df2)
except Exception as e:
    print("Exception occurred", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC data after appending one data frame to other data frame.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/FileStore/tables/DeltaSchema2`
