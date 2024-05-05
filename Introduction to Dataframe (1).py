# Databricks notebook source
x=100
print(type(x))

# COMMAND ----------

y="Python"
print (type(y))

# COMMAND ----------

z= False
print(type(z))

# COMMAND ----------

x=[10,20,30, 40]
print(type(x))

# COMMAND ----------

x=[7369, 'String', False]
print(type(x))

# COMMAND ----------

x={7369, 'String', False}
print(type(x))

# COMMAND ----------

# MAGIC %md
# MAGIC ## LIst is a collection of elements of same type or different type which are enclosed in Square brackets [].
# MAGIC ## List is a mutable object. Once the list object is created, the object can be modified at any moment of time. This is called mutable.
# MAGIC ## Tuple is a collection of elements of same type or different type which is enclosed in Paranthesis ().
# MAGIC ## Tuple is a immutable object. Once the tuple object is created,, it cannot be modified.
# MAGIC

# COMMAND ----------

x=[10,20,30, 40]
print(type(x))
x=[10,411, 30, 40]

# COMMAND ----------

x=[10,20,30, 40]
print(type(x))
print(x[1])
x[1] =200
print(x[1])
x.pop()
print(x)

# COMMAND ----------

x=(10,20,30, 40)
print(type(x))
print(x[1])
x[1] =200


# COMMAND ----------

#Create data frame

student_data = [(1, "prasanth", "hyderabad", 3000, "Databricks"),
           (2, "ravi", "AP", 4000, "ADF"),           
           (3, "harish", "hyderabad", 300, "DevOps"),
           (4, "ravi", "hyderabad", 9000, "SQL")]


student_cols = ["Student ID", "Student Name", "Fees", "Course"]

df_students = spark.createDataFrame(data= student_data, schema = student_cols)
display(df_students)
display(df_students.limit(2))
df_students.show(3, truncate=False )
df_students.printSchema()
df_students.columns
df_students.collect()

# COMMAND ----------

x= {"Customer name":"prasanth", "id":7001, "city": "Hyd", "country":"india"}
print(type(x))

# COMMAND ----------

x= [{"Customer name":"prasanth", "id":7001, "city": "Hyd", "country":"india"},
    {"Customer name":"ravi", "id":7002, "city": "Hyd", "country":"india"},
    {"Customer name":"jagannath", "id":7003, "city": "Hyd", "country":"india"},
    {"Customer name":"krishna", "id":7002, "city": "Hyd", "country":"india"}]
print(type(x))

df_customers = spark.createDataFrame(data=x)
display(df_customers)

df_customers_new = df_customers.select("Customer name", "id") 
display(df_customers_new)

# COMMAND ----------




from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType, LongType

student_schema1 = StructType([StructField("Student_id", IntegerType(), False),
                             StructField("Student_Name", StringType(), False),
                             StructField("Course", StringType(), True),
                             StructField("Duration", IntegerType(), False),
                             StructField("Fees", FloatType(), True)] )


student_data1 = [(1, "prasanth", "Databricks", 2, 3000.0),
           (2, "ravi",  "ADF", 3, 4000.1),           
           (3, "harish", "Devops", 5, 300.1),
           (4, "ravi", "SQL", 1, 9000.1)]


df = spark.createDataFrame(data= student_data1, schema= student_schema1)

display(df)


#### Using 2nd method DDL
student_schema2 = "Student_ID Integer, Student_name String, Course String, Duration Integer, Fees Float"

df_DDL = spark.createDataFrame(data= student_data1, schema= student_schema2)
display(df_DDL)

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType, LongType
from pyspark.sql.functions import to_date


sales_data = [("1", 100.1, "2023-10-01"),
              ("2", 100.1, "2023-01-21"),
              ("3", 100.1, "2023-03-31"),
              ("4", 100.1, "2023-07-01")
]
sales_schema = "SaleID String, SaleAMount Float, SaleDate String"
df_sales = spark.createDataFrame(data = sales_data, schema= sales_schema)

display(df_sales)


df_sales_newColumn = df_sales.withColumn("SalesDate_InDate", to_date("SaleDate", "yyyy-MM-dd"))
display(df_sales_newColumn)

