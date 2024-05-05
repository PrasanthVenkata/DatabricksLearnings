# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/tables/XML")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/XML")

# COMMAND ----------

df_employee = spark.read.format("xml").\
    option("inferSchema", True)\
        .option("rootTag", "cloudmonks").\
            option("rowTag", "Employee").\
                load("dbfs:/FileStore/tables/XML/Employees.xml")
df_employee.printSchema()
display(df_employee)

# COMMAND ----------

# reading a nested xml file
df_employee_nested = spark.read.format("xml").\
    option("inferSchema", True)\
        .option("rootTag", "cloudmonks").\
            option("rowTag", "Employee").\
                load("dbfs:/FileStore/tables/XML/Employees_Nested.xml")
df_employee_nested.printSchema()
display(df_employee_nested)

# COMMAND ----------

# reading a xml file by specifying the Schema using the DDL (Data definition Language Method)
schema_str= "Department_ID STRING, Employee_ID STRING, Job STRING, Name STRING, Salary Decimal(10,2)"

df_employee_DDL = spark.read.format("xml")\
                .schema(schema_str).\
    option("inferSchema", False)\
        .option("rootTag", "cloudmonks").\
            option("rowTag", "Employee").\
                load("dbfs:/FileStore/tables/XML/Employees_Nested.xml")
df_employee_DDL.printSchema()
display(df_employee_DDL)

# COMMAND ----------

# Flattening the data in the XML file using object reference when there is a presence of a Struct element
from pyspark.sql.functions import col
df_employee_nested = spark.read.format("xml").\
    option("inferSchema", True)\
        .option("rootTag", "cloudmonks").\
            option("rowTag", "Employee").\
                load("dbfs:/FileStore/tables/XML/Employees_Nested.xml")
df_employee_nested.printSchema()
display(df_employee_nested)

df_skills =  df_employee_nested.select(col("Name"), col("SkillSet.PrimarySkill").alias("PRIMARY SKILLS "), col("SkillSet.SecondarySkill").alias("SECONDARY SKILLS"))
display(df_skills)

