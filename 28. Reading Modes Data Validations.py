# Databricks notebook source
# Databricks notebook source
list_files = dbutils.fs.ls("dbfs:/FileStore/tables/prasanth/csv")
display(list_files)

# COMMAND ----------

for x in dbutils.fs.ls("dbfs:/FileStore/tables"):
    print(x.path)

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
cols = StructType([StructField("empno", IntegerType(), False),
                  StructField("First_Name", StringType(), False),
                  StructField("Last_Name", StringType(), False),
                  StructField("city", StringType(), False),
                  StructField("email", StringType(), False),
                  StructField("hiredate", StringType(), False),
                  StructField("phone", StringType(), False),
                  StructField("salary", IntegerType(), False)
                 ])


# COMMAND ----------

df_permissive = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", True).option("delimiter", ",").schema(cols).load("dbfs:/FileStore/tables/prasanth/csv/Employees_WestTampered.csv")
display(df_permissive)

# COMMAND ----------

df_FailFast = spark.read.format("csv").option("mode", "FAILFAST").option("header", True).option("delimiter", ",").schema(cols).load("dbfs:/FileStore/tables/prasanth/csv/Employees_WestTampered.csv")
display(df_FailFast)

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
Schema_withCorruptRecord = StructType([StructField("empno", IntegerType(), False),
                  StructField("First_Name", StringType(), False),
                  StructField("Last_Name", StringType(), False),
                  StructField("city", StringType(), False),
                  StructField("email", StringType(), False),
                  StructField("hiredate", StringType(), False),
                  StructField("phone", StringType(), False),
                  StructField("salary", IntegerType(), False),
                  StructField("CorruptRecord", StringType(), True)
                 ])

# COMMAND ----------

df_permissive = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", True).option("delimiter", ",").schema(cols).load("dbfs:/FileStore/tables/prasanth/csv/Employees_WestTampered.csv")
display(df_permissive)
df_permissive.count()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
NewSchema = StructType([StructField("empno", IntegerType(), False),
                  StructField("First_Name", StringType(), False),
                  StructField("Last_Name", StringType(), False),
                  StructField("city", StringType(), False),
                  StructField("email", StringType(), False),
                  StructField("hiredate", StringType(), False),
                  StructField("phone", StringType(), False),
                  StructField("salary", IntegerType(), False)                
                 ])
# here corrupted data is stored as NULL values
df_permissive = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", True).option("delimiter", ",").schema(NewSchema).load("dbfs:/FileStore/tables/prasanth/csv/Employees_WestTampered.csv")
display(df_permissive)
df_permissive.count()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
SchemaWithCOrruptRecord = StructType([StructField("empno", IntegerType(), False),
                  StructField("First_Name", StringType(), False),
                  StructField("Last_Name", StringType(), False),
                  StructField("city", StringType(), False),
                  StructField("email", StringType(), False),
                  StructField("hiredate", StringType(), False),
                  StructField("phone", StringType(), False),
                  StructField("salary", IntegerType(), False),
                  StructField("CorruptRecord", StringType(), True)                
                 ])
# here corrupted data is stored as NULL values
df_permissive = spark.read.format("csv").option("mode", "PERMISSIVE").option("header", True).option("delimiter", ",").schema(SchemaWithCOrruptRecord).\
    option("columnNameOfCorruptRecord","CorruptRecord").\
    load("dbfs:/FileStore/tables/prasanth/csv/Employees_WestTampered.csv")
display(df_permissive)
df_permissive.count()

# COMMAND ----------

from pyspark.sql.functions import col
df_validRecords = df_permissive.where(col("CorruptRecord").isNull() )
display(df_validRecords)

# COMMAND ----------

from pyspark.sql.functions import col
df_validRecords = df_permissive.where(col("CorruptRecord").isNull() ).drop("CorruptRecord")
display(df_validRecords)

# COMMAND ----------

from pyspark.sql.functions import col
df_validRecords = df_permissive.where(col("CorruptRecord").isNull() == False )
display(df_validRecords)

# COMMAND ----------

from pyspark.sql.functions import col
df_validRecords = df_permissive.where(col("CorruptRecord").isNull() == False ).drop("CorruptRecord")
display(df_validRecords)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
SchemaWithCOrruptRecord = StructType([StructField("empno", IntegerType(), False),
                  StructField("First_Name", StringType(), False),
                  StructField("Last_Name", StringType(), False),
                  StructField("city", StringType(), False),
                  StructField("email", StringType(), False),
                  StructField("hiredate", StringType(), False),
                  StructField("phone", StringType(), False),
                  StructField("salary", IntegerType(), False),
                  StructField("CorruptRecord", StringType(), True)                
                 ])
# here corrupted data is stored as NULL values
df_malformed = spark.read.format("csv").option("mode", "DROPMALFORMED").option("header", True).option("delimiter", ",").schema(SchemaWithCOrruptRecord).\
    option("columnNameOfCorruptRecord","CorruptRecord").\
    load("dbfs:/FileStore/tables/prasanth/csv/Employees_WestTampered.csv")
display(df_malformed)
df_malformed.count()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
SchemaWithCOrruptRecord = StructType([StructField("empno", IntegerType(), False),
                  StructField("First_Name", StringType(), False),
                  StructField("Last_Name", StringType(), False),
                  StructField("city", StringType(), False),
                  StructField("email", StringType(), False),
                  StructField("hiredate", StringType(), False),
                  StructField("phone", StringType(), False),
                  StructField("salary", IntegerType(), False),
                  StructField("CorruptRecord", StringType(), True)                
                 ])
# here corrupted data is stored as NULL values
df_FailFast = spark.read.format("csv").option("mode", "FAILFAST").option("header", True).option("delimiter", ",").schema(SchemaWithCOrruptRecord).\
    option("columnNameOfCorruptRecord","CorruptRecord").\
    load("dbfs:/FileStore/tables/prasanth/csv/Employees_WestTampered.csv")
display(df_FailFast)
df_FailFast.count()
