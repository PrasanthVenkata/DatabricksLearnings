# Databricks notebook source
# MAGIC %md
# MAGIC Full path of the notebook - hover over the notebook name above and you will get the path.
# MAGIC
# MAGIC go to the folder and copy path - /Users/venkataprasanth116@gmail.com/52. CallingNotebookA
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create 2 text widgets to add paramters to the notebook.

# COMMAND ----------

dbutils.widgets.text("First_Name", defaultValue="", label="Employee worker First Name")
dbutils.widgets.text("Last_Name", defaultValue="", label="Employee worker Last Name")

# COMMAND ----------

varFirstName = dbutils.widgets.get("First_Name")
varLastName = dbutils.widgets.get("Last_Name")
display(varFirstName + "," + varLastName)

# COMMAND ----------

def Wish():
    return "Hello, good morning  " + varFirstName +"," +varLastName

# COMMAND ----------

morning_wishes = Wish()
print (morning_wishes)

# COMMAND ----------

def add_numbers(x, y):
    return x + y

# COMMAND ----------

z = add_numbers(100, 200)
print(z)

# COMMAND ----------

def rev_string(s):
    return s[::-1]

# COMMAND ----------

print( rev_string("Welcome"))
