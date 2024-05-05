# Databricks notebook source
# MAGIC %md
# MAGIC ##Create 2 text widgets
# MAGIC ##Add parameters to the notebook
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("First_Name", "", "Enter First Name of the worker")
dbutils.widgets.text("Last_Name", "", "Enter Last Name of the worker")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve the values of the parameters from the widget
# MAGIC

# COMMAND ----------

fName = dbutils.widgets.get("First_Name")
lName = dbutils.widgets.get("Last_Name")
print(fName, lName)

# COMMAND ----------

def displayGM():
    return "Good morning !" + fName +"," +  lName

# COMMAND ----------

displayGM()

# COMMAND ----------

print(displayGM())

# COMMAND ----------

def rev(s):
    return s[::-1]

# COMMAND ----------

rev("James Bond")
