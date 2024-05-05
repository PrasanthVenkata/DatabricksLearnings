# Databricks notebook source
fname = "Ganesh"
lname = "Vaikuntam"

# COMMAND ----------

print(fname + "," + lname)

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %run /Users/venkataprasanth116@gmail.com/53.NotebookA  $First_Name ="Ganesh" $Last_Name="Vaikuntam"

# COMMAND ----------

print(fname + "," + lname)

# COMMAND ----------

dbutils.notebook.exit()

# COMMAND ----------

# MAGIC %md
# MAGIC #Please note that the run command (belo version usong dbutils.notebook.run) does not work in the Community version.

# COMMAND ----------

dbutils.notebook.run("/Users/venkataprasanth116@gmail.com/53.NotebookA", 300, {"fname":"Ganesh", "lname":"Vaikuntam"  })

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.run("/Users/venkataprasanth116@gmail.com/53.NotebookA", 300 )

# COMMAND ----------


