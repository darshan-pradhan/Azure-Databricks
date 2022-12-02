# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# ANSWER
username = "dpradhan"

# COMMAND ----------


classicPipelinePath = f"/FileStore/{username}/"
landingPath = classicPipelinePath + "landing/"

rawPath = classicPipelinePath + "raw/"
bronzePath = classicPipelinePath + "bronze/"
silverPath = classicPipelinePath + "silver/"
silverQuarantinePath = classicPipelinePath + "silverQuarantine/"
goldPath = classicPipelinePath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS movie_{username}")
spark.sql(f"USE movie_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities
