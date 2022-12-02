# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw Data Generation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest data from a remote source into our source directory, `rawPath`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Function
# MAGIC 
# MAGIC Run the following command to load the utility function, `retrieve_data`.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(classicPipelinePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data Directory

# COMMAND ----------

display(rawPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print the Contents of the Raw Files
# MAGIC **EXERCISE**: Add the correct file paths to display the contents of the two raw files you loaded.

# COMMAND ----------

# ANSWER
print(
    dbutils.fs.head(
        dbutils.fs.ls(rawPath)[0].path
    )
)

# COMMAND ----------

# ANSWER
print(
    dbutils.fs.head(
        dbutils.fs.ls(rawPath)[1].path
    )
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------


