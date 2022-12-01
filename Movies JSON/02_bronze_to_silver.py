# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - ETL into a Silver table
# MAGIC 
# MAGIC We need to perform some transformations on the data to move it from bronze to silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest raw data using composable functions
# MAGIC 1. Use composable functions to write to the Bronze table
# MAGIC 1. Develop the Bronze to Silver Step
# MAGIC    - Extract and transform the raw string to columns
# MAGIC    - Quarantine the bad data
# MAGIC    - Load clean data into the Silver table
# MAGIC 1. Update the status of records in the Bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run ./includes/main/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Bronze Paths

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land More Raw Data
# MAGIC 
# MAGIC Before we get started with this lab, let's land some more raw data.
# MAGIC 
# MAGIC In a production setting, we might have data coming in every
# MAGIC hour. Here we are simulating this with the function
# MAGIC `ingest_classic_data`.
# MAGIC 
# MAGIC 😎 Recall that we did this in the notebook `00_ingest_raw`.

# COMMAND ----------

# ANSWER
ingest_classic_data(hours=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Delta Architecture
# MAGIC Next, we demonstrate everything we have built up to this point in our
# MAGIC Delta Architecture.
# MAGIC 
# MAGIC We do so not with the ad hoc queries as written before, but now with
# MAGIC composable functions included in the file `classic/includes/main/python/operations`.
# MAGIC You should check this file for the correct arguments to use in the next
# MAGIC three steps.
# MAGIC 
# MAGIC 🤔 You can refer to `plus/02_bronze_to_silver` if you are stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the `rawDF` DataFrame
# MAGIC 
# MAGIC **Exercise:** Use the function `read_batch_raw` to ingest the newly arrived
# MAGIC data.

# COMMAND ----------

# ANSWER
rawDF = read_batch_raw(rawPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Raw Data
# MAGIC 
# MAGIC **Exercise:** Use the function `transform_raw` to ingest the newly arrived
# MAGIC data.

# COMMAND ----------

# ANSWER
transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion
# MAGIC 
# MAGIC The DataFrame `transformedRawDF` should now have the following schema:
# MAGIC 
# MAGIC ```
# MAGIC datasource: string
# MAGIC ingesttime: timestamp
# MAGIC status: string
# MAGIC value: string
# MAGIC p_ingestdate: date
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import *

assert transformedRawDF.schema == StructType(
    [
        StructField("datasource", StringType(), False),
        StructField("ingesttime", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("value", StringType(), True),
        StructField("p_ingestdate", DateType(), False),
    ]
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver Step
# MAGIC 
# MAGIC Let's start the Bronze to Silver step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load New Records from the Bronze Records
# MAGIC 
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Load all records from the Bronze table with a status of `"new"`.

# COMMAND ----------

# ANSWER

bronzeDF = spark.read.table("bronze_movie").filter("status = 'new'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract the Nested JSON from the Bronze Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Extract the Nested JSON from the `value` column
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Use `pyspark.sql` functions to extract the `"value"` column as a new
# MAGIC column `"nested_json"`.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import from_json

json_schema = """
      movie STRING,
      BackdropUrl string,
      Budget double,
      CreatedBy string,
      CreatedDate DATE,
      Id long,
      ImdbUrl string,
      OriginalLanguage string,
      Overview string,
      PosterUrl string,
      Price double,
      ReleaseDate string,
      Revenue double,
      RunTime long,
      Tagline string,
      Title string,
      TmdbUrl string,
      UpdatedBy string,
      UpdatedDate DATE,
      genres array
"""

bronzeAugmentedDF = bronzeDF.withColumn(
    "nested_json", from_json(col("value"), json_schema)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create the Silver DataFrame by Unpacking the `nested_json` Column
# MAGIC 
# MAGIC Unpacking a JSON column means to flatten the JSON and include each top level attribute
# MAGIC as its own column.
# MAGIC 
# MAGIC 🚨 **IMPORTANT** Be sure to include the `"value"` column in the Silver DataFrame
# MAGIC because we will later use it as a unique reference to each record in the
# MAGIC Bronze table

# COMMAND ----------

# ANSWER
silver_health_tracker = bronzeAugmentedDF.select("value", "nested_json.*")
