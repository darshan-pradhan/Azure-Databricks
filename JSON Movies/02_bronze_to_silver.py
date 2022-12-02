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
        StructField("movie", StringType(), True),
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
      genres ARRAY
"""

bronzeAugmentedDF = bronzeDF.withColumn(
    "nested_json", from_json(col("movie"), json_schema)
)

# COMMAND ----------

bronzeAugmentedDF.printSchema()

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
silver_movie = bronzeAugmentedDF.select("movie", "nested_json.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion
# MAGIC 
# MAGIC The DataFrame `silver_health_tracker` should now have the following schema:
# MAGIC 
# MAGIC ```
# MAGIC value: string
# MAGIC time: timestamp
# MAGIC name: string
# MAGIC device_id: string
# MAGIC steps: integer
# MAGIC day: integer
# MAGIC month: integer
# MAGIC hour: integer
# MAGIC ```
# MAGIC 
# MAGIC 💪🏼 Remember, the function `_parse_datatype_string` converts a DDL format schema string into a Spark schema.

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert silver_movie.schema == _parse_datatype_string(
    """
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
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform the Data
# MAGIC 
# MAGIC 1. Create a column `p_eventdate DATE` from the column `time`.
# MAGIC 1. Rename the column `time` to `eventtime`.
# MAGIC 1. Cast the `device_id` as an integer.
# MAGIC 1. Include only the following columns in this order:
# MAGIC    1. `value`
# MAGIC    1. `device_id`
# MAGIC    1. `steps`
# MAGIC    1. `eventtime`
# MAGIC    1. `name`
# MAGIC    1. `p_eventdate`
# MAGIC 
# MAGIC 💪🏼 Remember that we name the new column `p_eventdate` to indicate
# MAGIC that we are partitioning on this column.
# MAGIC 
# MAGIC 🕵🏽‍♀️ Remember that we are keeping the `value` as a unique reference to values
# MAGIC in the Bronze table.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

silver_movie = silver_movie.select(
    "movie", "Id", "Title", "Overview", "Tagline", "Budget", "Revenue", "ImdbUrl", "TmdbUrl", "PosterUrl", "BackdropUrl", 
    "OriginalLanguage", "ReleaseDate", "RunTime" ,"Price", "CreatedDate", "UpdatedDate", "UpdatedBy", "CreatedBy", "genres", "status", "p_ingestdate" 
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine the Bad Data
# MAGIC 
# MAGIC Recall that at step, `00_ingest_raw`, we identified that some records were coming in
# MAGIC with device_ids passed as uuid strings instead of string-encoded integers.
# MAGIC Our Silver table stores device_ids as integers so clearly there is an issue
# MAGIC with the incoming data.
# MAGIC 
# MAGIC In order to properly handle this data quality issue, we will quarantine
# MAGIC the bad records for later processing.

# COMMAND ----------

# MAGIC %md
# MAGIC Check for records that have nulls - compare the output of the following two cells

# COMMAND ----------

silver_movie.count()

# COMMAND ----------

silver_movie.na.drop().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the Silver DataFrame

# COMMAND ----------

silver_movie_clean = silver_movie.filter(col("Runtime") > 0 and col("budget")>=1000000)
silver_movie_quarantine = silver_movie.filter(col("Runtime") <= 0 or col("budget")<1000000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Quarantined Records

# COMMAND ----------

display(silver_movie_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Clean Batch to a Silver Table

# COMMAND ----------

# ANSWER
(
    silver_movie_clean.select(
        "Id", "Title", "Overview", "Tagline", "Budget", "Revenue", "ImdbUrl", "TmdbUrl", "PosterUrl", "BackdropUrl", 
    "OriginalLanguage", "ReleaseDate", "RunTime" ,"Price", "CreatedDate", "UpdatedDate", "UpdatedBy", "CreatedBy", "genres", "status", "p_ingestdate" 
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silver_movie
"""
)

spark.sql(
    f"""
CREATE TABLE silver_movie
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze table to Reflect the Loads
# MAGIC 
# MAGIC **EXERCISE:** Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ### Step 1: Update Clean records
# MAGIC Clean records that have been loaded into the Silver table and should have
# MAGIC    their Bronze table `status` updated to `"loaded"`.
# MAGIC 
# MAGIC 💃🏽 **Hint** You are matching the `value` column in your clean Silver DataFrame
# MAGIC to the `value` column in the Bronze table.

# COMMAND ----------

# ANSWER
from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_movie_clean.withColumn("status", lit("loaded")).dropDuplicates()

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCISE:** Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ### Step 2: Update Quarantined records
# MAGIC Quarantined records should have their Bronze table `status` updated to `"quarantined"`.
# MAGIC 
# MAGIC 🕺🏻 **Hint** You are matching the `value` column in your quarantine Silver
# MAGIC DataFrame to the `value` column in the Bronze table.

# COMMAND ----------

# ANSWER
silverAugmented = silver_movie_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.movie = quarantine.movie"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)
