# Databricks notebook source
# MAGIC %md
# MAGIC # Raw to Bronze Pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 1. Ingest Raw Data
# MAGIC 2. Augment the data with Ingestion Metadata
# MAGIC 3. Batch write the augmented data to a Bronze Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest raw data
# MAGIC 
# MAGIC Next, we will read files from the source directory and write each line as a string to the Bronze table.
# MAGIC 
# MAGIC 🤠 You should do this as a batch load using `spark.read`
# MAGIC 
# MAGIC Read in using the format, `"text"`, and using the provided schema.

# COMMAND ----------

# ANSWER
kafka_schema = "value ARRAY"

raw_movie_data_df = (
    spark.read.format("json").option("multiline","true").schema(kafka_schema).load(rawPath).select(explode("movie").alias("movies"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw Data
# MAGIC 
# MAGIC 🤓 Each row here is a raw string in JSON format, as would be passed by a stream server like Kafka.

# COMMAND ----------

display(raw_movie_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Metadata
# MAGIC 
# MAGIC As part of the ingestion process, we record metadata for the ingestion.
# MAGIC 
# MAGIC **EXERCISE:** Add metadata to the incoming raw data. You should add the following columns:
# MAGIC 
# MAGIC - data source (`datasource`), use `"files.training.databricks.com"`
# MAGIC - ingestion time (`ingesttime`)
# MAGIC - status (`status`), use `"new"`
# MAGIC - ingestion date (`ingestdate`)

# COMMAND ----------

# Ingestion Metadata
from pyspark.sql.functions import current_timestamp, lit

raw_movies_data_df = (
  raw_movies_data_df.select(
    "movie",
    lit("files.training.databricks.com").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate"),
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table
# MAGIC 
# MAGIC Finally, we write to the Bronze Table.
# MAGIC 
# MAGIC Make sure to write in the correct order (`"datasource"`, `"ingesttime"`, `"value"`, `"status"`, `"p_ingestdate"`).
# MAGIC 
# MAGIC Make sure to use following options:
# MAGIC 
# MAGIC - the format `"delta"`
# MAGIC - using the append mode
# MAGIC - partition by `p_ingestdate`

# COMMAND ----------

from pyspark.sql.functions import col
(raw_movie_data_df.select("datasource",
                          "ingesttime",
                          "movie",
                          "status",
                          col("ingestdate").alias("p_ingestdate"))
 .write.format("delta")
 .mode("append")
 .partitionBy("p_ingestdate")
 .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore
# MAGIC 
# MAGIC The table should be named `bronze_movie`.

# COMMAND ----------

# ANSWER
spark.sql(
    """
DROP TABLE IF EXISTS bronze_movie
"""
)

spark.sql(
    f"""
CREATE TABLE bronze_movie
USING DELTA
LOCATION "{bronzePath}"
"""
)
