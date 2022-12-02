# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Table Updates
# MAGIC 
# MAGIC We have processed data from the Bronze table to the Silver table.
# MAGIC 
# MAGIC We now need to do some updates to ensure high data quality in the Silver
# MAGIC table. Because batch loading has no mechanism for checkpointing, we will
# MAGIC need a way to load _only the new records_ from the Bronze table.
# MAGIC 
# MAGIC We also need to deal with the quarantined records.

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
# MAGIC ### Step 1: Load Quarantined Records from the Bronze Table

# COMMAND ----------

# ANSWER

bronzeQuarantinedDF = spark.read.table("bronze_movie").filter(
    "status = 'quarantined'"
)
display(bronzeQuarantinedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Quarantined Records
# MAGIC 
# MAGIC This applies the standard bronze table transformations.

# COMMAND ----------

bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
    "quarantine"
)
display(bronzeQuarTransDF)

# COMMAND ----------

bronzeQuarTransDF  = bronzeQuarTransDF.withColumn('Runtime', abs(col('Runtime')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Batch Write the Repaired (formerly Quarantined) Records to the Silver Table
# MAGIC 
# MAGIC After loading, this will also update the status of the quarantined records
# MAGIC to `loaded`.

# COMMAND ----------

bronzeToSilverWriter = batch_writer(
    dataframe=bronzeQuarTransDF, partition_column="p_ingestdate", exclude_columns=["movie"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, bronzeQuarTransDF, "loaded")

# COMMAND ----------

# movie genre silver table: junction table: Id, genre_id
movie_genre = bronzeQuarTransDF.select("genres")
silver_genre_exploded = (movie_genres.withColumn(
                         "genres", explode("genres")).select(
                                col("genres.id").alias("genre_id"),
                                col("genres.name").alias("genre_name")
                                ).dropDuplicates(["genre_id"]))


# COMMAND ----------

(
    GenreDF =  silver_genre_exploded.select(*)
    .write.format("delta")
    .mode("append")
    .save(silverGenrePath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silverGenre
"""
)

spark.sql(
    f"""
CREATE TABLE silverGenre
USING DELTA
LOCATION "{silverGenrePath}"
"""
)

# COMMAND ----------

LanguageDF = bronzeQuarTransDF.select("OriginalLanguage")

# COMMAND ----------

(
  LanguageDF.select("*")
      .write.format("delta")
      .mode("OVERWRITE")
      .save(silverOriginalLanguagesPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silverOriginalLanguages
"""
)

spark.sql(
    f"""
CREATE TABLE silverOriginalLanguages
USING DELTA
LOCATION "{silverOriginalLanguagesPath}"
"""
)
