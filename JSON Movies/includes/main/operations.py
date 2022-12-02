# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )

# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter(col("Runtime") > 0 and col("budget")>=1000000),
        dataframe.filter(col("Runtime") <= 0 or col("budget")<1000000),
    )


# COMMAND ----------

# ANSWER
def read_batch_bronze(spark: SparkSession) -> DataFrame:
    return spark.read.table("bronze_movie").filter("status = 'new'")

# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)

# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    kafka_schema = "value ARRAY"
    return spark.read.format("json").option("multiline","true").schema(kafka_schema).load(rawPath).select(explode(raw_movie_data_df.movie).alias("movie")

# COMMAND ----------

def transform_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

    json_schema = StructType(fields=[
        StructField('BackdropUrl', StringType(), True),
        StructField('Budget', StringType(), True),
        StructField('CreatedDate', DateType(), True),
        StructField('Id', IntegerType(), True),
        StructField('ImdbUrl', StringType(), True),
        StructField('OriginalLanguage', StringType(), True),
        StructField('Overview', StringType(), True),
        StructField('PosterUrl', StringType(), True),
        StructField('Price', DoubleType(), True),
        StructField('ReleaseDate', StringType(), True),
        StructField('Revenue', DoubleType(), True),
        StructField('RunTime', DoubleType(), True),
        StructField('Tagline', StringType(), True),
        StructField('Title', StringType(), True),
        StructField('TmdbUrl', StringType(), True),
        StructField(
            'genres', ArrayType(
                StructType([
                    StructField('id', IntegerType(), True),
                    StructField('name', StringType(), True)
                ])
            )
        )
    ])

    bronzeAugmentedDF = bronze.withColumn(
        "nested_json", from_json(col("movie"), json_schema)
    )

    silver_movie = bronzeAugmentedDF.select("movie", "nested_json.*")

    if not quarantine:
        silver_movie = silver_movie.select(
            "movie", "Id", "Title", "Overview", "Tagline", "Budget", "Revenue", "ImdbUrl", "TmdbUrl", "PosterUrl", "BackdropUrl", 
    "OriginalLanguage", "ReleaseDate", "RunTime" ,"Price", "CreatedDate", "UpdatedDate", "UpdatedBy", "CreatedBy", "genres", "status", "p_ingestdate" 
        )
    else:
        silver_health_tracker = silver_movie.select(
           "movie", "Id", "Title", "Overview", "Tagline", "Budget", "Revenue", "ImdbUrl", "TmdbUrl", "PosterUrl", "BackdropUrl", 
    "OriginalLanguage", "ReleaseDate", "RunTime" ,"Price", "CreatedDate", "UpdatedDate", "UpdatedBy", "CreatedBy", "genres", "status", "p_ingestdate" 
        )

    return silver_movie


# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str, userTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
    health_tracker_user_df = spark.read.table(userTable).alias("user")
    repairDF = bronzeQuarTransDF.join(
        health_tracker_user_df,
        bronzeQuarTransDF.device_id == health_tracker_user_df.user_id,
    )
    silverCleanedDF = repairDF.select(
        col("quarantine.value").alias("value"),
        col("user.device_id").cast("INTEGER").alias("device_id"),
        col("quarantine.steps").alias("steps"),
        col("quarantine.eventtime").alias("eventtime"),
        col("quarantine.name").alias("name"),
        col("quarantine.eventtime").cast("date").alias("p_eventdate"),
    )
    return silverCleanedDF

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
        lit("files.training.databricks.com").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        lit("new").alias("status"),
        "value",
        current_timestamp().cast("date").alias("p_ingestdate"),
    )


# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze.value = dataframe.value"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True
