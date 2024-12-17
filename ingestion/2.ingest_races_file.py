# Databricks notebook source
races_raw_df = spark.read \
    .option("header", True) \
    .csv("/mnt/f1winterdl/raw/races.csv")

# COMMAND ----------

display(races_raw_df)

# COMMAND ----------

races_raw_df = races_raw_df.select([c for c in races_raw_df.columns if c in ['raceId', 'year', 'round', 'circuitId', 'name', 'date', 'time', 'url']])

display(races_raw_df)

# COMMAND ----------

# set the datatypes. Note that the third parameter is `nullable`
from pyspark.sql.types import *

races_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_raw_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv("/mnt/f1winterdl/raw/races.csv")

# COMMAND ----------

display(races_raw_df)

# COMMAND ----------

from pyspark.sql.functions import col
# select and rename columns

