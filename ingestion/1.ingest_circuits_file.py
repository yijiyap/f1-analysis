# Databricks notebook source
# find location of files
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1winterdl/raw

# COMMAND ----------

# set the datatypes. Note that the third parameter is `nullable`
from pyspark.sql.types import *

circuits_schema = StructType(fields = [
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# read files with spark - setting header to true
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv("/mnt/f1winterdl/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# double check the schema
circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
# select and rename columns
circuits_selected_df = circuits_df.select(
    col("circuitId").alias("circuit_id"),
    col("circuitRef").alias("circuit_ref"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
)

# COMMAND ----------

# another way to rename columns in 
circuits_renamed_df = circuits_selected_df.withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# Track ingestion date (take current timestamp as a new column)
from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
display(circuits_final_df)

# COMMAND ----------

# write to datalake as a parquet
circuits_final_df.write.parquet("/mnt/f1winterdl/processed/circuits", mode="overwrite")

# COMMAND ----------

display(spark.read.parquet("/mnt/f1winterdl/processed/circuits"))
