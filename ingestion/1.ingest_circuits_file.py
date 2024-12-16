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
    .csv("dbfs:/mnt/f1winterdl/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# double check the schema
circuits_df.printSchema()

# COMMAND ----------

#
