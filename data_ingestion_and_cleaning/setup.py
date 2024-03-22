# Databricks notebook source
# MAGIC %md
# MAGIC #  Set up layered Data Lake with Databricks File System

# COMMAND ----------

# making sure its working
display("Hello World!")

# COMMAND ----------

spark.sparkContext

# COMMAND ----------

# the creation of cluster creates SparkSession object; to print out Spark session configuration parameters
#configurations = spark.sparkContext.getConf().getAll()
#for item in configurations:
    #print(item)

# another way to print the Spark session configuration
display(spark.sparkContext.getConf().getAll())

# COMMAND ----------

# Introduce the SparkConf class, also used to update parameters
from pyspark import SparkConf
display(SparkConf().getAll())

# COMMAND ----------

# another way of diplaying session configuration parameters
display(spark.sql("SET -v"))

# COMMAND ----------

dbutils.fs.help("mkdirs")

# COMMAND ----------

# create datalake directory under root directory
dbutils.fs.mkdirs('/datalake')

# create the 3 directories for the data lake layers
dbutils.fs.mkdirs('/datalake/raw')
dbutils.fs.mkdirs('/datalake/curated')
dbutils.fs.mkdirs('/datalake/serving')

# list the contents of datalake dir
display(dbutils.fs.ls('/datalake'))
