# Databricks notebook source
# MAGIC %md
# MAGIC ## Generate the Data Lake's Serving Layer

# COMMAND ----------

# MAGIC %md
# MAGIC The Serving Layer contains the optimized for querying data, used to generate insights

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Databases

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create wdi_serving database for the World Development Indicators tables on the serving layer
# MAGIC CREATE DATABASE IF NOT EXISTS wdi_serving;
# MAGIC
# MAGIC -- Create eea_serving database for the passenger cat CO2 emissions tables on the serving layer
# MAGIC CREATE DATABASE IF NOT EXISTS eea_serving;

# COMMAND ----------

# MAGIC %md
# MAGIC When database is created it is stored in DBFS and it persists across sessions, only accesiblee within the Databricks workspace and cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in wdi_curated;

# COMMAND ----------

# The DBFS locations 
CURATED_LAYER = 'dbfs:/datalake/curated'
SERVING_LAYER = 'dbfs:/datalake/serving'

# COMMAND ----------

# MAGIC %md
# MAGIC ## WDI data to the serving layer

# COMMAND ----------

# MAGIC %md
# MAGIC Implement the "business need" for changing the indicators' values representaion -  unpivoting the wdi data so that each year's indicator values as rows, adding an year column originally stored as column names

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/curated/wdi/data/year=2024/month=03/day=14"))

# COMMAND ----------

# Set the paths of the parquet partitions
year = '2024'
month = '03'
day = '14'
partition_path = f'dbfs:/datalake/curated/wdi/data/year={year}/month={month}/day={day}'

# Read the parquet partitions into a DataFrame
wdi_data = spark.read.parquet(partition_path)

newest_partition = dbutils.fs.ls(partition_path)[-1]
print(f'newest_partition: {newest_partition.name}')

# Resilient Distributed Dataset, collection of elements partitioned across the nodes of a cluster which can operate in parallel
rdd = wdi_data.rdd
num_partitions = rdd.getNumPartitions()
print(f'wdi data number of partitions: {num_partitions}')

# COMMAND ----------

# MAGIC %md
# MAGIC A PySpark RDD (Resilient Distributed Dataset) is an immutable distributed collection of objects. It represents a collection of elements partitioned across the nodes of a cluster and can operate in parallel. RDDs are the fundamental data structure in PySpark and are used for distributed computing in Spark. RDDs provide fault tolerance and allow operations like transformations and actions to be performed on the data in parallel. PySpark RDDs are lazily evaluated and the transformations on them are executed only when an action is called RDDs can be created from various data sources including Hadoop Distributed File System (HDFS), local file systems, and other RDDs.  They can also be created from data in memory or from data in other storage systems like Apache Cassandra, Apache HBase, and more.

# COMMAND ----------

display(wdi_data)

# COMMAND ----------

print(f'the size of the orginal wdi data: {wdi_data.count(), len(wdi_data.columns)}')

# COMMAND ----------

print(wdi_data.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Unpivot wdi_data by creating an empty DataFrame with the following schema Country_Name, Country_Code, Indicator_Name, Indiator_Code, Indicator_Value and year, loop over the years from 1960 to 2020 of wdi_data and append their data to the newly created DataFrame 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType

# Specify the schema used for unpivoted dataframe
schema_wdi = StructType([
               StructField('Country_Name', StringType(), True),
               StructField('Country_Code', StringType(), True),
               StructField('Indicator_Name', StringType(), True),
               StructField('Indicator_Code', StringType(), True),
               StructField('Indicator_Value', StringType(), True),
               StructField('year', StringType(), True)
             ])

# Create an empty RDD and then use it with the schema to create an empty dataframe
emptyRDD              = spark.sparkContext.emptyRDD()
wdi_data_unpivoted = spark.createDataFrame(emptyRDD,schema_wdi)

# loop through the years and then add the data of each year to the unpivoted dataframe
for year in range(1960, 2021):
  year_data = (wdi_data
               .select(
                'Country_Name',
                'Country_Code', 
                'Indicator_Name', 
                'Indicator_Code',
               # keep the column of the current year in the loop
               F.col(str(year)).alias('Indicator_Value')
             )
             .withColumn('year', F.lit(year)) # add a column that contains the value of the year
            )
  # append this year's data to the output dataframe via union()
  wdi_data_unpivoted = wdi_data_unpivoted.union(year_data)

# Printing the number of partitions of the output dataframe
print(wdi_data_unpivoted.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC Unpivoting the wdi data ends up in 61 partitons corresponding to the indicators data for 61 years from 1960 to 2021. They are created in the for loop with each iteration and combined with unito() operation.

# COMMAND ----------

print(f'the size of unpivoted dataframe after appending the year columns as rows: {wdi_data_unpivoted.count(), len(wdi_data_unpivoted.columns)}')

# COMMAND ----------

# MAGIC %md
# MAGIC Use Spark UI via the cluster page to see the Directed Acyclic Graph (DAG) visualization which provides a graphical representation of the stages and tasks involved in the job execution

# COMMAND ----------

print(f'the logical and physical plans generrated by Spark to execute a given statement without doing the actual. execution: {wdi_data_unpivoted.explain()}')

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/serving"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Serving layer and Create a Table

# COMMAND ----------

# Write the output data to the serving layer on DBFS
(wdi_data_unpivoted
 .write
 .mode('overwrite')
 .format('parquet')
 .partitionBy('year')
 .option('path', f'{SERVING_LAYER}/wdi/data_unpivoted')
 .saveAsTable('wdi_serving.wdi_data_unpivoted')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Indicators Average Values per Country

# COMMAND ----------

# Aggregate wdi_data_unpivoted by indicator and country; use avg() from the pyspark.sql.functions module to generate the average on a column from the grouped dataframe
wdi_data_average = (wdi_data_unpivoted
                       .groupBy(
                         'Country_Name',
                         'Country_Code', 
                         'Indicator_Name', 
                         'Indicator_Code',
                       )
                       .agg(
                        F.avg('Indicator_Value').alias('Indicator_Average_Value')
                       )
                      )

# COMMAND ----------

wdi_data_average.columns

# COMMAND ----------

# Write the agregated data to the serving layer on DBFS
(wdi_data_average
 .repartition('Indicator_Code')
 .write
 .mode('overwrite')
 .format('parquet')
 .partitionBy('Indicator_Code')
 .option('path', f'{SERVING_LAYER}/wdi/indicator_average')
 .saveAsTable('wdi_serving.average_indicators')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CO2 emissions data to the serving layer

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/curated/co2_emissions"))

# COMMAND ----------

co2_emissions = (spark.read
                         .parquet(f'{CURATED_LAYER}/co2_emissions'))

# COMMAND ----------

print(co2_emissions.count(), len(co2_emissions.columns))

# COMMAND ----------

import  pandas as pd
# to display the names of all 41 columns
column_names = co2_emissions.columns
column_names_df = pd.DataFrame({'Column Names': column_names})
print(column_names_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation and Window Functioons

# COMMAND ----------

# MAGIC %md
# MAGIC Retrive the 100 vehicles with highest CO2 emissions per year per. Member State, and write the dataset to the data lake's serving layer

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/serving/co2_emissions"))

# COMMAND ----------

from pyspark.sql.functions import desc, row_number
from pyspark.sql import Window

# Define the window partitioned by MS and year, rank by emissions in descending order
window_spec = Window.partitionBy("MS", "year").orderBy(desc("Enedc_g/km"))

# Add the rank column based on the Enedc_g/km values within the window
co2_emissions_ordered = (co2_emissions
                         .withColumn("rank", row_number().over(window_spec))
                         .where("rank <= 100")
                         .drop("rank")
                        ) 

# Repartition the data by year and Member State and write into serving layer 
co2_emissions_ordered = co2_emissions_ordered.repartition("year", "MS")

# Create the schema 'eea_serving'
spark.sql("CREATE SCHEMA IF NOT EXISTS eea_serving")

(co2_emissions_ordered
 .write
 .mode('overwrite')
 .format('parquet')
 .partitionBy("year", "MS")
 .option('path', '/datalake/serving/co2_emissions/highest_emissions_vehicles')
 .saveAsTable('eea_serving.highest_emissions')
)

