# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Curated Layer

# COMMAND ----------

# MAGIC %md
# MAGIC Begining of Spark-based data pipeline <br>
# MAGIC - Read data from raw layer uing Spark
# MAGIC - Filter data
# MAGIC - Ingest data in Parquet format to the DBFS curated layer
# MAGIC - Create tables on top of the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creat Data Bases

# COMMAND ----------

import pyspark.sql.functions as F
import re

from datetime import datetime
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-process WDI data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read raw csv files into dataframes

# COMMAND ----------

# set the different parts of the data path, using the escape character for quoted strings that themselves contain quotes
raw_layer = 'dbfs:/datalake/raw/'
wdi       = 'world_development_indicators'
partition = '240313'
#partition = datetime.today().strftime('%Y%m%d')


# Read the three data files we're interested in
wdi_data    = (spark.read
                       .option('header', 'true')
                       .option('escape', '"')
                       .csv(f'{raw_layer}/{wdi}/date={partition}/WDIData.csv'))
wdi_country = (spark.read
                       .option('header', 'true')
                       .option('escape', '"')
                       .csv(f'{raw_layer}/{wdi}/date={partition}/WDICountry.csv'))
wdi_series  = (spark.read
                       .option('header', 'true')
                       .option('escape', '"')
                       .csv(f'{raw_layer}/{wdi}/date={partition}/WDISeries.csv'))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter WDI data

# COMMAND ----------

wdi_dfs = {
    'WDI_Data'    : wdi_data, 
    'WDI_Country' : wdi_country,
    'WDI_Series'  : wdi_series
}

# Replace spaces in column namees with underscores
for df_name, df in wdi_dfs.items():
    #. assign a new alias (name) to columns; F.col(col) creates a column object
    wdi_dfs[df_name] = df.select([F.col(col).alias(col.replace(' ',  '_')) for col in df.columns])

# Drop null records
for df_name, df in wdi_dfs.items():
  print(f'Number of records of dataframe {df_name} before dropping nulls: {df.count()}')
  wdi_dfs[df_name] = df.na.drop('all')
  print(f'Number of records of dataframe {df_name} after dropping nulls: {wdi_dfs[df_name].count()}')

# Drop duplicates
for df_name, df in wdi_dfs.items():
  print(f'Number of records of dataframe {df_name} before dropping duplicates: {df.count()}')
  wdi_dfs[df_name] = df.distinct()
  print(f'Number of records of dataframe {df_name} after dropping duplicates: {wdi_dfs[df_name].count()}')


# COMMAND ----------

# drop all records that have a country code (column: Country_Code) with a size other than 3 and all records that contain a space character in the Series_Code column:
wdi_data    = wdi_dfs['WDI_Data'].where('length(Country_Code) = 3')
wdi_country = wdi_dfs['WDI_Country'].where('length(Country_Code) = 3')
wdi_series  = wdi_dfs['WDI_Series'].where(~F.col('Series_Code').contains(' '))

#wdi_country = wdi_country.filter("length(Country_Code) = 3")
#wdi_data = wdi_data.filter("length(Country_Code) = 3")
#wdi_series = wdi_series.filter("Series_Code NOT LIKE '% %'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Curate and Crerate db Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS wdi_curated;

# COMMAND ----------

dbfs_base_path   = 'dbfs:/datalake/curated/wdi'
output_partition = f'year={partition[:4]}/month={partition[4:6]}/day={partition[6:]}/'

# Use coalesce() to reduce the number of partitions before writing the data
# Use saveAsTable() to create tables on top of the data
(
  wdi_data
  .coalesce(1)
  .write
  .format('parquet')
  .mode('overwrite')
  .option('path', f'{dbfs_base_path}/data/{output_partition}')
  .saveAsTable('wdi_curated.data')
)
(
  wdi_country
  .coalesce(1)
  .write
  .format('parquet')
  .mode('overwrite')
  .option('path', f'{dbfs_base_path}/country/{output_partition}')
  .saveAsTable('wdi_curated.country')
)
(
  wdi_series
  .coalesce(1)
  .write
  .format('parquet')
  .mode('overwrite')
  .option('path', f'{dbfs_base_path}/series/{output_partition}')
  .saveAsTable('wdi_curated.series')
)

#dbutils.fs.rm('dbfs:/datalake/curated/co2_emissions', recurse=True)

# COMMAND ----------

# Display the head of the wdi_curated.country table
display(spark.sql("SELECT * FROM wdi_curated.country LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocess CO2 emission data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read raw JSON files into dataframes

# COMMAND ----------

# Prepare the different parts of the data path
raw_layer_base_path = 'dbfs:/FileStore/raw'
co2_emissions_feed  = 'co2_passenger_cars_emissions'

# Read the data using Spark
co2_df = (spark.read
               .option('multiline','true')
               .json(f'{raw_layer_base_path}/{co2_emissions_feed}'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter CO2 Data

# COMMAND ----------

# Replace spaces in column names with underscores and remove ()
co2_df = (co2_df.select(
                      [F.col(col).alias(re.sub('[()]', '', col.replace(' ', '_'))) for col in co2_df.columns]
                    )
          )
          
# Drop null records
print(f'Number of records of CO2 emissions dataframe before dropping nulls: {co2_df.count()}')
co2_df = co2_df.na.drop('all')
print(f'Number of records of CO2 emissions dataframe after dropping nulls: {co2_df.count()}')

# Drop duplicates
print(f'Number of records of CO2 emissions dataframe before dropping duplicates: {co2_df.count()}')
co2_df = co2_df.distinct()
print(f'Number of records of CO2 emissions dataframe after dropping duplicates: {co2_df.count()}')

# Filter records with corrupt Member State code - We keep values with two uppercase letters
print(f'Number of records of CO2 emissions dataframe before MS filter: {co2_df.count()}')
co2_df = co2_df.filter(co2_df['MS'].rlike('^[A-Z][A-Z]$'))
print(f'Number of records of CO2 emissions dataframe after MS filter: {co2_df.count()}')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Curate and Create db Table

# COMMAND ----------

dbfs_base_path = 'dbfs:/datalake/curated/co2_emissions/'

# We use repartition() to get one file per partition value
co2_df = co2_df.repartition('year')
(
  co2_df
  .write
  .mode('overwrite')
  .partitionBy('year')
  .option('path',dbfs_base_path)
  .format('parquet')
  .saveAsTable('eea_curated.co2_emissions')
)

# COMMAND ----------

# can use spark.sql() function to execute an SQL query on co2_emisssions table
result = spark.sql("SELECT * FROM eea_curated.co2_emissions WHERE year IN (2018,  2019)")

# Display the result
display(result)

#%sql
#SELECT * FROM eea_curated.co2_emissions WHERE year IN (2017, 2019)

# COMMAND ----------

# MAGIC %md
# MAGIC When written into a parquet file, the dataset is distributed into partitions,  resulting into creation of multiple files. To control the number of files and the partitioning behavior, use the repartition() or coalesce()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /datalake/curated/co2_emissions/year=2017
