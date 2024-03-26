# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion into Delta Lakes

# COMMAND ----------

# MAGIC %md
# MAGIC Explore schema enforcement and schema evolution when ingesting data ito an existing Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download data batch

# COMMAND ----------

# MAGIC %md
# MAGIC Download a new data batch from [Monitoring of CO2 emissions from passenger cars Reegulation (EU) 2019/631](https://www.eea.europa.eu/en/datahub/datahubitem-view/fa8b1229-3db6-495d-b18e-9c9b3267c02b) consisting of the 100,000 cars with the highest CO2 emissions for the year 2020 for the. yeearr 2020

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC apt-get install jq
# MAGIC # switch to the root directory of our driver node
# MAGIC cd /
# MAGIC
# MAGIC # create a directory called eea_input_data and move into it
# MAGIC mkdir eea_input_data
# MAGIC cd eea_input_data/
# MAGIC
# MAGIC # create a set of variables for the data URL
# MAGIC BASE_URL="https://discodata.eea.europa.eu"
# MAGIC TABLE="CO2Emission.latest.co2cars"
# MAGIC ORDER="ORDER%20BY%20%27Enedc%20(g%2Fkm)%27%20DESC"
# MAGIC
# MAGIC CONDITION="year%20%3D%202020"
# MAGIC COLUMNS="*%2C%20cast(%22Enedc%20(g%2Fkm)%22%20as%20float)%20*%201.1%20as%20%22Enedc%20(g%2Fkm)%20V2%22"
# MAGIC SQL_QUERY="SELECT%20$COLUMNS%20FROM%20$TABLE%20WHERE%20$CONDITION%20$ORDER"
# MAGIC FULL_URL="$BASE_URL/sql?query=$SQL_QUERY&p=1&nrOfHits=100000"
# MAGIC curl $FULL_URL | jq '.results' > co2_emissions_passenger_cars_2020.json

# COMMAND ----------

dbutils.fs.cp(
    f'file:/eea_input_data/co2_emissions_passenger_cars_2020.json',
    f'dbfs:/datalake/raw/co2_passenger_cars_emissions/year=2020/co2_emissions_passenger_cars_2020.json'
  )
display(dbutils.fs.ls( "dbfs:/datalake/raw/co2_passenger_cars_emissions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw JSON file into DataFrame

# COMMAND ----------

import pyspark.sql.functions as F
import re

# COMMAND ----------

display(dbutils.fs.ls( "dbfs:/datalake/raw/co2_passenger_cars_emissions/year=2020"))

# COMMAND ----------

# Prepare the different parts of the data path
raw_layer_base_path = 'dbfs:/datalake/raw'
co2_emissions_feed  = 'co2_passenger_cars_emissions'

# Read the data using Spark
co2_df = (spark.read
                .option('multiline','true')
                .json(f'{raw_layer_base_path}/{co2_emissions_feed}/year=2020')
                .withColumn('year', F.lit('2020')) # adding a new column named year and having '2020' assigned to every row in the DataFrame
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter 2020 batch CO2

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

# Filter records with corrupt Member State code, keep values with only two uppercase letters
print(f'Number of records of CO2 emissions dataframe before MS filter: {co2_df.count()}')
co2_df = co2_df.filter(co2_df['MS'].rlike('^[A-Z][A-Z]$'))
print(f'Number of records of CO2 emissions dataframe after MS filter: {co2_df.count()}')


# COMMAND ----------

# Newe column Enedc_g/km_V2
display(co2_df)

# COMMAND ----------

# Count the number of columns in co2_df
num_columns = len(co2_df.columns)
print(f'co2 emissions data set original number of columnns: {num_columns}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest new batch into existing Delta table on the curated layer

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake enforces the schema of the tables because has access to that metadata: the delta table performs schema validation of every column, and the source dataframe column data types must match the column data types in the target table. If they don’t match, an exception is raised.
# MAGIC
# MAGIC  This ensures quality as there can't be ingested data files that don't respect the table schema. Modifications can be tolerated with **schema evolution** by setting the schema `mergeSchema` option to `true` when writing the data.

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/datalake/curated/co2_emissions/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE eea_curated.co2_emissions

# COMMAND ----------

# Display the data types of each column in co2_df
display(co2_df.dtypes, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest the nwe batch of data into. existing CO2 emissions Delta table on the curated layer with .mode('append')

# COMMAND ----------

from pyspark.sql.functions import col

dbfs_base_path = 'dbfs:/datalake/curated/co2_emissions/'

# repartition() to get one file per partition value
# drop the column z_Wh/km because it only contains null values for this year
co2_df = co2_df.drop('z_Wh/km').repartition('year')
(
  co2_df
  .withColumn("year", col("year").cast("Integer"))
  .write
  .mode('append')
  .partitionBy('year')
  .option('path',dbfs_base_path)
  #.option('mergeSchema', 'true')
  .format('delta')
  .saveAsTable('eea_curated.co2_emissions')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Assume that you find out that the column Enedc_g/km_V2 contains the correct values for the year 2020 and that it should replace the existing data in the column Enedc_g/km

# COMMAND ----------

co2_df = (co2_df
            .withColumnRenamed('Enedc_g/km', 'Enedc_g/km_deprecated')
            .withColumnRenamed('Enedc_g/km_V2', 'Enedc_g/km')
        )

# Trying the ingest again
co2_df = co2_df.repartition('year')
(
  co2_df
  .write
  .mode('append')
  .partitionBy('year')
  .option('path',dbfs_base_path)
  .format('delta')
  .saveAsTable('eea_curated.co2_emissions')
)

# COMMAND ----------

from pyspark.sql.types import LongType, IntegerType

# casting Enedc_g/km as INT
co2_df = co2_df.withColumn('Enedc_g/km', F.col('Enedc_g/km').cast(LongType())).withColumn('year', F.col('year').cast(IntegerType()))

(
  co2_df
  .write
  .mode('append')
  .partitionBy('year')
  .option('path',dbfs_base_path)
  # allowing schema migration via this option
  .option('mergeSchema', 'true')
  .format('delta')
  .saveAsTable('eea_curated.co2_emissions')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM eea_curated.co2_emissions
# MAGIC WHERE year = 2020

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /datalake/curated/co2_emissions/year=2020

# COMMAND ----------

# MAGIC %md
# MAGIC ## Behavior of Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC  When dozens of pipelines moving data around the layers of a data lake and multiple data sources ingesting different forms of data its difficult to ensure that every pipeline is actually managing all the potential schema issues, since. there is a disconnect between the data and the metadata. Delta Lake enforces the schema out of the box and supports schema evolution, which makes managing data lakes feel more like managing traditional data warehouses.
