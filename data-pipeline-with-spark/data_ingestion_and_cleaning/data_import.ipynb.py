# Databricks notebook source
# MAGIC %md
# MAGIC # Import Data in the Data Lake Raw Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move data files from a landing zone on the driver's local file system into the dbfs raw layer using Databricks Utilities 

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

display(dbutils.fs)

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /
# MAGIC mkdir input_wb_data
# MAGIC cd input_wb_data
# MAGIC # create a variable for the URL from which. the data files are fettched
# MAGIC DATA_URL='https://databank.worldbank.org/data/download/WDI_csv.zip'
# MAGIC wget -O world_development_indicators.zip $DATA_URL
# MAGIC unzip world_development_indicators.zip
# MAGIC rm world_development_indicators.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /input_wb_data

# COMMAND ----------

from datetime import datetime
current_date = datetime.today().strftime('%y%m%d')

dbutils.fs.cp(
# change scheme to point to the csv files which are on the driver node
'file:/input_wb_data',
f'dbfs:/datalake/raw/world_development_indicators/date={current_date}',
recurse=True)

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/datalake/raw/world_development_indicators/date={current_date}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import data files via Web Terminal

# COMMAND ----------

# MAGIC %md
# MAGIC Fetch the CO2 emissions from passenger cars dataset from [Monitoring of CO2 emissions from passenger cars - Reegulation(EU) 2019/631](https://www.eea.europa.eu/en/datahub/datahubitem-view/fa8b1229-3db6-495d-b18e-9c9b3267c02b). Download the 100000 cars with the highest CO2 emission for  the years 2017-19 retrive the data via shellcode.
# MAGIC Write commands into a file using Vim and then execute it to retrive the input data. Enable the web terminal via Settings/ Workspace Admin tab

# COMMAND ----------

# MAGIC %sh
# MAGIC  apt-get install jq
# MAGIC  cd /
# MAGIC  mkdir eea_input_data
# MAGIC  cd eea_input_data/
# MAGIC  BASE_URL="https://discodata.eea.europa.eu"
# MAGIC  TABLE="CO2Emission.latest.co2cars"
# MAGIC  ORDER="order%20by%20emissions%20desc"
# MAGIC  for YEAR in 2017 2018 2019
# MAGIC  do
# MAGIC       CONDITION="year%20%3D%20$YEAR"
# MAGIC       SQL_QUERY="SELECT%20*%2C%20%22Enedc%20(g%2Fkm)%22%20as%20emissions%20FROM%20$TABLE%20WHERE%20$CONDITION%20$ORDER"
# MAGIC       FULL_URL="$BASE_URL/sql?query=$SQL_QUERY&p=1&nrOfHits=100000"
# MAGIC       curl $FULL_URL | jq '.results' > co2_emissions_passenger_cars_$YEAR.json
# MAGIC  done

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /eea_input_data

# COMMAND ----------

for year in [2017, 2018, 2019]:
  file_name = f'co2_emissions_passenger_cars_{year}.json'
  dbutils.fs.cp(
    f'file:/eea_input_data/{file_name}',
    f'dbfs:/FileStore/raw/co2_passenger_cars_emissions/year={year}/{file_name}'
  )

# COMMAND ----------

dbutils.fs.ls( "dbfs:/FileStore/raw/co2_passenger_cars_emissions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy data froom driver's local file system to dbfs via Apache Spark

# COMMAND ----------

sc = spark.sparkContext

wdid_path = f'dbfs:/datalake/raw/world_development_indicators/date={current_date}/WDIData.csv'
co2e_path = f'dbfs:/FileStore/raw/co2_passenger_cars_emissions'

# default delimeter is ","

#df = spark.read \
    #.format("csv") \
    #.option("header", "true") \
    #.load(wdid_path)

# anothere way of formating, no need of new line indicator, when using brakets
df_wdid = (spark.read 
            .option("header", "true")# add option(key, value) 
            .csv(wdid_path)
    )
df_co2e = (spark.read
                .option("multiline",  "true")
                .json(co2e_path)
    ) 

# COMMAND ----------

print(f'Number of records for World Development Indicators: {df_wdid.count()}')
display(df_wdid)

# COMMAND ----------

print(f'Numbber of records for CO2 emissions: {df_co2e.count()}')
display(df_co2e)

# COMMAND ----------

# Standart method is to use .format, there are also shortcut methods like
#df = spark.read.csv(path, 
                     #header = "true",
                     #inferSchema = "true")

#display(df)

# COMMAND ----------

df_wdid.printSchema()

# COMMAND ----------

display(df_co2e.printSchema())

# COMMAND ----------

# basic statistics about each column of dataframe
display(df_wdid.describe())
display(df_wdid.summary())

# COMMAND ----------

display(df_co2e.summary())
