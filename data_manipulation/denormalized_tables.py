# Databricks notebook source
# MAGIC %md
# MAGIC # Use of Joins to Create Denormalized Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Denormalization is used on a previously normalized database to increase performance by adding redundant copies of data or by grouping data. From the data in the curated layer, based. on business logic, filter and join datasets to generate two new tables for the data lake's serving layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## WDI dataset

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/curated/wdi/data/year=2024/month=03/day=14"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/curated/wdi/series/year=2024/month=03/day=14"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/curated/wdi/country/year=2024/month=03/day=14"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read partitionss from dataset

# COMMAND ----------

# Set the paths of the parquet partitions
year = '2024'
month = '03'
day = '14'
data_partition = f'dbfs:/datalake/curated/wdi/data/year={year}/month={month}/day={day}'
country_partition = f'dbfs:/datalake/curated/wdi/country/year={year}/month={month}/day={day}'
series_partition = f'dbfs:/datalake/curated/wdi/series/year={year}/month={month}/day={day}'

wdi_data = spark.read.parquet(data_partition)
wdi_country = spark.read.parquet(country_partition)
wdi_series = spark.read.parquet(series_partition)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Indicators that reference countries

# COMMAND ----------

# MAGIC %md
# MAGIC Filter the countries dataset to keep only rows that represent actual countries by removing.  rows which the Region value is null, keeping only the columns Country_Code, 2-alpha_code, Currency_Unit, Region, Income_Group

# COMMAND ----------

wdi_country.select("Region").show(20, truncate=False)

# COMMAND ----------

# filter the countries dataframe to keep data that references actual countries, 
#  i.e. not null Region column
wdi_country_filtered = (wdi_country
                             .where('Region is not Null')
                             .select(
                               'Country_Code',
                               '2-alpha_code',
                               'Currency_Unit',
                               'Region',
                               'Income_Group'
                            ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join tables

# COMMAND ----------

# MAGIC %md
# MAGIC Join the filtered countries dataframe with the manin data to add the countries information to the indicators values

# COMMAND ----------

wdi_country_filtered.columns

# COMMAND ----------

# Join wdi_country with filtered_wdi_country dataframe with  keeping rows that have a join key value present in both dataframes
joined_df = (wdi_country_filtered
             .join(
                 wdi_country,
                 on=['Country_Code', '2-alpha_code', 'Currency_Unit', 'Region', 'Income_Group'], 
                 how="inner"
                 )
            )
display(joined_df)

# COMMAND ----------

print(joined_df.count(), len((joined_df.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write dataframe in serving layer and generate table

# COMMAND ----------

# write the output dataframe in Parquet format to the serving layer and generate an external table on top of the data
(joined_df
 .write
 .mode('overwrite')
 .format('parquet')
 .partitionBy('Country_Code')
 .option('path', f'/datalake/serving/wdi/countries_data')
 .saveAsTable('wdi_serving.countries_data')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CO2 Emissions data

# COMMAND ----------

# MAGIC %md
# MAGIC Read partitions from CO2 emissions dataset into separate dataframes

# COMMAND ----------

display(dbutils.fs.ls(f'dbfs:/datalake/curated/co2_emissions'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read partitionss from dataset

# COMMAND ----------

# create a  dictionary to store the. co2_ variables mapped to their respecttive years
co2_data = {}

for year in [2017,  2018, 2019]:
    data_partition = f'dbfs:/datalake/curated/co2_emissions/year={year}'
    co2_data[year] = spark.read.parquet(data_partition)

# access the co2_ variables using the corresponding year
co2_2017 = co2_data[2017]
co2_2018 = co2_data[2018]
co2_2019 = co2_data[2019]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate the difference in emissions 

# COMMAND ----------

# MAGIC %md
# MAGIC Generate a new DataFrame that contains the difference in emissions for a given year compared to the previous year based. on thee emissions valuee stored in Enedc_g/km

# COMMAND ----------

for co2_data in [co2_2017,  co2_2018, co2_2019]:
  co2_data.groupBy('MS').sum('Enedc_g/km')

# COMMAND ----------

from pyspark.sql.functions import lit

# the lit() function is used to create a literal column expression with a specific value

# Group each DataFrame by member state and sum the emissions
grouped_2017 = co2_2017.groupBy('MS').sum('Enedc_g/km')
grouped_2018 = co2_2018.groupBy('MS').sum('Enedc_g/km')
grouped_2019 = co2_2019.groupBy('MS').sum('Enedc_g/km')

# Join each DataFrame with the DataFrame of the previous year, adding a new column Emissions_diff_yoy that contains the difference in emission. between the years
joined_2018 = grouped_2018.join(grouped_2017, grouped_2018['MS'] == grouped_2017['MS'], 'inner').\
    withColumn('emissions_diff_yoy', grouped_2018['sum(Enedc_g/km)'] - grouped_2017['sum(Enedc_g/km)']).\
    select(grouped_2018['MS'], grouped_2018['sum(Enedc_g/km)'].alias('sum_current_year'), grouped_2017['sum(Enedc_g/km)'].alias('sum_previous_year'), 'emissions_diff_yoy', lit(2018).alias('year'))

joined_2019 = grouped_2019.join(grouped_2018, grouped_2019['MS'] == grouped_2018['MS'], 'inner').\
    withColumn('emissions_diff_yoy', grouped_2019['sum(Enedc_g/km)'] - grouped_2018['sum(Enedc_g/km)']).\
    select(grouped_2019['MS'], grouped_2019['sum(Enedc_g/km)'].alias('sum_current_year'), grouped_2018['sum(Enedc_g/km)'].alias('sum_previous_year'), 'emissions_diff_yoy', lit(2019).alias('year'))

# withColumn() directtly generates a new column containing the difference bw the values of two columnns; rename dataframe column withColumnRenamed()

# Append the 2019 DataFrame to the 2018 DataFrame
final_df = joined_2018.union(joined_2019)

# COMMAND ----------

final_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write dataframe in serving layer and generate table

# COMMAND ----------

(final_df
 .write
 .mode('overwrite')
 .format('parquet')
 .option('path', f'/datalake/serving/co2_emissions/emissions_diff_yoy')
 .saveAsTable('eea_serving.emissions_diff_yoy')
 )
