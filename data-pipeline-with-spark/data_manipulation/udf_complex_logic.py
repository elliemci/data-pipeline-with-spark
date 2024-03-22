# Databricks notebook source
# MAGIC %md
# MAGIC # UDF to Implement Complex Business Logic

# COMMAND ----------

# MAGIC %md
# MAGIC User-defined functions are the most flexible option when implementing business logic within Spark applications. Even though some types of UDFs can be really efficient, they should always be last resort, because Spark’s native functions are nearly always more efficient since they are functions that Catalyst (Spark’s optimizer) understands, unlike UDFs.
# MAGIC
# MAGIC Using Python and Pandas Usser Defined Function, a decade column is added to the unpivoted dataset, to allow generration if decade-level insights

# COMMAND ----------


-- recreate the databases working on a new cluster
%sql
CREATE DATABASE IF NOT EXISTS wdi_serving;
CREATE DATABASE IF NOT EXISTS eea_serving;

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/datalake/serving/wdi/data_unpivoted"))

# COMMAND ----------

# Read the data into a DataFrame
df = spark.read.format("parquet").load("dbfs:/datalake/serving/wdi/data_unpivoted")

# Display the DataFrame
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Custom Function

# COMMAND ----------

# MAGIC %md
# MAGIC **UDF** function from pyspark.sql.functions allows to define custom, reusable function that can be applied to DataFrame columns. Udf function  creates an UDF object from the custom python function, which is applied to DataFrame columns using .withColumn()

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import udf, col

# COMMAND ----------

# Define a UDF to calculate the decade value
def calculate_decade(year):
    return f'{int(year / 10) * 10}s'

# Create a UDF object from the UDF
udf_calculate_decade = udf(calculate_decade)

# Add the decade column to the DataFrame
df_v1 = df.withColumn("decade", udf_calculate_decade(col('year')))

# COMMAND ----------

# start the timer
start = datetime.now()
# use noop format to simulate the write action
(df_v1
 .write
 .mode("overwrite")
 .format("noop")
 .save())

# We print the time taken by our job
print(f'Time taken: {datetime.now() - start}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pandas UDF

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType, col
from pyspark.sql.types import StringType

# COMMAND ----------

# define the logic as a Pandas UDF
@pandas_udf(StringType())
def calculate_decade(year):
    # the floor division retrurns the result. without the decimal part
    return (year // 10 * 10).astype(str) + "s"

# apply the pandas UDF to the DataFrame
df_v2 = df.withColumn("decade", calculate_decade(col("year")))


# COMMAND ----------

# start the timer
start = datetime.now()
# use noop format to simulate the write action
(df_v2
 .write
 .mode("overwrite")
 .format("noop")
 .save())

# We print the time taken by our job
print(f'Time taken: {datetime.now() - start}')

# COMMAND ----------

# MAGIC %md
# MAGIC Because Pandas UDF leeverages Apache Arrow for the data transfer it is faster than Python UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: When pandas UDFs are too slow use Scala-based UDFs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the generated data in serving layer and generate table

# COMMAND ----------

# write into the dataset in the serving layer partitioned by decade
(df_v2
 .write
 .mode('overwrite')
 .format('parquet')
 .partitionBy('decade')
 .option('path', f'/datalake/serving/wdi/decade_level_data')
 .saveAsTable('wdi_serving.decade_level_data')
)

# COMMAND ----------

df.columns

# COMMAND ----------

# Query the wdi_serving.decade_level_data table
query = """
SELECT Indicator_Name, Indicator_Value
FROM wdi_serving.decade_level_data
WHERE decade == '2020s' AND Country_Name == 'France'
"""

# Execute the query using spark.sql() method
result = spark.sql(query)

# Display the result
display(result)
