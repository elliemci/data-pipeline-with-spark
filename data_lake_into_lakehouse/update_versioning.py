# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Tables Updates and Versioning

# COMMAND ----------

# MAGIC %md
# MAGIC Two features of Delta Lake are to update tables and view their modifications. These are *data warehouse's* features with which a **Lakehouse** improves efficiency of query performance thans to the metadata that is  generrated for every data file. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table History of Modifications

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY eea_curated.co2_emissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Updates

# COMMAND ----------

# MAGIC %md
# MAGIC Update the Deta table by increasing the engine capacity value for specific manifacturer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM eea_curated.co2_emissions
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE eea_curated.co2_emissions
# MAGIC SET ec_cm3 = ec_cm3 + 10
# MAGIC WHERE Mh = 'FERRARI';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY eea_curated.co2_emissions

# COMMAND ----------

# MAGIC %md
# MAGIC Run query on the previous version table to retrive the originally ingested data

# COMMAND ----------

# SQL query to retrieve the originally ingested data for Mh='FERRARI' from the table eea_curated.co2_emissions
sql_query = """
SELECT *
FROM eea_curated.co2_emissions@v4
WHERE Mh = 'FERRARI'
LIMIT 10
"""

# Running the SQL query
spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Revert the changed engine capacity values to the original ones in the CO2 emissions Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- We use the RESTORE command to restore our table to version 2
# MAGIC RESTORE TABLE eea_curated.co2_emissions TO VERSION AS OF 4
