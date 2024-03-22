# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Format

# COMMAND ----------

# MAGIC %md
# MAGIC Delta, a columnar oriented format is a type of file storage technology that store modifications to files rather than whole amended files. The Delta Lake is an **open table format** developed and open-sourced by Databricks, is one of three open table formats. The other two, Apache Iceberg (open-sourced by Netflix) and Apache Hudi (open-sourced by Uber) are alternatives that offer similar capabilities. The lakehouse design is a scalable modern warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert CO2 Emissions tables into Delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- convert the currrated layer table
# MAGIC CONVERT TO DELTA eea_curated.co2_emissions 
# MAGIC -- PARTITIONED BY (Year STRING);
# MAGIC -- by omitting PARTITIONED BY the partition schema will be inferred

# COMMAND ----------

# MAGIC %sql
# MAGIC -- convert the serving layer tables
# MAGIC CONVERT TO DELTA eea_serving.highest_emissions; 
# MAGIC --  PARTITIONED BY (year STRING, MS STRING);
# MAGIC CONVERT TO DELTA eea_serving.emissions_diff_yoy; 
# MAGIC -- PARTITIONED BY (year STRING);

# COMMAND ----------

# MAGIC %md
# MAGIC CONVER TO DELTA command starts multiple jobs and generate the current table state, stored under _delta_log directory

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note**: Use Spark UI to view the DAG of the table conversion steps behind the scene: Compute -> Cluster -> Spark UI -> ect.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Apply Z-ordering to tables without member state column MS as a partitioning column

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE delta_co2_emissions ZORDER BY (MS)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE delta_emissions_diff_yoy ZORDER BY (MS);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE eea_serving.highest_emissions;
