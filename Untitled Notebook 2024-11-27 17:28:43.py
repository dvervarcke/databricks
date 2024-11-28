# Databricks notebook source
# MAGIC %sql
# MAGIC select * from dv.population_data where countryname = 'Belgium'

# COMMAND ----------

select records out of population_data table in schema dv for country = 'belgium'
