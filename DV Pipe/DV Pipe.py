# Databricks notebook source part 2
query = "SELECT * FROM population_data WHERE country = 'Belgium'"
df = spark.sql(query)
pandas_df = df.toPandas()  # Convert to pandas DataFrame
display(pandas_df)
