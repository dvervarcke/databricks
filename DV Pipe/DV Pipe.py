# Databricks notebook source
query = "SELECT * FROM population_data WHERE country = 'Belgium'"
df = spark.sql(query)
display(df)
