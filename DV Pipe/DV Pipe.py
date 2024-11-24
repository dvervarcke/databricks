# Databricks notebook source part 2
query = "SELECT * FROM population_data WHERE country = 'Belgium'"
df = spark.sql(query)
display(df)
