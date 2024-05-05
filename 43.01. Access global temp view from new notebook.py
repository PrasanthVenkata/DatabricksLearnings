# Databricks notebook source
df1 = spark.sql("select * from global_temp.vw_global")
display(df1)
