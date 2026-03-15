# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

spark.read.format("delta").load("/mnt/rawmro/LogOils").display()