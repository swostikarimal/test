# Databricks notebook source
import inspect
import metaIngestion
print(inspect.getsource(metaIngestion.getADSInsights))

# COMMAND ----------

import inspect
from TrackAndUpdate import trackAndTrace
print(inspect.getsource(trackAndTrace))

print(inspect.getsource(metaIngestion.getDim))