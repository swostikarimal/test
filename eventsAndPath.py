# Databricks notebook source
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace 
import pandas as pd
import time

properties = spark.table("analyticadebuddha.default.gainfo")
startdate = properties.select("startdate").collect()[0][0]
startdate_ = str(startdate) + "daysAgo"
enddate_ = "today"

daterange = [DateRange(start_date=startdate_, end_date=enddate_)]
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)

datalake ="/mnt/ga/reporting/"
subfolder = "events_pagepath"

configs = {
    "events": {
        "Dimension": ["date", "eventName"],
        "Metric": ["eventCount", "totalUsers", "activeUsers", "newUsers", "bounceRate"]
    },
    "pagepath": {
        "Dimension": ["date", "pagePath"],
        "Metric": ["totalUsers", "activeUsers", "averageSessionDuration"]

    },
    "unifiedPagePathScreen": {
        "Dimension": ["date", "unifiedPagePathScreen"],
        "Metric": ["totalUsers", "activeUsers", "averageSessionDuration"]
    },
    
}

def getData(request): 
    response = client.run_report(request=request)
    data = [
    ]
    for row in response.rows:
        data.append(
            [dimval.value for dimval in row.dimension_values] +
            [metric.value for metric in row.metric_values]
        )

    df = pd.DataFrame(data, columns=[
        *map(lambda x: x.name, response.dimension_headers),
        *map(lambda x: x.name, response.metric_headers)
    ])

    df["date"] = pd.to_datetime(df['date'], format='%Y%m%d')

    return df

def tolake(configs=configs):
    for file, params in configs.items():
        print(f"Procession for the file {file}")
        dimensions = params["Dimension"]
        metric = params["Metric"]

        dim_objects = [Dimension(name=d) for d in dimensions]
        metrics_obj = [Metric(name=m) for m in metric]

        requets = RunReportRequest(
            property=property_id,
            dimensions=dim_objects,
            metrics=metrics_obj,
            date_ranges=daterange
        )
        
        df = getData(requets)
        sdf = spark.createDataFrame(df)

        if sdf.isEmpty():
            print(f"No data for the file {file}")
            continue

        trackAndTrace(
        df=sdf, destinatation_Schema=datalake, subfolder=subfolder, filename=file, PKColumns=dimensions, spark=spark, dbutils=dbutils
        )

    return None
    
if __name__ == "__main__":
    tolake()