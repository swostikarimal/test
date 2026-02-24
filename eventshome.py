# Databricks notebook source
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace 
import pandas as pd
import time

datalake ="/mnt/ga/reporting/CTA/"
subfolder = "homepage"
metric = ["eventCount", "totalUsers", "newUsers", "sessions","userEngagementDuration", "engagementRate", "averageSessionDuration"]

properties = spark.table("analyticadebuddha.default.gainfo")
startdate = properties.select("startdate").collect()[0][0]
startdate_ = str(startdate) + "daysAgo"
enddate_ = "today"

daterange = [DateRange(start_date=startdate_, end_date=enddate_)]
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)

event_configs = {
    "searchpanel_click": {
        "Dimension": ["eventName"],
        "Metric": metric
    },
    "royalclub_buttons": {
        "Dimension": ["eventName"],
        "Metric": metric
    },
    "select_promotion": {
        "Dimension": ["eventName", "customEvent:previous_url"],
        "Metric": ["itemsClickedInPromotion","totalUsers", "sessions", "engagementRate"]
    },
    "homepageslider": {
        "Dimension": ["eventName"],
        "Metric": metric
    },
    "navigation_clicked": {
        "Dimension": ["eventName", "customEvent:custom_title"], 
        "Metric": metric
    },
    "blog_clicked": {
        "Dimension": ["eventName"],
        "Metric": metric
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

def toLake(configs):
    for file, params in configs.items():
        print(f"Procession for the file {file}")
        dimensions = ["date"] + params["Dimension"]
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

        trackAndTrace(
        df=sdf, destinatation_Schema=datalake, subfolder=subfolder, filename=file, PKColumns=dimensions, spark=spark, dbutils=dbutils
        )

def main():
    toLake(configs=event_configs)

if __name__ == "__main__":
    main()