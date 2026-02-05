# Databricks notebook source
'''
Data Ingestion from GA4 to data lake for UserJourney and flighbookingfunnel marts

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 12/10/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''

import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace
from typing import Dict, List, Any
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
import pyspark.sql.functions as F
import pandas as pd
import time

lakepath = "/mnt/ga/"
subfolder = "reporting/Conversion/funnel"

properties = spark.table("analyticadebuddha.default.gainfo")
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)

event_configs = {
    "hostName":{
        "Dimension": ["hostName", "customEvent:sector"],
        "Metric": ["totalUsers", "eventCount"]
    },
    "/": {
        "Dimension": ["pagePath"],
        "Metric": ["totalUsers"]
    },
    "flight_searched": {
        "Dimension": ["eventName",  "customEvent:sector"], 
        "Metric": ["eventCount", "totalUsers", "newUsers"]
    },
    "flight_contactdetail": {
        "Dimension": ["eventName", "customEvent:sector"], 
        "Metric": ["eventCount", "totalUsers", "newUsers"]
    },
    "flight_payment": {
        "Dimension": ["eventName", "customEvent:sector"], 
        "Metric": ["eventCount", "totalUsers", "newUsers"]
    },
    "purchase":{
        "Dimension": ["eventName", "customEvent:sector"],
        "Metric": ["totalUsers", "itemsPurchased", "itemRevenue", "itemRefundAmount", "itemDiscountAmount"]
    }
}

def getData(request: RunReportRequest, a:str):
    """
    Fetch data from Google Analytics and return a Pandas DataFrame.
    """
    response = client.run_report(request, timeout=60)
    data = []
    for row in response.rows:
        data.append(
            [dim.value for dim in row.dimension_values] +
            [metric.value for metric in row.metric_values]
        )

    data = pd.DataFrame(data, columns=[
        *map(lambda x: x.name, response.dimension_headers),
        *map(lambda x: x.name, response.metric_headers)
    ])

    if a == '/':
        data = data[data["pagePath"] == '/']
    elif a != 'hostName':
        data = data[data["eventName"] == a]

    data["property"] = property_id  
    data["eventfor"] = a
    data["date"] = pd.to_datetime(data['date'], format='%Y%m%d')

    return data


def to_datalake(
    event_configs: Dict[str, Dict[str, List[str]]],
    daterange:list[str] = None
):
    """
    Writes data to the data lake in overwrite/append mode in Parquet format.
    """    
    for event_name, event_config in event_configs.items():
        print(f"Processing for the event {event_name}")
        
        dimensions = ["date"] + event_config.get("Dimension", [])
        metrics = event_config.get("Metric", [])
        
        for iteration in range(2):
            if iteration > 0 and event_name != "/":
                dimensions = [d for d in dimensions if d != "customEvent:sector"]
                filename = f"f{event_name}"
                PK_columns = ["date", "hostName"] if event_name == "hostName" else ["date", "eventName"]

            elif iteration == 0 and event_name != "/":
                PK_columns = ["date", "hostName", "customEvent:sector"] if event_name == "hostName" else ["date", "eventName", "customEvent:sector"]
                filename = f"{event_name}"

            if event_name == '/':
                filename = "pagePath"
                PK_columns = ["date", "pagePath"]
            
            dim_objects = [Dimension(name=d) for d in dimensions]
            metric_objects = [Metric(name=m) for m in metrics]
            
            time.sleep(1)
            
            request = RunReportRequest(
                property=property_id,
                dimensions=dim_objects,
                metrics=metric_objects,
                date_ranges=daterange
            )
            
            try:
                data = getData(request=request, a=event_name)
                time.sleep(1)
                
                df = spark.createDataFrame(data).withColumn(
                    "date", F.date_format("date", "yyyy-MM-dd")
                )
                
                trackAndTrace(
                    df=df, destinatation_Schema=lakepath, subfolder=subfolder, 
                    filename=filename, PKColumns=PK_columns, spark=spark, dbutils=dbutils
                )
                                
            except Exception as e:
                print(f"Error processing {event_name}: {e}")
                continue
            
            if event_name == '/':
                break
    
    return None

def main():
    """
    """
    startdate = properties.select("startdate").collect()[0][0]
    startdate_ = str(startdate) + "daysAgo"
    enddate_ = "today"

    print(f"For date range {startdate_} to {enddate_}")
    daterange = [DateRange(start_date=startdate_, end_date=enddate_)]

    to_datalake(
        event_configs=event_configs,
        daterange=daterange
    )

if __name__ == "__main__":
    main()