# Databricks notebook source
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
import pyspark.sql.functions as F
import pandas as pd
import time

filenames = {
  "customEvent:sector": "sector",
  "itemCategory": "class",
  "itemCategory2": "flight",
  "itemCategory3": "flightdate",
  "itemCategory4": "ticketTypes",
  "country": "country",
  "city": "city",
  "cityId": "cityId"
}

lakepath = "/mnt/ga/"

seconddim = ["itemCategory", "itemCategory3", "itemCategory4", "country", "city"]

sourcedimension = [
    "sessionDefaultChannelGroup", "sessionMedium", "sessionSource",
    "sessionSourceMedium", "sessionCampaignName"
]

dimsec = ["customEvent:sector"]

dimfirst = ["eventName"] + dimsec
metrics =  ["totalUsers", "itemsPurchased", "itemRevenue", "itemRefundAmount", "itemDiscountAmount"]
filterin = ["purchase", "flight_payment", "flight_contactdetail", "flight_searched"]
filterbtop = ["flight_payment", "flight_contactdetail", "flight_searched"]
countryandcityDim = ["date", "eventName", "customEvent:sector","country", "city"]

event_configs = {
    "sectordetails": {
        "Dimension": ["date"] + dimfirst,
        "Metric": metrics
    },
    "sectordetailsStoP":{
        "Dimension": ["date"] + dimfirst,
        "Metric":["totalUsers"]
    },
    "sectorsource": {
        "Dimension": ["date"] + dimfirst,
        "Metric":metrics
    },
    "sectorsourceStoP": {
        "Dimension": ["date"] + dimfirst,
        "Metric": ["totalUsers"]
    },
    "sectoronly":{
        "Dimension": ["date"] + dimsec,
        "Metric": [
            "eventCount", "totalUsers", "newUsers", "sessions", 
            "userEngagementDuration", "averageSessionDuration"
        ]
    },
    "countryAndCity": {
        "Dimension": countryandcityDim,
        "Metric": metrics
    },
    # "countryAndCityStoP": {
    #     "Dimension": countryandcityDim,
    #     "Metric": ["totalUsers"]
    # }

}

properties = spark.table("analyticadebuddha.default.gainfo")

property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)

def runReport(request, filterin_):
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

    data["property"] = property_id  
    data["date"] = pd.to_datetime(data['date'], format='%Y%m%d')

    return data

def toDataLake(
    filterin_, newdim, metrics, filename, subfolder, daterange
):
    '''
    '''
    fdim = [Dimension(name=d) for d in newdim]
    fmetrics = [Metric(name=m) for m in metrics]

    request = RunReportRequest(
        property=property_id,
        dimensions=fdim,
        metrics=fmetrics,
        date_ranges=daterange
    )

    data = runReport(request, filterin_)

    sdf = spark.createDataFrame(data)
    if filterin_ is not None:
        sdf = sdf.filter(F.trim(F.lower("eventName")).isin(filterin_))

    trackAndTrace(
        df=sdf, destinatation_Schema=lakepath, subfolder=subfolder, 
        filename=filename, PKColumns=newdim, spark=spark, dbutils=dbutils
    )

    return None

def processEventConfigs(event_configs, daterange):
    """
    """
    for event_name, event_config in event_configs.items():
        print(f"procession for {event_name}")

        dimensions = event_config.get("Dimension", [])
        metrics = event_config.get("Metric", [])
        subfolder = "reporting/ecommerce/" + event_name

        if event_name in ["sectordetails", "sectordetailsStoP"]:
            for dim in seconddim:
                newdim = dimensions + [dim]
                filename = filenames.get(dim)
                if event_name == "sectordetails":
                  filterin_ = filterin
                else: filterin_ = filterbtop
                
                data = toDataLake(
                    filterin_, newdim, metrics, filename, subfolder, daterange
                )

        elif event_name in ["sectorsource", "sectorsourceStoP"]:
            for dim in sourcedimension:
                newdim = dimensions + [dim]
                filename = dim
                if event_name == "sectorsource":
                  filterin_ = filterin
                else: filterin_ = filterbtop
                
                data = toDataLake(
                    filterin_, newdim, metrics, filename, subfolder, daterange
                )

        elif event_name == "sectoronly":
            for dim in sourcedimension:
                newdim = dimensions + [dim]
                filterin_ = None
                filename = dim

                toDataLake(
                    filterin_, newdim, metrics, filename, subfolder, daterange
                )
        else:
            newdim = dimensions
            filterin_ = filterin
            if event_name == "countryAndCityStoP": 
                filterin_ = filterbtop
            
            filename = event_name
            newdim = dimensions
            toDataLake(
                filterin_, newdim, metrics, filename, subfolder, daterange
            )

    return None

def main():

    """
    """
    startdate = properties.select("startdate").collect()[0][0]
    startdate_ = str(startdate) + "daysAgo"
    enddate_ = "today"

    print(f"For date range {startdate_} to {enddate_}")
    daterange = [DateRange(start_date=startdate_, end_date=enddate_)]
    data = processEventConfigs(event_configs, daterange=daterange)

    return None

if __name__=="__main__":
    main()