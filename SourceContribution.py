# Databricks notebook source
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from pyspark.sql import functions as F
import pandas as pd
from TrackAndUpdate import trackAndTrace

lakepath = "/mnt/ga/"
subfolder = "reporting/Conversion/sourceContribution"

pkcolumns = ["Date","SourceName", "channelName"]
eventpk = ["EventName"]

sourcedimension = [
    "sessionDefaultChannelGroup", "sessionMedium", "sessionSource",
    "sessionSourceMedium", "sessionCampaignName"
]

keyevents = ["view_item_list", "add_to_cart", "view_item", "purchase"]

properties = spark.table("analyticadebuddha.default.gainfo")
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

startdate = properties.select("startdate").collect()[0][0]
startdate_ = str(startdate) + "daysAgo"
enddate_ = "today"
daterange = [DateRange(start_date=startdate_, end_date=enddate_)]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)

def EngagenmentTimepPerSession(df):
    """
    Returns a new DataFrame with the calculated average engagement time per session.
    """
    df = df.withColumn(
    "averageEngagenmentTimepPerSession", (F.col("userEngagementDuration")/60)/F.col("sessions")
    ).withColumn(
    "Minute", F.floor("averageEngagenmentTimepPerSession")
    ).withColumn(
    "Second", F.round((F.col("averageEngagenmentTimepPerSession") - F.col("Minute")) * 60)
    ).withColumn(
    "averageEngagenmentTimepPerSession", F.concat(F.col("Minute"), F.lit("m"), F.lit(' '), F.col("Second").cast("int"), F.lit("s"))
    ).drop("Minute", "Second")

    return df

def getData(dimensions, daterange=daterange):
    """
    """      
    request = RunReportRequest(
    property=property_id,
    dimensions= dimensions,
    metrics=[
        Metric(name="totalUsers"), Metric(name="activeUsers"),
        Metric(name="newUsers"), Metric(name="sessions"), 
        Metric(name="averageSessionDuration"),
        Metric(name="userEngagementDuration"), Metric(name="eventsPerSession"), 
        Metric(name="keyEvents"), Metric(name="totalRevenue"),
        Metric(name="countCustomEvent:ticket_purchased")
    ], 
    date_ranges=daterange
    )

    response = client.run_report(request)
    data = [
        [dim.value for dim in row.dimension_values] +
        [metric.value for metric in row.metric_values]
        for row in response.rows
    ]

    columns = (
        [header.name for header in response.dimension_headers] +
        [header.name for header in response.metric_headers]
    )
    df = pd.DataFrame(data, columns=columns)

    df["property"] = property_id
    df["date"] = pd.to_datetime(df['date'], format='%Y%m%d')
    
    return df

def toDestination(
    pd_df, filename, colname, iteration
):
    spark_df = spark.createDataFrame(pd_df)
    
    if iteration > 0:
        pkcolumns1 = pkcolumns

    else:
        pkcolumns1 = pkcolumns + eventpk
        spark_df = spark_df.filter(F.col("eventName").isin(keyevents))

    spark_df = spark_df.withColumn(
        "SourceName", F.lit(colname)
    ).withColumn(
        "ChannelName", F.col(f"{colname}")
    ).drop(F.col(f"{colname}"))

    spark_df = EngagenmentTimepPerSession(df=spark_df)

    trackAndTrace(
                df=spark_df, destinatation_Schema=lakepath, subfolder=subfolder, 
                filename=filename, PKColumns=pkcolumns1, spark=spark, dbutils=dbutils
    )
    
    return None

def fetch_ga_data(property_id: str, dimension_name: str) -> pd.DataFrame:
    """
    Fetches data from Google Analytics for a specific property and dimension.
    """
    event = [Dimension(name="eventName")]
    dimensions1 = [Dimension(name="date"), Dimension(name=dimension_name)]

    filename = dimension_name
    colname = dimension_name

    for i in range(0,2):
        if i > 0:
            dimensions = dimensions1
            filename = dimension_name + '1'   
            df = getData(dimensions)
            toDestination(pd_df=df, filename=filename, colname=colname, iteration=i)
        else:
            dimensions = dimensions1 + event
            filename = dimension_name
            df = getData(dimensions)
            toDestination(pd_df=df, filename=filename, colname=colname, iteration=i)

    return None

def processSourceContribution(metrics: list):
    """
    Processes user acquisition data for specified metrics and writes to the data lake.
    """
    for metric in metrics:
        print(f"Processing {metric} for property {property_id}...")
        fetch_ga_data(property_id=property_id, dimension_name=metric)

    return None

def main():
    """
    Main function to fetch and write Google Analytics data.
    """
    processSourceContribution(
        metrics=sourcedimension
    )

if __name__ == "__main__":
    main()