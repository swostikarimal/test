# Databricks notebook source
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from pyspark.sql import functions as F
import pandas as pd
from TrackAndUpdate import trackAndTrace

lakepath = "/mnt/ga/"
subfolder = "reporting/acquisition/firsttimevisitors"

pkcolumns = ["Date","SourceName","ChannelName", "unifiedPagepathScreen"]

sourcedimension = [
    "firstUserDefaultChannelGroup", "firstUserSourceMedium", "firstUserCampaignName", "firstUserSource", "firstUserMedium"
]

sourcedimension1 = [
    "sessionDefaultChannelGroup", "sessionSourceMedium", "sessionCampaignName", "sessionSource"
]

properties = spark.table("analyticadebuddha.default.gainfo")
startdate = properties.select("startdate").collect()[0][0]
startdate_ = str(startdate) + "daysAgo"
enddate_ = "today"

daterange = [DateRange(start_date=startdate_, end_date=enddate_)]
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

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

def getdata(client, dimension_name):
    """
    Fetches data from Google Analytics for a specific property and dimension.
    """
    request = RunReportRequest(
        property=property_id,
        dimensions= [Dimension(name="date"), Dimension(name="unifiedPagepathScreen"),Dimension(name=dimension_name)],
        metrics=[
            Metric(name="totalUsers"), Metric(name="newUsers"), Metric(name="sessions"), 
            Metric(name="userEngagementDuration"), Metric(name="engagementRate"),
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

def processFirstTimeVisitor(client, metrics):
    """
    Processes user acquisition data for specified metrics and writes to the data lake.
    """
    for metric in metrics:
        print(f"Processing {metric} for property {property_id}...")

        pd_df = getdata(client, metric)
        spark_df = spark.createDataFrame(pd_df).withColumn(
            "SourceName", F.lit(metric)
        ).withColumn(
            "ChannelName", F.col(f"{metric}")
        ).drop(F.col(f"{metric}"))

        spark_df = EngagenmentTimepPerSession(df=spark_df)

        trackAndTrace(
                    df=spark_df, destinatation_Schema=lakepath, subfolder=subfolder, 
                    filename=metric, PKColumns=pkcolumns, spark=spark, dbutils=dbutils
        )

    return None

def main():
    """
    Main function to fetch and write Google Analytics data.
    """
    credentials = service_account.Credentials.from_service_account_file(file)
    client = BetaAnalyticsDataClient(credentials=credentials)

    for acquisition in [sourcedimension, sourcedimension1]:
        processFirstTimeVisitor(
            client=client,
            metrics=acquisition
        )
        
if __name__ == "__main__":
    main()