# Databricks notebook source
'''
Dat Ingestion logic for Google page and screens tables into data storage

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 12/16/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from pyspark.sql import functions as F
from TrackAndUpdate import trackAndTrace
import pandas as pd
import time


selectCols = ["Date","PagesAndScreen","unifiedScreenClass", "sourceDimension","sessionChannel", "totalUsers", "activeUsers", "screenPageViews", 
             "userEngagementDuration", "viewsPerActiveUser","averageEngagenmentTimepPerActiveUser","bounceRate","engagementRate", "Property"] # "PagesAndScreenName",

PKColumns = ["Date", "PagesAndScreen", "unifiedScreenClass", "sourceDimension", "sessionChannel"]

lakepath = "/mnt/ga/reporting"
subfolder = "/pagewithsource"

METRIC_TO_FILENAME = {
    # "unifiedScreenClass": "pageTitleAndScreenClass",
    "unifiedPagepathScreen": "pagePathAndScreenClass"
    # "unifiedScreenName": "pageTitleAndScreenName",
    # "contentGroup": "contentGroup",
}

sourcedimension = [
    "sessionPrimaryChannelGroup", "sessionDefaultChannelGroup", "sessionMedium", 
    "sessionSourceMedium", "sessionSourcePlatform", "sessionCampaignName",
]

properties = spark.table("analyticadebuddha.default.gainfo")
startdate = properties.select("startdate").collect()[0][0]
startdate_ = str(startdate) + "daysAgo"
enddate_ = "today"

daterange = [DateRange(start_date=startdate_, end_date=enddate_)]
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]


def EngagenmentTimepPerActiveUser(df):
    """
    Returns a new DataFrame with the calculated Average engagement time per active user.
    """
    df = df.withColumn(
    "averageEngagenmentTimepPerActiveUser", (F.col("userEngagementDuration")/60)/F.col("activeUsers")
    ).withColumn(
    "Minute", F.floor("averageEngagenmentTimepPerActiveUser")
    ).withColumn(
    "Second", F.round((F.col("averageEngagenmentTimepPerActiveUser") - F.col("Minute")) * 60)
    ).withColumn(
    "averageEngagenmentTimepPerActiveUser", F.concat(F.col("Minute"), F.lit("m"), F.lit(' '), F.col("Second").cast("int"), F.lit("s"))
    ).drop("Minute", "Second")

    df = df.withColumn(
      "viewsPerActiveUser", F.col("screenPageViews")/F.col("activeUsers")
    )

    return df

def fetch_ga_data(
    client, property_id, dimension_name, daterange=daterange
):
    """
    Fetches data from Google Analytics for a specific property and dimension.
    """
    dfs = []
    for i in sourcedimension:
        request = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name="date"), Dimension(name="unifiedScreenClass"), Dimension(name=dimension_name), Dimension(name=i)],
            metrics=[
                Metric(name="totalUsers"), Metric(name="activeUsers"), Metric(name="screenPageViews"),
                Metric(name="userEngagementDuration"), Metric(name="engagementRate"),
                Metric(name="bounceRate")
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
        df["sourceDimension"] = i

        df["date"] = pd.to_datetime(df['date'], format='%Y%m%d')

        spark_df = spark.createDataFrame(df).withColumnRenamed(i, "sessionchannel").withColumnRenamed(dimension_name, "PagesAndScreen")

        dfs.append(spark_df)
    
    final = dfs[0]
    for df in dfs[1:]:
        final = final.unionByName(df)

    final= EngagenmentTimepPerActiveUser(final).select(*selectCols)

    return final

def process_pagesAndPath(client, property_id: str, metrics: list, subfolder: str):
    """
    Processes user acquisition data for specified metrics and writes to the data lake.
    """
    for metric in metrics:
        print(f"Processing {metric} for property {property_id}...")
        spark_df = fetch_ga_data(client, property_id, metric)
        filename = METRIC_TO_FILENAME.get(metric, metric)

        trackAndTrace(
            df=spark_df, destinatation_Schema=lakepath, subfolder=subfolder, 
            filename=filename, PKColumns=PKColumns, spark=spark, dbutils=dbutils
        )

    return None

def main():
    """
    Main function to fetch and write Google Analytics data.
    """
    credentials = service_account.Credentials.from_service_account_file(file)
    client = BetaAnalyticsDataClient(credentials=credentials)

    process_pagesAndPath(
        client=client,
        property_id=property_id,
        metrics=["unifiedPagepathScreen"],
        subfolder=subfolder,
    )

    time.sleep(5)
    
    spark.read.parquet(
        "/mnt/ga/reporting/pagewithsource/pagePathAndScreenClass"
    ).write.mode("overwrite").saveAsTable(
        "analyticadebuddha.google.pageWithSource"
    )
    

if __name__ == "__main__":
    main()