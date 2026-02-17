# Databricks notebook source
'''
Dat Ingestion logic for Google page and screens tables into data storage

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 12/12/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from TrackAndUpdate import trackAndTrace
from pyspark.sql import functions as F
import pandas as pd
import logging

destinatation_Schema = "/mnt/ga/reporting"

METRIC_TO_FILENAME = {
    "unifiedPagepathScreen": "pagePathAndScreenClass",
}
PKColumns = ["date","unifiedPagepathScreen", "unifiedScreenClass"]

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
    ).drop("userEngagementDuration")

    return df

def fetch_ga_data(client, property_id: str, dimension_name: str) -> pd.DataFrame:
    """
    Fetches data from Google Analytics for a specific property and dimension.
    """
    request = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="date"), Dimension(name=dimension_name), Dimension(name="unifiedScreenClass")],
        metrics=[
            Metric(name="totalUsers"), Metric(name="activeUsers"), Metric(name="screenPageViews"),
            Metric(name="keyEvents"), Metric(name="userEngagementDuration"),
            Metric(name="bounceRate"), Metric(name="totalRevenue")
        ],
        date_ranges=[DateRange(start_date="15daysAgo", end_date="today")]
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


def process_pagesAndPath(client, property_id: str, metrics: list, subfolder: str, overwrite: bool):
    """
    Processes user acquisition data for specified metrics and writes to the data lake.
    """
    for metric in metrics:
        logging.info(f"Processing {metric} for property {property_id}...")

        pd_df = fetch_ga_data(client, property_id, metric)
        spark_df = spark.createDataFrame(pd_df)
        filename = METRIC_TO_FILENAME.get(metric, metric)

        trackAndTrace(
            df=spark_df, destinatation_Schema=destinatation_Schema, subfolder=subfolder, 
            filename=filename, PKColumns=PKColumns,spark=spark, dbutils=dbutils
        )

def main():
    """
    Main function to fetch and write Google Analytics data.
    """
    credentials = service_account.Credentials.from_service_account_file('/dbfs/FileStore/tables/buddhatech.json')
    client = BetaAnalyticsDataClient(credentials=credentials)

    properties = {
        "properties/347471683": {"subfolder": "/pagesAndScreens", "overwrite": True} 
    }

    for property_id, config in properties.items():
        process_pagesAndPath(
            client=client,
            property_id=property_id,
            metrics=["unifiedPagepathScreen"],
            subfolder=config["subfolder"],
            overwrite=config["overwrite"]
        )

if __name__ == "__main__":
    main()