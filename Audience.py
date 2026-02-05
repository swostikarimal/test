# Databricks notebook source
'''
The data ingestion for the audience that contains the demographic data of the customers and their engagement with the website required by the business
@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 01/20/2025
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

datalake = "/mnt/ga/"
filename = "audience"
pkcolmns = ["date", "userAgeBracket", "userGender", "city", "country"]

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

def fetch_ga_data(client, property_id: str) -> pd.DataFrame:
    """
    Fetches data from Google Analytics for a specific property and dimension.
    """
    request = RunReportRequest(
        property=property_id,
        dimensions=[
            Dimension(name="date"), Dimension(name="userAgeBracket"), Dimension(name="userGender"), Dimension(name="city"), Dimension(name="country"),
        ],
        metrics=[
            Metric(name="totalUsers"), Metric(name="newUsers"), Metric(name="sessions"), 
            Metric(name="userEngagementDuration")
        ],
        date_ranges=[DateRange(start_date="1daysAgo", end_date="today")]
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


def process_user_audience(client, property_id: str, subfolder: str):
    """
    Processes user acquisition data for specified metrics and writes to the data lake.
    """
    pd_df = fetch_ga_data(client, property_id)
    spark_df = spark.createDataFrame(pd_df)
    spark_df = EngagenmentTimepPerSession(spark_df)

    trackAndTrace(
        df=spark_df, destinatation_Schema=datalake, subfolder=subfolder, filename=filename, PKColumns=pkcolmns, spark=spark, dbutils=dbutils
    )

def main():
    """
    Main function to fetch and write Google Analytics data.
    """
    credentials = service_account.Credentials.from_service_account_file('/dbfs/FileStore/tables/buddhatech.json')
    client = BetaAnalyticsDataClient(credentials=credentials)

    properties = {
        "properties/347471683": {"subfolder": "reporting/acquisition/audienceandtech"}
    }

    for property_id, config in properties.items():
        process_user_audience(
            client=client,
            property_id=property_id,
            subfolder=config["subfolder"],
        )

if __name__ == "__main__":
    main()