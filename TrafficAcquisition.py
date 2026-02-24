# Databricks notebook source
'''
Dat Ingestion logic for Google Analytics traffic acqusition tables into data storage

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 11/18/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from pyspark.sql import functions as F
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

GA_DATA_LAKE_PATH = "/mnt/ga/"

ACQUISITION_METRICS = [
    "sessionPrimaryChannelGroup", "sessionDefaultChannelGroup", "sessionMedium", "sessionSource",
    "sessionSourceMedium", "sessionSourcePlatform", "sessionCampaignName",
]

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

def write_to_data_lake(df, subfolder: str, filename: str, overwrite: bool = True):
    """
    Writes Spark DataFrame to the data lake in Parquet format.
    """
    mode = "overwrite" if overwrite else "append"
    path = f"{GA_DATA_LAKE_PATH}{subfolder}/{filename}"
    try:
        df.write.mode(mode).option("mergeSchema", "true").parquet(path)
        logging.info(f"Data written to {path} in mode '{mode}'.")
    except Exception as e:
        logging.error(f"Error writing data to {path}: {e}")


def fetch_ga_data(client, property_id: str, dimension_name: str) -> pd.DataFrame:
    """
    Fetches data from Google Analytics for a specific property and dimension.
    """
    request = RunReportRequest(
        property=property_id,
        dimensions=[Dimension(name="date"), Dimension(name=dimension_name)],
        metrics=[
            Metric(name="activeUsers"), Metric(name="sessions"), Metric(name="engagedSessions"), Metric(name="engagementRate"),
            Metric(name="userEngagementDuration"), Metric(name="eventsPerSession"), #, Metric(name="averageSessionDuration")
            Metric(name="keyEvents"), Metric(name="sessionKeyEventRate"), Metric(name="totalRevenue"), Metric(name="countCustomEvent:ticket_purchased")
        ], 
        date_ranges=[DateRange(start_date="3000daysAgo", end_date="today")]
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


def process_user_acquisition(client, property_id: str, metrics: list, subfolder: str, overwrite: bool):
    """
    Processes user acquisition data for specified metrics and writes to the data lake.
    """
    for metric in metrics:
        logging.info(f"Processing {metric} for property {property_id}...")
        pd_df = fetch_ga_data(client, property_id, metric)
        spark_df = spark.createDataFrame(pd_df)
        spark_df = EngagenmentTimepPerSession(df=spark_df)
        write_to_data_lake(spark_df, subfolder=subfolder, filename=metric, overwrite=overwrite)


def main():
    """
    Main function to fetch and write Google Analytics data.
    """
    credentials = service_account.Credentials.from_service_account_file('/dbfs/FileStore/tables/buddhatech.json')
    client = BetaAnalyticsDataClient(credentials=credentials)

    properties = {
        "properties/347471683": {"subfolder": "trafficAcquisition", "overwrite": True} 
    }

    for property_id, config in properties.items():
        process_user_acquisition(
            client=client,
            property_id=property_id,
            metrics=ACQUISITION_METRICS,
            subfolder=config["subfolder"],
            overwrite=config["overwrite"]
        )

if __name__ == "__main__":
    main()