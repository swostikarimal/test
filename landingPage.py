# Databricks notebook source
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from pyspark.sql import functions as F
import pandas as pd
from TrackAndUpdate import trackAndTrace

lakepath = "/mnt/ga/"
subfolder = "reporting/landingpage"
schema = "analyticadebuddha.landingpage"

properties = spark.table("analyticadebuddha.default.gainfo")
startdate = properties.select("startdate").collect()[0][0]
startdate_ = str(startdate) + "daysAgo"
enddate_ = "today"

daterange = [DateRange(start_date=startdate_, end_date=enddate_)]
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)

pathlandingPageMedium = "/mnt/ga/reporting/landingpage/landingPageMedium"
pathlandingPageSource = "/mnt/ga/reporting/landingpage/landingPageSource"
pathlandingPageCampign = "/mnt/ga/reporting/landingpage/landingPageCampign"
pathlandingPage = "/mnt/ga/reporting/landingpage/landingPage"
pathsources = "/mnt/ga/reporting/landingpage/landingpagesources"

metrics = ["sessions", "totalUsers", "newUsers", "totalRevenue", "userEngagementDuration", "averageSessionDuration"]

filterjourney = (
    F.col("landingPage").contains("/search") 
    | F.col("landingPage").contains("/payment") 
    | F.col("landingPage").contains("payment")
    | F.col("landingPage").contains("/book/flight")
)

config = {
    "landingpagesources":{
        "dimension": ["date","landingPage","sessionDefaultChannelGroup", "sessionSourceMedium", "sessionCampaignName"]
    },
    # "landingPage": {
    #     "dimension": ["date","landingPage"]
    # },
    # "landingPageSource": {
    #     "dimension": ["date","landingPage","sessionSource"]
    # },
    # "landingPageMedium": {
    #     "dimension": ["date","landingPage","sessionMedium"]
    # },
    # "landingPageCampign": {
    #     "dimension": ["date","landingPage","sessionCampaignName"]
    # }

}
def fetchData(config):
    '''
    '''
    for filename, dim in config.items():
        dimobj = dim.get("dimension")
        dimension = [Dimension(name=d) for d in dimobj]

        request = RunReportRequest(
            property=property_id,
            metrics=[Metric(name=m) for m in metrics],
            dimensions=dimension,
            date_ranges=daterange
        )

        response = client.run_report(request)
        data = [
            [
                value.value for value in row.dimension_values
            ] + [
                value.value for value in row.metric_values
            ] for row in response.rows
        ]

        column = [
            colname.name for colname in response.dimension_headers
        ] + [
            colname.name for colname in response.metric_headers
        ]

        df = pd.DataFrame(data, columns=column)
        df["date"] = pd.to_datetime(df['date'], format='%Y%m%d')

        sparkdf = spark.createDataFrame(df)

        trackAndTrace(
            df=sparkdf, destinatation_Schema=lakepath, subfolder=subfolder, 
            filename=filename, PKColumns=dimobj, spark=spark, dbutils=dbutils
        )

    return None

def createLandingPage(
    df, table
):
    df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{schema}.{table}")
    print(f"The table {table} has been created on {schema}.")

    return None

def main():
    """
    """
    fetchData(config)
    landingpagesource = spark.read.parquet(pathsources).filter(~filterjourney).withColumns(
        {
            "sessionSource": F.split("sessionSourceMedium", "/")[0], "sessionMedium": F.split("sessionSourceMedium", "/")[1]
        }
    ).withColumn(
        "sessionSource", F.when(
            F.col("sessionSource").contains("facebook"), F.lit("facebook.com")
        ).otherwise(F.col("sessionSource"))
    )
    
    createLandingPage(df=landingpagesource, table="landingpagesources")

if __name__=="__main__":
    main()