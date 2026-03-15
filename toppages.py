# Databricks notebook source
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace 
import pandas as pd
import time

metric = [
    "totalUsers", "eventCount","activeUsers", "screenPageViews", "userEngagementDuration", 
    "bounceRate", "engagementRate","averageSessionDuration"
]

pathaudiance = "/mnt/ga/reporting/toppages/pagePathAndUnifiedScreenClassAndAudience"
pathmedium = "/mnt/ga/reporting/toppages/PathAndUnifiedScreenSessionMedium"
pathsource = "/mnt/ga/reporting/toppages/PathAndUnifiedScreenSessionSource"
pathpage = "/mnt/ga/reporting/toppages/pagePathAndScreenClass"
pathbrowser = "/mnt/ga/reporting/toppages/pagePathAndBrowser"
pathos = "/mnt/ga/reporting/toppages/pagePathAndOs"

pathage = "/mnt/ga/reporting/toppages/pagePathAge"
pathgender = "/mnt/ga/reporting/toppages/pagePathgender"
pathdevice = "/mnt/ga/reporting/toppages/PathpathDevice"

filesourcemedium = "/mnt/ga/reporting/toppages/sourcemedium"

datalake ="/mnt/ga/reporting/"  
subfolder = "toppages"
catalog = "analyticadebuddha.toppages"

properties = spark.table("analyticadebuddha.default.gainfo")
startdate = properties.select("startdate").collect()[0][0]
startdate_ = str(startdate) + "daysAgo"
enddate_ = "today"

daterange = [DateRange(start_date=startdate_, end_date=enddate_)]
property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)

filterjourney = (
    F.col("unifiedPagepathScreen").contains("/search") 
    | F.col("unifiedPagepathScreen").contains("/payment") 
    | F.col("unifiedPagepathScreen").contains("payment")
    | F.col("unifiedPagepathScreen").contains("/book/flight")
)

event_configs = {
    "pagePathAndScreenClass": {
        "Dimension": ["unifiedPagepathScreen"],
        "Metric": metric
    },
    "pagePathAndaudiance": {
        "Dimension": ["unifiedPagepathScreen", "userAgeBracket", "userGender"],
        "Metric": metric
    },
    "pagePathAndOs": {
        "Dimension": ["unifiedPagepathScreen", "operatingSystem"],
        "Metric": metric
    },
    "pagePathAndBrowser": {
        "Dimension": ["unifiedPagepathScreen", "browser"],
        "Metric": metric
    },  
    "pagePathAge": {
        "Dimension": ["unifiedPagepathScreen", "userAgeBracket"],
        "Metric": metric
    }, 
    "pagePathgender": {
        "Dimension": ["unifiedPagepathScreen", "userGender"],
        "Metric": metric
    }, 
    "sourcemedium" : {
        "Dimension": ["unifiedPagepathScreen", "sessionDefaultChannelGroup","sessionSourceMedium"],
        "Metric": metric
    },
    # "PathAndUnifiedScreenSessionMedium": {
    #     "Dimension": ["unifiedPagepathScreen", "sessionMedium"],
    #     "Metric": metric
    # },
    # "PathAndUnifiedScreenSessionSource": {
    #     "Dimension": ["unifiedPagepathScreen", "sessionSource"],
    #     "Metric": metric
    # },
    "PathpathDevice": {
        "Dimension": ["unifiedPagepathScreen", "deviceCategory"],
        "Metric": metric
    }
}

def getData(request: RunReportRequest, a:str):
    """
    Fetch data from Google Analytics and return a Pandas DataFrame.
    """
    response = client.run_report(request, timeout=120.0) 
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

    print(f"Data for event: {a}")

def tolake(config):
    '''
    This function takes the config dictionary, ingestion the data from the GA4 and writes the data to the datalake.
    '''
    dfs = []
    for a, b in event_configs.items():
        print(F"Procession for the event {a}")
        dimension = ["date"] + b["Dimension"]
        metric = b["Metric"]

        dim_objects = [Dimension(name=d) for d in dimension]
        metric_objects = [Metric(name=m) for m in metric]

        request = RunReportRequest(
            property=property_id,
            dimensions=dim_objects,
            metrics=metric_objects,
            date_ranges=daterange
        )

        data = getData(request=request, a=a)

        time.sleep(1)

        if not data.empty:
            df = spark.createDataFrame(data)
        else:
            continue
        
        time.sleep(2)

        trackAndTrace(
            df, destinatation_Schema=datalake, subfolder=subfolder, filename=a, PKColumns=dimension, spark=spark, dbutils=dbutils
        )

        time.sleep(1)

    return None

def createDemographic(df, age, gender):
    df = df.withColumn(
        "id_demographic", 
        F.hash(
            F.concat(
                F.col("unifiedPagepathScreen"), 
                F.lit("_"), 
                F.col("userAgeBracket"), 
                F.lit("_"), F.col("userGender")
            )
        )
    ).drop("property")


    df1  = df.select(
        "date", "totalUsers", "eventCount", "activeUsers", "screenPageViews", 
        "userEngagementDuration", "bounceRate", "engagementRate", "averageSessionDuration", 
        "id_demographic"
    ).distinct()

    df2 = df.select(
        "id_demographic","unifiedPagepathScreen", "userAgeBracket", "userGender"
    ).distinct()

    df2.write.mode(
        "overwrite"
    ).option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.demographic_dim"
    )

    df1.write.mode(
        "overwrite"
    ).option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.demographic_metrics"
    )

    age.write.mode(
        "overwrite"
    ).option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.age"
    )

    gender.write.mode(
        "overwrite"
    ).option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.gender"
    )

    return None

def tech(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{table}")
    print(f"The table {table} has been written to {catalog}")

    return None

def SourMedium(medium, source):
    mdedium = medium.withColumnRenamed(
        "sessionMedium", "sessionMedium-Source"
    ).withColumn(
        "Name", F.lit("sessionMedium")
    )

    source = source.withColumnRenamed(
        "sessionSource", "sessionMedium-Source"
    ).withColumn(
        "Name", F.lit("sessionSource")
    ).drop("property")

    mdedium.unionByName(
        source, allowMissingColumns=True
    ).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.source_mdedium"
    )

def main():
    '''
    '''
    tolake(config=event_configs)

    df = spark.read.parquet(pathaudiance).drop("property").filter(~filterjourney)
    medium = spark.read.parquet(pathmedium).drop("property").filter(~filterjourney)
    source = spark.read.parquet(pathsource).drop("property").filter(~filterjourney)
    pagepath = spark.read.parquet(pathpage).drop("property").filter(~filterjourney)
    age = spark.read.parquet(pathage).drop("property").filter(~filterjourney)
    gender = spark.read.parquet(pathgender).drop("property").filter(~filterjourney)
    os = spark.read.parquet(pathos).drop("property").filter(~filterjourney)
    browser = spark.read.parquet(pathbrowser).drop("property").filter(~filterjourney)
    device = spark.read.parquet(pathdevice).drop("property").filter(~filterjourney)

    pagepath.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.pagepath"
    )

    sourcemedium = spark.read.parquet(filesourcemedium).withColumns(
    {
        "sessionSource": F.split("sessionSourceMedium", "/")[0],
        "sessionMedium": F.split("sessionSourceMedium", "/")[1],
    }

    ).drop("sessionSourceMedium", "property").filter(~filterjourney).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.pageandsource_new"
    )

    createDemographic(df, age, gender)
    # SourMedium(medium, source)
    tech(df=browser, table="os")
    tech(df=device, table="device")
    tech(df=os, table="browser")

    return None

if __name__ == "__main__":
    main()