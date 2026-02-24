# Databricks notebook source
'''
Dat Ingestion logic for CTA data ingestion and creation

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
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace 
import pandas as pd
import time


properties = spark.table("analyticadebuddha.default.gainfo")

property_id = properties.select("property_id").collect()[0][0]
file = properties.select("filelocation").collect()[0][0]

credentials = service_account.Credentials.from_service_account_file(file)
client = BetaAnalyticsDataClient(credentials=credentials)


catalog = "analyticadebuddha.google"

pathsearchclick = "/mnt/ga/reporting/CTA/searchpanel_click"
pathroyalclub = "/mnt/ga/reporting/CTA/royalclub_buttons"
pathselectpromotion = "/mnt/ga/reporting/CTA/select_promotion"
pathhomepageslider = "/mnt/ga/reporting/CTA/homepageslider"
pathflightstatussearched = "/mnt/ga/reporting/CTA/flight_status_searched"
pathblogclicked = "/mnt/ga/reporting/CTA/blog_clicked"
pathnavigationclicked = "/mnt/ga/reporting/CTA/navigation_clicked"
pathlinkclicked = "/mnt/ga/reporting/CTA/link_click"

filterpromotion = (F.trim("customEvent:previous_url") == "https://www.buddhaair.com/")
otherfilter = F.col("customEvent:custom_title").contains("Homepage")


string = ["totalUsers", "eventCount","screenPageViews", "screenPageViewsPerUser", "sessions", "userEngagementDuration", "averageSessionDuration", "engagementRate"]

casetitlefirst = (F.col("Title1").isNull() & F.col("Title2").isNotNull())
casetitles = F.when(
    casetitlefirst,
    F.when(
        F.lower(F.trim("Title")) == F.lower(F.trim("Title2")), None
    ).otherwise(F.col("Title2"))
).otherwise(F.col("Title1"))

datalake ="/mnt/ga/reporting/"  
subfolder = "CTA"

metric = ["eventCount", "totalUsers", "newUsers", "sessions","userEngagementDuration", "engagementRate", "averageSessionDuration"]

case1 = F.when(
    (
        F.trim(F.col("customEvent:click_url")).contains(F.trim("URL"))
    ) & (
        F.trim(F.col("customEvent:click_url")) != F.trim("URL")
    ), True
).otherwise(False)

case2 = F.when(
        F.col("SecondaryFooter") == True,  F.trim("customEvent:click_text")
)

case3 = F.when(
    F.col("Footer2").isNotNull(), F.expr("split(clickurl, '/')[size(split(clickurl, '/')) - 1]")
)

FILTERDUP = ((F.trim("Title") == "About Us") & (F.trim("clickUrl") == "https://www.buddhaair.com/about-us/catalogue/cargo"))


footercols = [
    F.date_format("date", "y-MM-dd").alias("date"), "eventName", "Footer", F.col("ButtonLink").alias("Title"), F.initcap("ButtonLink2").alias("Title1"), 
    F.col("customEvent:click_text").alias("Title2"), "clickUrl", "eventCount", "totalUsers", "newUsers", "engagementRate", "averageSessionDuration", 
    "averageEngagenmentTimepPerSession"
]

casefottersame = F.when(
    F.trim("Title") == F.trim("Title2"), None
).otherwise(F.col("Title2"))

event_configs = {
    "link_click": {
        "Dimension": ["eventName", "customEvent:custom_title","pageLocation","customEvent:click_text", "customEvent:click_url"],
        "Metric": metric
    },
    "link_click_main": {
        "Dimension": ["eventName", "customEvent:click_text"],
        "Metric": metric
    },
    "searchpanel_click": {
        "Dimension": ["eventName", "customEvent:click_text"],
        "Metric": metric
    },
    "royalclub_buttons": {
        "Dimension": ["eventName", "customEvent:click_text"], #"pageLocation","customEvent:previous_url"
        "Metric": metric
    },
    "select_promotion": {
        "Dimension": ["eventName", "itemName", "customEvent:previous_url"], #"customEvent:previous_url"
        "Metric": ["itemsClickedInPromotion","totalUsers", "sessions", "engagementRate"]
    },
    "homepageslider": {
        "Dimension": ["eventName", "customEvent:homepage_slider_imgname", "customEvent:click_url"], #"customEvent:click_url", "customEvent:click_text"
        "Metric": metric
    },
    "flight_status_searched": {
        "Dimension": ["eventName"],
        "Metric": metric
    },
    "navigation_clicked": {
        "Dimension": ["eventName", "customEvent:custom_title", "linkText"], #,"linkUrl", "pageLocation",
        "Metric": metric
    },
    "blog_clicked": {
        "Dimension": ["eventName","linkText"], #"customEvent:custom_title", "linkUrl"
        "Metric": metric
    },

    "pathandstringquery": {
        "Dimension": ["pagePathPlusQueryString"],
        "Metric": string + ["engagedSessions"]
    }
}

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

def time_to_seconds(time_str):
    """Converts a time string in the format 'Xm Ys' to seconds."""
    try:
        minutes_str, seconds_str = time_str.split("m ")
        minutes = int(minutes_str)
        seconds = int(seconds_str[:-1])  
        return (minutes * 60) + seconds
    except (ValueError, AttributeError):
        return None
    
time_to_seconds_udf = udf(time_to_seconds)

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

    if a not in ["link_click_main", "pathandquery", "pathandstringquery"]:
        data = data[data["eventName"] == a]

    data["property"] = property_id  
    data["date"] = pd.to_datetime(data['date'], format='%Y%m%d')

    return data

    print(f"Data for event: {a}")

def tolake(config:dict, daterange:str):
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

        time.sleep(1)

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

        if a != "select_promotion": 
            df = EngagenmentTimepPerSession(df)
        
        time.sleep(2)

        trackAndTrace(
        df, destinatation_Schema=datalake, subfolder=subfolder, filename=a, PKColumns=dimension, spark=spark, dbutils=dbutils
        )

        time.sleep(1)

def uniondf(dfs):
    if len(dfs) >= 2:
        df1 = dfs[0]
        for df in dfs[1:]:
            df1 = df1.unionByName(df, allowMissingColumns=True)
    else: 
        df1 = dfs[0].unionByName(dfs[1])
    
    return df1

def toMart(dictdf:dict):
    '''
    This functions takes the dfs dictionary, consolidates the data and writes to the catalog.
    '''
    for name, dic in dictdf.items():
        if name != "single_event":
            dfs = [df for a, df in dic.items()]
            union = uniondf(dfs)
            union.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{name}")
            print(f"The data for the table {name} has been saved to the table {catalog}.{name}")
        else:
            [
                (
                    print(f"The data for the table {table} has been saved to the table {catalog}.{table}"),
                    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{table}")
                ) for table, df in dic.items()
            ]
    return None

def titelOperation(df):
    df = df.withColumn(
        "Title1", casetitles 
    ).withColumn(
        "Title2", F.when(
            casetitlefirst | (F.trim("Title1") == F.trim("Title2")) , None
        ).otherwise(F.col("Title2"))

    ).withColumn(
        "Title1", F.when(
            F.col("Title1").contains("https://www.buddhaair.com"), None
        ).otherwise(F.col("Title1"))
    )

    df.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.footer")

    return df

def toFooter(df, df1, linkclick):
    df2 = df.alias("df1").join(
        df1.alias("df2"), (
            F.lower(F.trim(F.col("df1.customEvent:click_url"))).contains(F.lower(F.trim("df2.URL")))
    ), how="inner"
    ).select(
        "df1.*", "df2.Footer", F.col("df2.Event").alias("ButtonLink"), "df2.URL"
    ).withColumn(
            "SecondaryFooter", case1
        ).withColumn(
            "Footer2", case2
        ).withColumn("clickurl", F.col("customEvent:click_url")).withColumn(
            "ButtonLink2", case3
    ).select(*footercols).withColumn(
        "Title2", casefottersame
    ).filter(~FILTERDUP)
    
    df2 = df2.drop("averageSessionDuration").withColumn(
        "averageEngagenmentTimepPerSession", time_to_seconds_udf(df2["averageEngagenmentTimepPerSession"])
    )

    df2 = titelOperation(df = df2)

    linkclick.alias("df1").join(
        df2.alias("df2"),
        (F.lower(F.trim("customEvent:click_text")) ==  F.trim(F.lower(F.trim("df2.Title")))), how="inner"
    ).select(
        "df1.date",  "df2.Footer", F.col("df1.customEvent:click_text").alias("Title"), "df1.totalUsers", "df1.eventCount"
    ).dropDuplicates(subset=["date", "Title"]).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{catalog}.FooterMain"
    )

    return print(f"The data for the table footer has been saved to the table {catalog}.footer")

def main():
    """
    """
    startdate = properties.select("startdate").collect()[0][0]
    startdate_ = str(10) + "daysAgo"
    enddate_ = "today"

    print(f"For date range {startdate_} to {enddate_}")
    daterange = [DateRange(start_date=startdate_, end_date=enddate_)]

    tolake(config=event_configs, daterange=daterange)

    time.sleep(5)
    
    path = [
        pathsearchclick, pathroyalclub, pathselectpromotion, pathhomepageslider, 
        pathflightstatussearched, pathblogclicked, pathnavigationclicked, pathlinkclicked
    ]
    
    dfs = [spark.read.parquet(p) for p in path]

    searchclick = dfs[0]
    royalclub = dfs[1]
    selectpromotion = dfs[2]
    homepageslider = dfs[3]
    flightstatussearched = dfs[4]
    blogclicked = dfs[5]
    navigationclicked = dfs[6]
    linkclick = dfs[7]

    dictdf = {
        "single_event" : {
        "flightstatussearched": flightstatussearched,
        "homepageslider": homepageslider,
        "selectpromotion": selectpromotion,
        "linkclick": linkclick
    },
        "royalclub_searchpanel" :{
        "royalclub": royalclub,
        "searchclick": searchclick
    },
        "navigation_blog_click" : {
        "blogclicked":blogclicked,
        "navigationclicked":navigationclicked
    }
    }
    
    toMart(dictdf=dictdf)
    time.sleep(5)

    footer = spark.table("analyticadebuddha.google.linkclick").filter(F.col("eventName").contains("link_click"))
    df1 = spark.table("analyticadebuddha.businesstable.navblog")
    linkclick1 = spark.read.parquet("/mnt/ga/reporting/CTA/link_click_main")

    toFooter(df=footer, df1=df1, linkclick=linkclick1)

    return None

if __name__ == "__main__":
    main()