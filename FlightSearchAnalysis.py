# Databricks notebook source
'''
Data ingestion pipeline to ingest data for the fight search analysis

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
from TrackAndUpdate import trackAndTrace

lakepath = "/mnt/ga/"
subfolder = "reporting/flightsearch"
schema = "analyticadebuddha.searchandrouteanalysis"
schema1 = "analyticadebuddha.google"
property_id = "properties/347471683"
routetable  = "pathandquery"

filterflightsearch = (F.col("customEvent:previous_url") == "https://www.buddhaair.com/")
filterflightroute = (F.col("customEvent:previous_url").contains("flight-routes/"))

credentials = service_account.Credentials.from_service_account_file('/dbfs/FileStore/tables/buddhatech.json')
client = BetaAnalyticsDataClient(credentials=credentials)

metrics = [
    Metric(name="totalUsers"), Metric(name="eventCount"), Metric(name="activeUsers"), 
    Metric(name="newUsers"), Metric(name="sessions"), Metric(name="userEngagementDuration"), 
    Metric(name="averageSessionDuration")
]

eventconfig = {
    "flightsearch": {
        "dimension": ["date", "eventName", "customEvent:previous_url"],
    },
    "flightsearch_audience": {
        "dimension": ["date", "eventName", "customEvent:previous_url", "userAgeBracket", "userGender"]
    },
    "flightsearch_age": {
        "dimension": ["date", "eventName", "customEvent:previous_url", "userAgeBracket"]
    },
    "flightsearch_gender": {
        "dimension": ["date", "eventName", "customEvent:previous_url", "userGender"]
    }
}

def flightSearchAnalysis(client, df, filename, dimension):
    """
    Processes user acquisition data for specified metrics and writes to the data lake.
    """
    spark_df = spark.createDataFrame(df).filter(
        (F.col("eventName").contains("flight_searched"))
    ).withColumn(
            "eventCountPerActiveUser", F.col("eventCount") / F.col("activeUsers")
    ).withColumn(
        "returningUser", F.col("totalUsers") - F.col("newUsers")
    )

    trackAndTrace(
        df=spark_df, destinatation_Schema=lakepath, subfolder=subfolder, filename=filename, PKColumns=dimension, spark=spark, dbutils=dbutils
    )

def fetch_ga_data(client, property_id, eventconfig) -> pd.DataFrame:
    """
    Fetches data from Google Analytics for a specific property and dimension.
    """
    daterange = [DateRange(start_date="15daysAgo", end_date="today")]
    for filename, dim in eventconfig.items():
        dimension  = dim.get("dimension")
        dim_obj = [Dimension(name=d) for d in dimension]

        request = RunReportRequest(
            property=property_id,
            dimensions=dim_obj,
            metrics=metrics, 
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

        flightSearchAnalysis(client, df, filename, dimension)

    return None

def flightRoute(route, tablename):
    """
    """
    flightsearch = route.withColumn(
        "Sector", F.split(
            F.col("pagePathPlusQueryString"), "/"
        ).getItem(2)
    ).withColumn(
        "Sector", F.when(
            F.col("Sector").contains("?"), 
            F.split(
                F.split(F.col("Sector"), "\?").getItem(0), "-"
            )
        ).otherwise(
            F.split(F.col("Sector"), "-")
        )
    ).withColumn(
        "Sectors", F.when(
                F.col("pagePathPlusQueryString").contains("?"),
                F.when(
                    ~F.col("pagePathPlusQueryString").contains("flights-from"),
                    F.concat(
                        F.col("Sector").getItem(0), F.lit("-"), F.col("Sector").getItem(3)
                    )
                ).otherwise(
                    F.when(
                        F.col("pagePathPlusQueryString").contains("flights-from"),
                        F.concat(
                            F.col("Sector").getItem(2), F.lit("-"), F.col("Sector").getItem(4)
                        )
                    )
                )
        ).otherwise(
            F.when(
                F.col("pagePathPlusQueryString").contains("flights-to")
                |~ F.col("pagePathPlusQueryString").contains("flights-from"),
                F.concat(
                    F.col("Sector").getItem(0), F.lit("-"), F.col("Sector").getItem(2),
                )
            ).otherwise(
                F.concat(
                    F.col("Sector").getItem(2), F.lit("-"), F.col("Sector").getItem(4)
                )
            )
        )
    ).filter(
        F.col("Sectors").isNotNull()).drop(*["pagePathPlusQueryString", "property", "Sector"]
    ).withColumn("Sectors", F.lower(F.trim("Sectors")))

    flightsearch.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{schema}.{tablename}")

    return None

def flightrouteAnalysis(df,schema, table):
    df = df.filter(filterflightroute).withColumn(
        "Sector",
            F.split(
            "customEvent:previous_url", "/"
        ).getItem(4)
    ).withColumn(
        "Sector", F.split(
            F.when(
            ~F.col("Sector").startswith("flights-from-"), F.concat(F.lit("flights-from-"), F.col("Sector"))
        ).otherwise(F.col("Sector")), "-"
        )
    ).withColumn(
        "Sector", F.split(
            F.concat(
                F.col("Sector").getItem(2), F.lit("-"), F.col("Sector").getItem(4)
            ), "\?"
        ).getItem(0)
    ).drop("customEvent:previous_url").withColumn("Sector", F.lower(F.trim("Sector")))

    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{schema}.{table}")

    return None

def main():
    """
    Main function to fetch and write Google Analytics data.
    """
    fetch_ga_data(client, property_id, eventconfig)

    flightsearch_ = spark.read.parquet(
        "/mnt/ga/reporting/flightsearch/flightsearch"
    ).withColumn(
        "date", F.date_format("date", "yyyy-MM-dd")
    ).drop("property")

    route = spark.read.parquet("/mnt/ga/reporting/CTA/pathandstringquery").filter(
        (
            F.col("pagePathPlusQueryString").startswith("/flight-routes/")
        ) & ~(
            F.col("pagePathPlusQueryString").contains("/flight-routes/flight-routes?_x_tr_sl")
        )
    )

    # flightsearch_aud = spark.read.parquet(
    #     "/mnt/ga/reporting/flightsearch/flightsearch_audience"
    # ).withColumn(
    #     "date", F.date_format("date", "yyyy-MM-dd")
    # ).drop("property")

    # flightsearch_age = spark.read.parquet(
    #     "/mnt/ga/reporting/flightsearch/flightsearch_age"
    # ).withColumn(
    #     "date", F.date_format("date", "yyyy-MM-dd")
    # ).drop("property")

    # flightsearch_gender = spark.read.parquet(
    #     "/mnt/ga/reporting/flightsearch/flightsearch_gender"
    # ).withColumn(
    #     "date", F.date_format("date", "yyyy-MM-dd")
    # ).drop("property")

    flightsearch_.filter(
        filterflightsearch
    ).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{schema1}.flightsearchanalysis"
    )

    flightRoute(route=route, tablename=routetable)

    flightrouteAnalysis(
        df=flightsearch_, schema=schema, table="flightrouteanalysis"
    )
    # flightrouteAnalysis(
    #     df=flightsearch_aud, schema=schema, table="flightrouteanalysis_audience"
    # )
    # flightrouteAnalysis(
    #     df=flightsearch_age, schema=schema, table="flightrouteanalysis_age"
    # )
    # flightrouteAnalysis(
    #     df=flightsearch_gender, schema=schema, table="flightrouteanalysis_gender"
    # )


if __name__ == "__main__":
    main()