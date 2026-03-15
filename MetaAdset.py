# Databricks notebook source
import requests
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
import pandas as pd
import time
from datetime import datetime
from TrackAndUpdate import trackAndTrace
import warnings

warnings.filterwarnings("ignore")

# Storage paths
datalake = "/mnt/meta/adset/"
subfolder = "adset_campaign"

# Primary key columns
pkcolumns = [
    'date_start', "campaign_id", "ad_id", "ad_name", "adset_id", "adset_name"
]

# Fields to fetch
adfields = [
    AdsInsights.Field.account_id,
    AdsInsights.Field.account_name,
    AdsInsights.Field.date_start,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.spend,
    AdsInsights.Field.clicks,
    AdsInsights.Field.ctr,
    AdsInsights.Field.frequency,
    AdsInsights.Field.cpc,
    AdsInsights.Field.reach,
    AdsInsights.Field.impressions,
]

api_version = "v18.0"


def getAdInsights(my_account, pastdate):
    """
    Fetch daily ad insights and store in datalake
    """
    print(f"Processing data for {pastdate}")

    ad = my_account.get_insights(
        params={
            "time_range": f"{{'since': '{pastdate}', 'until': '{pastdate}'}}",
            'period': 'day',
            'level': 'ad',
        },
        fields=adfields
    )

    df = pd.DataFrame(ad)

    # Safety: skip if no data
    if df.empty:
        print(f"No data found for {pastdate}")
        return

    sdf = spark.createDataFrame(df)

    trackAndTrace(
        df=sdf.coalesce(1),
        destinatation_Schema=datalake,
        subfolder=subfolder,
        filename="campaign_adset",
        PKColumns=pkcolumns,
        spark=spark,
        dbutils=dbutils
    )


def getObjective(my_account):
    """
    Fetch adset objectives
    """
    ad_sets = my_account.get_ad_sets(fields=[
        'id',
        'name',
        'campaign_id',
        'optimization_goal',
        'destination_type',
    ])

    df = pd.DataFrame(ad_sets)

    if df.empty:
        print("No objective data found")
        return

    sdf = spark.createDataFrame(df)

    trackAndTrace(
        df=sdf.coalesce(1),
        destinatation_Schema=datalake,
        subfolder="adsetobjective",
        filename="objective",
        PKColumns=["campaign_id", 'id'],
        spark=spark,
        dbutils=dbutils
    )


def main():
    """
    Main execution: Fetch January 2025 data
    """

    app_id = '1311670110282149'
    app_secret = '4d00d38b2f2abd468cb952725c6ad914'
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")

    FacebookAdsApi.init(app_id, app_secret, access_token)
    my_account = AdAccount(account_id)

    # ✅ Date range for January 2025
    start_date = pd.to_datetime('2025-01-01')
    end_date = pd.to_datetime('2025-01-31')

    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        try:
            getAdInsights(my_account, date_str)
        except Exception as e:
            print(f"Error on {date_str}: {e}")

        current_date += pd.Timedelta(days=1)
        time.sleep(10)   # prevent API rate limits

    # Fetch objectives after insights
    try:
        getObjective(my_account)
    except Exception as e:
        print(f"Objective fetch error: {e}")


if __name__ == "__main__":
    main()

# COMMAND ----------

