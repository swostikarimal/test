# Databricks notebook source
import requests
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adreportrun import AdReportRun
import pandas as pd
import time
from datetime import datetime
from TrackAndUpdate import trackAndTrace 
import warnings

warnings.filterwarnings("ignore")

datalake = "/mnt/meta/adset/"
subfolder = "adset_campaign"

pkcolumns = [
    'date_start',"campaign_id", "ad_id", "ad_name", "adset_id", "adset_name"
]

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
    '''
    '''
    print(f"procession for {pastdate}")
    ad = my_account.get_insights(
        params={
            "time_range": f"{{'since': '{pastdate}', 'until': '{pastdate}'}}",
            'period': 'day',
            'level': 'ad', 
        },
        fields=adfields
    )
    sdf = spark.createDataFrame(pd.DataFrame(ad))

    trackAndTrace(
        df=sdf.coalesce(1), destinatation_Schema=datalake, subfolder=subfolder,
        filename="campaign_adset", PKColumns=pkcolumns, spark=spark, dbutils=dbutils
    )
    
    return None

def getObjective(my_account):
    '''
    '''
    ad_sets = my_account.get_ad_sets(fields=[
        'id',
        'name',
        'campaign_id',
        'optimization_goal',
        'destination_type',
    ])

    df = spark.createDataFrame(pd.DataFrame(ad_sets))

    trackAndTrace(
        df=df.coalesce(1), destinatation_Schema=datalake, subfolder="adsetobjective",
        filename="objective", PKColumns=["campaign_id", 'id'], spark=spark, dbutils=dbutils
    )

    return None

def main():
    '''
    '''
    app_id = '1311670110282149'
    app_secret = '4d00d38b2f2abd468cb952725c6ad914'
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")

    FacebookAdsApi.init(app_id, app_secret, access_token)
    my_account = AdAccount(account_id)

    today = datetime.now()
    upto  = (today - pd.Timedelta(days = 10)).strftime('%Y-%m-%d')
    pastdate = (today - pd.Timedelta(days = 10)).strftime('%Y-%m-%d')

    while pastdate <= upto:
        getAdInsights(my_account, pastdate)
        pastdate = (pd.to_datetime(pastdate) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        time.sleep(10)
    try:
        getObjective(my_account)
    except Exception as e:
        print(e)

    return None

if __name__ == "__main__":
    main()

# COMMAND ----------

