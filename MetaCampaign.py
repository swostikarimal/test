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
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace 
import warnings

warnings.filterwarnings("ignore")

datalake = "/mnt/meta/adset/budget"
subfolder_cmp = "/campaign"
subfolder_adset = "/adset"
pkcolumns = ["id"]

cols = [
    F.col("id").alias("CampaignId"), F.col("name").alias("CampaignName"),F.date_format("created_time", "yyyy-MM-dd").alias("CreatedDate"), 
    F.date_format("created_time", "hh:mm").alias("CreatedTime"), F.date_format("stop_time", "yyyy-MM-dd").alias("StoppedAt"),  
    F.date_format("stop_time", "hh:mm").alias("StoppedTime"), F.col("status").alias("CampaignStatus"), F.col("objective"), 
    F.col("daily_budget").alias("dailyBudget"), F.col("lifetime_budget").alias("lifetimeBudget"), F.col("bid_strategy").alias("BidStrategy")
]
adsetbudgetcol = [
    F.col("id").alias("Adsetid"), F.col("campaign_id").alias("CampaignId"),F.col("name").alias("CampaignName"), F.col("daily_budget").alias("dailyBudget"), 
    F.col("lifetime_budget").alias("lifetimeBudget"), F.col("effective_status").alias("Status"), F.col("start_time").alias("StartDate"), 
    F.col("end_time").alias("EndDate")
]
adcol = [F.col("id").alias("AdId"), F.col("campaign_id").alias("CampaignId"), F.col("effective_status").alias("Status")]

fields = [
    Campaign.Field.id,
    Campaign.Field.name,
    Campaign.Field.status,
    Campaign.Field.objective,
    Campaign.Field.daily_budget,
    Campaign.Field.lifetime_budget,
    Campaign.Field.start_time,
    Campaign.Field.stop_time,
    Campaign.Field.created_time,
    Campaign.Field.bid_strategy,
    Campaign.Field.recommendations,
    Campaign.Field.source_recommendation_type
]

def getAdsetBudget(account_id, access_token, url, params, ad=False):
    '''
    '''
    response = requests.get(url=url, params=params)
    data = response.json().get("data")
    sdf = spark.createDataFrame(pd.DataFrame(data))

    if ad:
        sdf = sdf.select(*adcol).write.mode("overwrite").option(
             "overwriteSchema", "true"
        ).saveAsTable(
            "analyticadebuddha.metaads.adstatus"
        )
    else:
        trackAndTrace(
            df=sdf.coalesce(1), destinatation_Schema=datalake, subfolder=subfolder_adset,
            filename="adset_budget", PKColumns=pkcolumns, spark=spark, dbutils=dbutils
        )

        spark.read.parquet(datalake + subfolder_adset + "/adset_budget").select(*adsetbudgetcol).write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(
            "analyticadebuddha.metaads.adsetBudget"
        )

    return None

def campaignsInfo(my_account):
    '''
    '''
    campaigns = my_account.get_campaigns(fields=fields)

    df2 = pd.DataFrame(campaigns)
    sdf = spark.createDataFrame(df2)

    trackAndTrace(
        df=sdf.coalesce(1), destinatation_Schema=datalake, subfolder=subfolder_cmp,
        filename="campaign_adset", PKColumns=pkcolumns, spark=spark, dbutils=dbutils
    )

    time.sleep(5)

    campaiginfo = spark.read.parquet(datalake + subfolder_cmp + "/campaign_adset").select(*cols)

    campaiginfo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.metaads.campaignInfo")

    return None

def customEventName(url_custom, access_token):
    '''
    '''
    params = {
        "fields": "id,name,custom_event_type,account_id,rule,data_sources, custom_data",
        "access_token": access_token,
    }
    response1 = requests.get(url_custom, params=params)
    event = response1.json().get("data")
    eventname = spark.createDataFrame(event)

    eventname.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.metaads.eventName")

    return None

def main():
    '''
    '''
    api_version = "v23.0"
    app_id = '1311670110282149'
    app_secret = '4d00d38b2f2abd468cb952725c6ad914'

    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")

    FacebookAdsApi.init(app_id, app_secret, access_token)
    my_account = AdAccount(account_id)

    url_custom = f"https://graph.facebook.com/{api_version}/{account_id}/customconversions"
    adsetbudget = f"https://graph.facebook.com/{api_version}/{account_id}/adsets"
    adstatus = f"https://graph.facebook.com/{api_version}/{account_id}/ads"

    
    adsetparmas = { 
            "access_token": access_token, 
            "fields": "id,name, daily_budget, lifetime_budget, spend, effective_status,campaign_id, start_time, end_time",
            "level": "ad",
            "limit": 1500
    }

    adparams = { 
            "access_token": access_token, 
            "fields": "id, campaign_id, effective_status",
            "level": "ad",
            "limit": 1500
    }

    getAdsetBudget(account_id, access_token, url=adsetbudget, params=adsetparmas)
    getAdsetBudget(account_id, access_token, url=adstatus, params=adparams, ad=True)
    campaignsInfo(my_account)
    customEventName(url_custom, access_token)

    return None

if __name__ == "__main__":
    main()