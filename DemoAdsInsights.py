# Databricks notebook source
import requests
import json
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adreportrun import AdReportRun
from TrackAndUpdate import trackAndTrace 
import pyspark.sql.functions as F
import metaIngestion

import pandas as pd
import time
import warnings
warnings.filterwarnings("ignore")

pathstatus = "analyticadebuddha.metaadtype.statusandobjective"

pkactions = ["date", "ad_id", "action_type"]
pkdim = [F.col("date_start").alias("date"), "ad_id", "reach", F.col("impressions").alias("impression"), "cpc", "frequency", "ctr", "spend"]
pkval = ["date", "ad_id", "action_type", "value"]

columns = ["campaign_id", "campaign_name", "ad_id", "impressions", "reach", "cpc", "frequency", "ctr", "spend"]

collist = [
    "actions", "action_values", "purchase_roas", "outbound_clicks_ctr", 
    "outbound_clicks", "cost_per_unique_outbound_click", 
    "cost_per_unique_action_type", "cost_per_ad_click", "cost_per_thruplay"
]

fields = "campaign_id, campaign_name,ad_id,spend, impressions, reach, cpc, frequency, ctr,  action_values, actions, purchase_roas, outbound_clicks_ctr, outbound_clicks, cost_per_unique_outbound_click, cost_per_unique_action_type, cost_per_ad_click, cost_per_thruplay"

config = {
    # "position": {
    #     "datalake": "/mnt/meta/platformAndPosition/",
    #     "subfolder": "position",
    #     "position": ["platform", "platform_position"],
    #     "subfolder_action": "position/positionactions",
    #     'pkdimension':["date", "ad_id", "platform", "platform_position"],
    #     'pkactions': pkactions + ["platform", "platform_position"],
    #     'colsdim': pkdim + ["platform", "platform_position"],
    #     'colsactions': pkval + ["platform", "platform_position"],
    #     'breakdowns': ["publisher_platform, platform_position"],
    #     'selectcols': columns + ["publisher_platform", "platform_position"]
    # },
    "age_gender": {
        "datalake": "/mnt/meta/platformAndPosition2/",
        "subfolder": "age_gender",
        "position": ["gender", "age"],
        "subfolder_action": "age_gender/actionsandvalues",
        'pkdimension': ["date", "ad_id", "gender", "age"],
        'pkactions': pkactions + ["gender", "age"],
        'colsdim': pkdim + ["gender", "age"],
        'colsactions': pkval + ["gender", "age"],
        'breakdowns': ["gender, age"],
        'selectcols': columns + ["gender", "age"]
    }
}

collist = ["actions","action_values", "purchase_roas", "outbound_clicks_ctr", 
        "outbound_clicks", "cost_per_unique_outbound_click", 
        "cost_per_unique_action_type", "cost_per_ad_click", "cost_per_thruplay"
]

def getFrames(df1, selectcols, date, colname):
    '''
    '''
    newlist = []
    for row in df1[selectcols].itertuples(index=False):
        (cid, campaign_name, ad_id, impressions, reach, cpc, frequency, ctr, spend, *optional, roas_list) = row

        platform = optional[0] if len(optional) > 0 else None
        platform_position = optional[1] if len(optional) > 1 else None

        for roas_dict in roas_list:
            roas_dict.update({
                "date": date,
                "campaign_id": str(cid),
                "campaign_name": campaign_name,
                "ad_id": str(ad_id),
                "impression": impressions,
                "reach": reach,
                "cpc": cpc,
                "frequency": frequency,
                "ctr": ctr,
                "spend": spend,
            })

            if platform is not None:
                roas_dict[colname[0]] = platform
            if platform_position is not None:
                roas_dict[colname[1]] = platform_position
            
            newlist.append(roas_dict)

        sdf = spark.createDataFrame(pd.DataFrame(newlist))

    return sdf

def main():
    '''
    '''
    typelevel = "ad"
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")
    url = f"https://graph.facebook.com/v18.0/{account_id}/insights"

    today = datetime.now()
    upto = (today - pd.Timedelta(days = 1)).strftime('%Y-%m-%d')
    pastdate =  (today - pd.Timedelta(days = 2)).strftime('%Y-%m-%d')

    activecampaign_df = spark.table(pathstatus).filter((F.col("Status") == "ACTIVE"))

    while pastdate <= upto:
        '''
        '''
        activecampaign = activecampaign_df.filter(
            (F.col("CreatedDate") <= pastdate)
        ).select("CampaignId").distinct().collect()

        focuseds = [cam[0] for cam in activecampaign]

        for focused in focuseds:
            print(f"procession for the campaign id {focused}")
            filtering = {
                "field": "campaign.id",
                "value" : [focused]
            }
            print(f"Procession for {pastdate}")
            
            try:
                metaIngestion.getADSInsights(
                    pastdate, access_token, url, fields=fields, config=config, isad=True, myaccount=None, spark=spark, 
                    dbutils=dbutils, typelevel=typelevel, collist=collist, columns=columns, getFrames=getFrames, lfilter=filtering
                )
            except Exception as e:
                print(f"{e}, the data for the camapign {focused} is not available for the {pastdate} and continuing forward to other campaigns")
                continue

        pastdate = (pd.to_datetime(pastdate) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        time.sleep(4)

    return None

if __name__ == "__main__":
    main()