# Databricks notebook source
import pyspark.sql.functions as F
import requests
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adreportrun import AdReportRun
from TrackAndUpdate import trackAndTrace
import metaIngestion
import pyspark.sql.functions as F

import pandas as pd
import time
import warnings

warnings.filterwarnings("ignore")


pkactions = ["date", "campaign_id", "action_type"]
pkdim = [F.col("date_start").alias("date"), "campaign_id", "estimated_ad_recallers","estimated_ad_recall_rate"]
pkval = ["date", "campaign_id", "action_type", "value"]
columns = ["campaign_id", "estimated_ad_recallers","estimated_ad_recall_rate"]
pathstatus = "analyticadebuddha.metaadtype.statusandobjective"

config = {
    "position": {
        "datalake": "/mnt/meta/platformAndPosition/",
        "subfolder": "position_estimated",
        "position": ["platform", "platform_position"],
        "subfolder_action": "position_estimated/positionactions",
        'pkdimension':["date", "campaign_id","platform_position","platform"],
        'pkactions': pkactions + ["platform", "platform_position"],
        'colsdim':   pkdim + ["platform", "platform_position"],
        'colsactions': pkval + ["platform", "platform_position"],
        'breakdowns': "publisher_platform, platform_position",
        'selectcols': columns + ["publisher_platform", "platform_position"]
    },

    "genderAndAge": {
        "datalake": "/mnt/meta/platformAndPosition/",
        "subfolder": "position_estimated",
        "position": ["age", "gender"],
        "subfolder_action": "position_estimated/demoactions",
        'pkdimension':["date", "campaign_id","age", "gender"],
        'pkactions': pkactions + ["age", "gender"],
        'colsdim':   pkdim + ["age", "gender"],
        'colsactions': pkval + ["age", "gender"],
        'breakdowns': "age, gender",
        'selectcols': columns + ["age", "gender"]
    }
}

collist = ["video_p25_watched_actions","video_p50_watched_actions","video_p75_watched_actions","video_p95_watched_actions"]

fields = [
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.estimated_ad_recallers,
    AdsInsights.Field.estimated_ad_recall_rate,
    AdsInsights.Field.video_p25_watched_actions,
    AdsInsights.Field.video_p50_watched_actions,
    AdsInsights.Field.video_p75_watched_actions,
    AdsInsights.Field.video_p95_watched_actions
]

def getFrames(df1, selectcols, date, colname):
    '''
    '''
    newlist = []
    for row in df1[selectcols].itertuples(index=False):
        (cid, estimated_ad_recallers, estimated_ad_recall_rate, *optional, roas_list) = row

        platform = optional[0] if len(optional) > 0 else None
        platform_position = optional[1] if len(optional) > 1 else None
        

        for roas_dict in roas_list:
            roas_dict.update({
                "date": date,
                "campaign_id": str(cid),
                "estimated_ad_recallers": estimated_ad_recallers,
                "estimated_ad_recall_rate": estimated_ad_recall_rate,
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
    typelevel ="campaign"
    app_id = '1311670110282149'
    app_secret = '4d00d38b2f2abd468cb952725c6ad914'
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")

    FacebookAdsApi.init(app_id, app_secret, access_token)
    my_account = AdAccount(account_id)

    activecampaign_df = spark.table(pathstatus).filter((F.col("Status") == "ACTIVE"))

    url = f"https://graph.facebook.com/v22.0/{account_id}/insights"

    today = datetime.now()
    upto = (today - pd.Timedelta(days = 1)).strftime('%Y-%m-%d') 
    pastdate =  (today - pd.Timedelta(days = 2)).strftime('%Y-%m-%d')

    while pastdate <= upto:
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
                    pastdate, access_token, url, fields=fields, config=config, isad=False, myaccount=my_account, spark=spark, 
                    dbutils=dbutils, typelevel=typelevel, collist=collist, columns=columns, getFrames=getFrames, lfilter=filtering
                )
            except Exception as e:
                print(f"{e}, the data for the camapign {focused} is not available for the {pastdate} and continuing forward to other campaigns")
                continue
        
        pastdate = (pd.to_datetime(pastdate) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        time.sleep(3)

    return None

if __name__ == "__main__":
    main()