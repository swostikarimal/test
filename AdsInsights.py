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

columns = ["campaign_id", "campaign_name", "ad_id", "ad_name", "adset_name", "impressions", "reach", "cpc", "frequency", "ctr", "spend"]

collist = [
    "actions", "action_values", "purchase_roas", "outbound_clicks_ctr", 
    "outbound_clicks", "cost_per_unique_outbound_click", 
    "cost_per_unique_action_type", "cost_per_ad_click", "cost_per_thruplay"
]

fields = "campaign_id, campaign_name,ad_id, ad_name, adset_name, spend, impressions, reach, cpc, frequency, ctr,  action_values, actions, purchase_roas, outbound_clicks_ctr, outbound_clicks, cost_per_unique_outbound_click, cost_per_unique_action_type, cost_per_ad_click, cost_per_thruplay" 

config = {
    "position": {
        "datalake": "/mnt/meta/platformAndPosition/", 
        "subfolder": "position",
        "position": ["platform", "platform_position"],
        "subfolder_action": "position/positionactions",
        'pkdimension':["date", "ad_id", "platform", "platform_position"],
        'pkactions': pkactions + ["platform", "platform_position"],
        'colsdim': pkdim + ["platform", "platform_position"], #+ ["campaign_name"],
        'colsactions': pkval + ["platform", "platform_position"],
        'breakdowns': ["publisher_platform, platform_position"],
        'selectcols': columns + ["publisher_platform", "platform_position"]
    },
    # "age_gender": {
    #     "datalake": "/mnt/meta/platformAndPosition2/",
    #     "subfolder": "age_gender",
    #     "position": ["gender", "age"],
    #     "subfolder_action": "age_gender/actionsandvalues",
    #     'pkdimension': ["date", "ad_id", "gender", "age"],
    #     'pkactions': pkactions + ["gender", "age"],
    #     'colsdim': pkdim + ["gender", "age"],
    #     'colsactions': pkval + ["gender", "age"],
    #     'breakdowns': ["gender, age"],
    #     'selectcols': columns + ["gender", "age"]
    # }
}

collist = ["actions","action_values", "purchase_roas", "outbound_clicks_ctr", 
        "outbound_clicks", "cost_per_unique_outbound_click", 
        "cost_per_unique_action_type", "cost_per_ad_click", "cost_per_thruplay"
]

def getFrames(df1, selectcols, date, colname, concat=True):
    '''
    '''
    newlist = []
    for row in df1[selectcols].itertuples(index=False):
        (cid, campaign_name, ad_id, ad_name, adset_name, impressions, reach, cpc, frequency, ctr, spend, *optional, roas_list) = row

        platform = optional[0] if len(optional) > 0 else None
        platform_position = optional[1] if len(optional) > 1 else None

        for roas_dict in roas_list:
            roas_dict.update({
                "date": date,
                "campaign_id": str(cid),
                "campaign_name": campaign_name,
                "ad_id": str(ad_id),
                "ad_name": ad_name,
                "adset_name": adset_name,
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
        # if concat:
        #     sdf= sdf.withColumn("campaign_name", F.concat(F.col("campaign_name"), F.lit("-"), F.col("ad_name"), F.lit("-"), F.col("adset_name")).alias("abc")).drop("campaign_name")

    return sdf

def main():
    typelevel = "ad"
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")
    url = f"https://graph.facebook.com/v23.0/{account_id}/insights"

    # Define date range for January 2025
    start_date = pd.to_datetime('2025-01-01')
    end_date = pd.to_datetime('2025-01-31')
    chunk_size = 5  # days per API call

    activecampaign_df = spark.table(pathstatus).filter((F.col("Status") == "ACTIVE"))

    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + pd.Timedelta(days=chunk_size-1), end_date)

        print(f"Processing data from {current_start.date()} to {current_end.date()}")

        activecampaign = activecampaign_df.filter(
            (F.col("CreatedDate") <= current_end.strftime('%Y-%m-%d'))
        ).select("AdsetId").distinct().collect()

        focuseds = [cam[0] for cam in activecampaign]

        for focused in focuseds:
            print(f"Processing adset id {focused} for dates {current_start.date()} to {current_end.date()}")

            filtering = {
                "field": "adset.id",
                "value": [str(focused)]
            }

            try:
                metaIngestion.getADSInsights(
                    pastdate=current_start.strftime('%Y-%m-%d'),
                    access_token=access_token,
                    url=url,
                    fields=fields,
                    config=config,
                    isad=True,
                    myaccount=None,
                    spark=spark,
                    dbutils=dbutils,
                    typelevel=typelevel,
                    collist=collist,
                    columns=columns,
                    getFrames=getFrames,
                    lfilter=filtering
                )
            except Exception as e:
                print(f"{e}, data for campaign {focused} not available for {current_start.date()}, continuing.")
                continue

        # Move to next chunk
        current_start = current_end + pd.Timedelta(days=1)
        time.sleep(4)  # avoid hitting API rate limits

    return None

if __name__ == "__main__":
    main()


# COMMAND ----------

# COMMAND ----------

