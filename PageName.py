# Databricks notebook source
import requests
import json
import pandas as pd
import pyspark.sql.functions as F

destinationtable = "analyticadebuddha.metaadtype.ad_destination"

cosl = [
    F.col("Campaign_id").alias("CampaignId"), F.col("adset_id").alias("AdSetId"), 
    F.col("id").alias("AdId"), "Name", "UserName"
]

def getDestination(access_token, focused, url):
    '''
    '''
    abc = []
    creative_params = {
        "fields": "id,name,object_story_spec,effective_object_story_id",
        "access_token": access_token
    }

    page_params = {
        "fields": "name,username",
        "access_token": access_token
    }

    params = {
        "fields": "id, name, adset_id, campaign_id, page_id,  creative",
        "access_token": access_token,
        "filtering": json.dumps([{
            "field": "campaign.id",
            "operator": "IN",
            "value":focused
        }])
    }

    respose = requests.get(url=url, params=params)

    for ad in respose.json().get("data"):
        creativeid  = ad.get("creative").get("id")

        creative_url = f"https://graph.facebook.com/v2.4/{creativeid}"

        creative_response = requests.get(creative_url, params=creative_params)
        creative_id = creative_response.json().get("effective_object_story_id").split("_")[0]

        page_url = f"https://graph.facebook.com/v2.4/{creative_id}"
        pagename = requests.get(page_url, params=page_params)
        
        page = pagename.json()
        page.update(ad)
        abc.append(page)

    return abc

def main():
    '''
    '''
    pathstatus = "analyticadebuddha.metaadtype.statusandobjective"
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")
    url = f"https://graph.facebook.com/v18.0/{account_id}/ads"

    activecampaign = spark.table(pathstatus).filter(
        (F.col("Status") == "ACTIVE") 
    ).select("CampaignId").distinct().collect()

    focused = [cam[0] for cam in activecampaign]

    abc = getDestination(access_token=access_token, focused=focused, url=url)

    df = pd.DataFrame(abc).drop("error", errors="ignore", axis=1)

    sdf = spark.createDataFrame(df).withColumn(
        "UserName", F.when(
            F.col("UserName").isNull(), "Buddha Holidays"
        ).otherwise("Buddha Air")
    ).select(*cosl)

    destination_old = spark.table(destinationtable)
    sdf.unionByName(destination_old, allowMissingColumns=True).distinct().write.mode("overwrite").option("mergeschema", "true").saveAsTable(destinationtable)

    return None

if __name__ == "__main__":
    main()