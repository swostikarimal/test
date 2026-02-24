# Databricks notebook source
import requests
import json
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

destinationtable = "analyticadebuddha.metaadtype.ad_destination"

cosl = [
    F.col("campaign_id").alias("CampaignId"), 
    F.col("adset_id").alias("AdSetId"), 
    F.col("id").alias("AdId"), 
    "name",  # keep the original name column from API
    "UserName"
]

def getDestination(access_token, focused, url):
    """
    Fetch ads and page information safely from Meta Ads API.
    """
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
        "fields": "id,name,adset_id,campaign_id,page_id,creative",
        "access_token": access_token,
        "filtering": json.dumps([{
            "field": "campaign.id",
            "operator": "IN",
            "value": focused
        }])
    }

    respose = requests.get(url=url, params=params)
    ads_data = respose.json().get("data", [])

    for ad in ads_data:
        creative = ad.get("creative")
        if not creative:
            continue

        creativeid = creative.get("id")
        if not creativeid:
            continue

        creative_url = f"https://graph.facebook.com/v2.4/{creativeid}"
        creative_response = requests.get(creative_url, params=creative_params)
        creative_json = creative_response.json()

        if "error" in creative_json:
            continue

        effective_story_id = creative_json.get("effective_object_story_id")
        if effective_story_id:
            creative_id = effective_story_id.split("_")[0]

            page_url = f"https://graph.facebook.com/v2.4/{creative_id}"
            pagename_response = requests.get(page_url, params=page_params)
            page_json = pagename_response.json()

            if "error" in page_json:
                page_json = {}
        else:
            page_json = {}

        page_json.update(ad)
        abc.append(page_json)

    return abc

def main():
    pathstatus = "analyticadebuddha.metaadtype.statusandobjective"
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")
    url = f"https://graph.facebook.com/v18.0/{account_id}/ads"

    activecampaign = spark.table(pathstatus).filter(
        (F.col("Status") == "ACTIVE")
    ).select("CampaignId").distinct().collect()

    focused = [cam[0] for cam in activecampaign]

    abc = getDestination(access_token=access_token, focused=focused, url=url)

    # Create DataFrame safely
    df = pd.DataFrame(abc).drop("error", errors="ignore", axis=1)

    if df.empty:
        print("No ad data found. Creating empty Spark DataFrame.")
        schema = StructType([
            StructField("campaign_id", StringType(), True),
            StructField("adset_id", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("UserName", StringType(), True)
        ])
        sdf = spark.createDataFrame([], schema)
    else:
        sdf = spark.createDataFrame(df)

        # Ensure UserName column exists
        if "username" in df.columns:
            sdf = sdf.withColumn(
                "UserName",
                F.when(F.col("username").isNull(), "Buddha Holidays").otherwise(F.col("username"))
            )
        else:
            sdf = sdf.withColumn("UserName", F.lit("Buddha Holidays"))

    sdf = sdf.select(*cosl)

    # Write to destination table
    destination_old = spark.table(destinationtable)
    sdf.unionByName(destination_old, allowMissingColumns=True).distinct()\
        .write.mode("overwrite").option("mergeschema", "true").saveAsTable(destinationtable)

    return None

if __name__ == "__main__":
    main()


