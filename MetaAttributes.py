# Databricks notebook source
import requests
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adreportrun import AdReportRun
import pyspark.sql.functions as F
import pandas as pd
from TrackAndUpdate import trackAndTrace 
import warnings

warnings.filterwarnings("ignore")
datalake = "/mnt/meta/attributions/"
subfolder = "dsa"

pkdsa = ["campaign_id"]

dict_lis_dict_list = []
dict_list_dict_obj = []
dict_list_obj = []
dict_dict_list_dict = []
dict_dict_list_obj = []
dict_dict_obj = []
dict_obj = []
list_dict = []


lis = [dict_lis_dict_list, dict_list_dict_obj, dict_list_obj, dict_dict_list_dict, dict_dict_list_obj, dict_dict_obj, dict_obj, list_dict]

attcol = ["campaign_id", "campaign_name", "event_type", "type", "window_days"]

def dicMerge(a,b,c, appendto):
    dic = {**a, **b, **c}
    appendto.append(dic)
    return None

def tolake(dfs):
    '''
    '''
    for df in dfs:
        col = list(df.select("type").distinct().collect())
        if len(col) > 1:
            for file in col:
                filename = file[0]
                df1 = df.filter(F.col("type") == filename)

                if filename == "attribution_spec":
                    df1 = df1.select(*attcol)
                    print(df1.display())

                trackAndTrace(
                    df=df1.coalesce(1), destinatation_Schema=datalake, subfolder=subfolder, filename=filename, PKColumns=pkdsa, spark=spark, dbutils=dbutils
                )
                
        elif len(col) == 1:
            trackAndTrace(
                df=df.coalesce(1), destinatation_Schema=datalake, subfolder=subfolder, filename=col[0][0], PKColumns=pkdsa, spark=spark, dbutils=dbutils
            )
        else:
            continue

    return None

def getAttributes(data):
    '''
    '''
    for e in data:
        adset = {"campaign_id":e.get("id"), "campaign_name":e.get("name")}
        for key, value in e.items():
            if isinstance(value, dict): 
                for a, b in value.items():
                    if isinstance(b, list):  
                        for lis in b:
                            if isinstance(lis, dict):
                                for c, d in lis.items():
                                    if isinstance(d, list):
                                        for i in d:
                                            if isinstance(i, dict):
                                                fortype = {"type": c}
                                                dicMerge(a=adset,b=i,c=fortype, appendto=dict_lis_dict_list)
                                            else:
                                                print("There is another pattern inside 101")
                                    elif type(d) in [int, float, str]:
                                        fortype = {"type": a}
                                        dicMerge(a=adset,b=lis,c=fortype, appendto=dict_list_dict_obj)
                                
                                        
                                    else: print("There is another pattern inside 102")

                            elif type(lis) in [int, float, str]:
                                fortype = {"type": key}
                                temp = {"col": a, "val":lis}
                                dicMerge(a=adset,b=temp,c=fortype, appendto=dict_list_obj)

                            else: print("There is another pattern inside 103")

                    elif isinstance(b, dict): 
                        for c, d in b.items():
                            if isinstance(d, list):
                                for i in d:
                                    if isinstance(i, dict):
                                        fortype = {"type": a}
                                        dicMerge(a=adset,b=i,c=fortype, appendto=dict_dict_list_dict)

                                    elif type(i) in [str, int, float]:
                                        temp = {"col": c, "value":i}
                                        fortype = {"type": c}
                                        dicMerge(a=adset,b=temp,c=fortype, appendto=dict_dict_list_obj)

                                    else: print("There is another pattern inside 104")

                            elif type(d) in [str, int, float]:
                                temp = {"col": c, "value":d}
                                fortype = {"type": a}
                                dicMerge(a=adset,b=temp,c=fortype, appendto=dict_dict_obj)         

                            else: print("There is another pattern inside 105")

                    elif type(b) in [str, int, float]:
                        temp = {"col": a, "value":b}
                        type1 = {"type":f'{key}1'}
                        dicMerge(a=adset,b=temp,c=type1, appendto=dict_obj)   

                    else: print("There is another pattern inside 106")

            elif isinstance(value, list):
                for i in value:
                    if isinstance(i, dict):
                        type1 = {"type": key}
                        dicMerge(a=adset,b=i,c=type1, appendto=list_dict)   
                    else: print("There is another pattern inside 107")

            elif(key in ["id", "name"]) & (type(value) in [int, str, float]):
                continue
            else: print("There is another pattern inside 108")

    return None

def main():
    '''
    '''
    access_token = dbutils.secrets.get(scope="metaads", key="meta_access_key")
    account_id = dbutils.secrets.get(scope="metaads", key="meta_account_id")
    url = f"https://graph.facebook.com/v23.0/{account_id}/adsets"

    params = {
        "period": "lifetime",
        "fields": "id, name, targeting, recommendations, attribution_spec, content",
        "access_token": access_token
    }
    response = requests.get(url, params=params)
    data = response.json().get("data")
    getAttributes(data)
    dfs = [spark.createDataFrame(dic) for dic in lis if isinstance(dic, dict) and len(dic) > 0]
    tolake(dfs)

    return None

if __name__=="__main__":
    main()