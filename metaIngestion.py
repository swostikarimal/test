import sys
import os
import json
import requests
import pandas as pd
import TrackAndUpdate
from TrackAndUpdate import trackAndTrace


def getDim(sdf, config, file, spark=None, dbutils=None):
    '''
    '''
    filename = file
    colsdim = config.get(file).get("colsdim")
    pkcolumns = config.get(file).get("pkdimension")
    subfolder = config.get(file).get("subfolder")
    datalake = config.get(file).get("datalake")

    trackAndTrace(
        df=sdf, destinatation_Schema=datalake, subfolder=subfolder,
        filename=filename, PKColumns=pkcolumns, spark=spark, dbutils=dbutils
    )
    return None

def getdata(
    url, params,fields, my_account=None, ad=False, spark=None, dbutils=None
):
    '''
    '''
    if ad:
        response = requests.get(url, params=params)
        df = pd.DataFrame(response.json().get("data"))

    else:
        response = my_account.get_insights(fields=fields, params=params)
        df = pd.DataFrame(response)
    
    return df

def getADSInsights(
    pastdate, access_token, url, fields, config=None, isad=False, myaccount=None, spark=None, 
    dbutils=None, typelevel=None, collist=None, columns=None, getFrames=None, lfilter=None
):
    '''
    '''
    for file in config:
        breakdowns = config.get(file).get("breakdowns")
        colname = config.get(file).get("position")
        selectcolumns = config.get(file).get("selectcols")

        if not lfilter is None:
            filtering = json.dumps(
                [{
                    "field":lfilter.get("field"), #"campaign.id",
                    "operator": "IN",
                    "value": lfilter.get("value")
                }]
            )
            params = {
                "fields": fields,
                "time_range":json.dumps({"since": pastdate, "until": pastdate}),
                "access_token": access_token,
                "level":typelevel,
                "breakdowns": breakdowns,
                "filtering": filtering
            }
        else:
            params = {
                "fields": fields,
                "time_range": f"{{'since': '{pastdate}', 'until': '{pastdate}'}}",
                "access_token": access_token,
                "level":typelevel,
                "breakdowns": breakdowns
            }

        df = getdata(url, params, ad=isad, my_account=myaccount, fields=fields, spark=spark, dbutils=dbutils)

        colsactions = config.get(file).get("colsactions")
        subfolder = config.get(file).get("subfolder_action")
        pkcolumns = config.get(file).get("pkactions")
        datalake = config.get(file).get("datalake")
       
        for col in collist:
            if col in df.columns and not df[col].empty:
                date = list(df.date_start.values)[0]
                print(f"Procession for {col} {date}")

                filename = col
                selectcols = selectcolumns + [col]
                df1 = df[selectcols]
                df1 = df1.dropna(subset=col)
                sdf = getFrames(df1, selectcols, date, colname)

                sdf1 = sdf.select(*colsactions).distinct()
                
                trackAndTrace(
                    df=sdf1.coalesce(1), destinatation_Schema=datalake, subfolder=subfolder,
                    filename=filename, PKColumns=pkcolumns, spark=spark, dbutils=dbutils
                )
            else:
                print(f"No data for {col}")

        cols = ["date_start"]+ columns + colname
        
        if "publisher_platform" in df.columns:
            df["platform"] = df["publisher_platform"]
            df = df.drop("publisher_platform", axis=1)

        sdf = spark.createDataFrame(
            df[["date_start"]+ columns + colname]
        ).select(*config.get(file).get("colsdim")).distinct()

        if not sdf.isEmpty():
            getDim(sdf.coalesce(1), config, file, spark=spark, dbutils=dbutils)
        else: print(f"No data for {file} to pass it to getdim")

    return None