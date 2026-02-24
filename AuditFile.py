# Databricks notebook source
import dbconnect
import pandas as pd
import MROModule as MROModule
import pyspark.sql.functions as F
from TrackAndUpdate import Mergers, unsupported_dtype
import datetime
import time

filepath = "/mnt/rawmro"
catalog = "analyticadebuddha.mro"

today = datetime.datetime.now().strftime("%Y-%m-%d")
filterdate = (pd.to_datetime(today) - pd.Timedelta(days=30)).strftime("%Y-%m-%d")

HostDB = '13.228.112.54'
db = dbutils.secrets.get(scope="mro", key="db_name")
UserDB= dbutils.secrets.get(scope="mro", key="db_user")
PasswordDB = dbutils.secrets.get(scope="mro", key="db_password")
jdbc_url, connection_properties = dbconnect.connectSQLServer(HostDB, db, UserDB, PasswordDB, spark)

def getjoincon(ids):
    '''
    '''
    for id in ids:
        for i in id:
            if (len(id) >= 1) & ((i != []) & (i != [''])):
                joincon = " AND ".join([f'df1.{col} = df2.{col}' for col in i])
            else:
                joincon = "Avoid"
                
    return joincon

def featchData(t1query, jdbc_url, connection_properties):
    '''
    '''
    df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({t1query}) as status_query",
        properties=connection_properties
    )
    sdf = unsupported_dtype(df)

    return sdf

def consolidateUpdate(tablepath, updatedinfo):
    '''
    '''
    old =  spark.table(tablepath)

    olddonottouch = df.alias("df1").join(
        updates.alias("df2"), F.col("df1.TableName") == F.col("df2.TableName"), how="left_anti"
    )

    df = old.unionByName(olddonottouch, allowMissingColumns=True)
    df.write.mode("overwrite").saveAsTable(tablepath)
    
    return None

def getTables(tables):
    '''
    '''
    dictinfo = []
    n = 1
    for table in tables:
        print(f"Iteratio {n} table {table[0]}")
        tablename = table[0]
        fullpath = f"{filepath}/{tablename}"
        tableinfo = status.filter(F.col('TableName') == tablename).collect()[0]
        joincol = status.filter(F.col('TableName') == tablename).select("Pkcolumns").collect()
        havedate = tableinfo.havedate
        matchcon = getjoincon(joincol)

        spec = {
            "match": matchcon,
            "destination": f"{fullpath}",
        }

        if havedate == "YES":
            created = updates.filter(F.col("TableName") == tablename).select("upto").collect()[0][0]
            created = (pd.to_datetime(created) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

            t1query = f'''
                SELECT * FROM [{tablename}]
                WHERE CAST(CreationTime AS DATE) >= '{created}'
            '''
            sdf = featchData(t1query)

            spec["datesince"] = created 

        else:
            t1query = f'''
                SELECT * FROM [{tablename}]
            '''
            sdf = featchData(t1query)
        
        if "CreationTime" in sdf.columns:
                df = spark.read.format("delta").load(f"{fullpath}")
                UpToDate = df.select(F.max(F.col("CreationTime"))).collect()[0][0]

                newupdated = spark.read.format("delta").load(fullpath)
                datasince = newupdated.select(F.min(F.date_format("CreationTime", "yyyy-MM-dd"))).collect()[0][0]
                dataupto = newupdated.select(F.max(F.date_format("CreationTime", "yyyy-MM-dd"))).collect()[0][0]

                fileinfo = {
                    "TableName": tablename,
                    "Filepath": fullpath,
                    "since": datasince,
                    "upto": dataupto,
                    "LastRun":  datetime.datetime.now()
                }
        else:
            print("No CreationTime column")
            fileinfo = {
                "TableName": tablename,
                "Filepath": fullpath,
                "since": None,
                "upto": None,
                "LastRun":  datetime.datetime.now()
            }
        
        print(spec)
        Mergers(newDF=sdf, fullPath=fullpath, spark=spark, dbutils=dbutils, matchcon=matchcon)
        
        dictinfo.append(fileinfo)
        n += 1

        time.sleep(2)

    updatedinfo = spark.createDataFrame(pd.DataFrame(dictinfo))

    tablepath = f"{catalog}.mroupdates"
    consolidateUpdate(tablepath, updatedinfo)

    return None

def main():
    status = spark.table("analyticadebuddha.mro.audit").filter(
        F.to_date(F.col("upDatedUpTo"), "yyyy-MM-dd") >= filterdate
    ).withColumn(
        "Pkcolumns", 
        F.when(
            F.col("Pkcolumns").isNull(), F.lit('')
        ).otherwise(
            F.col("Pkcolumns")
        )
    ).withColumn(
        "Pkcolumns", F.split("PkColumns", ',')
    )

    updates = spark.table(f"{catalog}.mroupdates")
    tables = status.select(F.col("TableName")).collect()
    getTables(tables)

    return None

if __name__ == "__main__":
    main()