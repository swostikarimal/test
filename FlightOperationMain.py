# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import time
import datetime
import dbconnect
from TrackAndUpdate import Mergers, unsupported_dtype

tablepath = "analyticadebuddha.default.flightop_updateinfo"

today = datetime.datetime.now().strftime("%Y-%m-%d")
filterdate = (pd.to_datetime(today) - pd.Timedelta(days=40)).strftime("%Y-%m-%d")
    
def getjoincon(ids):
    '''
    '''
    for id in ids:
        for i in id:
            if (len(id) >= 1) & (i != []):
                joincon = " AND ".join([f'df1.{col} = df2.{col}' for col in i])
            else:
                joincon = "Avoid"
                
    return joincon

def consolidateUpdate(tablepath, updatedinfo):
    '''
    '''
    old =  spark.table(tablepath)

    olddonottouch = old.alias("df1").join(
        updatedinfo.alias("df2"), F.col("df1.TableName") == F.col("df2.TableName"), how="left_anti"
    )

    df = updatedinfo.unionByName(olddonottouch, allowMissingColumns=True)
    df.write.mode("overwrite").saveAsTable(tablepath)
    
    return None

def getFiles(engineconn, tables=list, updates=None, NonPks=None):
    """
    """
    dictinfo = []
    iter = 1
    for tablename in tables:
        print(f"Iter {iter}")
        table = tablename[0]
        fullpath = "/mnt/fo" + "/raw/" + table

        if table not in NonPks:
            dateval = updates.filter(F.col("TABLENAME") == table)
            updated = dateval.select(F.col("upDatedUpTo")).collect()[0][0]
            created = dateval.select(F.col("CreatedUpto")).collect()[0][0]
            ids = dateval.select(F.col("columnspks")).collect()
            
            matchcon = getjoincon(ids)
            
            print(f"procession for {table}")
            print(str(matchcon))

            if (matchcon != "Avoid") & (not dateval.isEmpty()):
                try:
                    updated =  (pd.to_datetime(updated) - pd.Timedelta(days=15)).strftime("%Y-%m-%d")
                    created = (pd.to_datetime(created) - pd.Timedelta(days=15)).strftime("%Y-%m-%d")
                    
                    query = f"""
                        SELECT * 
                        FROM `operation`.`{table}` 
                        WHERE
                            TO_CHAR(updated_at,  'YYYY-MM-DD') >= {updated} 
                            AND TO_CHAR(created_at,  'YYYY-MM-DD') >= {created}
                    """ 
                except Exception as e:
                    print(f"Error {e}")
                    query = f"""
                        SELECT * 
                        FROM `operation`.`{table}`
                    """
                try:
                    df = pd.read_sql(text(query), engineconn)
                    sdf = spark.createDataFrame(df)
                    sdf = unsupported_dtype(df=sdf)
                    
                    PKColumns = sdf.columns
                    Mergers(newDF=sdf, fullPath=fullpath, PKColumns=PKColumns, spark=spark, dbutils=dbutils, matchcon=matchcon)
                except Exception as e:
                    print(f"Error {e}")
                    continue

            else:
                query = f"""
                        SELECT * 
                        FROM `operation`.`{table}`
                    """
                try:
                    pdf = pd.read_sql(text(query), engineconn)
                    sdf = spark.createDataFrame(pdf)
                    sdf = unsupported_dtype(df=sdf)
                    
                    print(f"The file {table} is has no primary keys and will be overwritten")
                    sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(fullpath)
                except Exception as e:
                    print(f"Error {e}")
                    continue

        else:
            print(f"Table {table} has no pksa") 
            query = f"""
                    SELECT * 
                    FROM `operation`.`{table}`
                """
            try:
                pdf = pd.read_sql(text(query), engineconn)
                sdf = spark.createDataFrame(pdf)
                sdf = unsupported_dtype(df=sdf)
                sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(fullpath)
            
            except Exception as e:
                print(f"Error {e}")
                continue
        
        newupdated = spark.read.format("delta").load(fullpath)
        count = newupdated.count()
       
        try:
            maxdate = newupdated.select(F.max(F.date_format("updated_at", "yyyy-MM-dd"))).collect()[0][0]
            maxcreated = newupdated.select(F.min(F.date_format("created_at", "yyyy-MM-dd"))).collect()[0][0]
            
            fileinfo = {
                "TableName": table,
                "Filepath": fullpath,
                "Count": count,
                "upDatedUpTo": maxdate,
                "CreatedUpto": maxcreated,
                "LastRun":  datetime.datetime.now()
            }
            dictinfo.append(fileinfo)

        except Exception as e:
            fileinfo = {
                "TableName": table,
                "Filepath": fullpath,
                "Count": count,
                "upDatedUpTo": maxdate,
                "CreatedUpto": maxcreated,
                "LastRun":  datetime.datetime.now()
            }
            dictinfo.append(fileinfo)
            continue

        time.sleep(2)
        iter += 1

    updatedinfo = spark.createDataFrame(pd.DataFrame(dictinfo))
    consolidateUpdate(tablepath, updatedinfo)
    
    return dictinfo

    
def getEngine():
    '''
    '''
    HostDB = dbutils.secrets.get(scope="flightoperation", key="fo_db_host")
    db = "operation"
    UserDB= dbutils.secrets.get(scope="flightoperation", key="fo_db_user")
    PasswordDB =  dbutils.secrets.get(scope="flightoperation", key="fo_db_pass")
    engine = dbconnect.MysqlConnectAlchemy(HostDB, UserDB, db, PasswordDB)
    engineconn = engine.connect()

    updates = spark.table("analyticadebuddha.default.updatefoandkeys")

    highpriority_ = spark.table("analyticadebuddha.businesstable.FO_PRIORITY")
    
    highpriority = highpriority_.filter(
       (
            (F.to_date("upDatedUpTo", "yyyy-MM-dd") >= filterdate) | (F.to_date("CreatedUpto", "yyyy-MM-dd") >= filterdate) |
            (F.size(F.col("columnspks")) == 0)
        )
    ).select("TableName").collect()

    dfNonPk = highpriority_.filter(F.size(F.col("columnspks")) == 0)

    TablesNonPk = [row.TableName for row in dfNonPk.select("TableName").collect()]

    getFiles(engineconn, tables=highpriority, updates=updates, NonPks=TablesNonPk)

    return engineconn

if __name__ == "__main__":
    getEngine()