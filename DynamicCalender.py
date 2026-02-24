# Databricks notebook source
"""
Code to create a Dynamic Calender required by business

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 08/15/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
"""

import pytz
import pandas as pd
from datetime import datetime, timedelta
import pyspark.sql.functions as F

def DynamicCalender(start_year=None):
    """This creates the dynamic calender for current year to date and last year in Ktm time zone"""
    nepal_zone = pytz.timezone("Asia/Kathmandu")
    current_date = datetime.now(nepal_zone)
    
    if start_year is None:
        start_year = current_date.year - 1
    
    start_date = datetime(start_year, 1, 1, tzinfo=nepal_zone) 
    
    end_date = current_date 

    datelist = []
    while start_date <= end_date:
        datelist.append(start_date.strftime("%Y-%m-%d"))
        start_date += timedelta(days=1)
    
    dic = {
        "Date": datelist
    }

    calender = spark.createDataFrame(pd.DataFrame(dic)).withColumn(
        "Date", F.to_date("Date")
    ).withColumn(
        "Quarter", F.quarter("Date")
    ).withColumn(
        "Week", F.weekofyear("Date")
    ).withColumn(
        "Month", F.month("Date")
    ).withColumn(
        "DayOfMonth", F.dayofmonth("Date")
    ).withColumn(
        "Day", F.date_format("Date", "E")
    ).withColumn(
        "DayOfWeek", F.dayofweek("Date")
    )

    writeToCatalog(df=calender, catalog_name="analyticadebuddha.sectors", table_name="Calender", selectCols=None, repartation=None)
    
    return None

def writeToCatalog(df, catalog_name=str, table_name=str, selectCols=None, repartation=None):
    """This writes the tables to catalog"""
    if repartation:
        df.repartition(
            repartation
        ).write.format('delta').mode(
            "overwrite"
        ).option(
            "overwriteSchema", "true"
        ).saveAsTable(
            f"{catalog_name}.{table_name}"
        )
    else:
        df.write.format('delta').mode(
            "overwrite"
        ).option(
            "overwriteSchema", "true"
        ).saveAsTable(
            f"{catalog_name}.{table_name}"
        )


def main():
    DynamicCalender()

if __name__=="__main__":
    main()