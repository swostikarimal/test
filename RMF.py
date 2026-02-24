# Databricks notebook source
'''
RMF analysis table as required by business

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 08/27/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
 1.1 - Added the Current Changes, 01/12/2025
'''

import pyspark.sql.functions as F
from CDC import CDC
from pyspark.sql.window import Window
from datetime import datetime
import pandas as pd
import time


PathSales = "/mnt/silver/consolidated/sales"
PathCustomer = "/mnt/silver/consolidated/users"
destination_schema = "analyticadebuddha.customer"

rmftable = "rmfnew"
tableTrend = "rmftrend"
tablehist = 'userhist'
currentcustomer = "rmfcurrent"

PKColumns = ['Identifier', "Category", "LTV", "currency"]
userhistcols = [F.col("Bookedon").alias("Date"), F.col("email").alias("Identifier"), "Currency", "Sectors", "TransactionRevenue", "PassengerMonth"]
rangecols = ["Currency","LifeTimeValue", "Repetition", "Recency", "Category", "UpdatedOn"]

rmfcol = [
    F.trim(F.lower("Identifier")).alias("Identifier"), "PassengerName", "LastName","Nationality", "Contact",
    "Gender","Repetition","LTV","BookedOn","TransactionRevenue", "PassengerMonth","NPassenger",
    "FirstPurchase","RecentPurchase","Currency","Sectors", "DeviceType", "DeviceTypes",
    "UserType","UserStatus", "AgeGroup", F.col("df1.FQuantile").alias("ScoreFrequency"), 
    "Recency", F.col("df1.RQuantile").alias("ScoreRecency"), F.col("MQuantile").alias("ScoreMonetary"), 
    F.col("ConQuantile").alias("MFR"),F.col("df2.Customer").alias("Category")
]


customerCurrentCols = [
    "Identifier", "Contact", "Nationality", F.concat(
        F.col("PassengerName"), F.lit(' '), F.col("LastName")
    ).alias("FullName"), 
    "Gender", "Repetition", F.col("LTV").alias("LifeTimeValue"), "FirstPurchase",
    "RecentPurchase", "Currency", "DeviceType", "UserType", 
    "UserStatus", "AgeGroup", "Recency", "Category", "Changed", "ChangedThisMONTH", "UpDatedOn"
]

statusChanged = Window.partitionBy("identifier", "Currency").orderBy("UpdatedOn")

def CustomerGrouped(Sales, Customer):
    """This Aggregates the Revenue per BookingId and Sector and Customer to get the LTV"""
    CustomerSales = Customer.alias("df1").join(
        Sales.alias("df2"), (
            F.trim("df1.FlightBookingId") == F.trim("df2.flight_booking_id")
        ) 
        & (
            F.trim("df1.sector") == F.trim("df2.sector")
        )
        # & (F.col("df1.Identifier").isNotNull())
        & (
            F.trim("df1.Currency") == F.trim("df2.Currency")
        ), how = "inner"
    ).select(
        "df1.*", F.col("df2.NetFare").alias("TotalRevenue"), ("df2.NumberOfPassenger"), 
        "df2.created_at", "df2.PassengerName", "df2.LastName"
    )

    return CustomerSales

def CustomerLTV(dfs=list):
    """This gives the LTV for each Customer"""
    groupeddfs = []
    for df in dfs:
        date = df.select(F.max("created_at")).collect()[0][0]
        
        CustomerGrouped = df.orderBy(F.col("created_date").desc()).groupBy(F.trim(F.lower("Identifier")).alias("Identifier")).agg(
            F.countDistinct("FlightBookingId").alias("Repetition"),
            F.first("Contact").alias("Contact"),
            F.first("Gender").alias("Gender"),
            F.first("PassengerName").alias("PassengerName"),
            F.first("Nationality").alias("Nationality"), 
            F.first("LastName").alias("LastName"),
            F.first("DeviceType").alias("DeviceType"),
            F.collect_list("DeviceType").alias("DeviceTypes"),
            F.round(F.sum("TotalRevenue"), 2).alias("LTV"),
            F.first("Currency").alias("Currency"),
            F.first("created_at").alias("created_at"),
            F.collect_list("sector").alias("Sectors"),
            F.collect_list(F.to_date("created_at")).alias("BookedOn"),
            F.collect_list("TotalRevenue").alias("TransactionRevenue"),
            F.collect_list("NumberOfPassenger").alias("PassengerMonth"),
            F.max(F.to_date("CreatedAt", "y-MM-dd")).alias("RecentPurchase"),
            F.min(F.to_date("CreatedAt", "y-MM-dd")).alias("FirstPurchase"),
            F.first("UserType").alias("UserType"),
            F.first("UserStatus").alias("UserStatus"),
            F.first("AgeGroup").alias("AgeGroup"),
            F.first("DateToday").alias("DateToday"),
            F.sum("NumberOfPassenger").alias("NPassenger")
        ).withColumn(
            "Date", F.lit(date)
        ).withColumn(
            "Recency", F.round(
                (F.unix_timestamp(F.to_timestamp("RecentPurchase")) - F.unix_timestamp(F.to_timestamp("Date"))) / (3600 * 24)
            )
        )
        groupeddfs.append(CustomerGrouped)

    return groupeddfs

def monitor(df):
    """This returns the quantile ranges for each RMF"""
    df = df.groupBy("FQuantile").agg(
        F.min("Repetition").alias("MinFrq"),
        F.max("Repetition").alias("MaxFrq")
    ).alias("df1").join(
        df.groupBy("RQuantile").agg(
        F.min(F.abs("Recency")).alias("MinDays"),
        F.max(F.abs("Recency")).alias("MaxDays")
    ).alias("df2"),F.col("df1.FQuantile") == F.col("df2.RQuantile"), how="left"
    ).join(
        df.groupBy("MQuantile").agg(
            F.min("LTV").alias("Minm"),
            F.max("LTV").alias("MaxM")
    ).alias("df3"),F.col("df1.FQuantile") == F.col("df3.MQuantile"), how="left"
    ).select("df1.*",F.col("df2.MaxDays"),F.col("df2.MinDays"),F.col("df3.Minm"), F.col("df3.MaxM"))

    return df

def custom_qcut(series, q, labels):
    """Calculate the Percentile"""
    try:
        return pd.qcut(series, q=q, labels=labels)
    except ValueError:
        ranks = series.rank(method='first')
        return pd.cut(ranks, bins=q, labels=labels, include_lowest=True)
    
def toQuantile(dfs=list):
    """This converts the RMF to Quantile and writes the RMF Range"""
    dataframes = []
    for pdf in dfs:
        try:
            if pdf.count() > 0:
                pdf = pdf.toPandas()
                pdf["FQuantile"] = custom_qcut(pdf["Repetition"], q=5, labels=['1','2','3','4','5']) #.astype("float64")
                pdf["RQuantile"] = pd.qcut(pdf["Recency"], q=5, labels=['1','2','3','4','5'])
                pdf["MQuantile"] = pd.qcut(pdf["LTV"], q=5, labels=['1','2','3','4','5'])
                pdf["ConQuantile"] = pdf.MQuantile.astype(str) + pdf.FQuantile.astype(str) + pdf.RQuantile.astype(str)  
                df = spark.createDataFrame(pdf)

                if df.filter(F.upper("Currency") == "USD").first():
                    print(f"Writing RMF Range for USD")
                    rangeDf = monitor(df)
                    rangeDf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.monitor.range_rmf_usd")
                else:
                    print(f"Writing RMF Range for NPR to monitor catalog")
                    rangeDf = monitor(df)
                    rangeDf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.monitor.range_rmf_npr")

                dataframes.append(df)

        except Exception as e:
            print(e)

    return dataframes

def toRMF(catagory, dataframes=list):
    """This combines the RMF for both the currency and writes the Currency"""
    data = dataframes[0].unionByName(dataframes[1])
    date = data.select(F.max("created_at")).collect()[0][0]
    frames = []


    df = data.alias("df1").join(
        catagory.alias("df2"), F.trim("df1.ConQuantile") == F.trim("df2.RMF_SCORE"), how="inner"
    ).select(rmfcol).withColumn(
            "UpDatedOn", F.lit(date) 
    )
    
    return df

def getTrend(df, tableTrend:str, tablehist:str):
    lateststatus = df.groupBy("Identifier", "currency").agg(
        F.max("UpDatedOn").alias("LatedstUpdate")
    ).distinct()

    customer = df.alias("df1").join(
        lateststatus.alias("df2"), (F.col("df1.Identifier") ==  F.col("df2.Identifier")) & (F.col("df1.Currency") ==  F.col("df2.Currency"))
        & (F.col("df1.UpDatedOn") ==  F.col("df2.LatedstUpdate")), how="inner"
    ).select("df1.*").distinct()

    hist = customer.orderBy(F.desc("UpDatedOn")).select(
        F.posexplode(F.col("BookedOn")).alias("pos", "BookedOn"), 
        F.col("TransactionRevenue").getItem(F.col("pos")).alias("TransactionRevenue"),  
        F.col("PassengerMonth").getItem(F.col("pos")).alias("PassengerMonth"),     
        F.col("Sectors").getItem(F.col("pos")).alias("Sectors"),      
        F.col("Identifier").alias("email"),
        F.col("Currency").alias("Currency")
    )

    userinfo = df.select(
        "Identifier", "Nationality","Contact","Currency","Category", "Repetition", 
        "Recency", "LTV","UpDatedOn", "Gender","AgeGroup", "UserType", "UserStatus", "DeviceType"
    ).distinct()

    userinfo = userinfo.withColumn(
        "UpDateDate", F.date_format(F.col("UpDatedOn"), "y-MM")
    )

    windpowspec = Window.partitionBy(
        F.trim(F.lower("Identifier")), "UpDatedOn", "Currency"
    ).orderBy("UpDatedOn")

    hist = hist.withColumn(
        "BookedOnDate", F.date_format(F.col("Bookedon"), "y-MM")
    ).distinct()

    userhistory = userinfo.alias("df1").join(
        hist.alias("df2"), (
            F.trim(F.lower("df1.Identifier")) ==  F.trim(F.lower("df2.email"))
        ) 
        & (
            F.col("df1.UpDateDate") ==  F.col("df2.BookedOnDate")
        )
        & (
            F.col("df1.Currency") ==  F.col("df2.Currency")
        ), how="left"
    ).select(
        "df1.*", "df2.TransactionRevenue", "df2.PassengerMonth"
    )

    rmftrend = userhistory.withColumn(
        "RevenueMonth", F.sum("TransactionRevenue").over(windpowspec)
    ).withColumn(
        "PassengerMonth", F.sum("PassengerMonth").over(windpowspec)
    ).withColumn(
        "TiecketMonth", F.count("TransactionRevenue").over(windpowspec)
    ).withColumn(
        "UpDatedOn",F.to_date("UpDatedOn", "y-MM-d")
    ).dropDuplicates(
        subset=[
            "Identifier", "UpDateDate", "Currency","RevenueMonth", "PassengerMonth", "TiecketMonth"
        ]
    ).drop("UpDateDate", "TransactionRevenue")

    rmftrend.write.mode("overwrite").option("overwriteschema", "true").saveAsTable(f"{destination_schema}.{tableTrend}")
    
    hist.select(
        *userhistcols
    ).write.mode("overwrite").option("overwriteschema", "true").saveAsTable(f"{destination_schema}.{tablehist}")

    return print(f"The rmf trend table has been written to the {destination_schema}.{tableTrend}") 

def customerCurrent(df, rmfcurrent:str,):
    '''
    This function writes the current customer table to the destination schema
    '''
    maxupdated = df.select(F.max("UpDatedOn")).collect()[0][0]

    latestdate = df.groupBy("Identifier", "Currency").agg(
        F.max(F.date_format("UpDatedOn", "y-MM-dd")).alias("UpDatedOn")
    ) 

    df = df.withColumns(
        {
        "Changed": F.when(
             F.lag("Category").over(statusChanged) != F.col("Category"), True
        ).otherwise(False),

        "ChangedThisMONTH": F.when(
            (F.col("Changed") == True) & (F.col("UpDatedOn") == F.lit(maxupdated)), True
        ).otherwise(False)

        }
    )

    latest = df.alias("df1").join(
        latestdate.alias("df2"), 
        (
            F.col("df1.Identifier") ==  F.col("df2.Identifier")
        ) 
        & (
            F.col("df1.Currency") ==  F.col("df2.Currency")
        )
        & (
            F.date_format("df1.UpDatedOn", "y-MM-dd") ==  F.col("df2.UpDatedOn")
        )
    ).select("df1.*").select(*customerCurrentCols)

    latest.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{destination_schema}.{rmfcurrent}"
    )
    
    print(f"The current customer table has been written to the {destination_schema}.{rmfcurrent}")

    return None

def rangeRMF(ranges, schemarange):
    DateMax = ranges.select(F.max("UpdatedOn")).collect()[0][0]
    latestrange = ranges.filter(F.col("UpdatedOn") == F.lit(DateMax))

    latestrange.groupBy(
        "Currency", "Category"
    ).agg(
        F.max("LifeTimeValue").alias("MaxLifeTimeValue"), F.min("LifeTimeValue").alias("MinLifeTimeValue"),
        F.max("Repetition").alias("MaxFrequency"), F.min("Repetition").alias("MinFrequency"),
        F.min(F.abs("Recency")).alias("Latest"), F.max(F.abs("Recency")).alias("Earliest"),
    ).write.format("delta").mode("overwrite").option("overwriteschema", "true").saveAsTable(f"{schemarange}")

    return None

def main(override_date=None):
    '''
    '''
    schemarange = "analyticadebuddha.monitor.rmfranges"
    datefilter = F.lit(override_date) if override_date else F.current_date()-2

    Customer = spark.read.parquet(PathCustomer)
    Sales = spark.read.parquet(PathSales).filter(
        F.date_format("created_at", "y-MM-d")  <= F.lit(datefilter)
    ).dropDuplicates(
        ["flight_booking_id", "sector", "currency", "class_id", "FlightDate", "FlightTime"]
    )
    catagory = spark.table("analyticadebuddha.businesstable.rmf_score")
    CustomerSales = CustomerGrouped(Sales, Customer)
    SalesNrp = CustomerSales.filter(F.trim("Currency").contains("NPR"))
    SalesUsd = CustomerSales.filter(F.trim("Currency").contains("USD"))
    dfs =  CustomerLTV(dfs=[SalesNrp, SalesUsd])
    dfs = toQuantile(dfs)

    RMF = toRMF(catagory, dfs)
    CDC(df=RMF, destination_schema=destination_schema, tablename=rmftable, PKColumns=PKColumns, lake=False, spark=spark, dbutils=dbutils)
    time.sleep(5)

    df = spark.table("analyticadebuddha.customer.rmfnew")
    getTrend(df=df, tableTrend=tableTrend, tablehist=tablehist)

    customerCurrent(df=df, rmfcurrent=currentcustomer)
    ranges = spark.table(f"{destination_schema}.{currentcustomer}").select(rangecols)
    rangeRMF(ranges, schemarange)

    CustomerSales.unpersist()
    SalesNrp.unpersist()
    SalesUsd.unpersist()

if __name__== "__main__":
    main(override_date="2025-12-01")
    main(override_date="2026-01-31")
    main()


# COMMAND ----------

import pyspark.sql.functions as F
spark.table("analyticadebuddha.customer.rmfnew").select(F.max("UpDatedOn"), F.min("UpDatedOn")).display()

# COMMAND ----------

