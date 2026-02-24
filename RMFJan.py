# Databricks notebook source
time.sleep(15)

rmf1 = spark.table("analyticadebuddha.customer.rmfnew")

CustomerSales.alias("df1").join(
    rmf1.alias("df2"), (F.trim("df1.Identifier") == F.trim("df2.Identifier")) 
    & (F.date_format("df1.created_at", "y-MM") == F.date_format("df2.UpDatedOn", "y-MM"))
    & (F.col("df1.currency") == F.col("df2.currency")), how = "inner"
).select(
    "df1.Identifier","df1.sector","df1.created_at", "df2.Category", F.col("df1.TotalRevenue").alias("RevenueMonth"), F.col("NumberOfPassenger").alias("PassengerMonth"),
    "df1.currency", "df2.UpDatedOn", F.col("df1.created_at").alias("TransactionDate"), "df1.Nationality", "df1.Gender", 
    "df1.UserStatus", "df1.CustomerStatus", "df1.UserType", "df1.DeviceType"
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.customer.rmftrend1")

# COMMAND ----------

'''
RMF analysis table as required by business

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 08/27/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
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


rmftable = "rmfnew2"
tableTrend = "rmftrend"
lakepath = "/mnt/integration/mailing/"
PKColumns = ['Identifier', "Category", "LTV", "currency"]

currentcustomer = "rmfcurrent"
schemarange = "analyticadebuddha.monitor.rmfranges"
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
    "Identifier", "Contact", "Nationality", F.concat(F.col("PassengerName"), F.lit(' '), F.col("LastName")).alias("FullName"), 
    "Gender", "Repetition", F.col("LTV").alias("LifeTimeValue"), "Bookedon", "TransactionRevenue", "PassengerMonth", 
    F.col("NPassenger").alias("TotalPassgener"), "FirstPurchase","RecentPurchase", "Currency", "DeviceType", "DeviceTypes", "UserType", 
    "UserStatus", "AgeGroup", "Recency", "Category", "Changed", "UpDatedOn"
]

statusChanged = Window.partitionBy("identifier", "Currency").orderBy("UpdatedOn")

def CustomerGrouped(Sales, Customer):
    """This Aggregates the Revenue per BookingId and Sector and Customer to get the LTV"""
    CustomerSales = Customer.alias("df1").join(
        Sales.alias("df2"), (F.trim("df1.FlightBookingId") == F.trim("df2.flight_booking_id")) 
        & (F.trim("df1.sector") == F.trim("df2.sector"))
        & (F.col("df1.Identifier").isNotNull())
        & (F.trim("df1.Currency") == F.trim("df2.Currency")), how = "inner"
    ).select("df1.*", F.col("df2.NetFare").alias("TotalRevenue"), ("df2.NumberOfPassenger"), "df2.created_at", "df2.PassengerName", "df2.LastName")

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
                pdf["RQuantile"] = custom_qcut(pdf["Recency"], q=5, labels=['1','2','3','4','5'])
                pdf["MQuantile"] = custom_qcut(pdf["LTV"], q=5, labels=['1','2','3','4','5'])
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
        catagory.alias("df2"), F.trim("df1.ConQuantile") == F.trim("df2.RMF_SCORE"), how="left"
    ).select(rmfcol).withColumn(
            "UpDatedOn", F.lit(date) 
    )
    
    return df

def getTrend(df, tableTrend:str):
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

    windpowspec = Window.partitionBy(F.trim(F.lower("Identifier")), "UpDatedOn", "Currency").orderBy("UpDatedOn")

    hist = hist.withColumn(
        "BookedOnDate", F.date_format(F.col("Bookedon"), "y-MM")
    ).distinct()

    rmftrend = userinfo.alias("df1").join(
        hist.alias("df2"), (F.trim(F.lower("df1.Identifier")) ==  F.trim(F.lower("df2.email"))) 
        & (F.col("df1.UpDateDate") ==  F.col("df2.BookedOnDate"))
        & (F.col("df1.Currency") ==  F.col("df2.Currency")), how="left"
    ).select("df1.*", "df2.TransactionRevenue", "df2.PassengerMonth")

    rmftrend = rmftrend.withColumn(
        "RevenueMonth", F.sum("TransactionRevenue").over(windpowspec)
    ).withColumn(
        "PassengerMonth", F.sum("PassengerMonth").over(windpowspec)
    ).withColumn(
        "TiecketMonth", F.count("TransactionRevenue").over(windpowspec)
    ).withColumn(
        "UpDatedOn",F.to_date("UpDatedOn", "y-MM-d")
    ).dropDuplicates(subset=["Identifier", "UpDateDate", "Currency","RevenueMonth", "PassengerMonth", "TiecketMonth"]).drop("UpDateDate", "TransactionRevenue")

    rmftrend.write.mode("overwrite").option("overwriteschema", "true").saveAsTable(f"{destination_schema}.{tableTrend}")

    return print(f"The rmf trend table has been written to the {destination_schema}.{tableTrend}") 

def customerCurrent(df, rmfcurrent:str,):
    '''
    This function writes the current customer table to the destination schema
    '''
    latestdate = df.select(F.max(F.date_format("UpDatedOn", "y-MM-dd"))).collect()[0][0]
    
    df = df.withColumn(
    "Changed", F.when(
        F.lag("Category").over(statusChanged) != F.col("Category"), True
    ).otherwise(False)
    )

    latest = df.filter(F.date_format("UpDatedOn", "y-MM-dd") == F.lit(latestdate)).select(*customerCurrentCols)

    latest.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{destination_schema}.{rmfcurrent}"
    )

    latest.write.mode("overwrite").option("overwriteSchema", "true").parquet(f"{lakepath}{rmfcurrent}")
    
    print(f"The current customer table has been written to the {destination_schema}.{rmfcurrent} and the file for interation has been written to the {lakepath}{rmfcurrent}")

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

def main():
    '''
    '''
    datefilter = F.current_date() - 1
    Customer = spark.read.parquet(PathCustomer)
    Sales = spark.read.parquet(PathSales).filter(F.date_format("created_at", "y-MM-d")  <= F.lit(datefilter)).dropDuplicates(
        ["flight_booking_id", "sector", "currency", "class_id", "flight_date", "flight_time"]
    )
    catagory = spark.table("analyticadebuddha.businesstable.rmf_score")
    CustomerSales = CustomerGrouped(Sales, Customer)
    SalesNrp = CustomerSales.filter(F.trim("Currency").contains("NPR"))
    SalesUsd = CustomerSales.filter(F.trim("Currency").contains("USD"))
    dfs =  CustomerLTV(dfs=[SalesNrp, SalesUsd])
    dfs = toQuantile(dfs)
    RMF = toRMF(catagory, dfs)

    time.sleep(4)
    CDC(df=RMF, destination_schema=destination_schema, tablename=rmftable, PKColumns=PKColumns, lake=False, spark=spark, dbutils=dbutils)

    time.sleep(4)
    df = spark.table("analyticadebuddha.customer.rmfnew")

    getTrend(df=df, tableTrend=tableTrend)
    time.sleep(4)

    customerCurrent(df=df, rmfcurrent=currentcustomer)
    ranges = spark.table(f"{destination_schema}.{currentcustomer}").select(rangecols)
    rangeRMF(ranges, schemarange)

    CustomerSales.unpersist()
    SalesNrp.unpersist()
    SalesUsd.unpersist()

if __name__:= "__main__":
    main()

# COMMAND ----------

from datetime import datetime, timedelta
import calendar
import time

count = 1
Customer = spark.read.parquet(PathCustomer)
Customer.persist()
datefilter = datetime(2019, 1, 31).date()

while datefilter <= datetime(2024, 12, 31).date():
    print(f"Run {count} for date before {datefilter}")
    Sales = spark.read.parquet(PathSales).filter(F.date_format("created_at", "y-MM-d")  <= F.lit(datefilter)).dropDuplicates(
        ["flight_booking_id", "sector", "currency", "class_id", "flight_date", "flight_time"]
    )
    catagory = spark.table("analyticadebuddha.businesstable.rmf_score")
    CustomerSales = CustomerGrouped(Sales, Customer)
    SalesNrp = CustomerSales.filter(F.trim("Currency").contains("NPR"))
    SalesUsd = CustomerSales.filter(F.trim("Currency").contains("USD"))
    dfs =  CustomerLTV(dfs=[SalesNrp, SalesUsd])
    dfs = toQuantile(dfs)

    RMF = toRMF(catagory, dfs)

    CDC(df=RMF, destination_schema=destination_schema, tablename=rmftable, PKColumns=PKColumns, lake=False, spark=spark, dbutils=dbutils)

    CustomerSales.unpersist()
    SalesNrp.unpersist()
    SalesUsd.unpersist()

    time.sleep(5)

    next_month = datefilter.replace(day=1) + timedelta(days=32)

    datefilter = next_month.replace(day=calendar.monthrange(next_month.year, next_month.month)[1])

    count += 1
    time.sleep(5)



# COMMAND ----------

Customer.unpersist()

# COMMAND ----------

16918 

# COMMAND ----------

time.sleep(2)
df = spark.table(f"{destination_schema}.{rmftable}") #.filter(F.date_format("UpDatedOn", "y-MM") == "2024-08")

df = spark.table("analyticadebuddha.customer.rmfnew2")


getTrend(df=df, tableTrend=tableTrend)
time.sleep(4)

customerCurrent(df=spark.table(f"{destination_schema}.{rmftable}"), rmfcurrent=currentcustomer)
ranges = spark.table(f"{destination_schema}.{currentcustomer}").select(rangecols)
rangeRMF(ranges, schemarange)

# CustomerSales.unpersist()
# SalesNrp.unpersist()
# SalesUsd.unpersist()

# df = spark.table(f"{destination_schema}.{rmftable}").filter(F.date_format("UpDatedOn", "y-MM") == "2024-08")
# customerCurrent(df, destination_schema=destination_schema, tablename=currentcustomer)

# ranges = spark.table("analyticadebuddha.customer.rmfcurrent").select(rangecols)
# rangeRMF(ranges, schemarange)

# COMMAND ----------

import pyspark.sql.functions as F

# df2 =  df.filter(F.date_format("UpDatedOn", "y-MM") != "2023-11")
# df1 = df.filter(F.date_format("UpDatedOn", "y-MM") != "2023-10")
# df2 = spark.table(f"{destination_schema}.d2310")
# df1.write.mode("overwrite").option("overwriteschema", "true").saveAsTable(f"{destination_schema}.d2310")
df = spark.table("analyticadebuddha.customer.rmftrend")
df.filter(F.col("Updatedon").contains("2024-12-31")).display()

# COMMAND ----------

df = spark.table(f"{destination_schema}.{rmftable}") #.filter(F.date_format("UpDatedOn", "y-MM") == "2024-08")

getTrend(df, TrendTable=TrendTable)

# COMMAND ----------

def getTrend(df, TrendTable:str):
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

    userinfo = df.select("Identifier", "Currency","Category", "Repetition", "Recency", "LTV","UpDatedOn", "Gender","AgeGroup", "UserType", "UserStatus", "DeviceType").distinct()
    userinfo = userinfo.withColumn(
        "UpDateDate", F.date_format(F.col("UpDatedOn"), "y-MM")
    )

    windpowspec = Window.partitionBy(F.trim(F.lower("Identifier")), "UpDatedOn", "Currency").orderBy("UpDatedOn")
    hist = hist.withColumn(
        "BookedOnDate", F.date_format(F.col("Bookedon"), "y-MM")
    ).distinct()

    rmftrend = userinfo.alias("df1").join(
        hist.alias("df2"), (F.trim(F.lower("df1.Identifier")) ==  F.trim(F.lower("df2.email"))) 
        & (F.col("df1.UpDateDate") ==  F.col("df2.BookedOnDate"))
        & (F.col("df1.Currency") ==  F.col("df2.Currency")), how="left"
    ).select("df1.*", "df2.TransactionRevenue", "df2.PassengerMonth")

    rmftrend = rmftrend.withColumn(
        "RevenueMonth", F.sum("TransactionRevenue").over(windpowspec)
    ).withColumn(
        "PassengerMonth", F.sum("PassengerMonth").over(windpowspec)
    ).withColumn(
        "TiecketMonth", F.count("TransactionRevenue").over(windpowspec)
    ).withColumn(
        "UpDatedOn",F.to_date("UpDatedOn", "y-MM-d")
    ).dropDuplicates(subset=["Identifier", "UpDateDate", "Currency","RevenueMonth", "PassengerMonth", "TiecketMonth", "Bookedon"]).drop("UpDateDate", "TransactionRevenue")

    # rmftrend.write.mode("overwrite").option("overwriteschema", "true").saveAsTable(f"{destination_schema}.{TrendTable}")
    
    # return print(f"The rmf trend table has been written to the {destination_schema}.{TrendTable}") 

# COMMAND ----------

# time.sleep(15)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

df = spark.table("analyticadebuddha.customer.rmfnew").dropDuplicates(subset=["Identifier", "UpDatedOn", "Currency"])


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

userinfo = df.select("Identifier", "Currency","Category", "Repetition", "Recency", "LTV","UpDatedOn", "Gender","AgeGroup", "UserType", "UserStatus", "DeviceType").distinct()
userinfo = userinfo.withColumn(
    "UpDateDate", F.date_format(F.col("UpDatedOn"), "y-MM")
)

windpowspec = Window.partitionBy(F.trim(F.lower("Identifier")), "UpDatedOn", "Currency").orderBy("UpDatedOn")
hist = hist.withColumn(
    "BookedOnDate", F.date_format(F.col("Bookedon"), "y-MM")
).distinct()

rmftrend = userinfo.alias("df1").join(
    hist.alias("df2"), (F.trim(F.lower("df1.Identifier")) ==  F.trim(F.lower("df2.email"))) 
    & (F.col("df1.UpDateDate") ==  F.col("df2.BookedOnDate"))
    & (F.col("df1.Currency") ==  F.col("df2.Currency")), how="left"
).select("df1.*", "df2.TransactionRevenue", "df2.PassengerMonth", "df2.Bookedon")

rmftrend = rmftrend.withColumn(
    "RevenueMonth", F.sum("TransactionRevenue").over(windpowspec)
).withColumn(
    "PassengerMonth", F.sum("PassengerMonth").over(windpowspec)
).withColumn(
    "TiecketMonth", F.count("TransactionRevenue").over(windpowspec)
).withColumn(
    "UpDatedOn",F.to_date("UpDatedOn", "y-MM-d")
).dropDuplicates(subset=["Identifier", "UpDateDate", "Currency","RevenueMonth", "PassengerMonth", "TiecketMonth", "Bookedon"]).drop("UpDateDate", "TransactionRevenue", "Bookedon")

rmftrend.write.mode("overwrite").option("overwriteschema", "true").saveAsTable("analyticadebuddha.customer.rmftrend")

# COMMAND ----------

rmftrend.groupBy(
    F.date_format("UpdatedON", "y-MM"), "currency", "category"
).agg(
    F.sum("RevenueMonth")
).display()

# COMMAND ----------

import pandas as pd
dates = list(pd.DataFrame(dates, columns = ["date"])['date'])

for datefilter in dates:
    print(f"Run {count} for date before {datefilter}")
    Customer = spark.read.parquet(PathCustomer)

    Sales =  spark.read.parquet(PathSales).filter(F.date_format("created_at", "y-MM-dd")  <= f"{datefilter}").dropDuplicates(
        ["flight_booking_id", "sector", "currency", "class_id", "flight_date", "flight_time"]
    )
    catagory = spark.table("analyticadebuddha.businesstable.rmf_score")
    CustomerSales = CustomerGrouped(Sales, Customer)
    SalesNrp = CustomerSales.filter(F.trim("Currency").contains("NPR"))
    SalesUsd = CustomerSales.filter(F.trim("Currency").contains("USD"))
    dfs =  CustomerLTV(dfs=[SalesNrp, SalesUsd])
    dfs = toQuantile(dfs)
    
    RMF = toRMF(catagory,datefilter, dfs)

    CDC(df = RMF, destination_schema = "analyticadebuddha.customer", tablename = "rmfvalidatation", PKColumns = ['Identifier', "Category", "LTV", "currency"], lake=False, spark=spark, dbutils=dbutils)

# COMMAND ----------

rmf = spark.table("analyticadebuddha.customer.rmftrend")
rmf1 = spark.table("analyticadebuddha.customer.rmftrend1")
val = spark.table("analyticadebuddha.customer.rmfnew")

rmf.display()

# COMMAND ----------

val.filter(F.col("Identifier") == "dpkmagar@gmail.com").display()

# COMMAND ----------

val.filter(F.col("Identifier") == "argraphic750@gmail.com").display()

# COMMAND ----------

rmf.filter(F.col("Identifier") == "argraphic750@gmail.com").display()

# COMMAND ----------

rmf1.filter(F.col("Category").isNull()).display()

# COMMAND ----------

rmf.filter(F.col("UpDatedOn").isNull()).display()

# COMMAND ----------

rmf.groupBy(
    F.date_format("UpdatedON", "y-MM"), "currency"
).agg(
    F.sum("RevenueMonth")
).display()

# COMMAND ----------

rmf.groupBy(
    F.date_format("UpDatedOn", "y-MM"), "currency"
).agg(
    F.sum("RevenueMonth")
).display()

# COMMAND ----------

CustomerSales.filter(F.col("Identifier") == "argraphic750@gmail.com").display()

# COMMAND ----------

CustomerSales.filter(F.col("Identifier") == "bharatshrestha7@gmail.com").display() 

# COMMAND ----------

CustomerSales.alias("df1").join(
    rmf.alias("df2"), (F.col("df1.Identifier") == F.col("df2.Identifier")) 
    & (F.date_format("df1.created_at", "y-MM") == F.date_format("df2.UpDatedOn", "y-MM"))
    & (F.col("df1.currency") == F.col("df2.currency")), how = "left"
).select("df1.Identifier","df1.sector", "df2.Category", "df1.TotalRevenue", "df1.currency", "df2.UpDatedOn", "df1.created_at").groupBy(F.date_format("created_at", "y-MM").alias("Date"), "currency").agg(
    F.round(F.sum(
        "TotalRevenue"
    ), 2).alias("Revenue")
)

# COMMAND ----------

CustomerSales.groupBy(F.date_format("CreatedAt", "y-MM").alias("Date"), "currency").agg(
    F.round(F.sum(
        "TotalRevenue"
    ), 2).alias("Revenue")
).display() 

# COMMAND ----------

CustomerSales.groupBy(F.date_format("created_at", "y-MM").alias("Date"), "currency").agg(
    F.round(F.sum(
        "TotalRevenue"
    ), 2).alias("Revenue")
).display() 

# COMMAND ----------



# COMMAND ----------

def toQuantile(dfs=list):
    """This converts the RMF to Quantile and writes the RMF Range"""
    dataframes = []
    for pdf in dfs:
        try:
            if pdf.count() > 0:
                pdf = pdf.toPandas()
                pdf["FQuantile"] = custom_qcut(pdf["Repetition"].astype("float64"), q=5, labels=['1','2','3','4','5'])
                pdf["RQuantile"] = custom_qcut(pdf["Recency"], q=5, labels=['1','2','3','4','5'])
                pdf["MQuantile"] = custom_qcut(pdf["LTV"], q=5, labels=['1','2','3','4','5'])
                pdf["ConQuantile"] = pdf.MQuantile.astype(str) + pdf.FQuantile.astype(str) + pdf.RQuantile.astype(str)  
                pdf["Score"] = pd.qcut(pdf["ConQuantile"].rank(method='first'), q=5, labels=['1','2','3','4','5'])
                df = spark.createDataFrame(pdf)

                if df.filter(F.upper("Currency") == "USD").first():
                    print(f"Writing RMF Range for USD")
                    rangeDf = monitor(df)
                    # rangeDf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.monitor.range_rmf_usd")
                else:
                    print(f"Writing RMF Range for NPR to monitor catalog")
                    rangeDf = monitor(df)
                    # rangeDf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.monitor.range_rmf_npr")

                dataframes.append(df)

        except Exception as e:
            print(e)

    return dataframes


def toRMF(catagory, dataframes=list):
    """This combines the RMF for both the currency and writes the Currency"""
    data = dataframes[0].unionByName(dataframes[1])
    date = data.select(F.max("Date")).collect()[0][0]

    selectcol = [
        "Identifier","Gender","Repetition","LTV","BookedOn","TransactionRevenue", "PassengerMonth","NPassenger","RecentPurchase","Currency","Sectors", "DeviceType", "DeviceTypes",
        "UserType","UserStatus", "AgeGroup", F.col("df1.FQuantile").alias("ScoreFrequency"), 
        "Recency", F.col("df1.RQuantile").alias("ScoreRecency"), F.col("MQuantile").alias("ScoreMonetary"), 
        F.col("ConQuantile").alias("MFR"), "Score",F.col("df2.Customer").alias("Category")
        
    ]
    
    data = data.alias("df1").join(
        catagory.alias("df2"), F.trim("df1.ConQuantile") == F.trim("df2.RMF_SCORE"), how="left"
    ).select(selectcol).withColumn(
            "UpDatedOn", F.lit(date) #  F.current_timestamp() 
    )
# RMF = toRMF(catagory, dfs)

SalesNrp = CustomerSales.filter(F.trim("Currency").contains("NPR"))
SalesUsd = CustomerSales.filter(F.trim("Currency").contains("USD"))
dfs =  CustomerLTV(dfs=[SalesNrp, SalesUsd])
dfs1 = toQuantile(dfs)

# COMMAND ----------

dfs1[1]

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.table("analyticadebuddha.customer.rmftrend")

# COMMAND ----------

df.display()

# COMMAND ----------

datefilter= datetime(2019, 2, 28)

# COMMAND ----------

datefilter.date()

# COMMAND ----------

last_day_next_month

# COMMAND ----------

