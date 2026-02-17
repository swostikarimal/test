# Databricks notebook source
'''
Logic to create UserJourney and flighbookingfunnel marts

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 12/10/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''

import pyspark.sql.functions as F
import pyspark

pathPage = "/mnt/ga/reporting/Conversion/funnel/pagePath"
pathHost = "/mnt/ga/reporting/Conversion/funnel/fhostName"
pathSearched = "/mnt/ga/reporting/Conversion/funnel/flight_searched"
fpathSearched = "/mnt/ga/reporting/Conversion/funnel/fflight_searched"
pathdetail = "/mnt/ga/reporting/Conversion/funnel/flight_contactdetail"
fpathdetail = "/mnt/ga/reporting/Conversion/funnel/fflight_contactdetail"
pathpayment = "/mnt/ga/reporting/Conversion/funnel/flight_payment"
fpathpayment = "/mnt/ga/reporting/Conversion/funnel/fflight_payment"
pathpurchase = "/mnt/ga/reporting/Conversion/funnel/purchase"
fpathpurchase = "/mnt/ga/reporting/Conversion/funnel/fpurchase"
filterHost = F.col("hostName") == "www.buddhaair.com"
pathPageCol = ["date", "totalUsers"]


date = F.col("date").alias("Date")
sector = F.col("customEvent:sector").alias("Sector")
path = [F.col("TotalHostUser"), "EventCountHost"]

columns =  [
    "totalUsers_flight_searched", "eventCount_flight_searched", "totalUsers_flight_contactdetail", "eventCount_flight_contactdetail",
    "totalUsers_flight_payment", "eventCount_flight_payment","totalUsers_purchase", "itemRevenue_purchase", "itemsPurchased_purchase"
]

othercols = [F.col("totalUsers_flight_searched").alias("TotalSearchedUsers"), 
            F.col("eventCount_flight_searched").alias("EventCountSearched"),
            F.col("totalUsers_flight_contactdetail").alias("TotalContactUsers"),
            F.col("eventCount_flight_contactdetail").alias("EventCountContact"),
            F.col("totalUsers_flight_payment").alias("TotalPaymentUsers"),
            F.col("eventCount_flight_payment").alias("EventCountPayment"),
            F.col("totalUsers_purchase").alias("TotalPurchaseUsers"), 
            F.col("itemsPurchased_purchase").alias("Quantity"),
            F.col("itemRevenue_purchase").alias("ItemRevenue")
]

def getGrouped(df, gcol: str, acol: list):
    '''
    Return the grouped data for the given group column and aggregation columns
    '''
    name = df.select(F.col("eventfor")).collect()[0][0]

    agg_exprs = {col: F.sum(col).alias(f"{col}_{name}") for col in acol}
    grouped_df = df.groupBy("date", gcol).agg(*agg_exprs.values())

    return grouped_df

def getJourney(groupedflightsearch, groupedcontact, groupedpayments, groupedpurchase):
    '''
    Returns the consoloidated customer journey data
    '''
    df = (
        groupedflightsearch.alias("df1")
        .join(
            groupedcontact.alias("df2"),
            (F.col("df1.date") == F.col("df2.date"))
            & ((F.col("df1.customEvent:sector") == F.col("df2.customEvent:sector"))),
            how="left",
        )
        .join(
            groupedpayments.alias("df3"),
            (F.col("df1.date") == F.col("df3.date"))
            & ((F.col("df1.customEvent:sector") == F.col("df3.customEvent:sector"))),
            how="left",
        )
        .join(
            groupedpurchase.alias("df4"),
            (F.col("df1.date") == F.col("df4.date"))
            & (F.col("df1.customEvent:sector") == F.col("df4.customEvent:sector")),
            how="left",
        )
        .select(
            "df1.*",
            "df2.totalUsers_flight_contactdetail", "df2.eventCount_flight_contactdetail",
            "df3.totalUsers_flight_payment", "df3.eventCount_flight_payment",
            "df4.totalUsers_purchase","df4.itemRevenue_purchase", "df4.itemsPurchased_purchase"
        )
    )

    return df

def getBookingFunnel(pagepath, fsearch, fdetails, fpayments, fpurchase, fhostname):
    '''
    Returns the consoloidated booking funnel data
    '''
    df = pagepath.alias("df1").join(
        fsearch.alias("df2"), F.col("df1.date") == F.col("df2.date"), how="left"
    ).join(
        fdetails.alias("df3"), F.col("df1.date") == F.col("df3.date"), how="left"
    ).join(
        fpayments.alias("df4"), F.col("df1.date") == F.col("df4.date"), how="left"
    ).join(
        fpurchase.alias("df5"), F.col("df1.date") == F.col("df5.date"), how="left"
    ).join(
        fhostname.alias("df6"), F.col("df1.date") == F.col("df6.date"), how="left"
    ).select(
        [
            F.col("df6.totalUsers_hostName").alias("TotalHostUser"), F.col("df6.eventCount_hostName").alias("EventCountHost"),
            F.col("df1.date"), "df2.totalUsers_flight_searched", "df2.eventCount_flight_searched", "df3.totalUsers_flight_contactdetail", 
            "df3.eventCount_flight_contactdetail",
            "df4.totalUsers_flight_payment", "df4.eventCount_flight_payment", "df5.totalUsers_purchase", 
            "df5.itemRevenue_purchase","df5.itemsPurchased_purchase"
        ]
    )

    return df

def renamedColumns(df: pyspark.sql.DataFrame, columns=list):
    for col in columns:
        colname = str(col)
        df = df.withColumn(
            colname, F.when(
                F.col(colname).isNull(), 0).otherwise(F.col(colname)
            )
        )

    return df

def getConversionRatio(df: pyspark.sql.DataFrame):
    df = df.withColumns(
        {
            "HostToSearch": F.round(F.col("TotalSearchedUsers")/F.col("TotalHostUser"), 2)
        ,
        
            "SearchToContact": F.round(F.col("TotalContactUsers")/F.col("TotalSearchedUsers"), 2)
        ,
        
           "ContactToPayment": F.round(F.col("TotalPaymentUsers")/F.col("TotalContactUsers"), 2)
        ,
        
           "PaymentToPurchase": F.round(F.col("TotalPurchaseUsers")/F.col("TotalPaymentUsers"), 2)
        ,
        
            "ConversionRatio": F.round(F.col("TotalPurchaseUsers")/F.col("TotalHostUser"), 2)
        }
    )

    return df

def DeltaLake(df, catalog, filename):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{filename}")
    print(f"The table has been written to the Delta Lake table {catalog}.{filename}")

def main():
    '''Call all the functions'''
    pagepath = spark.read.parquet(pathPage).select(pathPageCol)
    hostname = spark.read.parquet(pathHost).filter(filterHost)
    searched = spark.read.parquet(pathSearched)
    ffsearched = spark.read.parquet(fpathSearched)
    details = spark.read.parquet(pathdetail)
    ffdetails = spark.read.parquet(fpathdetail)
    payments = spark.read.parquet(pathpayment)
    ffpayments = spark.read.parquet(fpathpayment)
    purchase = spark.read.parquet(pathpurchase)
    ffpurchase = spark.read.parquet(fpathpurchase)

    catalog = "analyticadebuddha.google"


    groupedflightsearch = getGrouped(
        searched, gcol="customEvent:sector", acol=["totalUsers", "eventCount"]
    )
    groupedcontact = getGrouped(
        details, gcol="customEvent:sector", acol=["totalUsers", "eventCount"]
    )
    groupedpayments = getGrouped(
        payments, gcol="customEvent:sector", acol=["totalUsers", "eventCount"]
    )
    groupedpurchase = getGrouped(
        purchase, gcol="customEvent:sector", acol=["totalUsers","itemsPurchased", "itemRevenue"]
    )

    fhostname = getGrouped(
        df=hostname, gcol="date", acol=["totalUsers", "eventCount"]
    )

    fsearch = getGrouped(
        df=ffsearched, gcol="date", acol=["totalUsers", "eventCount"]
    )
    fdetails = getGrouped(
        df=ffdetails, gcol="date", acol=["totalUsers", "eventCount"]
    )
    fpayments = getGrouped(
        df=ffpayments, gcol="date", acol=["totalUsers", "eventCount"]
    )
    fpurchase = getGrouped(
        df=ffpurchase, gcol="date", acol=["totalUsers","itemsPurchased", "itemRevenue"]
    )

    journey = renamedColumns(
        getJourney(
        groupedflightsearch,
        groupedcontact,
        groupedpayments,
        groupedpurchase,
    ), columns
    )

    funnel = renamedColumns(
        getBookingFunnel(
        pagepath, 
        fsearch, 
        fdetails, 
        fpayments, 
        fpurchase,
        fhostname
    ), columns
    )

    journeyCols = [date, sector] + othercols
    funnelCol = [date] + path + othercols

    journey = journey.select(*journeyCols)
    filenamecus = "customerjourney"
    filenamebooking = "bookingfunnel"
    funnel = getConversionRatio(funnel.select(*funnelCol))


    DeltaLake(df= journey, catalog=catalog, filename=filenamecus)
    DeltaLake(df= funnel, catalog=catalog, filename=filenamebooking)

    return None

if __name__ == "__main__":
    main()