# Databricks notebook source
'''
Business logic to create the main sales consolidated file and individual tables for sales per sector required by business

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 08/14/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''

import pycountry
from time import time
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

FlightbookingPath = "/mnt/bronze/flight_booking" 
TicketsPath = "/mnt/bronze/ticket" 
DetailsPath = "/mnt/bronze/flight_detail" 

tickets_col = [
    "flight_booking_id", "airline", "flight_no", "flight_date", "flight_time", "sector", 
    "free_baggage", "currency", "fare","surcharge", "tax", "commission_amount", "ticket_no",
    "cashback","netfare", "agency_name", "issue_from", "class_id", "nationality", "title", "created_at"
]
flightbookings_col = ["id","booking_id", "pax_name", "country_id", "contact", "email", "NoofPax", "ip_address", "user_id", "created_at", "type"]
flightdetails_col = ["flight_booking_id", "FlightDate", "FlightTime", "SectorPair", "class_id", "created_at"]

FilterTickets = ((F.col("fare") != 0) & (F.col("surcharge") != 0)) #& (F.year("created_at") >= "2023")
Filteryear = F.year("created_at").between("2019", "2024")

def CombinedDf(flighbooking, tickets, flightdetails, year):
    """This combines the datasets for sales and customer report purpose"""
    df = tickets.drop("created_at").alias("df1").join(
    flighbooking.alias("df2"), (F.col("df1.flight_booking_id") == F.col("df2.booking_id")) & (~F.lower(F.col("pax_name")).endswith("test")), how="left"
    ).join(
        flightdetails.alias("df3"), (F.col("df2.id") == F.col("df3.flight_booking_id")) 
        & ((F.trim("df1.class_id") == F.trim("df3.class_id")))
        & ((F.lower("df1.sector") == F.lower("df3.sectorpair"))), how="left",
    ).select("df1.*",F.col("df2.created_at"), F.col("df2.pax_name").alias("PassengerName"), F.col("df2.country_id"), F.col("df2.contact"),
        F.col("df2.email"), F.col("df2.NoofPax").alias("NumberOFPassenger"), F.col("df2.ip_address"), F.col("df2.user_id"),
        F.col("df3.FlightDate"), F.col("df3.FlightTime"), F.col("df2.type")
    ).withColumn(
        "NetFare", F.when(
            F.col("currency") == "INR", F.col("netfare")*1.6
        ).otherwise(F.col("NetFare"))
    ).withColumn(
        "currency", F.when(
            F.col("currency") == "INR", "NPR"
        ).otherwise(F.col("currency"))
    ).withColumn(
        "Year", F.year("created_at")
    )

    print(f"Writting mlsales consolidated ..............>>>>>")

    df.drop_duplicates(
        ["flight_booking_id", "sector", "created_at", "ticket_no"]
    ).write.mode("overwrite").option("overwriteSchema", "true").partitionBy("Year").parquet("/mnt/silver/consolidated/mlsales")

    df = spark.read.parquet("/mnt/silver/consolidated/mlsales").filter(F.year("created_at") >= year)

    df = df.withColumn(
        "NetFare", F.round(F.col("netfare")*F.col("NumberOFPassenger"), 2)
    ).drop("ticket_no").drop_duplicates(["flight_booking_id", "sector"])

    return df

def get_country_name(abbreviation):
    """Converts the country abbrevation to ISO"""
    try:
        return pycountry.countries.get(alpha_2=abbreviation).name
    except AttributeError:
        return abbreviation

get_country_name_udf = udf(get_country_name, StringType())

def transColumn(df):
    Gateway = F.when(
        F.upper(F.col("agency_name")).contains("BUDDHA AIR") 
        | F.upper(F.col("agency_name")).contains("GW"),
        F.expr("substring_index(substring_index(agency_name, ' ', 3), ' ', -1)")
    ).otherwise(F.col("agency_name"))

    Male = ["Mr.", "Mstr", "Mstr.", "Mr"]
    Female = ["Miss", "Miss.", "Mrs", "Mrs.", "Ms", "Ms."]

    gender = F.when(
        F.col("title").isin(Male), "Male"
        ).when(
            F.col("title").isin(Female), "Female"
        ).otherwise("Other")

    df = df.withColumn(
        "created_date", F.date_format("created_at", "y-MM-d HH") 
    ).withColumn(
        "time", F.hour("created_at")
    ).withColumn(
        "type", F.when(
            F.trim(F.lower("type")) == "web", "Web"
        ).otherwise("Mobile")
    ).withColumn(
        "nationality", get_country_name_udf(df["nationality"])
    ).withColumn(
            "Gateway", Gateway
    ).withColumn(
    "Gender", gender
    ).distinct()

    print(f"Writting sales consolidated..............>>>>>")

    df.write.mode("overwrite").option("overwriteSchema", "true").parquet( "/mnt/silver/consolidated/sales")

    print(f"The Consolidated Sales data has been written to the Silver/Consolidated/sales folder")


    return spark.read.parquet("/mnt/silver/consolidated/sales")

def getReturnTicket(df, catalog_name=str):
    '''Return the return ticket data per booking_id'''
    tickettype = df.select("flight_booking_id", "sector").distinct().groupBy("flight_booking_id").agg(
        F.countDistinct("sector").alias("TicketType")
    ).withColumn(
        "ReturnTicket", F.when(F.col("TicketType") > 1, 'Yes').otherwise('No')
    )

    returnticket = df.alias("df1").join(
        tickettype.alias("df2"), F.col("df1.flight_booking_id") == F.col("df2.flight_booking_id"), how='inner'
    ).select("df1.*", "df2.ReturnTicket").orderBy("FlightDate").dropDuplicates(
        ["flight_booking_id", "created_date", "ReturnTicket"]
        
    )

    SelectCols = [F.col("created_date").alias("Date"), F.col("nationality").alias("Nationality"),F.col("class_id").alias("Class"), 
                F.col("currency").alias("Currency"), F.col("sector").alias("Sector"), F.col("Gateway"), F.col("Type").alias("DeviceType"),
                F.col("ReturnTicket")
    ]

    returnticket = returnticket.orderBy("FlightDate").dropDuplicates(["flight_booking_id", "created_date", "ReturnTicket"]).select(SelectCols)
 
    writeToCatalog(df=returnticket, catalog_name=catalog_name, table_name="returnticket", reparation=True, col= F.col("sector"))

    return None

def SectorGouped(df, year, catalog_name=str):
    """This aggregates the sales in different sectors"""
    groupeddf = df.filter(
        (F.year("created_date") >= year) 
        & ((F.col("type").isNotNull()) 
        | (F.col("type") != "NULL"))
    ).groupBy(
        "created_date","nationality", "currency", "sector", "Type", "class_id", "Gateway"
    ).agg(
            F.sum("netfare").alias("NetFare"),
            F.sum("NumberOFPassenger").alias("NoOfPassenger"),
            F.count_distinct("flight_booking_id").alias("TypeCount"),
            F.count("class_id").alias("ClassCount")
    )

    SelectCols = [F.col("created_date").alias("Date"), F.col("nationality").alias("Nationality"),F.col("class_id").alias("Class"), 
                  F.col("currency").alias("Currency"), F.col("sector").alias("Sector"), F.col("Gateway"), F.col("Type").alias("DeviceType"),
                  F.col("NetFare").alias("Revenue"), F.col("NoOfPassenger"), F.col("TypeCount").alias("TicketGenerated")
    ]

    groupeddf = groupeddf.withColumn(
            "created_date", F.date_format("created_date", "y-MM-d HH")
        ).withColumn(
            "created_date", F.to_timestamp("created_date", "y-MM-d HH")
        ).select(SelectCols)

    writeToCatalog(df=groupeddf, catalog_name=catalog_name, table_name="salesagg", reparation=True, col= F.hour("Date"))

    return groupeddf

def toSector(groupeddf, catalog_name=str):
    """Individual tables per sectors"""
    pdf = groupeddf.select(F.col("sector")).distinct().toPandas()
    SectorList = list(pdf.sector)

    for sector in SectorList:
        dfsec = groupeddf.filter(F.col("sector") == f"{sector}")
        sector_name = str(sector).replace("-", "_") 

        writeToCatalog(df=dfsec, catalog_name=catalog_name, table_name=sector_name, reparation=True, col= F.hour("Date"))

    return None

def writeToCatalog(df, catalog_name=str, table_name=str, selectCols=None, reparation=None, col=None):
    """This writes the tables to catalog"""
    if reparation:
        df.coalesce(1).repartition(
            col
        ).write.format('delta').mode(
            "overwrite"
        ).option(
            "overwriteSchema", "true"
        ).saveAsTable(
            f"{catalog_name}.{table_name}"
        )
    else:
        df.coalesce(1).write.format('delta').mode(
            "overwrite"
        ).option(
            "overwriteSchema", "true"
        ).saveAsTable(
            f"{catalog_name}.{table_name}"
        )

    print(f"The sector {table_name} has {df.count()} number of rows from 2023-1 to 2024-7 and has been written to catalog {catalog_name} as {table_name}")

    return None

def AvgTime(df, catalog=str, table_name=str):
    """This calcultes the time differece between booking time and flight time"""
    avgtime = df.select("flight_booking_id", "Nationality", "currency","sector","created_at", "FlightDate", "FlightTime").distinct()
    avgtime = avgtime.withColumn(
        "CreatedDate", F.date_format("created_at", "y-MM-d HH:MM")
    ).withColumn(
        "Created_date", F.to_timestamp("created_at", "y-MM-d HH:MM")
    ).withColumn(
        "FlightDateTime", F.concat(F.col("FlightDate"), F.lit(' '), F.col("FlightTime"))
    ).withColumn(
        "FlightDateTime", F.date_format("FlightDateTime", "y-MM-d HH:mm")
    ).withColumn(
        "DiffHrs", F.round((F.unix_timestamp(F.to_timestamp("FlightDateTime")) -  F.unix_timestamp(F.to_timestamp("Created_at")))/ (3600), 2)
    ).withColumn(
        "Nationality", F.when(
            F.col("Nationality").isin(["Nepal", "India"]), F.col("Nationality")
        ).otherwise(F.lit("Others"))
    ).dropDuplicates(["flight_booking_id", "FlightDate", "FlightTime"])

    df = avgtime.select(
        "Nationality", F.col("currency").alias("Currency"),F.col("sector").alias("Sector"), 
        F.to_timestamp(F.date_format("created_at", "y-MM-d HH")).alias("Date"), "DiffHrs", 
        F.round(F.col("DiffHrs")/24, 2).alias("DiffDay")
    )

    writeToCatalog(df=df, catalog_name=catalog, table_name=table_name, reparation=True, col=F.month("Date"))

def main():
    year = F.year(F.current_date()) - 2
    flighbooking = spark.read.parquet(FlightbookingPath).filter(Filteryear).select(flightbookings_col)
    tickets = spark.read.parquet(TicketsPath).filter(Filteryear).select(tickets_col)
    flightdetails = spark.read.parquet(DetailsPath).filter(Filteryear).select(flightdetails_col).distinct()

    df = CombinedDf(flighbooking, tickets, flightdetails, year)
    catalog_name = "analyticadebuddha.sectors"
    df = transColumn(df)
    getReturnTicket(df, catalog_name=catalog_name)
    groupeddf = SectorGouped(df, catalog_name=catalog_name, year=year)
    # toSector(groupeddf, catalog_name=catalog_name)
    table = "averagebookingtime"
    AvgTime(df, catalog=catalog_name, table_name=str(table))

    return None

if __name__== "__main__" :
   main()