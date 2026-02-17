# Databricks notebook source
'''
Business logic to create the main sales consolidated file and individual tables for sales per sector required by business

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 09/17/2024
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
CommissionPath = "analyticadebuddha.businesstable.gateway_comission"

tickets_col = [
    "flight_booking_id","first_name","last_name", F.date_format(F.to_date("flight_date", "dd-MMM-yyyy"), "y-MM-d").alias("FlightDate"), 
    "flight_time", "sector", "free_baggage", "currency", "fare","surcharge", "tax", "commission_amount", "ticket_no", "cashback",
    "discount", "child_discount","netfare", "agency_name", "issue_from", "class_id", "title", "created_at", "nationality", "pax_no"
]

casecountry = F.when(
    F.col("nationality").contains('[" "]'), F.col("country")
).otherwise(F.col("nationality"))

case = (
    F.col("contact").isNull()
) & (
    F.col("email").isNull()
) & (
    (F.col("user_id") == 0) 
    | ( F.col("user_id").isNull())
)

flightbookings_col = ["id","booking_id", "country_id", "country", "contact", "email", "NoofPax", "ip_address", "user_id", "created_at", "type", "pax_name"]
flightdetails_col = ["flight_booking_id", "FlightDate", "FlightTime", "SectorPair", "class_id", "created_at"]

FilterTickets = ((F.col("fare") != 0) & (F.col("surcharge") != 0))
CurrentYear = F.year(F.current_date())
Filteryear = F.year("created_at").between("2019", CurrentYear)

def CalRevenue(df):
    df1 = df.withColumn(
        "cashback", F.when(
            F.col("cashback").isNull(), 0
        ).otherwise(F.col("cashback"))
    ).withColumn(
        "discount", F.when(
            F.col("discount").isNull(), 0
        ).otherwise(F.col("discount"))
    ).withColumn(
        "child_discount", F.when(
            F.col("child_discount").isNull(), 0
        ).otherwise(F.col("child_discount"))
    ).withColumn(
        "DisAmount", (F.col("cashback") + F.col("discount") +  F.col("child_discount"))
    )
    
    agg =df1.groupBy(
        "flight_booking_id","sector", "currency", "class_id"
    ).agg(
        F.round(F.sum("netfare"), 2).alias("GrossFare"),
        F.round(F.sum("DisAmount"), 2).alias("SubAmount"),
        F.max("pax_no").alias("NumberOFPassenger")
    ).withColumn(
        "netfare", F.col('GrossFare') - F.col("SubAmount")
    )

    ticket = agg.alias("df1").join(
        df.drop("netfare").alias("df2"), (F.col("df1.flight_booking_id") == F.col("df2.flight_booking_id")) 
        & (F.trim("df1.sector") == F.trim("df2.sector")) & (F.col("df1.currency") == F.col("df2.currency")) 
        & (F.col("df1.class_id") == F.col("df2.class_id")), how ="inner"
    ).select("df1.netfare", "df1.SubAmount", "df1.NumberOFPassenger","df2.*").dropDuplicates(["flight_booking_id","sector", "currency", "class_id"])

    return ticket

def getConsolidatedBooking(flighbookings):
    """
    """
    void  = flighbookings.filter(case)
    flighbooking = flighbookings.filter(~case)

    dup = flighbooking.alias("df1").join(
        void.alias("df2"), F.col("df1.booking_id") == F.col("df2.booking_id"), how = "inner"
    ).select("df1.*")

    left = void.alias("df1").join(
        flighbooking.alias("df2"), F.col("df1.booking_id") == F.col("df2.booking_id"), how = "left_anti"
    ).select("df1.*")

    left1 = flighbooking.alias("df1").join(
        void.alias("df2"), F.col("df1.booking_id") == F.col("df2.booking_id"), how = "left_anti"
    ).select("df1.*")

    bookings = dup.union(left).union(left1)

    return bookings

def CombinedDf(flighbooking, tickets, flightdetails):
    """This combines the datasets for sales and customer report purpose"""
    df = tickets.alias("df1").join(
        flighbooking.alias("df2"),
            (
                F.col("df1.flight_booking_id") == F.col("df2.booking_id")
            ) & (
                ~((F.lower(F.col("pax_name")).endswith("test")) & (F.col("first_name").isNotNull()))
            ), how="left"
        ).select(
            "df1.*",  F.trim("df2.country").alias("country") ,F.col("df2.country_id"), F.col("df2.contact"),
            F.col("df2.email"), F.col("df2.ip_address"), F.col("df2.user_id"), 
            F.col("df2.id").alias("id2"), F.col("df2.type")

        ).withColumnsRenamed(
            {
                "first_name": "PassengerName", 
                "last_name":"LastName",
                "flight_time": "FlightTime",
            }
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
        ).withColumn(
            "nationality", casecountry
        ).dropDuplicates(
            subset = ["flight_booking_id", "sector", "currency", "class_id", "FlightDate", "FlightTime"]
        )

    return df

def get_country_name(abbreviation):
    """Converts the country abbreviation (alpha_2) to full country name."""
    try:
        country = pycountry.countries.get(alpha_2=abbreviation)
        return country.name if country else abbreviation
    except (LookupError, AttributeError, TypeError):
        return abbreviation

get_country_name_udf = F.udf(get_country_name, StringType())

def transColumn(df, commission):
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
        "nationality", get_country_name_udf(df["nationality"])
    ).withColumn(
            "Gateway", Gateway
    ).withColumn(
        "Gateway", F.when(
        F.upper(F.trim("agency_name")).endswith("BUDDHA AIR NMB GW"), "wechat"
    ).otherwise(F.col("Gateway"))
    ).withColumn(
    "Gender", gender
    ).distinct()

    df = df.alias("df1").join(
        commission.alias("df2"), F.trim(F.lower("df1.Gateway")) == F.trim(F.lower("df2.gateways")), how="left"
    ).select("df1.*", "df2.commission").withColumn(
        "mlnetfare", F.col("netfare")
    ).withColumn(
        "commission", F.when(
            F.col("commission").isNull(), 0
        ).otherwise(F.col("commission"))
    ).withColumn(
        "netfare", F.col("netfare") - (F.col("netfare") * (F.col("commission")/100))
    )

    return df

def getReturnTicket(df):
    '''Return the return ticket data per booking_id'''
    tickettype = df.select("flight_booking_id", "sector").distinct().groupBy("flight_booking_id").agg(
        F.countDistinct("sector").alias("TicketType")
    ).withColumn(
        "ReturnTicket", F.when(F.col("TicketType") > 1, 'Yes').otherwise('No')
    )

    returnticket = df.alias("df1").join(
        tickettype.alias("df2"), F.col("df1.flight_booking_id") == F.col("df2.flight_booking_id"), how='inner'
    ).select("df1.*", "df2.ReturnTicket").orderBy("FlightDate").dropDuplicates(
       ["flight_booking_id", "sector", "currency", "class_id", "FlightDate", "FlightTime"]
        
    )

    return returnticket

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

    return None

def AvgTime(df, year, catalog=str, table_name=str):
    """This calcultes the time differece between booking time and flight time"""
    avgtime = df.select("flight_booking_id","sector","created_at", "FlightDate", "FlightTime").distinct()
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
    
    ).dropDuplicates(["flight_booking_id", "sector", "DiffHrs"])

    timediff = avgtime.select(
        "flight_booking_id", "DiffHrs","FlightDateTime", F.round(F.col("DiffHrs")/24, 2).alias("DiffDay"), "sector"
    )

    avgtimediff = df.alias("df1").join(
        timediff.alias("df2"),( F.col("df1.flight_booking_id") == F.col("df2.flight_booking_id")) & (F.col("df1.sector") == F.col("df2.sector"))
    ).select("df1.*", "df2.DiffHrs", "df2.DiffDay", "df2.FlightDateTime").dropDuplicates(["flight_booking_id", "sector", "currency", "class_id", "FlightDate", "FlightTime"])

    SelectCols = [F.col("flight_booking_id").alias("FlightBookingId"), F.date_format("created_date", "y-MM-d HH:MM").alias("Date"), 
                "FlightDateTime", F.col("nationality").alias("Nationality"), F.col("class_id").alias("Class"), F.col("currency").alias("Currency"), 
                F.col("sector").alias("Sector"), F.col("Gateway"), F.col("Gender"), F.col("Type").alias("DeviceType"), F.col("NetFare").alias("Revenue"),
                F.col("NumberOfPassenger"), F.col("ReturnTicket"),F.col("DiffHrs"), F.col("DiffDay"), F.col("user_id").alias("UserId"), "commission", "SubAmount"
            ]

    finaldf = avgtimediff.select(SelectCols)

    print(f"Writing SalesMart to {catalog} as {table_name} which contains the data since {year}")
    writeToCatalog(finaldf, catalog_name=catalog, table_name=table_name, selectCols=None, reparation=True, col=F.col("Sector"))

    return None

def Mlbi(flighbooking, tickets, flightdetails, commission, table=None, ml=False, year=None):
    if ml:
        df = CombinedDf(flighbooking, tickets, flightdetails)
        df = transColumn(df, commission)
        print(f"Writting mlsales consolidated ..............>>>>>")
        df.orderBy("created_at").drop_duplicates(["flight_booking_id", "sector", "created_at", "ticket_no"]
        ).drop("ticket_no").write.mode("overwrite").option("overwriteSchema", "true").partitionBy("Year").parquet("/mnt/silver/consolidated/mlsales")
    else:
        tickets =CalRevenue(df=tickets)
        flighbooking = getConsolidatedBooking(flighbookings=flighbooking)
        df = CombinedDf(flighbooking, tickets, flightdetails)
        catalog_name = "analyticadebuddha.reporting"
        df = transColumn(df, commission)

        df.write.mode("overwrite").option("overwriteSchema", "true").partitionBy("Year").parquet("/mnt/silver/consolidated/sales")
        df  = spark.read.parquet("/mnt/silver/consolidated/sales").filter(F.year("created_at") >= year)
        returnticket = getReturnTicket(df)
        df= AvgTime(df=returnticket, year=year, catalog=catalog_name, table_name=str(table))

def main():
    year = CurrentYear - 2
    flighbooking = spark.read.parquet(FlightbookingPath).filter(Filteryear).select(flightbookings_col)
    tickets = spark.read.parquet(TicketsPath).filter(Filteryear).select(tickets_col)
    flightdetails = spark.read.parquet(DetailsPath).filter(Filteryear).select(flightdetails_col).distinct()
    commission = spark.table(CommissionPath)
    table="sales"
    Mlbi(flighbooking, tickets, flightdetails, commission, table=table, ml=False, year=year)
    # Mlbi(flighbooking, tickets, flightdetails, commission, table=table, ml=True, year=year)

if __name__ == "__main__":
    main()