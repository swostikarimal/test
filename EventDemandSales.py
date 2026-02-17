# Databricks notebook source
'''
Logic to create the demand per yyyy-mm-dd HH:MM for tickets sales and the events on ticket sold and flight date

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 09/12/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''
import pyspark.sql.functions as F
from pyspark.sql.window import Window

DetailsPath = "/mnt/bronze/flight_detail" 
SearchPath = "/mnt/bronze/flight_searche"
SalesPath = "/mnt/silver/consolidated/mlsales"
FlightbookingPath = "/mnt/bronze/flight_booking"
EventPath= "analyticadebuddha.businesstable.calendar"

def getPlead(details, booking, search):
    """Return the possiable lead"""
    plead1= details.drop("created_at").alias("df1").join(
        booking.alias("df2"), F.col("df1.flight_booking_id") == F.col("df2.ID"), how="inner"
    ).select(F.col("df1.id"), F.col("df1.SectorPair"), F.col("df2.created_at"))

    plead2 = search.withColumn(
        "SectorPair", F.concat(F.col("sector_from"), F.lit("-"), F.col("sector_to"))
    ).select("id", "SectorPair", "created_at")

    plead = plead1.union(plead2)

    plead = plead.withColumn(
        "date", F.date_format("created_at", "y-MM-d HH:mm")
    ).withColumn(
        "created_at_epoch", F.unix_timestamp(F.to_timestamp("date"))
    )

    return plead

def getDemand(plead, sales):
    '''Returns the demand based on past 15 minitues activities'''
    time_window_duration = 900
    window_spec = Window.partitionBy("SectorPair").orderBy(F.asc("created_at_epoch")).rangeBetween(-time_window_duration, 0)

    demand = plead.withColumn(
        "Demand", F.count('id').over(window_spec)
    )

    sold = sales.withColumn(
        "NetFareBeforeTax", (F.col("fare") + F.col("surcharge"))  * F.col("NumberOfPassenger")
    ).withColumn(
        "GrossFare", F.round(F.col("NetFare")  * F.col("NumberOfPassenger"), 2)
    ).select(
        "flight_booking_id",
        "sector",
        "created_at",
        "FlightDate",
        "FlightTime",
        "Currency",
        "fare",
        "surcharge",
        "GrossFare",
        "NetFareBeforeTax",
        "class_id",
        "NumberOfPassenger"
    ).withColumn(
        "Date", F.date_format("created_at", "y-MM-d HH:mm")
    )

    ActualDemand = sold.alias("df1").join(
        demand.alias("df2"),(F.col("df1.date") == F.col("df2.date")) & (F.col("df1.sector") == F.col("df2.sectorpair")), how="left"
    ).select("df1.*", "df2.Demand").withColumn(
        "ActualDemand", F.when(
            F.col("NumberOfPassenger") > 1, (F.col("NumberOfPassenger") -1) + F.col("Demand")
        ).otherwise(F.col("Demand"))
    ).drop_duplicates(["flight_booking_id", "sector", "FlightDate"])#.drop("flight_booking_id")

    return ActualDemand

def addEvents(demand, calendar):
    """Returns sales data set with events"""
    cols = ["df1.*", F.col("df2.EventStatus").alias("EventStatusOnTicketDate"), F.col("df2.tithi").alias("TithiOnTicketDate"), F.col("df2.is_public_holiday").alias("IsPublicHolidayOnTicketDate"), F.col("df3.EventStatus").alias("EventStatusOnFlightDate"), F.col("df3.tithi").alias("TithiOnFlighttDate"), F.col("df3.is_public_holiday").alias("IsPublicHolidayOnFlightDate")]

    SalesEvent = demand.alias("df1").join(
        F.broadcast(calendar).alias("df2"), F.date_format("df1.date", "y-MM-d") == F.col("df2.date"), how="left"
    ).join(
        F.broadcast(calendar).alias("df3"), F.col("df1.FlightDate") == F.col("df3.date"), how="left"
    ).select(cols).withColumn("Year", F.year("created_at"))

    SalesEvent.coalesce(1).write.mode("overwrite").option("overwriteSchema", "true").partitionBy("Year").parquet("/mnt/silver/consolidated/SalesEvent")

    return None

def main():
    details = spark.read.parquet(DetailsPath)
    search = spark.read.parquet(SearchPath)
    sales = spark.read.parquet(SalesPath)
    lead = sales.select("flight_booking_id","sector", "created_at").drop_duplicates(["flight_booking_id","sector", "created_at"])
    booking = spark.read.parquet(FlightbookingPath)
    events = spark.table(EventPath)

    plead = getPlead(details, booking, search)
    demand = getDemand(plead, sales)
    SalesEvent = addEvents(demand, calendar=events)

    return None

if __name__ == "__main__":
    main()