# Databricks notebook source
import pyspark.sql.functions as F
from Fomodule import *

pathflightlog = "/mnt/fo/raw/flight_logs"
pathcrewdetails = "/mnt/fo/raw/flight_log_crew_details"
pathflightdetails = "/mnt/fo/raw/flight_log_flight_details"
pathsectorpairs = "/mnt/fo/raw/flight_sector_pairs"
pathcrewhours = "/mnt/fo/raw/daily_crew_flight_hours"

pathflightnumber = "/mnt/fo/raw/flight_numbers"
pathaircraftregistration = "/mnt/fo/raw/aircraft_registrations"

pathsectorpairs = "/mnt/fo/raw/flight_sector_pairs"
pathsectorlist = "/mnt/fo/raw/flight_sector_lists"

fullpath =  "/mnt/fo/processed/actual/"

flightdetailscols = [
    "Id", F.col("flight_log_id").alias("FlightLogId"), F.col("flight_number_id").alias("FlightNumberId"), 
    F.col("flight_sector_pair_id").alias("FlightSectorPairId"), F.col("chocks_off").alias("ChocksOff"), 
    F.col("chocks_on").alias("ChocksOn"),"BlockTime", F.col("pay_time").alias("PayTime"), F.col("take_off").alias("TakeOff"), 
    F.col("touch_down").alias("TouchDown"), "AirTime", "ChockoffTOtakeoff", 
    "TouchDownToChockOn", "Landings", F.date_format("chocks_off", "yyyy-MM-dd").alias("FlightDate"),
    F.col("created_at").alias("CreatedAt"), F.col("updated_at").alias("UpdatedAt")
]

flightinfocol = ["AircraftRegistrationId","FlightNumberId","FlightNumber", "FlightName", "AircraftTypeId", "Type", "IsApproved"]

ChockoffTOtakeoff = (F.unix_timestamp(F.col("take_off")) - F.unix_timestamp(F.col("chocks_off")))/60
BlockTime =  (F.unix_timestamp(F.col("chocks_on")) - F.unix_timestamp(F.col("chocks_off")))/60
AirTime = (F.unix_timestamp(F.col("touch_down")) - F.unix_timestamp(F.col("take_off")))/60
TouchDownToChockOn = (F.unix_timestamp(F.col("chocks_on")) - F.unix_timestamp(F.col("touch_down")))/60

deleteddate =  F.col("deleted_at").isNull()

def getActualFlightDetails(flightdetails, flightlog, flightnumber, name):
    '''
    '''
    detailsdf = flightdetails.withColumns(
        {   "ChockoffTOtakeoff": ChockoffTOtakeoff,
            "BlockTime": BlockTime,
            "AirTime": AirTime,
            "TouchDownToChockOn":TouchDownToChockOn 
        }
    ).select(*flightdetailscols)

    detailsdf1 = detailsdf.alias("df1").join(
        flightlog.alias("df2"), F.col("df1.FlightLogId") == F.col("df2.id"), how="inner"
    ).select(
        "df1.*","df2.LogPageNo", F.col("df2.TlpNo"), 
        "df2.AircraftRegistrationId", "df2.Type", "df2.IsApproved"
    ).distinct()

    actualitem = getFlightInfo(itemdf=detailsdf1, flightnumber=flightnumber, flightname=name)

    return actualitem

def getSectorInfo(actualsector, sectorlis):
    '''
    '''
    sectors = actualsector.alias("df1").join(
        sectorlis.alias("df2"), F.col("df1.from_sector_id") == F.col("df2.id"), how="left"
    ).join(
        sectorlis.alias("df3"), F.col("df1.to_sector_id") == F.col("df3.id"), how="left"
    ).select(
        F.col("df1.Id").alias("SectorPairId"), 
        F.col("df1.from_sector_id").alias("FromSectorId"),
        F.col("df1.to_sector_id").alias("ToSectorId"),
        F.col("df2.sector_code").alias("FromSectorCode"),
        F.col("df3.sector_code").alias("ToSectorCode"),
        F.col("df2.sector_name").alias("FromSectorName"),
        F.col("df3.sector_name").alias("ToSectorName")
    )

    return sectors

def main():
    '''
    '''
    flightlog = getAlias(filterdeleted(spark.read.format("delta").load(pathflightlog)))
    crewdetails = getAlias(filterdeleted(spark.read.format("delta").load(pathcrewdetails)))
    flightdetails = filterdeleted(spark.read.format("delta").load(pathflightdetails))
    crewhour = getAlias(filterdeleted(spark.read.format("delta").load(pathcrewhours)))

    flightnumber = getAlias(filterdeleted(spark.read.format("delta").load(pathflightnumber)))
    name  = getAlias(filterdeleted(spark.read.format("delta").load(pathaircraftregistration)))

    actualsector = spark.read.format("delta").load(pathsectorpairs).filter(deleteddate).select("id", "from_sector_id", "to_sector_id").distinct()
    sectorlis = spark.read.format("delta").load(pathsectorlist).filter(deleteddate).select("id", "sector_code", "sector_name", "is_international").distinct()
    sectorinfo = getSectorInfo(actualsector, sectorlis)
    actutal = getActualFlightDetails(flightdetails, flightlog, flightnumber, name)

    config = {
        "sectorinfo": sectorinfo,
        "actualflightitems": actutal,
        "crewdetails": crewdetails
    }

    tolake(config=config, fullpath=fullpath)

    return None

if __name__ == "__main__":
    main()