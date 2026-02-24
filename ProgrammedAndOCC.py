# Databricks notebook source
import pyspark.sql.functions as F
from Fomodule import *

pathflightprogramme = "/mnt/fo/raw/daily_flight_programmes"
pathprogrammeitems = "/mnt/fo/raw/daily_flight_programme_items"
pathflightprogrammecrew = "/mnt/fo/raw/daily_flight_programme_crews"
pathcrewroletype = "/mnt/fo/raw/daily_flight_programme_standby_crews"
pathstatus = "/mnt/fo/raw/progress_statuses"
pathflightnumber = "/mnt/fo/raw/flight_numbers"
pathaircraftregistration = "/mnt/fo/raw/aircraft_registrations"

filtercurrentstatus =((F.col("default") == "1") & F.col("statusable_type").contains("daily-flight-programme-item"))

fullpath =  "/mnt/fo/processed/programmedAndActuals/"

colspro = [
    "Id", F.col("daily_flight_programme_id").alias("DailyFlightProgrammeId"), F.col("aircraft_registration_id").alias("AircraftRegistrationId"), 
    F.col("flight_number_id").alias("FlightNumberId"), F.col("flight_sector_pair_id").alias("FlightSectorPairId"), 
    "EstimatedDeparture", "EstimatedArrival",
    "EstimatedTime", "Remarks",F.col("prepared_by").alias("PreparedBy"), F.col("is_complete").alias("IsComplete"), 
    F.col("display_order").alias("DisplayOrder"), 
    F.col("created_at").alias("ProgrammedCreatedAt"), F.col("updated_at").alias("ProgrammedUpdatedAt") 
]

colsactual = [
    "Id", F.col("parent_id").alias("ParentId"), F.col("daily_flight_programme_id").alias("DailyFlightProgrammeId"), 
    F.col("aircraft_registration_id").alias("AircraftRegistrationId"), F.col("flight_number_id").alias("FlightNumberId"), 
    F.col("flight_sector_pair_id").alias("FlightSectorPairId"), "ActualDeparture", "ActualArrival", "ActualTime", "Remarks",
    F.col("prepared_by").alias("PreparedBy"), F.col("is_complete").alias("IsComplete"), F.col("display_order").alias("DisplayOrder"), 
    F.col("created_at").alias("ActualCreatedAt"), F.col("updated_at").alias("ActualUpdatedAt") 

]

def getProgrammedStatus(programmed, status):
    '''
    '''
    programmedstatus = programmed.alias("df1").join(
        status.alias("df2"), F.col("df1.Id") == F.col("df2.ParentId"), how="left"
    ).select("df1.*", "df2.Status", "df2.Comments")

    return programmedstatus

def getProgrammed(programmeditemsdf, programmedfordate):
    '''
    '''
    programmed = programmeditemsdf.filter(
        (F.col("type") != "occ") 
    ).withColumns(
        {
            "EstimatedDeparture": 
                
                (
                    F.split((F.split("etd", "'")[1]), " ")[1]
                ),
            "EstimatedArrival":
                (
                    F.split((F.split("eta", "'")[1]), " ")[1]
                ),
            "EstimatedTime": 
                (
                    F.to_unix_timestamp(
                        F.to_timestamp("EstimatedArrival", "HH:mm:ss")
                    ) - F.to_unix_timestamp(
                        F.to_timestamp("EstimatedDeparture", "HH:mm:ss")
                    )
                )/60
        }
    ).select(*colspro)
    
    joincon =  F.col("df1.DailyFlightProgrammeId") == F.col("df2.id")
    selectcon = [F.col("df1.*"), F.col("df2.Date"), "df2.FprNo"]

    programmed = getfordate(df1=programmed, df2=programmedfordate, joincon=joincon, selectcon=selectcon)

    return programmed

def getStatus(occ, status):
    '''
    '''
    statusandcomment = occ.alias("df1").join(
        status.alias("df2"), F.trim("df1.id") == F.trim("df2.StatusableId"), how="inner"
    ).select("df1.ParentId", "df2.Status", "df2.Comments")

    # programmedstatus_ = programmed_.alias("df1").join(
    #     statusandcomment.alias("df2"), F.trim("df1.Id") == F.trim("df2.ParentId"), how="left"
    # ).select("df1.*", "df2.Status", "df2.Comments")

    return statusandcomment

def getOCC(programmeditemsdf, programmedfordate):
    '''
    '''
    actual = programmeditemsdf.filter(
        (F.col("type") == "occ") 
    ).withColumns(
        {
            "ActualDeparture": 
                
                (
                    F.split((F.split("atd", "'")[1]), " ")[1]
                ),
            "ActualArrival":
                (
                    F.split((F.split("ata", "'")[1]), " ")[1]
                ),
            "ActualTime": 
                (
                    F.to_unix_timestamp(
                        F.to_timestamp("ActualArrival", "HH:mm:ss")
                    ) - F.to_unix_timestamp(
                        F.to_timestamp("ActualDeparture", "HH:mm:ss")
                    )
                )/60
        }
    ).select(*colsactual)

    joincon =  F.col("df1.DailyFlightProgrammeId") == F.col("df2.id")
    selectcon = [F.col("df1.*"), F.col("df2.Date"), "df2.FprNo"]

    actual = getfordate(df1=actual, df2=programmedfordate, joincon=joincon, selectcon=selectcon)

    return actual

def main():
    '''
    '''
    flightnumber = getAlias(filterdeleted(spark.read.format("delta").load(pathflightnumber)))
    name  = getAlias(filterdeleted(spark.read.format("delta").load(pathaircraftregistration)))
    programmedfordate = getAlias(filterdeleted(spark.read.format("delta").load(pathflightprogramme)))
    status1 = getAlias(filterdeleted(spark.read.format("delta").load(pathstatus).filter(filtercurrentstatus)))

    programmeditemsdf = filterdeleted(spark.read.format("delta").load(pathprogrammeitems))
    programcrew = getAlias(filterdeleted(spark.read.format("delta").load(pathflightprogrammecrew)))
    standbycrew = getAlias(filterdeleted(spark.read.format("delta").load(pathcrewroletype)))

    occ = getFlightInfo(getOCC(programmeditemsdf, programmedfordate), flightnumber=flightnumber, flightname=name)
    status = getStatus(occ, status1)
    programmed = getFlightInfo(getProgrammed(programmeditemsdf, programmedfordate), flightnumber=flightnumber, flightname=name)

    programmedstatus = getProgrammedStatus(programmed, status)

    config = {
        "programmeditems": programmedstatus,
        "occitems": occ,
        "standby": standbycrew,
        "programcrew": programcrew,
        "status": status
    }

    tolake(config=config, fullpath=fullpath)

    return None

if __name__ == "__main__":
    main()