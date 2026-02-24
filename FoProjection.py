# Databricks notebook source
import pyspark.sql.functions as F
from Fomodule import *

pathcrewhour = "/mnt/fo/raw/daily_crew_flight_hour_projections"
pathprojectioncrew = "/mnt/fo/raw/flight_programme_projection_crews"
pathprojectionitem ="/mnt/fo/raw/flight_programme_projection_items"
pathstandbycrew = "/mnt/fo/raw/flight_programme_projection_stand_by_crews"
pathprogram = "/mnt/fo/raw/flight_programme_projections"
pathflightnumber = "/mnt/fo/raw/flight_numbers"
pathaircraftregistration = "/mnt/fo/raw/aircraft_registrations"

fullpath =  "/mnt/fo/processed/projected/"

def getProjectionitems(projectionitem, flightnumber, name):
    '''
    '''
    projectionitemdf = projectionitem.withColumns(
        {
            "EstimatedDepature": F.split(F.split("etd", "'")[1], " ")[1],
            "EstimatedArrival": F.split(F.split("eta", "'")[1], " ")[1],
            "EstimatedAirTime": (
                F.to_unix_timestamp(
                    F.to_timestamp(F.col("EstimatedArrival"), "HH:mm:ss")
                ) 
                - F.to_unix_timestamp(
                    F.to_timestamp(F.col("EstimatedDepature"), "HH:mm:ss")
                )
            )/60
        }
    ).drop("Etd", "Eta")

    projectionitemdf = projectionitemdf.alias("df1").join(
        flightnumber.alias("df2"), F.col("df1.FlightNumberId") == F.col("df2.id"), how="left"
    ).join(
        name.alias("df3"), F.col("df1.AircraftRegistrationId") == F.col("df3.id"), how="left"
    ).select("df1.*", "df2.FlightNo", "df3.Name", "df3.AircraftTypeId", "df3.IsActive").distinct()

    return projectionitemdf

def main():
    '''
    '''
    joincon =  (F.col("df1.FlightProgrammeProjectionId") == F.col("df2.id"))
    selectcon = ["df1.*", F.col("df2.Date").alias("ForDate"), "df2.FprpNo"]
    
    flightnumber = getAlias(filterdeleted(spark.read.format("delta").load(pathflightnumber)))
    name  = getAlias(filterdeleted(spark.read.format("delta").load(pathaircraftregistration)))
    projectionitem = getAlias(filterdeleted(spark.read.format("delta").load(pathprojectionitem))) 
    crewprojection = getAlias(filterdeleted(spark.read.format("delta").load(pathprojectioncrew))) 
    standby = getAlias(filterdeleted(spark.read.format("delta").load(pathstandbycrew)))
    projectionfordate = getAlias(filterdeleted(spark.read.format("delta").load(pathprogram))) 
    projectedhour = getAlias(filterdeleted(spark.read.format("delta").load(pathcrewhour)))
    projectionitemdf = getProjectionitems(projectionitem, flightnumber, name)
    
    projectionitemfor = getfordate(df1=projectionitemdf, df2=projectionfordate, joincon=joincon, selectcon=selectcon)

    config = {
        "Crewprojection":crewprojection,
        "Projectionitem":projectionitemfor,
        "Projectionsandby":standby,
        "Projectedhour":projectedhour
    }

    tolake(config=config, fullpath=fullpath)

    return None

if __name__ == "__main__":
    main()