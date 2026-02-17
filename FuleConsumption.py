# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import datetime

pathlogdetails = "/mnt/rawmro/LogDetails"
pathlog = "/mnt/rawmro/Logs"
pathemployees = "/mnt/rawmro/Employees"
pathcrew = "/mnt/rawmro/LogCrews"
pathstation = "/mnt/rawmro/Stations"
pathfosector = "analyticadebuddha.flightoperation.sectorinfo"

currentYear = datetime.datetime.now().year
previousYear = f"{currentYear - 1}-01-01T00:00:00.000+00:00"

filterdeleted = (F.col("isDeleted") == "false")
filterlogdetails =  ((F.col("TakeOffLocalDateTime") >= previousYear) & filterdeleted)
filtercrewlog =  (F.col("CreationTime") >= previousYear) & filterdeleted
filterlogs = ((F.year("Date") >= (currentYear - 1)) & (F.col("LogTypeId") == "1") & filterdeleted)

colslog = ["Id", "Date", F.col("LogText").alias("FlightName"), "AircraftId", "PilotId", "CopilotId", "LogNo"]
colscrew = ["Id", "LogId", "CrewId", "DutyAsId"]
colsemployees = [
    "Id", "EmployeeNumber", "FullName", "DesignationId", "DateOfBirth", "LicenseNumber", "Gender", 
    "IsExpat", "IsWorking", "IsActive", "IsContracted", "JoinDate", "LeaveDate", "Nationality"
]
colslogdetails = [
    "Id", "LogId", "SourcePlaceId", "DestinationId", "FlightNo", "TakeoffLocalDateTime", "TakeOffUniverseDateTime", "TouchDownLocalDateTime", 
    "TouchDownUniverseDateTime","Pax", "CargoWeight", "TakeOffWeight", "Landings", "PaxAdult", "PaxChild", "PaxInfant", "FuelOnDeparture", 
    "FuelOnAdded", "FuelOnArrival","FuelConsumption", "BlockTime", "TimeInAir", "CreationTime", "IsDiverted", "ActualDestinationId", "PFIDoneById"
]

colspassenger = [
    "id", F.date_format("TakeoffLocalDateTime", "yyyy-MM-dd").alias("FlightDate"), 
    F.regexp_replace(F.col("SectorPair"), " - ", "-").alias("SectorPair"), "PaxAdult", "PaxChild", "PaxInfant", "FlightNo"
]


hashcolumn = [
    F.coalesce(F.col("FlightNo").cast("string"), F.lit("")),
    F.coalesce(F.col("FlightSectorPairId").cast("string"), F.lit("")),
    # F.coalesce(F.col("FlightName").cast("string"), F.lit("")),
    F.coalesce(F.date_format(F.col("TakeoffLocalDateTime"), "yyyy-MM-dd"), F.lit(""))
]

def toDouble(df):
    '''
    '''
    fields = df.schema.fields
    for field in fields:
        if (isinstance(field.dataType, DecimalType)):
            colname = field.name
            df = df.withColumn(colname, F.col(colname).cast("double"))
    
    return df

def getCommonSectors(sector, station, sectorfo):
    '''
    To Find Common Key to connect with the FO sector if needed
    '''
    fromsec = sector.alias("df1").join(
        station.alias("df2"), F.col("df1.SourcePlaceId") == F.col("df2.id"), how="left"
    ).select("df1.SourcePlaceId", "df1.DestinationId",F.col("df2.ShortName").alias("From"))

    mrosector = fromsec.alias("df1").join(
        station.alias("df2"), F.col("df1.DestinationId") == F.col("df2.id"), how="left"
    ).select("df1.*", F.col("ShortName").alias("To")).withColumns(
        {
            "MroPairId": F.concat(F.col("SourcePlaceId"), F.lit(" - "), F.col("DestinationId")),
            "SectorPair": F.concat(F.col("From"), F.lit(" - "), F.col("To"))
        }
    )

    common = mrosector.alias("df1").join(
        sectorfo.alias("df2"), F.col("df1.SectorPair") == F.col("df2.SectorPair"), how="left"
    ).select("df1.*", F.col("df2.SectorPairId").alias("FoPairId")).select("MroPairId", "SectorPair", "FoPairId", "SourcePlaceId", "DestinationId")

    return common

def getInfo(logdetails, logs, common):
    '''
    '''
    logdetails = logdetails.alias("df1").join(
        logs.alias("df2"), F.col("df1.LogId") == F.col("df2.Id"), how="left"
    ).select("df1.*", "df2.FlightName", "df2.AircraftId").dropDuplicates(subset = ["id"])

    logdetails_ = logdetails.alias("df1").join(
        F.broadcast(common).alias("df2"), (
            (F.col("df1.SourcePlaceId") == F.col("df2.SourcePlaceId")) 
            & (F.col("df1.DestinationId") == F.col("df2.DestinationId"))
        ), how="left"
    ).select("df1.*", F.col("df2.FoPairId").alias("FlightSectorPairId"), "df2.SectorPair")

    logdetailsKey = logdetails_.withColumn(
        "Key", F.sha2(F.concat_ws("||", *hashcolumn), 256)
    )

    return logdetailsKey

def getTotalPassenger(loginfo):
    '''
    '''
    passenger = loginfo.select(*colspassenger).withColumns(
        {
            "PaxAdult": F.when(
                F.col("PaxAdult") >= 1000, F.round(F.col("PaxAdult")/100)
            ).when(
                (F.col("PaxAdult") < 1000) & (F.col("PaxAdult") >= 100), F.round(F.col("PaxAdult")/10)
            ).otherwise(F.col("PaxAdult")),
            "PaxChild": F.when(
                F.col("PaxChild") >= 1000, F.round(F.col("PaxChild")/100)
            ).when(
                (F.col("PaxChild") < 1000) & (F.col("PaxChild") >= 100), F.round(F.col("PaxChild")/10)
            ).otherwise(F.col("PaxChild"))
        }
    ).groupBy("FlightDate", "SectorPair").agg(
        F.sum("PaxAdult").alias("TotalAdult"), F.sum("PaxChild").alias("TotalChild"), 
        F.count("FlightNo").alias("TotalFlight")
    ).write.mode("overwrite").option("mergeSchema", "true").saveAsTable("analyticadebuddha.reporting.totalpassenger")

    return None

logdetails = toDouble(spark.read.format("delta").load(pathlogdetails).filter(filterlogdetails).select(*colslogdetails))
crewlog = spark.read.format("delta").load(pathcrew).filter(filtercrewlog).select(*colscrew)
employees = spark.read.format("delta").load(pathemployees).filter(filterdeleted).select(*colsemployees)
logs = spark.read.format("delta").load(pathlog).filter(filterlogs).select(*colslog)

station = spark.read.format("delta").load(pathstation).filter(filterdeleted).select(["Id", "ShortName"]).dropDuplicates(subset = ["id"])
sector = logdetails.select("SourcePlaceId", "DestinationId").distinct()
sectorfo = spark.table(pathfosector).withColumn("SectorPair", F.concat(F.col("FromSectorCode"), F.lit(" - "), F.col("ToSectorCode")))
common = getCommonSectors(sector, station, sectorfo)
loginfo = getInfo(logdetails, logs, common)

getTotalPassenger(loginfo)

# COMMAND ----------


toDouble(spark.read.format("delta").load(pathlogdetails)).filter(F.col("logId") == "118395").display()

# COMMAND ----------

from pyspark.sql.window import Window

winfunc = Window.partitionBy("FlightName", "AircraftId").orderBy(F.col("TakeoffLocalDateTime"))

# COMMAND ----------

cols = [
    "Id", "LogId", "FlightNo", "TakeoffLocalDateTime", "TouchDownLocalDateTime", "Pax", 
    "CargoWeight", "TakeOffWeight", "Landings", "PaxAdult", "PaxChild", "FuelOnDeparture", 
    "FuelOnAdded", "FuelOnArrival", "FuelConsumption", "BlockTime","TimeInAir", "CreationTime", 
    "IsDiverted", "ActualDestinationId", "PFIDoneById", "FlightName", "AircraftId", "FlightSectorPairId", 
    "SectorPair", "Key"
]
# .filter(F.col("TakeoffLocalDateTime") >= "2025-09-31T00:00:00.000+00:00")

limited = loginfo.select(
    "LogId", "FlightNo", "FuelOnDeparture", "FuelOnAdded", "FuelOnArrival", "FuelConsumption", 
    "FlightName", "AircraftId", "SectorPair", "TakeoffLocalDateTime", "TouchDownLocalDateTime",
)

limited.withColumns(
    {
        "PrevFuelONArrival": F.lag("FuelOnArrival").over(winfunc),
        "Equal": F.when(
            (F.col("FuelOnAdded") + F.col("PrevFuelONArrival")) == F.col("FuelOnDeparture"), True
        ).otherwise(False)
    }
).filter(
    (F.col("Equal") == False) # (F.col("FuelConsumption") <= 0) #& 
)
limited.filter(F.date_format("TakeoffLocalDateTime", "yyyy-MM-dd") >= "2025-10-31").display() # & (F.col("LogId") == "114326")

# COMMAND ----------



# COMMAND ----------

cols = [
    "Id", "LogId", "FlightNo", "TakeoffLocalDateTime", "TouchDownLocalDateTime", "Pax", 
    "CargoWeight", "TakeOffWeight", "Landings", "PaxAdult", "PaxChild", "FuelOnDeparture", 
    "FuelOnAdded", "FuelOnArrival", "FuelConsumption", "BlockTime","TimeInAir", "CreationTime", 
    "IsDiverted", "ActualDestinationId", "PFIDoneById", "FlightName", "AircraftId", "FlightSectorPairId", 
    "SectorPair", "Key"
]
# .filter(F.col("TakeoffLocalDateTime") >= "2025-09-31T00:00:00.000+00:00")

limited = loginfo.select(
    "LogId", "FlightNo", "FuelOnDeparture", "FuelOnAdded", "FuelOnArrival", "FuelConsumption", 
    "FlightName", "AircraftId", "SectorPair", "TakeoffLocalDateTime", "TouchDownLocalDateTime",
)

limited.withColumns(
    {
        "PrevFuelONArrival": F.lag("FuelOnArrival").over(winfunc),
        "Equal": F.when(
            (F.col("FuelOnAdded") + F.col("PrevFuelONArrival")) == F.col("FuelOnDeparture"), True
        ).otherwise(False)
    }
).filter(
    (F.col("Equal") == False) # (F.col("FuelConsumption") <= 0) #& 
)
limited.filter(F.date_format("TakeoffLocalDateTime", "yyyy-MM-dd") >= "2025-09-31").display() # & (F.col("LogId") == "114326")

# COMMAND ----------

loginfo.select(F.max("TakeoffLocalDateTime")).display()

# COMMAND ----------

filtered = loginfo.select(*cols).filter(
    (F.year("TakeoffLocalDateTime") >= "2025")& (F.col("TakeoffLocalDateTime").contains("2025-06-22")) & (F.col("FlightName") == "9N-ANI")
).select(
    "LogId", "FlightNo", "FuelOnDeparture", 
    "FuelOnAdded", "FuelOnArrival", "FuelConsumption", "FlightName", "AircraftId", "SectorPair", "TakeoffLocalDateTime", "TouchDownLocalDateTime",
)

filtered.display()

# COMMAND ----------

allcount = limited.groupBy(F.year("TakeoffLocalDateTime").alias("Year")).count()

allcount.display()

# COMMAND ----------

limited.filter(
     (F.col("FuelConsumption") <= 0) #(F.year("TakeoffLocalDateTime") >= "2025") &
).groupBy(
    F.year("TakeoffLocalDateTime").alias("Year")
).agg(
    F.count("LogId").alias("CountFuelConsumptionZeroOrLess")
).withColumn(
    "%OfTotal", F.when(
        F.col("year") == "2024", F.round((F.col("CountFuelConsumptionZeroOrLess")/48788*100), 2)
    ).otherwise(F.round((F.col("CountFuelConsumptionZeroOrLess")/39304*100), 2))
).alias("df1").join(
    allcount.alias("df2"), F.col("df1.Year") == F.col("df2.Year")
).select(
    "df1.Year", "df1.CountFuelConsumptionZeroOrLess",
    F.col("df2.Count").alias("TotalCount"), "df1.%OfTotal"
).display()

# COMMAND ----------

limited.filter(
     (F.col("FuelConsumption") <= 0) #(F.year("TakeoffLocalDateTime") >= "2025") &
).display()

# COMMAND ----------

countzero = loginfo.filter(
    (F.year("TakeoffLocalDateTime") >= "2024") & (F.col("FuelConsumption") <= 0)
).count()

countall = loginfo.count()

print(countzero)
print(countall)

# COMMAND ----------

66462/88092

# COMMAND ----------

logdetails = logdetails.alias("df1").join(
        logs.alias("df2"), F.col("df1.LogId") == F.col("df2.Id"), how="left"
).select("df1.*", "df2.FlightName", "df2.AircraftId").dropDuplicates(subset = ["id"]).filter(F.col("LogId") == "114326").display()

# COMMAND ----------

spark.table("analyticadebuddha.mro.fuelconsumption").display()

# COMMAND ----------

380/(33/60)

# COMMAND ----------

loginfo.select(*colspassenger).drop("SectorPair").filter((F.col("PaxAdult") + F.col("PaxChild")) > 72).display()

# COMMAND ----------

loginfo.filter((F.col("TakeoffLocalDateTime") >="2025-06-01T00:00:00.000+00:00") & (F.col("FuelOnDeparture") <= 0)).display() #.filter(F.col("FuelOnArrival") == 0) 

# COMMAND ----------

loginfo.filter(
    (F.col("TakeoffLocalDateTime").between("2025-06-22T00:00:00.000+00:00", "2025-06-23T00:00:00.000+00:00")) 
    # & (F.col("FlightName") == "9N-ANZ") & (F.col("AircraftId") == "21")
).select(F.sum("PaxAdult") + F.sum("PaxChild")).display()

# COMMAND ----------

spark.table("analyticadebuddha.flightoperation.actualhours").filter(F.col("FlightDate") >= "2025-08-01").display()

# COMMAND ----------



# COMMAND ----------

tablestatus = spark.table("analyticadebuddha.mro.tablestatus")
tablestatus.display()

# COMMAND ----------

fks = spark.table("analyticadebuddha.mro.tablefk")
fks.display()

# COMMAND ----------

spark.read.format("delta").load("/mnt/rawmro/LogTypes").display()

# COMMAND ----------

