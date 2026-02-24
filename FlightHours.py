# Databricks notebook source
# import pyspark.sql.functions as F
# import dbconnect
# import pandas as pd
# from TrackAndUpdate import Mergers

# projectionpath = "/mnt/fo/processed/projected/Projectionitem"
# programmedpath = "/mnt/fo/processed/programmedAndActuals/programmeditems"
# occpath = "/mnt/fo/processed/programmedAndActuals/occitems"
# flightlogpath = "/mnt/fo/processed/actual/actualflightitems"
# occstatus = "/mnt/fo/processed/programmedAndActuals/status"

# pathprojectedcrew = "/mnt/fo/processed/projected/Crewprojection"
# pathactual = "/mnt/fo/processed/actual/crewdetails"
# pathprogrammed = "/mnt/fo/processed/programmedAndActuals/programcrew"

# crewactuals = [F.col("df2.EmployeeId"), F.col("df2.Role")]

# actualscols = [
#     "df1.FlightNo", "df1.FlightSectorPairId", F.col("df1.Name").alias("FlightName"), 
#     F.col("df1.FlightDate"), F.col("df1.AirTime"), F.col("df1.BlockTime"), F.col("df1.ChockoffTOtakeoff"),
#     F.col("df1.TouchDownToChockOn"),F.col("df1.Landings")
# ]

# programmedcols = [
#     "df1.Id", "df1.DailyFlightProgrammeId", "df1.FlightSectorPairId", "df1.EstimatedTime",
#     "df1.FlightNo", F.col("df1.Name").alias("Flightname"), F.col("df1.Date").alias("FlightDate")
# ]

# crewprogrammedcol = [
#     "df2.EmployeeId", F.col("df2.CrewRoleTypeId").alias("Role")
# ]

# projectedcols = [
#     "df1.FlightProgrammeProjectionId","df1.FlightSectorPairId", "df1.FlightNo", F.col("df1.Name").alias("FlightName"), 
#     F.col("ForDate").alias("FlightDate"), F.col("EstimatedAirTime").alias("ProjectedAirTime")
# ]

# projectedcrewcol = [F.col("df2.EmployeeId"), F.col("df2.CrewRoleTypeId").alias("Role")]

# selectprograms = programmedcols + crewprogrammedcol
# selectcolsactuals = actualscols + crewactuals
# selectprojected = projectedcols + projectedcrewcol


# actualgroup = [F.col("FlightNo"), F.col("FlightSectorPairId"), F.col("FlightName"), F.col("FlightDate"), F.col("EmployeeId"), F.col("Role")]

# actualaggfun = [
#     F.sum("AirTime").alias("AirTime"),
#     F.sum("BlockTime").alias("BlockTime"), 
#     F.sum("ChockoffTOtakeoff").alias("ChockoffTOtakeoff"),
#     F.sum("TouchDownToChockOn").alias("TouchDownToChockOn"),
#     F.sum("Landings").alias("Landings"),
# ]

# programfunc = [
#     F.sum("EstimatedTime").alias("EstimatedTime"),
# ]

# projectedfunc = [
#     F.sum("ProjectedAirTime").alias("ProjectedAirTime")
# ]

# actualsjoincon = (F.col("df1.FlightLogId") == F.col("df2.FlightLogId"))
# joinprogram = F.col("df1.DailyFlightProgrammeId") == F.col("df2.DailyFlightProgrammeItemId")
# projectedjoincon = F.col("df1.Id") == F.col("df2.FlightProgrammeProjectionItemId")

# hashcolumn = [
#     F.coalesce(F.col("FlightNo").cast("string"), F.lit("")),
#     F.coalesce(F.col("FlightSectorPairId").cast("string"), F.lit("")),
#     F.coalesce(F.col("FlightName").cast("string"), F.lit("")),
#     F.coalesce(F.date_format(F.col("FlightDate"), "yyyy-MM-dd"), F.lit(""))
# ]

# catalog = "analyticadebuddha.flightoperation"

# def getAgg(df1, df2, joincon, selectcols, groupbycols, aggfun, hashkey=hashcolumn, tablename=None):
#     '''
#     '''

#     df = df1.alias("df1").join(
#         df2.alias("df2"), joincon, how = "left"
#     ).select(*selectcols)

#     agg = df.groupBy(groupbycols).agg(*aggfun).filter(
#         F.col("FlightDate").isNotNull()
#     ).withColumn(
#         "Key", F.sha2(F.concat_ws("||", *hashcolumn), 256)
#     )

#     agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{tablename}")

#     return None

# def main():
#     '''
#     '''
#     projected = spark.read.format("delta").load(projectionpath)
#     programmed = spark.read.format("delta").load(programmedpath)
#     occ = spark.read.format("delta").load(occpath)
#     flightlog = spark.read.format("delta").load(flightlogpath)
#     status = spark.read.format("delta").load(occstatus)

#     crewprojection = spark.read.format("delta").load(pathprojectedcrew)
#     crewprogrammed = spark.read.format("delta").load(pathprogrammed)
#     crewactual = spark.read.format("delta").load(pathactual)

#     getAgg(df1=flightlog, df2=crewactual, joincon=actualsjoincon, selectcols=selectcolsactuals, groupbycols=actualgroup, aggfun=actualaggfun, tablename="actualhours")
#     getAgg(df1=programmed, df2=crewprogrammed, joincon=joinprogram, selectcols=selectprograms, groupbycols=actualgroup, aggfun=programfunc, tablename="programmedhours")
#     getAgg(df1=projected, df2=crewprojection, joincon=projectedjoincon, selectcols=selectprojected, groupbycols=actualgroup, aggfun=projectedfunc, tablename="projectedhours")

#     return None

# if __name__ == "__main__":
#     main()

# COMMAND ----------

import pyspark.sql.functions as F
import dbconnect
import pandas as pd
from TrackAndUpdate import Mergers

projectionpath = "/mnt/fo/processed/projected/Projectionitem"
programmedpath = "/mnt/fo/processed/programmedAndActuals/programmeditems"
occpath = "/mnt/fo/processed/programmedAndActuals/occitems"
flightlogpath = "/mnt/fo/processed/actual/actualflightitems"
occstatus = "/mnt/fo/processed/programmedAndActuals/status"

pathprojectedcrew = "/mnt/fo/processed/projected/Crewprojection"
pathactual = "/mnt/fo/processed/actual/crewdetails"
pathprogrammed = "/mnt/fo/processed/programmedAndActuals/programcrew"

crewactuals = [F.col("df2.EmployeeId"), F.col("df2.Role")]

actualscols = [
    "df1.FlightNo", "df1.FlightSectorPairId", F.col("df1.Name").alias("FlightName"), 
    F.col("df1.FlightDate"), F.col("df1.AirTime"), F.col("df1.BlockTime"),F.col("df1.PayTime"), F.col("df1.ChockoffTOtakeoff"),
    F.col("df1.TouchDownToChockOn"), F.col("df1.Landings")
]

programmedcols = [
    "df1.Id", "df1.DailyFlightProgrammeId", "df1.FlightSectorPairId", "df1.EstimatedTime",
    "df1.FlightNo", F.col("df1.Name").alias("Flightname"), F.col("df1.Date").alias("FlightDate"), "df1.Status", "df1.Comments"
]

crewprogrammedcol = [
    "df2.EmployeeId", F.col("df2.CrewRoleTypeId").alias("Role")
]

projectedcols = [
    "df1.FlightProgrammeProjectionId","df1.FlightSectorPairId", "df1.FlightNo", F.col("df1.Name").alias("FlightName"), 
    F.col("ForDate").alias("FlightDate"), F.col("EstimatedAirTime").alias("ProjectedAirTime")
]

projectedcrewcol = [F.col("df2.EmployeeId"), F.col("df2.CrewRoleTypeId").alias("Role")]

selectprograms = programmedcols + crewprogrammedcol
selectcolsactuals = actualscols + crewactuals
selectprojected = projectedcols + projectedcrewcol

othercol = ["EmployeeId", "Role"] + ["FlightNo", "FlightSectorPairId", "FlightName", "FlightDate"] + ["CrewFrom", "Key"]

actualgroup = [F.col("FlightNo"), F.col("FlightSectorPairId"), F.col("FlightName"), F.col("FlightDate")] #, F.col("EmployeeId"), F.col("Role")]

actualaggfun = [
    F.sum("AirTime").alias("AirTime"),
    F.sum("BlockTime").alias("BlockTime"), 
    F.sum("PayTime").alias("PayTime"),
    F.sum("ChockoffTOtakeoff").alias("ChockoffTOtakeoff"),
    F.sum("TouchDownToChockOn").alias("TouchDownToChockOn"),
    F.sum("Landings").alias("Landings")
]

programfunc = [
    F.sum("EstimatedTime").alias("EstimatedTime"), F.first("Status").alias("Status"), F.first("Comments").alias("Comments")
]

projectedfunc = [
    F.sum("ProjectedAirTime").alias("ProjectedAirTime")
]

actualsjoincon = (F.col("df1.FlightLogId") == F.col("df2.FlightLogId"))
joinprogram = F.col("df1.id") == F.col("df2.DailyFlightProgrammeItemId")
projectedjoincon = F.col("df1.Id") == F.col("df2.FlightProgrammeProjectionItemId")

hashcolumn = [
    F.coalesce(F.col("FlightNo").cast("string"), F.lit("")),
    F.coalesce(F.col("FlightSectorPairId").cast("string"), F.lit("")),
    # F.coalesce(F.col("FlightName").cast("string"), F.lit("")),
    F.coalesce(F.date_format(F.col("FlightDate"), "yyyy-MM-dd"), F.lit(""))
]

dup = ["FlightNo", "FlightSectorPairId", "FlightDate"]
programmeddup = ["DailyFlightProgrammeId", "FlightSectorPairId", "FlightNo", "Date"]

catalog = "analyticadebuddha.flightoperation"

def getAgg(df1, df2, joincon, cols1, cols2, groupbycols, aggfun, hashkey=hashcolumn, tablename=None, litval=str, duplicates = None):
    '''
    '''
    df = df1.alias("df1").join(
        df2.alias("df2"), joincon, how = "inner"
    ).select(*cols1)

    if not duplicates is None:
        df = df.dropDuplicates(subset = [*duplicates])


    agg = df1.alias("df1").select(cols2).groupBy(groupbycols).agg(*aggfun).filter(
        F.col("FlightDate").isNotNull()
    ).withColumn(
        "Key", F.sha2(F.concat_ws("||", *hashcolumn), 256)
    )

    crew = df.withColumn(
        "Key", F.sha2(F.concat_ws("||", *hashcolumn), 256)
    ).withColumn(
        "CrewFrom", F.lit(litval)
    ).select(*othercol).distinct()

    return crew, agg


def getUnplanned(actuals, programmed):
    '''
    '''
    maxflightdate = actuals.select(F.max("FlightDate")).collect()[0][0]

    # plannedstatus = F.when(
    #     F.col("key2").isNull(), F.lit("AddOn")
    # ).otherwise(F.lit("Planned"))

    # mergedorcancelled = F.when(
    #     (F.col("key2").isNull()) & (F.col("Status").contains("cancelled")) 
    #     & (F.col("FlightDate") <= maxflightdate), F.lit("Cancelled")
    # ).when(
    #     (F.col("key2").isNull()) & (~F.col("Status").contains("cancelled")) 
    #     & (F.col("FlightDate") <= maxflightdate), F.lit("Merged")
    # ).when(
    #     (F.col("key2").isNotNull()) & (F.col("FlightDate") <= maxflightdate), F.lit("Flew")
    # ).otherwise(F.lit("DataYetToBeUpdated"))

    joincon = (
        F.col("df1.FlightNo") == F.col("df2.FlightNo")
    ) & (
        F.col("df1.FlightSectorPairId") == F.col("df2.FlightSectorPairId")
    ) & (
        F.col("df1.FlightDate") == F.col("df2.FlightDate")
    ) 

    plannedstatus = F.when(
        F.col("key2").isNull(), F.lit("AddOn")
    ).otherwise(F.lit("Planned"))

    mergedorcancelled = F.when(
        (F.col("key2").isNull()) & (F.col("FlightDate") <= maxflightdate) , F.lit("Cancelled-Merged") #& (F.col("Status").contains("cancelled"))
    ).when(
        (F.col("key2").isNotNull()) & (F.col("FlightDate") <= maxflightdate), F.lit("Flew")
    ).when(
        (F.col("key2").isNull()) & (F.col("FlightDate") > maxflightdate), F.lit("DataYetToBeUpdated")
    ).otherwise(F.lit("MightBeMerged"))

    actuals_ = actuals.alias("df1").join(
        programmed.alias("df2"), (
            F.col("df1.key") == F.col("df2.key")
        ), how="left"
    ).select("df1.*", F.col("df2.key").alias("key2")).withColumn("Planned", plannedstatus).drop("key2")

    programmed_ = programmed.alias("df1").join(
        actuals.alias("df2"), F.col("df1.key") == F.col("df2.key"), how="left"
    ).select("df1.*", F.col("df2.key").alias("key2")).withColumn("Cancelled", mergedorcancelled).drop("key2")

    return actuals_.dropDuplicates(subset = ["key"]), programmed_.dropDuplicates(subset = ["key"])

def toCatalog(config):
    '''
    '''
    for frame in config.get("data"):
        key, value = list(frame.items())[0]
        print(f"Writing {key} to catalog {catalog}")

        value.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{key}")
        
    return None

def getCrews(dfs=list):
    df = dfs[0]
    for frame in dfs[1:]:
        df = df.unionByName(frame, allowMissingColumns=True)

    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.crew")

    return None

def main():
    '''
    '''
    projected = spark.read.format("delta").load(projectionpath)
    programmed = spark.read.format("delta").load(programmedpath).dropDuplicates(subset = programmeddup)
    occ = spark.read.format("delta").load(occpath)
    flightlog = spark.read.format("delta").load(flightlogpath)
    status = spark.read.format("delta").load(occstatus)

    crewprojection = spark.read.format("delta").load(pathprojectedcrew) 
    crewprogrammed = spark.read.format("delta").load(pathprogrammed) 
    crewactual = spark.read.format("delta").load(pathactual)

    crew1, atuals_agg = getAgg(
        df1=flightlog, df2=crewactual, joincon=actualsjoincon, cols1=selectcolsactuals, cols2=actualscols,
        groupbycols=actualgroup, aggfun=actualaggfun, litval= "flightlog"
    )

    crew2, programmed_agg = getAgg(
        df1=programmed, df2=crewprogrammed, joincon=joinprogram, cols1=selectprograms, cols2=programmedcols,
        groupbycols=actualgroup, aggfun=programfunc, litval= "programmed"
    )

    crew3, projected_agg = getAgg(
        df1=projected, df2=crewprojection, joincon=projectedjoincon, cols1=selectprojected, cols2=projectedcols,
        groupbycols=actualgroup, aggfun=projectedfunc, litval= "projected"
    )

    actuals_, programmed_ = getUnplanned(actuals=atuals_agg, programmed=programmed_agg)

    config = {
        "data": [{"actualhours": actuals_}, {"programmedhours": programmed_}, {"projectedhours": projected_agg.dropDuplicates(subset = ["key"])}]
    }

    toCatalog(config=config)
    getCrews(dfs=[crew1, crew2, crew3])

    return None

if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %md
# MAGIC **For Delay Report**

# COMMAND ----------

# actuals = flightlog.select("Id", "FlightLogId", "FlightNo", "Name", "AircraftRegistrationId", "ChocksOff", "ChocksOn", "TakeOff", "TouchDown", "AirTime", "TouchDownToChockOn", "TouchDownToChockOn", "FlightDate", "FlightNumberId", "FlightSectorPairId").distinct()

# occdf = occ.select("Id", "ParentId", "DailyFlightProgrammeId","FlightNo", "Name", "AircraftRegistrationId", "FlightNumberId", "FlightSectorPairId", "ActualDeparture", "ActualArrival", "Remarks", "Date").distinct()

# programmeddf = programmed.select("Id", "DailyFlightProgrammeId","FlightNo", "Name", "AircraftRegistrationId", "FlightNumberId", "FlightSectorPairId", "EstimatedDeparture", "EstimatedArrival","EstimatedTime","Remarks", "Date").distinct()

# projecteddf = projected.select("Id", "FlightProgrammeProjectionId", "FlightSectorPairId", "FlightNo", "Name", "AircraftRegistrationId", "FlightNumberId", F.col("EstimatedDepature").alias("ProjectdDeparture"), F.col("EstimatedArrival").alias("ProjectedArrival"), F.col("EstimatedAirTime").alias("ProjectedAirTime"), "ForDate").distinct()

# def forDelayReport(actuals, occdf, programmeddf, projecteddf):
#     '''
#     '''
#     logandocc = actuals.alias("df1").join(
#         occdf.alias("df2"), (
#             (
#                 F.col("df1.FlightNo") == F.col("df2.FlightNo")
#             ) & (
#                 F.col("df1.FlightSectorPairId") == F.col("df2.FlightSectorPairId")
#             )  & (
#                 F.col("df1.FlightDate") == F.col("df2.Date")
#             )
#         ) , how = "left"
#     ).select("df1.*", "df2.ParentId", "df2.ActualDeparture", "df2.ActualArrival", "df2.Remarks", "df2.Date")

#     logoccprogrammed = logandocc.alias("df1").join(
#         programmeddf.alias("df2"), (
#             (
#                 F.col("df1.FlightNo") == F.col("df2.FlightNo")
#             ) & (
#                 F.col("df1.FlightSectorPairId") == F.col("df2.FlightSectorPairId")
#             ) & (
#                 F.col("df1.FlightDate") == F.col("df2.Date")
#             )
#         ) , how = "left"
#     ).select("df1.*", F.col("df2.id").alias("ProgrammedId"), "df2.DailyFlightProgrammeId","df2.EstimatedDeparture", 
#             "df2.EstimatedArrival", "df2.EstimatedTime", F.col("df2.Remarks").alias("Remarks1"), 
#             F.col("df2.Date").alias("ProgrammedFor")
#     ).distinct()

#     logoccprogrammed =logoccprogrammed.alias("df1").join(
#         projecteddf.alias("df2"), (
#             (
#                 F.col("df1.FlightNo") == F.col("df2.FlightNo")
#             ) & (
#                 F.col("df1.FlightSectorPairId") == F.col("df2.FlightSectorPairId")
#             ) &  (
#                 F.col("df1.FlightDate") == F.col("df2.ForDate")
#             )
#         ) , how = "left"
#     ).select("df1.*", "df2.ProjectdDeparture", "df2.ProjectedArrival", "df2.ProjectedAirTime", "df2.ForDate").distinct()

#     return logoccprogrammed

# fordelayreportdf  = forDelayReport(actuals, occdf, programmeddf, projecteddf)

# def getDelayReport(fordelayreportdf):
#     '''
#     '''
#     pass


# statusonlog = F.when(
#     (
#         F.to_timestamp(F.concat(
#             F.col("Date"), " " F.col("EstimatedDeparture")
#         ))
        
#     )

# )
# fordelayreportdf.withColumns(
#     {
#         "statusOnLog":,
#         "statusOnOcc":,

#     }
# ).display()