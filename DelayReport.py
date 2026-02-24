# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

programmedpath = "/mnt/fo/processed/programmedAndActuals/programmeditems"
occpath = "/mnt/fo/processed/programmedAndActuals/occitems"
flightlogpath = "/mnt/fo/processed/actual/actualflightitems"
occstatus = "/mnt/fo/processed/programmedAndActuals/status"

pathoccremarks = "/mnt/fo/raw/occ_remarks"
pathdevation  = "/mnt/fo/raw/daily_flight_record_deviations"

programmed = spark.read.format("delta").load(programmedpath)
occ = spark.read.format("delta").load(occpath)

flightlog = spark.read.format("delta").load(flightlogpath)
status = spark.read.format("delta").load(occstatus)

occremarks = spark.read.format("delta").load(pathoccremarks)
devation = spark.read.format("delta").load(pathdevation)

partcols = [F.col("FlightNo"), F.col("Name"), F.col("FlightSectorPairId"), F.col("FlightDate")]
partcols1 = [F.col("FlightNo"), F.col("Name"), F.col("FlightSectorPairId"), F.col("Date").alias("FlightDate")]

ordercolActual = [F.col("FlightDate"), F.col("TakeOff")]
ordercolProgrammed = [F.col("Date"), F.col("EstimatedDeparture")]

selectcolumns = [
    "Id", "FlightNo", "FlightSectorPairId", "FlightDate", "ChockoffTOtakeoff", "TakeOff", "AirTime","TouchDown", "TouchDownToChockOn", 
    "Type", F.col("Name").alias("FlightName"), "ProgrammedRemarks", "OccRemarksP", "DepatureDiff", "ArrivalDiff", "DepatureStatus", "DelayRange", 
    "ProgramId", "OccId", "Planned"
]

dropcol = ["ProgramId", "OccId", "ProgrammeDate", "DailyFlightProgrammeId"]

hashcolumn = [
    F.coalesce(F.col("FlightNo").cast("string"), F.lit("")),
    F.coalesce(F.col("FlightSectorPairId").cast("string"), F.lit("")),
    F.coalesce(F.col("FlightName").cast("string"), F.lit("")),
    F.coalesce(F.date_format(F.col("FlightDate"), "yyyy-MM-dd"), F.lit(""))
]

catalog = "analyticadebuddha.flightoperation"

'''
'''
delayrange = F.when(
    F.abs("DepatureDiff").between(31, 40), F.lit("30-40")
).when(
    F.abs("DepatureDiff").between(40, 50), F.lit("40-50")
).when(
    F.abs("DepatureDiff").between(50, 60), F.lit("50-60")
).when(
    F.abs("DepatureDiff") > 60, F.lit("Above60")
).otherwise("NotDelay")


def getRanked(df, partcols, ordercol):
    '''
    '''
    windowrank = Window.partitionBy(*partcols)
    df = df.withColumn("Rank", F.row_number().over(windowrank.orderBy(*[col.asc() for col in ordercol])))

    return df

def getOccAndDevationRemarks(devation, occremarks):
    '''
    '''
    remarksanddevation = devation.alias("df1").join(
        occremarks.alias("df2"), F.col("df1.occ_remark_id") == F.col("df2.id")
    ).select(
        "df1.Id",F.col("df1.daily_flight_programme_item_id").alias("DailyFlightProgrammeId"), F.col("programme_date").alias("ProgrammeDate"),
        F.col("df1.remarks").alias("DeviationRemarks"), F.initcap("df2.remarks").alias("OccRemarks"), F.initcap("deviatable_type").alias("DevationType")
    )
    
    return remarksanddevation

    
def getPlanned(actuals, programmedInActual1):
    '''
    '''
    mapinboth = actuals.alias("df1").join(
        programmedInActual1.alias("df2"),
        (
            (
                F.col("df1.FlightNo") == F.col("df2.FlightNo")
            ) & (
                F.col("df1.Name") == F.col("df2.Name")
            ) & (
                F.col("df1.FlightSectorPairId") == F.col("df2.FlightSectorPairId")
            ) & (
                F.col("df1.FlightDate") == F.col("df2.Date")
            ) & (
                F.col("df1.Rank") == F.col("df2.Rank")
            )
        ), how="left"
    ).select(
        "df1.*", "df2.EstimatedDeparture", "df2.EstimatedArrival", "df2.EstimatedTime", 
        F.col("df2.Date").alias("Planned"), F.col("df2.Remarks").alias("ProgrammedRemarks"), F.col("df2.Id").alias("ProgramId")
    )

    planned = mapinboth.filter(F.col("Planned").isNotNull()).withColumn("Planned", F.lit("True"))
    unplanned = mapinboth.filter(F.col("Planned").isNull()).withColumn("Planned", F.lit("False")).drop("ProgramId")

    return planned, unplanned
    
def getParentId(planned, occ):
    '''
    '''
    planned = planned.alias("df1").join(
        occ.alias("df2"), F.col("df1.ProgramId") == F.col("df2.ParentId"), how="left"
    ).select("df1.*", F.col("df2.Id").alias("OccId"), F.col("df2.Remarks").alias("OccRemarksP"))

    return planned

def getDelayReport(planned_, occdevation_):
    '''
    '''  
    delyareport = planned_.withColumns(
        {
            "EstimatedDeparture":(F.date_format(F.concat(F.col("FlightDate"), F.lit(" "), F.col("EstimatedDeparture")),  "yyyy-MM-dd HH:mm")),
            "EstimatedArrival": (F.date_format(F.concat(F.col("FlightDate"), F.lit(" "), F.col("EstimatedArrival")),  "yyyy-MM-dd HH:mm")),
            "TakeOff": (
                F.date_format("TakeOff", "yyyy-MM-dd HH:mm")
            ),
            "TouchDown": (
                F.date_format("TouchDown", "yyyy-MM-dd HH:mm")
            )
        }
    ).withColumns(
        {
            "DepatureDiff":  F.round(
                ((F.to_unix_timestamp(F.to_timestamp("TakeOff"))) - (F.to_unix_timestamp(F.to_timestamp("EstimatedDeparture")))))/60
            , 
            "ArrivalDiff": F.round(
                ((F.to_unix_timestamp(F.to_timestamp("TouchDown"))) - (F.to_unix_timestamp(F.to_timestamp("EstimatedArrival")))))/60,
            
            "DepatureStatus": F.when(
                F.abs("DepatureDiff") <= 30, "WithIn3oMinutesWindow"
            ).when(
                F.col("DepatureDiff") > 30, "Delay"
            ).when(
                F.col("DepatureDiff") < - 30 , "Early"
            ).otherwise(F.lit("Unknown")),

            "DelayRange": delayrange
        }
    )

    delay = delyareport.select(*selectcolumns).alias("df1").join(
        occdevation_.drop("id").alias("df2"), F.col("df1.OccId") == F.col("DailyFlightProgrammeId"), how="left"
    ).distinct().withColumn("Key", F.sha2(F.concat_ws("||", *hashcolumn), 256)).drop(*dropcol)

    delay.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.delayinfo")

    return None


def main():
    '''
    '''
    occdevation_ = getOccAndDevationRemarks(devation, occremarks)
    actuals = getRanked(df=flightlog, partcols=partcols, ordercol=ordercolActual)
    programmedInActual1 = getRanked(df=programmed, partcols=partcols1, ordercol=ordercolProgrammed)
    planned, unplanned = getPlanned(actuals, programmedInActual1)
    planned_ = getParentId(planned, occ)
    getDelayReport(planned_, occdevation_)

    return None

if __name__ == "__main__":
    main()