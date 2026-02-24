# Databricks notebook source
import pyspark.sql.functions as F
import datetime
import pandas as pd

today = datetime.datetime.now().strftime("%Y-%m-%d")
pastdate = (pd.to_datetime(today) - pd.Timedelta(days=30)).strftime("%Y-%m-%d")

catalog = "analyticadebuddha.flightoperation"

pathofficialduty = "/mnt/fo/raw/official_duties"
pathtrainingcat = "/mnt/fo/raw/training_categories"
pathcrewflighttraining = "/mnt/fo/raw/crew_flight_trainings"
pathcrewviolation = "/mnt/fo/raw/crew_violations"
pathrosteroff = "/mnt/fo/raw/roster_offs"
pathdevationreson = "/mnt/fo/raw/daily_flight_record_deviation_reasons"
pathleavetype = "/mnt/fo/raw/leave_types"
pathabsence = "/mnt/fo/raw/absences"
pathemployeeinfo = "analyticadebuddha.flightoperation.employeeinfo"

employeeid = ["EmployeeId"] 
dates = ["StartDate", "EndDate"]
key = ["key"]

leavesectorflightcols = ["ProgrammedSector", "ProgrammedFlight", "ProjectedSector", "ProjectedFlight"]
colssummary = ["EmployeeId", "LatestProgrammedDeviation", "LatestRosterDeviation", "AbsenceCategories","Reasons"]
PKColumns = employeeid + dates+ leavesectorflightcols

rolecon = F.coalesce(F.col("Role1"), F.col("Role2"))

gendercon = F.when(
    F.col("gender").contains("m"), F.lit("Male")
).when(
    F.col("gender").contains("f"), F.lit("Female")
).otherwise(F.lit("Other"))

InLeave = F.when(
    (
        (F.col("isProgrammed") == True) & (F.col("isProjected") == True)
    ), F.lit("Both")
).when(
    (
        (F.col("isProgrammed") == False) & (F.col("isProjected") == True)
    ), F.lit("Roster")
).when(
    (
        (F.col("isProgrammed") == True) & (F.col("isProjected") == False)
    ), F.lit("Programmed")
).otherwise(F.lit("Not Assigned"))

leavecols = ["Id", "Name", "leavekey"]

cols = [
    F.col("employee_id").alias("EmployeeId"),
    F.col("start_date").alias("StartDate"),
    F.col("end_date").alias("EndDate"),
    F.col("absenceable_type").alias("Absence"),
    F.col("type").alias("Type"),
    F.col("absenceable_id").alias("AbsenceableId"),
    F.col("involved_employee_id").alias("InvolvedEmployeeId"),
    F.col("requested_by_employee_id").alias("RequestedByEmployeeId"),
    F.date_format("created_at","yyyy-MM-dd").alias("CreatedAt"),
    F.date_format("updated_at", "yyyy-MM-dd").alias("UpdatedAt"),
    F.initcap("source").alias("Source")
]

def getLeaveConsoliodated():
    """
    """
    duties = spark.read.format("delta").load(pathofficialduty).withColumn(
        "leavekey", F.concat(F.lit("official-duty"), F.lit("-"), F.col("id"))
    ).select(*leavecols)

    training = spark.read.format("delta").load(pathtrainingcat).withColumn(
        "leavekey", F.concat(F.lit("training-categories"), F.lit("-"), F.col("id"))
    ).select(*leavecols)

    crewflighttraining = spark.read.format("delta").load(pathcrewflighttraining).withColumn(
        "leavekey", F.concat(F.lit("crew-flight-training"), F.lit("-"), F.col("id"))
    ).select(*leavecols)

    voilation = spark.read.format("delta").load(pathcrewviolation).withColumn(
        "leavekey", F.concat(F.lit("crew-violation"), F.lit("-"), F.col("id"))
    ).select(*leavecols)

    rosteroff = spark.read.format("delta").load(pathrosteroff).withColumn(
        "leavekey", F.concat(F.lit("roster-off"), F.lit("-"), F.col("id"))
    ).select(*leavecols)

    devationreson = spark.read.format("delta").load(pathdevationreson).withColumn(
        "leavekey", F.concat(F.lit("deviation"), F.lit("-"), F.col("id"))
    ).select(*leavecols)

    leavetype = spark.read.format("delta").load(pathleavetype).withColumn(
        "leavekey", F.concat(F.lit("leave"), F.lit("-"), F.col("id"))
    ).select(*leavecols)


    dfs = [duties, training, crewflighttraining, voilation, rosteroff, devationreson, leavetype]

    df1 = dfs[0]

    for df in dfs[1:]:
        df1 = df1.union(df)

    return df1.withColumn("leavekey", F.sha2("leavekey", 256))

def getEmployeeInfo(leavereport, employeeinfo):
    """
    """
    leavereport = leavereport.alias("df1").join(
        employeeinfo.alias("df2"),
        F.col("df1.EmployeeId") == F.col("df2.EmployeeIdMain"), how="left"
    ).select(
        F.col("df2.EmployeeId").alias("EmployeeIdMain"), 
        "df2.FullName", "df2.MobileNumber", "df1.*"
    ).withColumn(
        "InLeave", InLeave
    )

    return leavereport

def getLeaveReport(absence, crewprogram, crewprojected):
    """
    """
    inprogrammed_check = absence.alias("df1").join(
        crewprogram.alias("df2"), 
        (
            F.col("df1.EmployeeId") == F.col("df2.EmployeeId")
        ) & 
        (
            F.col("df2.FlightDate").between(
                F.col("df1.StartDate"), F.col("df1.EndDate")
            )
        ),
        how="left"
    ).select(
        "df1.*", F.col("df2.Role").alias("Role1"),F.col("df2.FlightDate").alias("ProgrammedFlightDate"), 
        F.col("df2.FlightSectorPairId").alias("ProgrammedSector"), 
        F.col("df2.FlightNo").alias("ProgrammedFlight")
    ).dropDuplicates(
        subset=[
            "EmployeeId", "ProgrammedFlightDate", "StartDate", 
            "EndDate", "ProgrammedSector", "ProgrammedFlight"
        ]
    ).withColumn(
        "isProgrammed", F.when(
            F.col("ProgrammedFlightDate").isNotNull(), True
        ).otherwise(False)
    )

    inprojected = inprogrammed_check.alias("df1").join(
        crewprojected.alias("df2"), 
        (
            F.col("df1.EmployeeId") == F.col("df2.EmployeeId")
        ) & 
        (
            F.col("df2.FlightDate").between(
                F.col("df1.StartDate"), F.col("df1.EndDate")
            )
        ),
        how="left"
    ).select(
        "df1.*",F.col("df2.Role").alias("Role2"), 
        F.col("df2.FlightDate").alias("ProjectedFlightDate"), 
        F.col("df2.FlightSectorPairId").alias("ProjectedSector"), 
        F.col("df2.FlightNo").alias("ProjectedFlight")
    ).dropDuplicates(
        subset=[
            "EmployeeId", "ProjectedFlightDate", "StartDate", 
            "EndDate", "ProjectedSector", "ProjectedFlight"
        ]
    ).withColumn(
        "isProjected", F.when(
            F.col("ProjectedFlightDate").isNotNull(), True
        ).otherwise(False)
    ).withColumns(
        {
            "Role": rolecon,
            "LeaveKey": F.sha2(
                F.concat(
                    F.col("Absence"), F.lit("-"), F.col("AbsenceableId")
                ), 256
            ),
        }
    ).drop(*["AbsenceableId", "Role1", "Role2"])

    inprojected1 = inprojected.withColumns(
        {
            "key": F.sha2(F.concat_ws("||", *PKColumns), 256),
            "Absence": F.initcap(F.array_join(F.split("Absence", "-"), " "))
        }
    )

    leavesetor = inprojected1.select(*[leavesectorflightcols + key]).distinct()
    
    leavereport = inprojected1.drop(*leavesectorflightcols).dropDuplicates(
        subset=[
            "EmployeeId", "StartDate","EndDate", 
            "ProjectedFlightDate", "ProgrammedFlightDate"
        ]
    )

    return leavereport, leavesetor

def explodeFrame(absence, leaveType):
    '''
    '''
    # absence = spark.table("analyticadebuddha.flightoperation.leavereport")

    absence = absence.alias("df1").join(
        leaveType.alias("df2"), 
        F.col("df1.LeaveKey") == F.col("df2.leavekey"), how="left"
    ).select("df1.*", "df2.Name")

    leavecat = F.when(
        F.col("Absence") == "Leave", "Personal"
    ).otherwise("Professional")

    absence = absence.withColumn("AbsenceCategory", leavecat)

    df = absence.withColumn(
        "LeaveDate",
        F.explode(F.sequence(F.col("StartDate"), F.col("EndDate"), F.expr("INTERVAL 1 DAY")))
    )

    return df


def getDeviation(df):
    '''
    '''
    return df.filter(~F.col("InLeave").contains("Not Assigned")).groupBy(F.col("EmployeeId").alias("EmployeeId_")).agg(
        F.countDistinct("ProjectedFlightDate").alias("LifetimeRosterDeviation"), 
        F.max("ProjectedFlightDate").alias("LatestRosterDeviation"),
        F.countDistinct("ProgrammedFlightDate").alias("ProgrammedDeviation"),
        F.max("ProgrammedFlightDate").alias("LatestProgrammedDeviation"),
        F.concat_ws(" -- ", F.collect_set("Name")).alias("Reasons"), 
        F.concat_ws(" -- ", F.collect_set("AbsenceCategory")).alias("AbsenceCategories"),
        F.concat_ws(" -- ", F.collect_set("ProjectedFlightDate")).alias("ProjectedDevationsOn"),
        F.concat_ws(" -- ", F.collect_set("ProgrammedFlightDate")).alias("ProgrammedDevationsOn")
    )

def getLeaveExploded(leaveType, df):
    '''
    '''
    leaveTypeConsoliodated = leaveType.alias("df1").join(
        df.select("leavekey", "AbsenceCategory", "Absence").distinct().alias("df2"),
        F.col("df1.leavekey") == F.col("df2.LeaveKey"), how="left"
    ).select(
        "df1.*", "df2.AbsenceCategory", "df2.Absence"
    ).filter(F.col("AbsenceCategory").isNotNull())

    return leaveTypeConsoliodated

def getNotInAbsenceInfo(absence, leaveTypeConsoliodated):
    '''
    '''
    employeeinleaves = absence.select("EmployeeId", "LeaveKey").distinct()
    allcombination = absence.select("EmployeeId").distinct().crossJoin(leaveTypeConsoliodated.select("leavekey"))

    notinleave = allcombination.alias("df1").join(
        employeeinleaves.alias("df2"), 
        (F.col("df1.EmployeeId") == F.col("df2.EmployeeId"))
        & 
        (F.col("df1.LeaveKey") == F.col("df2.LeaveKey")), how="left_anti"
    )

    notinleave = notinleave.alias("df1").join(
        leaveTypeConsoliodated.alias("df2"), 
        (F.col("df1.LeaveKey") == F.col("df2.LeaveKey")), how="left"
    ).select("df1.*", "df2.AbsenceCategory", "df2.Absence", "df2.Name")

    notinleave = notinleave.groupBy("EmployeeId").agg(
        F.countDistinct("leavekey").alias("NotInAbsenceNameCount"),
        F.concat_ws(" -- ", F.collect_set("Name")).alias("NotInAbsenceName"),
        F.countDistinct("Absence").alias("NotInAbsenceCount"),
        F.concat_ws(" -- ", F.collect_set("Absence")).alias("NotInAbsence"),
        F.concat_ws(" -- ", F.collect_set("AbsenceCategory")).alias("NotInAbsenceCategory")
    )

    return notinleave

def getCompleteSummary(notinleave, deviated):
    '''
    '''
    return notinleave.alias("df1").join(
        deviated.alias("df2"), 
        (F.col("df1.EmployeeId") == F.col("df2.EmployeeId_")) & (F.col("df2.EmployeeId_").isNotNull()), how="left"
    ).filter(F.col("EmployeeId_").isNotNull()).select(*colssummary)

def toCatalog(config):
    '''
    Write DataFrames from config to catalog with retry logic.
    '''
    for frame in config.get("data"):
        key, value = list(frame.items())[0]
        value = value.localCheckpoint()
        
        for attempt in range(1, 4):
            try:
                print(f"Writing {key} to catalog {catalog} (attempt {attempt})")
                value.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{key}")
                break  
            except Exception as e:
                if attempt >= 3:
                    print(f"Error writing {key} to catalog {catalog}: {e}")
                else:
                    print(f"Error writing {key} to catalog {catalog}, trying again")

    return None

def main():
    """
    """
    absence = spark.read.format("delta").load(pathabsence).select(*cols)

    employeeinfo = spark.table(pathemployeeinfo).withColumn(
        "Gender", gendercon
    )
    crew = spark.table("analyticadebuddha.flightoperation.crew")
    crewlog = crew.filter(F.col("CrewFrom") == "flightlog")
    crewprogram = crew.filter(F.col("CrewFrom") == "programmed")
    crewprojected = crew.filter(F.col("CrewFrom") == "projected")

    leaves = getLeaveConsoliodated()
    leavereport, leavesetor = getLeaveReport(absence, crewprogram, crewprojected)

    leavereport_ = getEmployeeInfo(leavereport, employeeinfo)

    df = explodeFrame(absence = leavereport_, leaveType = leaves)
    deviated = getDeviation(df)
    leaveTypeConsoliodated =  getLeaveExploded(leaves, df)
    notinleave = getNotInAbsenceInfo(absence = leavereport_, leaveTypeConsoliodated=leaveTypeConsoliodated)
    summary = getCompleteSummary(notinleave, deviated)

    config = {
        "data": [{"leavetype": leaves}, {"leavesetor": leavesetor}, {"leavereport": leavereport_}, {"absenceSummary": summary}]
    }


    toCatalog(config=config)

    return None

if __name__ == "__main__":
    main()    