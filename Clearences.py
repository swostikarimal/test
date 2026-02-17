# Databricks notebook source
import pyspark.sql.functions as F

pathaircraftcrewC= "/mnt/fo/raw/aircraft_crew_clearances"
pathflightsectorC= "/mnt/fo/raw/flight_sector_clearance_settings"
pathflightsectorcrewC= "/mnt/fo/raw/flight_sector_crew_clearances"
pathflightsectorcrewCItem= "/mnt/fo/raw/flight_sector_crew_clearance_items"
pathinternationalC = "/mnt/fo/raw/international_sector_crew_clearances"
pathsector = "/mnt/fo/processed/actual/sectorinfo"
pathemployeeinfo = "analyticadebuddha.flightoperation.employeeinfo"

catalog = "analyticadebuddha.flightoperation"

clearedlogic = F.when(
    F.col("ClearedSectors") > 3, "All"
).when(
    F.col("ClearedSectors") == 3, "Three"
).when(
    F.col("ClearedSectors") == 2, "Two"
).when(
    F.col("ClearedSectors") == 1, "One"
).otherwise("None")

def getSectorCleareance(flightsectorcrewC, sector, employeeinfo):
    '''
    '''
    flightsectorcrewC1 = flightsectorcrewC.select(
        "Id", F.col("employee_id").alias("EmployeeId"), F.col("sector_id").alias("SectorId"), 
        F.col("aircraft_type_id").alias("AircraftId"), F.col("expires_in").alias("ExpiresIn")
    )

    totalemployeeclearence = flightsectorcrewC1.groupBy("EmployeeId").agg(
        F.countDistinct("SectorId").alias("NumberOFSectors"),
        F.countDistinct("AircraftId").alias("NumberOFAircraft")
    )

    clearencebysector = flightsectorcrewC1.groupBy("EmployeeId", "SectorId").agg(F.countDistinct("AircraftId").alias("NumberOFAircraft"))
    clerencebycraft = flightsectorcrewC1.groupBy("EmployeeId", "AircraftId").agg(F.countDistinct("SectorId").alias("NumberOFSectors"))
    clerencebysector = flightsectorcrewC1.groupBy("EmployeeId").agg(F.countDistinct("SectorId").alias("ClearedSectors"))

    flightsectorcrewC1.alias("df1").join(
        sector.alias("df2"), F.col("df1.SectorId") == F.col("df2.ToSectorId"), how = "left"
    ).select("df1.*", "df2.ToSectorName").distinct().write.mode("overwrite").saveAsTable(f"{catalog}.sectorclearence")

    employeeinfo.alias("df1").join(
        clerencebysector.alias("df2"), F.col("df1.EmployeeIdMain") == F.col("df2.EmployeeId"), how = "left"
    ).select("df1.*", "df2.ClearedSectors").withColumn("ClearedSectors", clearedlogic).write.mode("overwrite").option(
        "mergSchema", "true"
    ).option("overwriteSchema", "true").saveAsTable(pathemployeeinfo)
        
    sector.write.mode("overwrite").saveAsTable(f"{catalog}.sectorinfo")

    return None

def main():
    '''
    '''
    aircraftcrewC = spark.read.format("delta").load(pathaircraftcrewC)
    flightsectorC = spark.read.format("delta").load(pathflightsectorC)
    flightsectorcrewC = spark.read.format("delta").load(pathflightsectorcrewC).filter(F.col("deleted_at").isNull())
    flightsectorcrewCItem = spark.read.format("delta").load(pathflightsectorcrewCItem)
    internationalC = spark.read.format("delta").load(pathinternationalC)
    sector = spark.read.format("delta").load(pathsector)
    employeeinfo = spark.table(pathemployeeinfo)

    getSectorCleareance(flightsectorcrewC, sector, employeeinfo)

if __name__ == "__main__":
    main()