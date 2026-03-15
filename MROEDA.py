# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

spark.table("analyticadebuddha.mro.tablefk").display()

# COMMAND ----------

spark.table("analyticadebuddha.mro.audit").display() #.filter((F.lower("TABLENAME").contains("inspections")) | (F.lower("TABLENAME").contains("directive")))

# COMMAND ----------

colwo = ["Id", "Uid", "WorkOrderText", "WorkOrderNo", "WorkOrderDate", "WorkOrderSubmittedDateTime", "WorkOrderCompletedDateTime", "StatusId", "RegNo", "ModelName", "SerialNo", "WorkOrderStartDate", "WorkOrderCloseDate", "WorkOrderByName", "WorkOrderStatusId", "IsAuthorized", "AuthorizedByName", "IsClosed", "ClosedByName", "HourType", "WorkShopId", "isInHouse", "IsThirdParty", "WorkOrderJobTypeId", "IssueNo", "RevisionNo", "IsFMC", "IsCustomerApprovedObtained", "CustomerApprovedByEmailWorkOrder", "TransTypeId", "WorkOrChangeOrderEnumId", "ModelId", "IsCriticalWorkOrder", "IsDigitalSignatureAdded"]

asondate  = ["AsOnDate"] # AssemblyInspectionStatusId
AISColumns = ["Id", "UID","AssemblyId","ModelInspectionId", "IsMaster", "IsLater", "DoneOnDate", "DoneOnDateApplicable", "WorkOrderNumber", "DoneByEmployeeId", "Place", "ActualManHours", "Remarks", "isDone", "IsCompleted", "IsApplicable", "isLastRecord","WorkOrderId", "LogId", "CreationTime"]

# COMMAND ----------

deleteFilter = (F.col("isDeleted") == 'false')

LogMaintenanceActivities = spark.read.format("delta").load("/mnt/rawmro/LogMaintenanceActivities")

Parts = spark.read.format("delta").load("/mnt/rawmro/Parts")

Periods = spark.read.format("delta").load("/mnt/rawmro/Periods")
PeriodUnits = spark.read.format("delta").load("/mnt/rawmro/PeriodUnits")
ModelInspectionFrequencies = spark.read.format("delta").load("/mnt/rawmro/ModelInspectionFrequencies")
AssemblyInspectionFrequencies = spark.read.format("delta").load("/mnt/rawmro/AssemblyInspectionFrequencies")
AssemblyInspectionStatusPeriods = spark.read.format("delta").load("/mnt/rawmro/AssemblyInspectionStatusPeriods")
AssemblyStatus = spark.read.format("delta").load("/mnt/rawmro/AssemblyStatus")
Assemblies = spark.read.format("delta").load("/mnt/rawmro/Assemblies")
AssemblyInspections = spark.read.format("delta").load("/mnt/rawmro/AssemblyInspections").filter(deleteFilter).select(*AISColumns)
# Aircraft = spark.read.format("delta").load("/mnt/rawmro/Aircraft")
atachapter = spark.read.format("delta").load("/mnt/rawmro/AtaChapters")
AssemblyInspectionStatus = spark.read.format("delta").load("/mnt/rawmro/AssemblyInspectionStatus").filter(deleteFilter).select(*AISColumns + asondate)
ModelInspections = spark.read.format("delta").load("/mnt/rawmro/ModelInspections")
WorkOrders = spark.read.format("delta").load("/mnt/rawmro/WorkOrders").filter((F.col("WorkOrderDate") >= "2025-01-01T00:00:00.000+00:00") & deleteFilter).select(*colwo)

AssemblyTypes= spark.read.format("delta").load("/mnt/rawmro/AssemblyTypes")
ModelMonitorInspectionTypes= spark.read.format("delta").load("/mnt/rawmro/ModelMonitorInspectionTypes")
Models = spark.read.format("delta").load("/mnt/rawmro/Models")
ModelInspectionParts = spark.read.format("delta").load("/mnt/rawmro/ModelInspectionParts")

# COMMAND ----------

AssemblyInspectionStatusPeriods.display()

# COMMAND ----------

Models.display()

# COMMAND ----------

WorkOrders.display()

# COMMAND ----------

ModelInspectionFrequencies.columns

# COMMAND ----------

ModelInspections.display()

# COMMAND ----------

Assemblyinspectionunit = AssemblyInspectionStatus.alias("df1").join(
    ModelInspectionFrequencies.alias("df2"), F.col("df1.ModelInspectionId") == F.col("df2.ModelInspectionId"), how="left"
).select("df1.*", F.col("df2.FrequencyValue").alias("ModelFrequencyValue"), "df2.PeriodUnitId")

tofindmodel = Assemblyinspectionunit.alias("df1").join(
    PeriodUnits.alias("df2"), F.col("df1.PeriodUnitId") == F.col("df2.Id"), how="left"
).select("df1.*", "df2.Name").filter(F.col("DoneOnDate") >= "2024-01-01T00:00:00.000+00:00")

AssemblyInspectionStatus.alias("df1").join(
    ModelInspections.alias("df2"), F.col("df1.ModelInspectionId") == F.col("df2.Id"), how="left"
).select(
    "df1.*", "df2.ModelId", "df2.Code", "df2.AtaChapterId", "df2.TaskSourceReference", "df2.Description", "df2.ModelMonitorInspectionTypeId", 
    "df2.Zone", "df2.Area"
).display()

# COMMAND ----------

AssemblyInspectionStatus.alias("df1").join(
    ModelInspections.alias("df2"), F.col("df1.ModelInspectionId") == F.col("df2.Id"), how="left"
).select(
    "df1.*", "df2.ModelId", "df2.Code", "df2.AtaChapterId", "df2.TaskSourceReference", "df2.Description", "df2.ModelMonitorInspectionTypeId", 
    "df2.Zone", "df2.Area"
).display()

# COMMAND ----------

AssemblyInspectionStatusPeriods.display()

# COMMAND ----------

PeriodUnits.display()

# COMMAND ----------

Periods.display()

# COMMAND ----------

AssemblyStatus.display()

# COMMAND ----------

model_insepction_FandU = ModelInspectionFrequencies.alias("df1").join(
    PeriodUnits.alias("df2"), 
    (F.col("df1.PeriodUnitId") == F.col("df2.Id")) 
    & ((F.col("df1.IsDeleted") == 'false') & (F.col("df2.IsDeleted") == 'false'))
    , how="left"
).select(
    "df1.Id", "df1.ModelInspectionId", "df2.Code", "df2.Name", "df1.FrequencyValue", "df2.CreationTime"
)

model_insepction_FandU.display()

# COMMAND ----------

AssemblyInspectionFrequencies.columns

# COMMAND ----------

Periods.display()

# COMMAND ----------

selcols = [
    "df1.Id", "df1.AssemblyInspectionId", "df1.ModelInspectionFrequencyId", "df1.FrequencyValue", 
    "df2.Name","df2.DueLimit", "df2.DueLimitForFas","df1.DoneOnValue", "df1.CurrentValue", "df1.DueOnValue",
    "CurrentValueToDisplay","df1.DoneOnValueToDisplay","df1.DueOnValueToDisplay", "df2.CreationTime"
]

Assembly_insepction_FandU = AssemblyInspectionFrequencies.alias("df1").join(
    Periods.alias("df2"), 
    (F.col("df1.PeriodId") == F.col("df2.Id")) 
    & ((F.col("df1.IsDeleted") == 'false') & (F.col("df2.IsDeleted") == 'false'))
    ,how="left"
).select(*selcols)

Assembly_insepction_FandU.display()

# COMMAND ----------

AssemblyInspections.display()

# COMMAND ----------

ModelInspections.display()

# COMMAND ----------

ModelMonitorInspectionTypes.display()

# COMMAND ----------

AssemblyTypes.display()

# COMMAND ----------

AssemblyInspectionStatusPeriods.display()

# COMMAND ----------



# COMMAND ----------

ComponentInspectionPeriods   = spark.read.format("delta").load("/mnt/rawmro/ComponentInspectionPeriods")
ComponentInspections   = spark.read.format("delta").load("/mnt/rawmro/ComponentInspections")
ComponentModificationStatuses   = spark.read.format("delta").load("/mnt/rawmro/ComponentModificationStatuses")
ComponentModificationStatusPeriods   = spark.read.format("delta").load("/mnt/rawmro/ComponentModificationStatusPeriods")
ComponentPeriods   = spark.read.format("delta").load("/mnt/rawmro/ComponentPeriods")
Components   = spark.read.format("delta").load("/mnt/rawmro/Components")
ComponentServiceInspectionStatuses   = spark.read.format("delta").load("/mnt/rawmro/ComponentServiceInspectionStatuses")
ComponentServiceInspectionStatusPeriods   = spark.read.format("delta").load("/mnt/rawmro/ComponentServiceInspectionStatusPeriods")
ComponentServicePeriods   = spark.read.format("delta").load("/mnt/rawmro/ComponentServicePeriods")
ComponentServices   = spark.read.format("delta").load("/mnt/rawmro/ComponentServices")
ComponentStatuses   = spark.read.format("delta").load("/mnt/rawmro/ComponentStatuses")
ComponentStatusPeriods   = spark.read.format("delta").load("/mnt/rawmro/ComponentStatusPeriods")

# COMMAND ----------

ComponentInspections.display()