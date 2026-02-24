# Databricks notebook source
import pyspark.sql.functions as F
# import Fomodule

# COMMAND ----------

spark.table("analyticadebuddha.flightoperation.delayinfo").filter(F.col("FlightDate") >= "2025-10-01").display()

# COMMAND ----------

spark.table("analyticadebuddha.businesstable.flightoperation_keys")

# COMMAND ----------

# spark.table("analyticadebuddha.businesstable.flightoperation_keys").filter(F.size(F.col("columnspks")) == 0).select(F.col("table_name").alias("TableName"), F.concat(F.lit("/mnt/fo/raw/"), F.col("table_name")).alias("Filepath"), F.col("columnspks")).unionByName(
#     spark.table("analyticadebuddha.businesstable.FO_PRIORITY"), allowMissingColumns = True
# ).write.mode("overwrite").option("megeSchema", "true").saveAsTable("analyticadebuddha.businesstable.FO_PRIORITY")

# COMMAND ----------

# df = spark.table("analyticadebuddha.default.updatefoandkeys").filter(F.size(F.col("columnspks")) == 0)
# table_names = [row.TableName for row in df.select("TableName").collect()]

# table_names

# COMMAND ----------

spark.table("analyticadebuddha.businesstable.flightoperation_foreignkey").display()

# COMMAND ----------

filterdeleted = F.col("deleted_at").isNull()

recorddateacols = ["Id", "training_id", "start_date", "end_date"]
categorycols = ["Id", "Name", "division_id", "is_onetime", "has_hour", "post_rest_days", "pre_rest_days"]
trainingcols = ["Id", "training_category_id", "employee_id", "expire_date", "renewed_by_id"]

training = spark.read.format("delta").load("/mnt/fo/raw/trainings").filter(filterdeleted).select(*trainingcols)
recorddate = spark.read.format("delta").load("/mnt/fo/raw/training_record_dates").filter(filterdeleted).select(*recorddateacols)
category = spark.read.format("delta").load("/mnt/fo/raw/training_categories").filter(filterdeleted).select(*categorycols)
program = spark.read.format("delta").load("/mnt/fo/raw/training_programme_items")

# Instructor
trainingdate = spark.read.format("delta").load("/mnt/fo/raw//instructor_training_date")
trainingrecorddate = spark.read.format("delta").load("/mnt/fo/raw//training_record_dates")
trainingauthority = spark.read.format("delta").load("/mnt/fo/raw//training_authority")
traininginstructor = spark.read.format("delta").load("/mnt/fo/raw//training_instructor")
trainingparticipant = spark.read.format("delta").load("/mnt/fo/raw/training_participant")

# training_schedules = spark.read.format("delta").load("/mnt/fo/raw/training_schedules")

# COMMAND ----------

isdeleted = (F.col("deleted_at").isNull())
cathr = spark.read.format("delta").load("/mnt/hrt/raw/trainings").filter(isdeleted)

# COMMAND ----------

catfo = category.withColumn(
    "Name", F.initcap(F.regexp_replace(F.trim(F.col("Name")), "\\s+", " "))
)

cathr_  = cathr.withColumn(
    "title", F.initcap(F.regexp_replace(F.trim(F.col("title")), "\\s+", " "))
)

catfo.alias("df1").join(
    cathr_.alias("df2"), F.col("df1.Name") == (F.col("df2.title")), how="inner"
).select("df1.Name","df2.Title").display()

# COMMAND ----------

leavereport = spark.table("analyticadebuddha.flightoperation.leavereport")
leaveinfo = leavereport.select("EmployeeId", "StartDate", "EndDate", "Absence", "LeaveKey").distinct()
leaveinfo.display()

# COMMAND ----------

preleavedays = (
    (
        F.unix_timestamp(F.to_timestamp(F.col("PreLeaveEnd"))) - F.unix_timestamp(F.to_timestamp(F.col("PreLeaveStart")))
    )/(3600*24)
)
preleavedays_ = F.when(
    preleavedays == 0, 1
).otherwise(preleavedays)

postleavedays = (
    (
        F.unix_timestamp(F.to_timestamp(F.col("PostLeaveEnd"))) - F.unix_timestamp(F.to_timestamp(F.col("PostLeaveStart")))
    )/(3600*24)
)

postleavedays_ = F.when(
    postleavedays == 0, 1
).otherwise(postleavedays)


ntrainingdays = (
    (F.unix_timestamp(F.col("EndDate")) - F.unix_timestamp(F.col("StartDate")))
)/(3600*24)

ntrainingdays_ = F.when(
    ntrainingdays == 0, 1
).otherwise(ntrainingdays)

# COMMAND ----------

afterTrainingCols  = ["Id", "TrainingId","EmployeeId", "PreLeaveStart", "PreLeaveEnd", "PostLeaveStart", "PostLeaveEnd", "PreAbsenceCat", "PostAbsenceCat", "PreRestDays", "PostRestDays"]

# COMMAND ----------

from pyspark.sql.window import Window

windowfunc = Window.partitionBy("EmployeeId", "TrainingCategoryId").orderBy(F.desc("EndDate"))

records  = recorddate.alias("df1").join(
    training.alias("df2"), F.col("df1.training_id") == F.col("df2.Id"), how="left"
).select(
    "df1.*", "df2.training_category_id", "df2.employee_id", "df2.expire_date"
)

trainingDetails = Fomodule.getAlias(
    records.alias("df1").join(
        category.alias("df2"), (
            F.col("df1.training_category_id") == F.col("df2.Id")
        ), how="left"
    ).select(
        "df1.*", F.col("df2.Name").alias("CategoryName"), 
        "df2.division_id", "df2.is_onetime", "df2.has_hour"
    )
).dropDuplicates(subset = ["EmployeeId", "StartDate", "EndDate", "TrainingId"])

# trainingDetails.alias("df1").join(
#     trainingdate.alias("df2"), F.col("df1.Id") == F.col("df2.training_date_id"), how="left"
# ).select("df1.*", F.col("df2.employee_id").alias("InstructorId"))

cats = trainingDetails.select(F.col("Categoryname")).distinct()


trainingdetailsccols = ["Id", "TrainingId","EmployeeId", "StartDate", "EndDate", "TrainingCategoryId"]
employeetraninginfo = trainingDetails.select(*trainingdetailsccols)

laginfo = employeetraninginfo.withColumns(
    {
        "StartDateLag": F.date_add(F.col("StartDate"), -1),
        "EndDateLead": F.date_add(F.col("EndDate"), 1)
    }
)

postPreDays = laginfo.alias("df1").join(
    leaveinfo.alias("df2"), (F.col("df1.EmployeeId") == F.col("df2.EmployeeId")) &
    (
        (F.col("df1.StartDateLag") >= F.col("df2.StartDate")) & (F.col("df1.StartDateLag") == F.col("df2.EndDate"))
    ), how="left"
).join(
    leaveinfo.alias("df3"), (F.col("df1.EmployeeId") == F.col("df3.EmployeeId")) &
    (
        (F.col("df1.EndDateLead") == F.col("df3.StartDate")) & (F.col("df1.EndDateLead") <= F.col("df3.EndDate"))
    ), how="left"
).select(
    "df1.*", F.col("df2.StartDate").alias("PreLeaveStart"), F.col("df2.EndDate").alias("PreLeaveEnd"), F.col("df3.StartDate").alias("PostLeaveStart"),
    F.col("df3.EndDate").alias("PostLeaveEnd"), F.col("df2.Absence").alias("PreAbsenceCat"), F.col("df3.Absence").alias("PostAbsenceCat")
).drop("EndDateLead", "StartDateLag").withColumns(
    {
        "PreRestDays": preleavedays_,
        "PostRestDays": postleavedays_
    }
).select(*afterTrainingCols)

# laginfo.alias("df1").join(
#     leaveinfo.alias("df2"), (F.col("df1.EmployeeId") == F.col("df2.EmployeeId")) &
#     (
#         (F.col("df1.EndDateLead") >= F.col("df2.StartDate")) & (F.col("df1.EndDateLead") <= F.col("df2.EndDate"))
#     ), how="left"
# 

postPreDays.display()

# COMMAND ----------

Phases = trainingDetails.groupBy(
    "TrainingId", "TrainingCategoryId", "EmployeeId"
).agg(
    F.countDistinct("StartDate").alias("Counts"), F.first("ExpireDate").alias("ExpireDate"), F.min("StartDate").alias("StartDate1")
)

MultiDays = Phases.filter(F.col("Counts") > 1)

Phases.display()

# COMMAND ----------



# COMMAND ----------

MultiDays = trainingDetails.groupBy("TrainingId", "TrainingCategoryId", "EmployeeId").agg(F.countDistinct("StartDate").alias("Counts")).filter(F.col("Counts") > 1)

MultiDays_ = trainingDetails.alias("df1").join(
    MultiDays.alias("df2"), 
    (F.col("df1.TrainingId") == F.col("df2.TrainingId")) 
    & (F.col("df1.EmployeeId") == F.col("df2.EmployeeId"))
    & (F.col("df1.TrainingCategoryId") == F.col("df2.TrainingCategoryId")), how="inner"
).select("df1.*")

SingleDays = trainingDetails.alias("df1").join(
    MultiDays.alias("df2"), 
    (F.col("df1.TrainingId") == F.col("df2.TrainingId")) 
    & (F.col("df1.EmployeeId") == F.col("df2.EmployeeId"))
    & (F.col("df1.TrainingCategoryId") == F.col("df2.TrainingCategoryId")), how="left_anti"
).select("df1.*")


expiry = trainingDetails.withColumns(
    {
        "LatestTraining": F.row_number().over(windowfunc),
        "LastTrainingEndedOn": F.lead("EndDate").over(windowfunc),
        "LastExpireDate": F.lead("ExpireDate").over(windowfunc),
        "DaysFromExpire": (
            F.unix_timestamp(F.col("LastExpireDate")) - F.unix_timestamp(F.col("EndDate"))
        )/(3600*24),
        "IsBeforeExpire": F.when(
            F.col("LastExpireDate") > F.col("StartDate"), F.lit("Earlier")
        ).when(
            F.col("LastExpireDate") == F.col("StartDate"), F.lit("SameDay") 
        ).when(
            F.col("LastExpireDate") < F.col("StartDate"), F.lit("Later") 
        ).otherwise(F.lit("First")),
        "NTrainingDays": ntrainingdays_
    }
)

traininginfo_ = expiry.alias("df1").join(
    postPreDays.alias("df2"),
    (F.col("df1.Id") == F.col("df2.Id")) &
    (F.col("df1.TrainingId") == F.col("df2.TrainingId")) &
    (F.col("df1.EmployeeId") == F.col("df2.EmployeeId")), how="left"
).select(
    "df1.*", "df2.PreRestDays","df2.PreAbsenceCat",
    "df2.PostRestDays","df2.PostAbsenceCat"
).filter(F.col("EmployeeId").isNotNull()).drop("TrainingCategoryId", "HasHour")

# .write.mode("overwrite").saveAsTable("analyticadebuddha.flightoperation.trainingdetails")

traininginfo_.display()

# COMMAND ----------

training_ = trainingDetails.groupBy("TrainingId", "TrainingCategoryId", "EmployeeId").agg(F.max("StartDate").alias("StartDate"), F.max("ExpireDate").alias("ExpireDate"))

training_.display()

# COMMAND ----------

trainingDetails.display()

# COMMAND ----------

expieryinfo = Phases.alias("df1").join(
    training_.alias("df2"),
    (F.col("df1.TrainingId") < F.col("df2.TrainingId")) 
    & (F.col("df1.EmployeeId") == F.col("df2.EmployeeId"))
    & (F.col("df1.TrainingCategoryId") == F.col("df2.TrainingCategoryId"))
    & (F.col("df1.StartDate1") < F.col("df2.StartDate")) 
    , how="left"
).select(
    "df1.*", F.col("df2.TrainingId").alias("TrainingId2"), F.col("df2.StartDate").alias("StartDate2"), F.col("df2.ExpireDate").alias("ExpireDate2")
).withColumns(
    {
        "TrainingBeforeExpire": F.when(
            F.col("ExpireDate") > F.col("StartDate2"), F.lit("Earlier")
        ).when(
            F.col("ExpireDate") == F.col("StartDate2"), F.lit("SameDay") 
        ).when(
            F.col("ExpireDate") < F.col("StartDate2"), F.lit("Later")
        ).otherwise(F.lit("Not Occured Yet")),

        "ExpireDueDays": F.when(
            F.col("TrainingBeforeExpire") == "Not Occured Yet",
            F.round((F.unix_timestamp(F.col("ExpireDate")) - F.unix_timestamp(F.current_timestamp()))/(3600*24))
        ),

        "Exceed": F.when(
            F.col("ExpireDueDays") <= 0, F.lit("Days Exceed")
        ).when(
            F.col("ExpireDueDays") > 0, F.lit("Days Remaining")
        )
    }
)

trainingDetails.alias("df1").join(
    expieryinfo.alias("df2"), 
    (F.col("df1.TrainingId") == F.col("df1.TrainingId"))
    & (F.col("df1.EmployeeId") == F.col("df2.EmployeeId"))
    & (F.col("df1.TrainingCategoryId") == F.col("df2.TrainingCategoryId")), how="inner"
).select("df1.*", "df2.TrainingBeforeExpire", "df2.ExpireDueDays", "df2.Exceed").display()

# COMMAND ----------



# COMMAND ----------

catfo = spark.table("analyticadebuddha.flightoperation.trainingdetails").select("Categoryname").distinct().withColumn(
    "CategoryName", F.initcap(F.regexp_replace(F.trim(F.col("CategoryName")), "\\s+", " "))
)
catfo = catfo.withColumn(
    "Name", F.initcap(F.regexp_replace(F.trim(F.col("Categoryname")), "\\s+", " "))
)

# cathr_  = cathr.withColumn(
#     "title", F.initcap(F.regexp_replace(F.trim(F.col("title")), "\\s+", " "))
# )

common = catfo.alias("df1").join(
    cathr_.alias("df2"), F.col("df1.CategoryName") == (F.col("df2.title")), how="inner"
).select("df1.CategoryName","df2.title")

common.display()

#common category between recorded training category (Fo) and all the avaiable category in the hr training database

# COMMAND ----------

traininginfo_

# COMMAND ----------

category.display()

# COMMAND ----------

trainings.display() Aircraft Wheels and brakes

# COMMAND ----------



# COMMAND ----------

budgetcols = [
    "Id", "training_schedule_id", "tada_estimation", "tada", "venue_budget", "miscellaneous_price", "miscellaneous_report", "total_budget", "organization_budget","total_final_budget", "sod", "instructor_allowance"
]
secheduleCols = [
    "Id", "training_id","type", "status", "is_complete", "certificate_issued", 
    "instructor_allowance_released", "disapproval_report"
]

trainingcols = [
    "id", "title", "code", "type", "is_mandatory", "is_recurring", "status", "expiry_time_frame", "last_concluded"
]

isdeleted = (F.col("deleted_at").isNull())

trainings = spark.read.format("delta").load("/mnt/hrt/raw/trainings").filter(isdeleted).select(*trainingcols)
trainingdates = spark.read.format("delta").load("/mnt/hrt/raw/training_dates").filter(isdeleted)
trainingparties = spark.read.format("delta").load("/mnt/hrt/raw/training_participants").filter(isdeleted)
trainingbudget = spark.read.format("delta").load("/mnt/hrt/raw/training_budgets").filter(isdeleted).select(*budgetcols)
trainingsechedule = spark.read.format("delta").load("/mnt/hrt/raw/training_schedules").filter(isdeleted) #.select(*secheduleCols)
approvredSecheduled  = trainingsechedule.filter(F.col("status").contains("approved"))

# COMMAND ----------

trainingdates.display()

# COMMAND ----------

trainingSechedule = approvredSecheduled.alias("df1").join(
    cathr.alias("df2"), F.col("df1.training_id") == F.col("df2.id"), how="left"
).select(
    "df1.*", "df2.title", "df2.type", "df2.is_mandatory", "df2.is_recurring", 
    "df2.status", "df2.expiry_time_frame", "df2.last_concluded"
)

parties  = trainingSechedule.alias("df1").join(
    trainingparties.alias("df2"), F.col("df2.training_schedule_id") == F.col("df1.id"), how="left"
).select("df2.traineeable_id", "df2.traineeable_type", "df2.training_schedule_id", "df1.*")


parties_ = parties.alias("df1").join(
    trainingdates.alias("df2"), F.col("df1.id") == F.col("df2.training_schedule_id"), how="inner"
).select("df1.*", F.col("df2.date").alias("StartDate"))

bastaff = parties_.filter(F.col("traineeable_type").contains("BaStaff"))
externalstaff = parties_.filter(~F.col("traineeable_type").contains("BaStaff"))

# COMMAND ----------

parties.display()

# COMMAND ----------

emp = spark.table("analyticadebuddha.flightoperation.employeeinfo").filter(F.col("IsActive") == "1")
desig = spark.table("analyticadebuddha.flightoperation.department_designation") #.filter(F.col("OperationName").contains("Crew"))
empdeg = emp.alias("emp").join(desig.alias("desig"), emp.EmployeeIdMain == desig.EmployeeIdMain, how ="inner").select(
    "emp.*", "desig.OperationName", "desig.DesignationName"
)

# COMMAND ----------

# user: bt_data
# host: 13.228.112.54
# db: hrtraining_data
# pwd: TV3669WZTpWDo3uP
import dbconnect
import pandas as pd
from sqlalchemy import text

HostDB= "13.228.112.54"
UserDB= "bt_data"
PasswordDB= "TV3669WZTpWDo3uP"
db = "hrtraining_data"

engine = dbconnect.MysqlConnectAlchemy(HostDB, UserDB, db, PasswordDB, spark=None)

engineconn = engine.connect()
table = "ba_staff"
table1 = "externals"
query = f"""
            SELECT * 
            FROM `{table}`
        """

query1 = f"""
            SELECT * 
            FROM `{table1}`
        """
    
bastaffdb = spark.createDataFrame(pd.read_sql(text(query), engineconn))
externalstaffdb = spark.createDataFrame(pd.read_sql(text(query1), engineconn))

# employeetraninginfo.alias("df1").join(
#     df.alias("df2"), F.col("df1.EmployeeId") == F.col("df2.employee_id"), how="left"
# ).select("df1.*", "df2.employee_id").display()

bastaffdb.display()

# COMMAND ----------

traininginfohr  = bastaff.alias("df1").join(
    bastaffdb.alias("df2"), F.col("df1.traineeable_id") == F.col("df2.id"), how="inner"
).select("df1.*", "df2.employee_id").withColumn(
    "Categoryname", F.initcap(F.regexp_replace(F.trim(F.col("title")), "\\s+", " "))
)

traininginfohr_ext = externalstaff.alias("df1").join(
    externalstaffdb.alias("df2"), F.col("df1.traineeable_id") == F.col("df2.id"), how="inner"
).select("df1.*", "df2.employee_id").withColumn(
    "Categoryname", F.initcap(F.regexp_replace(F.trim(F.col("title")), "\\s+", " "))
)

traininginfo_fo = traininginfo_.alias("df1").join(
    empdeg.alias("df2"), F.col("df1.EmployeeId") == F.col("df2.EmployeeIdmain"), how="inner"
).select(
    "df1.*", F.col("df2.EmployeeId").alias("Empid"), "df2.OperationName", "DesignationName"
).withColumn(
    "CategoryName", F.initcap(F.regexp_replace(F.trim(F.col("Categoryname")), "\\s+", " "))
)

traininginfo_fo.alias("df1").join(
    traininginfohr.alias("df2"), 
    (F.col("df1.Categoryname") == F.col("df2.Categoryname")) &
    (F.col("df1.Empid") == F.col("df2.employee_id")) &
    (F.col("df1.StartDate") == F.col("df2.StartDate")), how="inner"
).display()


# COMMAND ----------

traininginfo_fo.alias("df1").join(
    traininginfohr_ext.alias("df2"), 
    (F.col("df1.Categoryname") == F.col("df2.Categoryname")) &
    (F.col("df1.Empid") == F.col("df2.employee_id")) &
    (F.col("df1.StartDate") == F.col("df2.StartDate")), how="inner"
).display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

bastaff_ = bastaff.withColumn(
    "title", F.initcap(F.regexp_replace(F.trim(F.col("title")), "\\s+", " "))
).alias("df1").join(
    common.alias("df2"), F.col("df1.title") == F.col("df2.title"), how="inner"
).select("df1.*")

empinvolved = bastaff_.select("traineeable_id").distinct().alias("df1").join(
    bastaffdb.alias("df2"), F.col("df1.traineeable_id") == F.col("df2.id"), how="inner"
).select("df2.*")

empinvolved.display()

# COMMAND ----------

traingfoemp = traininginfo_.select("EmployeeId").distinct()
traingfoemp.display()

# COMMAND ----------

traingfoemp

# COMMAND ----------

emp = spark.table("analyticadebuddha.flightoperation.employeeinfo").filter(F.col("IsActive") == "1")
desig = spark.table("analyticadebuddha.flightoperation.department_designation") #.filter(F.col("OperationName").contains("Crew"))
empdeg = emp.alias("emp").join(desig.alias("desig"), emp.EmployeeIdMain == desig.EmployeeIdMain, how ="inner").select(
    "emp.*", "desig.OperationName", "desig.DesignationName"
)

empincludehr = empdeg.alias("df1").join(
    empinvolved.alias("df2"), F.col("df1.EmployeeId") == F.col("df2.Employee_id"), how="inner"
).select("df1.*")

empincludefo = empdeg.alias("df1").join(
    traingfoemp.alias("df2"), F.col("df1.EmployeeIdMain") == F.col("df2.EmployeeId"), how="inner"
).select("df1.*").display()
# empdeg.display()

# COMMAND ----------

desig.display() #2809

# COMMAND ----------

emp.display()

# COMMAND ----------



# COMMAND ----------

isdeleted = (F.col("deleted_at").isNull())

training = spark.read.format("delta").load("/mnt/hrt/raw/trainings").filter(isdeleted).select(*trainingcols)
trainingdates = spark.read.format("delta").load("/mnt/hrt/raw/training_dates") #.filter(isdeleted)
trainingparties = spark.read.format("delta").load("/mnt/hrt/raw/training_participants").filter(isdeleted)
trainingbudget = spark.read.format("delta").load("/mnt/hrt/raw/training_budgets").filter(isdeleted).select(*budgetcols)
trainingsechedule = spark.read.format("delta").load("/mnt/hrt/raw/training_schedules").filter(isdeleted) #.select(*secheduleCols)
approvredSecheduled  = trainingsechedule.filter(F.col("status").contains("approved"))

categorycols = ["Id", "Name", "division_id", "is_onetime", "has_hour", "post_rest_days", "pre_rest_days"]
category = spark.read.format("delta").load("/mnt/fo/raw/training_categories").filter(isdeleted).select(*categorycols)

# spark.read.format("delta").load("/mnt/hrt/raw/ba_staff").display()

# COMMAND ----------

trainingparties.alias("df1").join(
    df.alias("df2"), F.col("df1.traineeable_id") == F.col("df2.id"), how = "inner"
).display()

# COMMAND ----------



# COMMAND ----------

import dbconnect
import pandas as pd
from sqlalchemy import create_engine, text

HostDB = '13.126.2.235'
db = "operation"
UserDB= dbutils.secrets.get(scope="flightoperation", key="fo_db_user")
PasswordDB =  dbutils.secrets.get(scope="flightoperation", key="fo_db_pass")
engine = dbconnect.MysqlConnectAlchemy(HostDB, UserDB, db, PasswordDB)
engineconn = engine.connect()
query = """
    SELECT * FROM training_expense_parameters
"""

pd.read_sql(text(query), engineconn)
# spark.createDataFrame(pd.read_sql(text(query), engineconn)).display() #instructor_training_date training_instructor

# COMMAND ----------

category.display()

# COMMAND ----------

