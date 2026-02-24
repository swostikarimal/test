# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import Fomodule

windowfunc = Window.partitionBy("EmployeeId", "TrainingCategoryId").orderBy(F.desc("EndDate"))

filterdeleted = F.col("deleted_at").isNull()

recorddateacols = ["Id", "training_id", "start_date", "end_date"]
categorycols = ["Id", "Name", "division_id", "is_onetime", "has_hour", "post_rest_days", "pre_rest_days"]
trainingcols = ["Id", "training_category_id", "employee_id", "expire_date", "renewed_by_id"]

trainingdetailsccols = ["Id", "TrainingId","EmployeeId", "StartDate", "EndDate","ExpireDate", "TrainingCategoryId", "CategoryName"]

prepostcols = ["df1.*", F.col("df2.StartDate").alias("PreLeaveStart"), F.col("df2.EndDate").alias("PreLeaveEnd"), 
                F.col("df3.StartDate").alias("PostLeaveStart"), F.col("df3.EndDate").alias("PostLeaveEnd"), 
                F.col("df2.Absence").alias("PreAbsenceCat"), F.col("df3.Absence").alias("PostAbsenceCat")
]

afterTrainingCols  = ["Id", "TrainingId","EmployeeId", "TrainingCategoryId", "CategoryName","StartDate", 
                      "EndDate", "TrainingDuration" ,"ExpireDate","PreAbsenceCat", "PostAbsenceCat", "PreRestDays", "PostRestDays"
]

'''----------------------------------------------------------------------------------------------------------------'''

phases_ = F.when(
    F.col("Counts") > 1, "Multiple"
).otherwise("Single")


preleavedays = (
    (
        F.unix_timestamp(F.to_timestamp(F.col("PreLeaveEnd"))) - F.unix_timestamp(F.to_timestamp(F.col("PreLeaveStart")))
    )/(3600*24)
)
preleavedays_ = F.when(
    preleavedays == 0, 1
).otherwise(preleavedays +1)

postleavedays = (
    (
        F.unix_timestamp(F.to_timestamp(F.col("PostLeaveEnd"))) - F.unix_timestamp(F.to_timestamp(F.col("PostLeaveStart")))
    )/(3600*24)
)

postleavedays_ = F.when(
    postleavedays == 0, 1
).otherwise(postleavedays + 1)


ntrainingdays = (
    (F.unix_timestamp(F.col("EndDate")) - F.unix_timestamp(F.col("StartDate")))
)/(3600*24)

ntrainingdays_ = F.when(
    ntrainingdays == 0, 1
).otherwise(ntrainingdays + 1)

'''----------------------------------------------------------------------------------------------------------------'''

def getPrePostLeave(recorddate, training, category, leaveinfo):
    '''
    '''
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
    ).dropDuplicates(subset = ["EmployeeId", "StartDate", "EndDate"])

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
            (F.col("df1.StartDateLag") >= F.col("df2.StartDate")) 
            & (F.col("df1.StartDateLag") == F.col("df2.EndDate"))
        ), how="left"
    ).join(
        leaveinfo.alias("df3"), (F.col("df1.EmployeeId") == F.col("df3.EmployeeId")) &
        (
            (F.col("df1.EndDateLead") == F.col("df3.StartDate")) 
            & (F.col("df1.EndDateLead") <= F.col("df3.EndDate"))
        ), how="left"
    ).select(*prepostcols).drop("EndDateLead", "StartDateLag").withColumns(
        {
            "PreRestDays": preleavedays_,
            "PostRestDays": postleavedays_,
            "TrainingDuration": ntrainingdays_
        }
    ) .select(*afterTrainingCols)

    return postPreDays

def getExpieryInfo(postPreDays, Phases):
    '''
    '''
    training_ = postPreDays.groupBy(
        "TrainingId", "TrainingCategoryId", "EmployeeId"
    ).agg(
        F.max("StartDate").alias("StartDate"), 
        F.max("ExpireDate").alias("ExpireDate")
    )

    expieryinfo = Phases.alias("df1").join(
        training_.alias("df2"),
        (F.col("df1.TrainingId") < F.col("df2.TrainingId")) 
        & (F.col("df1.EmployeeId") == F.col("df2.EmployeeId"))
        & (F.col("df1.TrainingCategoryId") == F.col("df2.TrainingCategoryId"))
        & (F.col("df1.StartDate1") < F.col("df2.StartDate")) , how="left"
    ).select(
        "df1.*", F.col("df2.TrainingId").alias("TrainingId2"), F.col("df2.StartDate").alias("StartDate2"), 
        F.col("df2.ExpireDate").alias("ExpireDate2"),
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
                (
                F.round(
                    (
                        F.unix_timestamp(F.col("ExpireDate")) - 
                        F.unix_timestamp(F.current_timestamp())
                    )/(3600*24)
                )
                )
            ).otherwise(
                (
                F.round(
                    (
                        F.unix_timestamp(F.col("ExpireDate")) - 
                        F.unix_timestamp(F.col("StartDate2"))
                    )/(3600*24)
                )
                )
            ),

            "Days": F.when(
                F.col("TrainingBeforeExpire") == "Not Occured Yet",
                F.when(
                    F.col("ExpireDueDays") <= 0, F.lit("Days Exceed")
                ).when(
                    F.col("ExpireDueDays") > 0, F.lit("Days Remaining")
                )
            ).otherwise(
                F.concat(
                    F.lit("Days"), F.lit(" "), F.col("TrainingBeforeExpire")
                )
            ),

            "Phases": phases_
        }
    )

    expieryinfo = expieryinfo.select(
        "TrainingId", "TrainingCategoryId", "EmployeeId", "TrainingBeforeExpire", F.abs("ExpireDueDays").alias("ExpireDueDays"), 
        "Days", F.col("TrainingId2").alias("FollowedByTrainingId"), "Phases"
    )

    return expieryinfo

def getTrainingFrequency(df):
    '''
    '''
    df = df.withColumn(
        "First", F.row_number().over(
            Window.partitionBy(
                "TrainingId", "TrainingCategoryId"
            ).orderBy(F.asc("StartDate"))
        )
    ).filter(F.col("First") == 1)

    dffrequency = df.withColumns(
        {
        "Compositkey": F.concat("TrainingCategoryId", F.lit("||"), "StartDate"),
        }
    ).select("Compositkey", "StartDate", "CategoryName").distinct()

    dffrequency.withColumn(
        "LastTrainingDate", F.lag("startDate").over(Window.orderBy("StartDate"))
    ).drop("Compositkey").write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
         "analyticadebuddha.flightoperation.trainingfrequency"
    )

    print("The table training frequency has been created as analyticadebuddha.flightoperation.trainingfrequency....")

    return None

def getPlanTrend(postPreDays, expieryinfo):
    '''
    '''
    plantrend = postPreDays.alias("df1").join(
        expieryinfo.alias("df2"), 
        (F.col("df1.TrainingId") == F.col("df2.TrainingId"))
        & (F.col("df1.EmployeeId") == F.col("df2.EmployeeId"))
        & (F.col("df1.TrainingCategoryId") == F.col("df2.TrainingCategoryId")), how="left"
    ).select(
    "df1.*", "df2.TrainingBeforeExpire", "df2.ExpireDueDays", 
    "df2.Days", "Phases", "df2.FollowedByTrainingId"
    )

    plantrend.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("analyticadebuddha.flightoperation.plantrend")
    print("The table plan trend has been created as analyticadebuddha.flightoperation.plantrend.....")

    getTrainingFrequency(plantrend)


    return None

def main():
    '''
    '''
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

    leaveinfo = spark.table(
        "analyticadebuddha.flightoperation.leavereport"
    ).select(
        "EmployeeId", "StartDate", "EndDate", "Absence", "LeaveKey"
    ).distinct()

    postPreDays = getPrePostLeave(recorddate, training, category, leaveinfo)

    Phases = postPreDays.groupBy(
        "TrainingId", "TrainingCategoryId", "EmployeeId"
    ).agg(
        F.countDistinct("StartDate").alias("Counts"), 
        F.first("ExpireDate").alias("ExpireDate"), 
        F.min("StartDate").alias("StartDate1")
    )

    expieryinfo = getExpieryInfo(postPreDays, Phases)
    plantrend = getPlanTrend(postPreDays, expieryinfo)

    return None

if __name__== "__main__": 
    main()