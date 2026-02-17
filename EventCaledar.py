import pyspark.sql.functions as F
from pyspark.sql.window import Window

EventPath= "analyticadebuddha.businesstable.EventCalendar"

def eventCalender(events):
    '''Returns coming event, postevent and major event features'''
    windowpost = Window.orderBy("Date").rowsBetween(-2, 0)
    windowprev = Window.orderBy("Date").rowsBetween(0, 2)

    windowdashainprev = Window.orderBy("Date").rowsBetween(0, 10)
    windowdashainpost = Window.orderBy("Date").rowsBetween(-5, 0)

    windowdbhaitikaprev = Window.orderBy("Date").rowsBetween(0, 7)
    windowdbhaitikapost = Window.orderBy("Date").rowsBetween(-10, 0)

    if "english_date" in events.columns:
        events = events.withColumn(
            "Date", F.to_date(F.col("english_date"), "yyyy-MM-dd")
        )
    else:
        events = events.withColumn(
            "Date", F.concat(F.col("saal_eng"), F.lit("-"), F.col("mahina_eng"), F.lit("-"), F.col("eng_day_number"))
        ).withColumn(
            "Date", F.to_date("Date", "y-M-d")
        ).select("Date", "event", "tithi", "is_public_holiday").filter(F.year("date").between("2019", "2025"))

    events = events.withColumn(
        "IsMajorEvent", F.when(
            F.col("is_public_holiday") == 1, F.col("event")
        )
    ).select("Date", "IsMajorEvent", "event","tithi", "is_public_holiday").withColumn(
        'Datetime', F.to_unix_timestamp(F.col("Date"))
    )

    calendar= events.withColumn(
        "EventStatus",
        F.when(F.col("IsMajorEvent").isNotNull(), "MajorEvent")
        .when(
            F.max(F.when(F.col("IsMajorEvent").isNotNull(), "EventComing")).over(windowprev) == "EventComing",
            "EventComing"
        )
        .when(
            F.max(F.when(F.col("IsMajorEvent").isNotNull(), "PostEvent")).over(windowpost) == "PostEvent",
            "PostEvent"
        )
    )
    calendar= calendar.withColumn(
        "Events",
        F.greatest(
            F.max(F.col("isMajorEvent")).over(windowprev),
            F.max(F.col("isMajorEvent")).over(windowpost)
        )
    ).withColumn(
        "Dashain",
        F.when(
            F.col("isMajorEvent").contains("विजया दशमी"), "विजया दशमी"
        ).otherwise(F.lit(None))
    ).withColumn(
        "Tihar",
        F.when(
            F.col("isMajorEvent").contains("भाइटीका"), "भाइटीका"
        )
    ).withColumn(
        'Dashain',
            F.greatest(
                F.max(F.col("Dashain")).over(windowdashainprev),
                F.max(F.col("Dashain")).over(windowdashainpost)
        )
    ).withColumn(
        'Tihar',
            F.greatest(
                F.max(F.col("Tihar")).over(windowdbhaitikaprev),
                F.max(F.col("Tihar")).over(windowdbhaitikapost)
    )
    )

    return calendar

def getEventStatus(df):
    '''Returns the ranked events'''
    eventwindow = Window.partitionBy("Events", F.year("Date")).orderBy(F.col("Date"))
    dashaineventwindow = Window.partitionBy("Dashain", F.year("Date")).orderBy(F.col("Date"))
    tihareventwindow = Window.partitionBy("Tihar", F.year("Date")).orderBy(F.col("Date"))

    rankdt = df.withColumn(
        "Rank", F.dense_rank().over(eventwindow)
    ).withColumn(
        "DashainRank", F.dense_rank().over(dashaineventwindow)
    ).withColumn(
        "TiharRank", F.dense_rank().over(tihareventwindow)
    )
    rankdt = rankdt.withColumn(
        "FinalEvents",
        F.when(
            F.col("Dashain").isNotNull(), F.concat(F.col("Dashain"), F.col("DashainRank"))
        ).when(
            F.col("Tihar").isNotNull(), F.concat(F.col("Tihar"), F.col("TiharRank"))
        ).otherwise(
            F.when(
                F.col("Dashain").isNull() & F.col("Tihar").isNull() & F.col("Events").isNotNull(),
                F.concat(F.col("EventStatus"), F.col("Rank"))
            )
        )
    ).withColumn(
        "FinalEvents", F.when(
        (~(F.col("FinalEvents").contains("भाइटीका") 
            |  F.col("FinalEvents").contains("विजया दशमी")) 
            ),
        F.col("EventStatus")
        ).otherwise(F.col("FinalEvents"))
    ).withColumn(
        "FinalEvents", F.when(
            F.col("FinalEvents").isNull(), F.lit("NoEvent")
        ).otherwise(F.col('FinalEvents'))
    ).select("Date", "Tithi", "is_public_holiday", F.col("FinalEvents").alias("EventStatus"), "Event")
    
    # if append:
    #     rankdt.write.format('delta').mode("append").saveAsTable(tablepath)
    # else:
    #     rankdt.coalesce(1).write.format('delta').mode(
    #                 "overwrite"
    #             ).option(
    #                 "overwriteSchema", "true"
    #             ).saveAsTable(
    #                 tablepath
    #             )

    return rankdt

# def main():
#     events = spark.table(EventPath)
#     df = eventCalender(events)
#     rankdt = getEventStatus(df)

#     return None
# if __name__ == "__main__":
#     main()