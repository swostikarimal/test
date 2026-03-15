# Databricks notebook source
import pyspark.sql.functions as F
from datetime import datetime
from getAdsPerformance import concat_ as platconcat

catalog = "analyticadebuddha.metaadtype"

pathposition = "/mnt/meta/platformAndPosition/position/positionactions/actions"
pathageandgender = "/mnt/meta/platformAndPosition2/age_gender/actionsandvalues/actions"
pathadsetobjective = "/mnt/meta/adset/adsetobjective/objective"
pathcampaigninfo = "analyticadebuddha.metaads.campaigninfo"
pathobjetype = "analyticadebuddha.businesstable.facebookobjective"
pathpositionestimated = "/mnt/meta/platformAndPosition/position_estimated/position"
pathadstatus = "analyticadebuddha.metaads.adstatus"
pathestimatedgender = "/mnt/meta/platformAndPosition/position_estimated/genderAndAge"

objectivead_join = (F.trim("df1.AdId") == F.trim("df2.ad_id")) 
selectcolsadobj = ["df1.campaign_id", F.col("df1.id").alias("AdsetId"), "df2.*","df1.Category"]

agegenderperfomrmance = [F.col("Campaign_id").alias("CampaignId"), "AdsetId", "Date", F.col("ad_id").alias("AdId"), "Value","Gender", "Age", "ActionType"]
agegenderrecaller = ["Date", F.col("campaign_id").alias("CampaignId"),F.col("Ad_id").alias("AdId"), "Gender", "Age", "AdRecallers", "Recallrate"]

positionperformance = [
    F.col("Campaign_id").alias("CampaignId"), "AdsetId", "Date", F.col("ad_id").alias("AdId"), "Value","Platform",  
    F.col("Platform_Position").alias("PlatformPosition"), "ActionType", "platformposition1"
]

positionrecaller = [
    F.col("Campaign_id").alias("CampaignId"), "AdsetId", "Date", F.col("ad_id").alias("AdId"), "AdRecallers", "Recallrate","Platform", 
    F.col("Platform_Position").alias("PlatformPosition"), "platformposition1"
]

configposition = {
    "colsname": "platformposition",
    "colsconcat":["platform_position","platform"]
}

joincongender = (
    (
        F.col("df1.Date") == F.col("df2.Date")
    )
    &
    (
        F.col("df1.campaign_id") == F.col("df2.campaign_id")
    ) & (
        F.col("df1.Age") == F.col("df2.Age")
    ) &
    (
        F.col("df1.Gender") == F.col("df2.Gender")
    )
)

joinconplatform  = (
    F.col("df1.Campaign_id") == F.col("df2.campaign_id")
) & (
    F.lower("df1.Platform") == F.lower("df2.platform")
        
) & (
    F.col("df1.date") ==  F.col("df2.date")
)

joinconposition = (
    F.col("df1.Campaign_id") == F.col("df2.campaign_id")
) & (
    F.lower("df1.platform_position") == F.lower("df2.platform_position")
) & (
    F.col("df1.date") ==  F.col("df2.date")
)

def getObjectives(estimated, campaigninfo, objectivedf, adstatus):
    '''
    '''
    objective = estimated.alias("df1").join(
        campaigninfo.alias("df2"), (
            F.trim("df1.campaign_id") == F.trim("df2.CampaignId")
        ), how="inner"
    ).join(
        objectivedf.alias("df3"), (
            F.trim("df2.objective") == F.trim("df3.Objective")
        ), how="left"
    ).select(
        "df1.*", "df2.CampaignName", 
        "df2.objective", "df3.Category"
    ).drop("objective")

    objectives = objective.alias("df1").join(
        adstatus.alias("df2"), (
            F.trim("df1.campaign_id") == F.trim("df2.CampaignId")
        ), how="left"

    ).select("df2.AdId", "df1.*").dropDuplicates(["AdId", "campaign_id", "id"]).filter(F.col("AdId").isNotNull())

    return objectives

def getCapatilizedCol(df):
    cols = [col.capitalize() for col in df.columns]
    return df.select(*cols)

def getAwareness(objective, df, joincon, seelctcol=None, filtercon=None):
    '''
    '''
    df1  = objective.alias("df1").join(
        df.alias("df2"), (
            joincon
        ), how ="left"
    ).select(*seelctcol)
    
    if "action_type" in df1.columns:
        df1 = df1.withColumn(
            "ActionType", F.initcap(
                F.array_join(F.split("action_type", "_"), " ")
            )
        ).drop("action_type")
    
    if not filtercon is None:
        df1= df1.filter(filtercon)

    return getCapatilizedCol(df=df1)


def platformlevel(df1, df2, joincon):
    df = df1.alias("df1").join(
        df2.alias("df2"), (
        joincon
        ), how = "left"
    ).select(
        "df1.*", F.col("df2.estimated_ad_recallers").alias("AdRecallers"), 
        F.col("df2.estimated_ad_recall_rate").alias("Recallrate")
    )

    return df

def positionmlevel(df1, df2, joincon):
    df = df1.alias("df1").join(
        df2.alias("df2"), (
            joincon
        ), how = "left"
    ).select(
       "df1.*", F.col("df2.estimated_ad_recallers").alias("AdRecallers"), 
        F.col("df2.estimated_ad_recall_rate").alias("Recallrate")
    )

    return df

def ppOrP(df1, df2, joincon, platform=False):
    if platform: df = platformlevel(df1, df2, joincon=joincon)  
    else: df = positionmlevel(df1, df2, joincon=joincon)

    return df


def filterTypes(df, cat, colsperfomrmance, colsrecaller, catalog, filename):
    '''
    '''
    df = df.filter(F.col("Category") == cat)
    df1 = df.select(*colsperfomrmance).distinct()
    df2 = df.select(*colsrecaller).distinct()

    df1.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.{cat}_{filename}_Performance")
    print(f"Created a file called {cat}_{filename}_Performance")
    df2.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.{cat}_{filename}_Recaller")
    print(f"Created a file called {cat}_{filename}_Recaller")

    return None

def main():
    '''
    '''
    ageandgenderdf = spark.read.parquet(pathageandgender)
    adsetobjective  = spark.read.parquet(pathadsetobjective)
    campaigninfo = spark.table(pathcampaigninfo) 
    objectivedf = spark.table(pathobjetype)
    positiondf = spark.read.parquet(pathposition)
    estimaeddfposition  = spark.read.parquet(pathpositionestimated)
    adstatus = spark.table(pathadstatus)
    estimatedgender = spark.read.parquet(pathestimatedgender)

    objectives = getObjectives(
        estimated=adsetobjective, campaigninfo=campaigninfo, objectivedf=objectivedf, adstatus=adstatus
    )

    estimaeddfposition1 = getAwareness(
        objective=estimaeddfposition, df=objectives, joincon = (F.col("df1.campaign_id") == (F.col("df1.campaign_id"))), 
        seelctcol=(["df1.*", "df2.category"]), filtercon= (F.col("Category") == "Awareness")
    )

    typecampaign_position = getAwareness(
        objectives, df=positiondf, joincon=objectivead_join, seelctcol=selectcolsadobj, 
        filtercon=(
        (F.col("date") >= "2025-02-01") # (F.col("Category") == "Awareness") &
        )
    )

    typecampaign_gender = getAwareness(
        objectives, df=ageandgenderdf, joincon=objectivead_join, seelctcol=selectcolsadobj, 
        filtercon=(
            (F.col("date") >= "2025-02-01") # (F.col("Category") == "Awareness") &
        )
    )

    estimaeddfgender1 = getAwareness(
            objective=estimatedgender, df=objectives, joincon = (F.col("df1.campaign_id") == (F.col("df1.campaign_id"))), 
            seelctcol=(["df1.*", "df2.category"]), filtercon= (F.col("Category") == "Awareness")
        )


    genderawareness = ppOrP(
        df1=typecampaign_gender, df2=estimaeddfgender1, joincon=joincongender, platform=False
    ).distinct()



    positionawareness = ppOrP(
        df1=typecampaign_position, df2=estimaeddfposition1, joincon=joinconposition, platform=False
    ).distinct()

    awarenesscamp = platconcat(df=positionawareness, config = configposition)


    cat = awarenesscamp.select(F.col("Category")).distinct().collect()

    cats = [i[0] for i in cat]

    for cat in cats:
        print(f"Procession for {cat}")
 
        if cat != "Sales":
            filterTypes(
                df=awarenesscamp, cat=cat, colsperfomrmance=positionperformance, 
                colsrecaller=positionrecaller, catalog= catalog, filename="position"
            )

            filterTypes(
                df=genderawareness, cat=cat, colsperfomrmance=agegenderperfomrmance, 
                colsrecaller=agegenderrecaller, catalog= catalog, filename="agegender"
            )
    return None

if __name__ == "__main__":
    main()