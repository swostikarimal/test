# Databricks notebook source
from pyspark.sql import functions as F

firstuser = "acquisition_FirstTimeVisitors"
sourcetable = "acquisition_source"

cols = [
    "Date", F.col("SourceName").alias("Name"),"ChannelName", F.col("unifiedPagepathScreen").alias("pageName"), "totalUsers", "newUsers", "returningUsers","sessions","userEngagementDuration", "engagementRate", "averageEngagenmentTimepPerSession"
]

pathfirstdefult = "/mnt/ga/reporting/acquisition/firsttimevisitors/firstUserDefaultChannelGroup" 
pathfirstsourcemedium = "/mnt/ga/reporting/acquisition/firsttimevisitors/firstUserSourceMedium" 
pathfirstcampaign = "/mnt/ga/reporting/acquisition/firsttimevisitors/firstUserCampaignName"

pathsessionSource = "/mnt/ga/reporting/acquisition/firsttimevisitors/sessionSource" 
pathsourcemedium = "/mnt/ga/reporting/acquisition/firsttimevisitors/sessionSourceMedium" 
pathcampaign =  "/mnt/ga/reporting/acquisition/firsttimevisitors/sessionCampaignName"
pathdefault = "/mnt/ga/reporting/acquisition/firsttimevisitors/sessionDefaultChannelGroup"

filterjourney = (
    F.col("unifiedPagepathScreen").contains("/search") 
    | F.col("unifiedPagepathScreen").contains("/payment") 
    | F.col("unifiedPagepathScreen").contains("payment")
    | F.col("unifiedPagepathScreen").contains("/book/flight")
)

def toMart(dfs=list, table= firstuser):
    consolidated = dfs[0]
    for d1 in dfs[1:]:
        consolidated = consolidated.unionByName(d1)

    consolidated.filter(~filterjourney).withColumn(
        "returningUsers", F.col("totalUsers") - F.col("newUsers")
    ).select(cols).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).saveAsTable(
        f"analyticadebuddha.google.{table}"
    )

    return None

def main():
    firsdefault = spark.read.parquet(pathfirstdefult)
    firstsourcemedium = spark.read.parquet(pathfirstsourcemedium)
    firstcampaign = spark.read.parquet(pathfirstcampaign)
    
    source = spark.read.parquet(pathsessionSource)
    sourcemedium = spark.read.parquet(pathsourcemedium)
    campaign = spark.read.parquet(pathcampaign)
    default = spark.read.parquet(pathdefault)

    dfs = [firsdefault, firstsourcemedium, firstcampaign]
    dfs1 = [sourcemedium, campaign, default,source]

    toMart(dfs, table=firstuser)
    toMart(dfs1, table=sourcetable)

    return None

if __name__ == "__main__":
    main()