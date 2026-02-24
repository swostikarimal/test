# Databricks notebook source
import pyspark.sql.functions as F

cols = [
    F.date_format("Date", "y-MM-dd").alias("Date"), "SourceName","ChannelName","EventName", "sessions", "totalUsers", "activeUsers"
    # F.col("activeUsers").alias("totalUsers"), 
    # F.col("totalUsers").alias("activeUsers"), 
    ,"userEngagementDuration", 
    "eventsPerSession", "keyEvents", "totalRevenue", F.col("countCustomEvent:ticket_purchased").alias("ticketPurchased"), 
    "averageEngagenmentTimepPerSession"
]

cols1 = [
    "Date", "SourceName","ChannelName", "sessions", F.col("activeUsers").alias("totalUsers"),
    F.col("totalUsers").alias("activeUsers"), "userEngagementDuration", "eventsPerSession", "keyEvents", "totalRevenue", 
    F.col("countCustomEvent:ticket_purchased").alias("ticketPurchased"),"averageEngagenmentTimepPerSession"
]
catalog = "analyticadebuddha.google"

pathdefult = "/mnt/ga/reporting/Conversion/sourceContribution/sessionDefaultChannelGroup" 
pathsourcemedium = "/mnt/ga/reporting/Conversion/sourceContribution/sessionSourceMedium" 
pathmedium = "/mnt/ga/reporting/Conversion/sourceContribution/sessionMedium"  
pathsourcesession = "/mnt/ga/reporting/Conversion/sourceContribution/sessionSource"
pathcampaign = "/mnt/ga/reporting/Conversion/sourceContribution/sessionCampaignName"
pathsessionsource1  = "/mnt/ga/reporting/Conversion/sourceContribution/sessionSource1"

keyevents = ["view_item_list", "add_to_cart", "view_item", "purchase"]


def toMart(dfs, table, source):
    if len(dfs) > 1:
        consolidated = dfs[0]
        for d1 in dfs[1:]:
            consolidated = consolidated.unionByName(d1).filter(F.col("eventName").isin(keyevents))
        cols2 = cols
    else: 
        consolidated = dfs[0] 
        cols2 = cols1

    consolidated = consolidated.select(cols2)
    consolidated = consolidated.alias("df1").join(
        source.alias("df2"), F.col("df1.SourceName") == F.col("df2.Ganame"), how="inner"
    ).select("df1.*","df2.Name").drop("SourceName")
    
    consolidated.write.mode(
        "overwrite"
    ).option(
        "overwriteSchmea", "true"
    ).saveAsTable(
        f"{catalog}.{table}"
    )

    return None

def main():
    source = spark.read.parquet(pathsourcesession)
    default = spark.read.parquet(pathdefult)
    sourcemedium = spark.read.parquet(pathsourcemedium)
    medium = spark.read.parquet(pathmedium)
    campaign = spark.read.parquet(pathcampaign)
    source1 = spark.read.parquet(pathsessionsource1)
    sourcename = spark.table("analyticadebuddha.default.sourcename")

    dfs = [source, default, sourcemedium, medium, campaign]

    toMart(dfs, table="sourcecontribution", source=sourcename)
    toMart([source1], table="SessionSource", source=sourcename)

    return None

if __name__ == "__main__":
    main()