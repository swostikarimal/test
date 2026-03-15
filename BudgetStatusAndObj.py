# Databricks notebook source
import pyspark.sql.functions as F
from datetime import datetime

fileposition = "/mnt/meta/adset/adset_campaign/campaign_adset"

objcol = [
    "CampaignId", "AdsetId", "CampaignName", F.initcap(F.array_join(F.split("Objective", "_"), ' ')).alias("Objective"), 
    F.initcap(F.array_join(F.split("optimization_goal", "_"), ' ')).alias("Result"), "CreatedDate", "StoppedAt", 
    F.col("Status1").alias("Status"), "Category"
]

dailybudget = F.when(
    (
        (F.col("dc").isNull()) | (F.col("dc") == 0)
    ) & (
        (F.col("da").isNotNull()) & (F.col("da") != 0)
    ), F.col("da")
).otherwise(F.col("dc"))

liftimebudget = F.when(
    (
        (
            F.col("lc").isNull()
        ) | (F.col("lc") == 0)
    ) & (
        (
            F.col("la").isNotNull()
        ) & (
            F.col("la") != 0
        )
    ), F.col("la")
).otherwise(F.col("lc"))

casedaily = (
    F.col("dailyBudget") != 0
) & (
    F.col("dailyBudget").isNotNull()
)
budgetype = F.when(
    casedaily, "Daily"
).otherwise("Lifetime")

onebudget = F.when(
    casedaily, F.round(
        F.col("dailybudget")/100, 2
    )
).otherwise(
    F.round(
        F.col("lifetimeBudget")/100, 2
    )
)

casestatus  = (
(
    F.col("CampaignStatus") == F.col("AdsetStatus")
) & (
    F.col("AdsetStatus") == F.col("Status")
)
)

caseundefined = (
    (
        F.col("AdsetStatus").contains("PAUSED") | F.col("Status").contains("PAUSED")
    )
)

caseotherstatus = F.when(
    (
    (
        (
            F.col("StoppedAt").isNotNull()
        ) & (
            F.col("StoppedAt") < datetime.now().strftime('%Y-%m-%d')
        )
    ) & (
        F.col("CampaignStatus") == "ACTIVE"
    )
    ), F.lit("PAUSED")
).otherwise(
    F.col("CampaignStatus")
)

def getBudget(adsetbudget, campaigninfo):
    '''
    '''
    budget = adsetbudget.alias("df1").join(
        campaigninfo.alias("df2"), (
            F.trim("df1.CampaignId") == F.trim("df2.CampaignId")
        ), how="left"
    ).select(
        "df1.CampaignId", "df1.Adsetid", "df1.CampaignName", 
        F.col("df1.dailyBudget").alias("dc"), 
        F.col("df1.lifetimebudget").alias("lc"), 
        F.col("df2.dailyBudget").alias("da"),
        F.col("df2.lifetimebudget").alias("la")
    )

    budget = budget.withColumns(
        {
            "dailyBudget": dailybudget,
            "lifetimeBudget": liftimebudget,
            "Budget": onebudget,
            "Budgetype": budgetype,
        }
    ).drop("da", "la", "dc", "lc", "dailyBudget", "lifetimeBudget")

    budget.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("analyticadebuddha.metaadtype.budget")

    return None

def objInitial(campaigninfo, adsetobjective):
    '''
    '''
    objective = campaigninfo.alias("df1").join(
        adsetobjective.alias("df2"), (
            F.trim("df1.CampaignId") == F.trim("df2.campaign_id")
        ), how="left"
    ).select(
        F.col("df2.campaign_id").alias("CampaignId"), F.col("df2.id").alias("AdsetId"), 
        F.col("df2.name").alias("campaignName"), "df1.objective", "df2.optimization_goal",
        F.col("df1.BidStrategy"), F.col("df1.CreatedDate"), F.col("df1.StoppedAt"), 
        F.col("df1.CampaignStatus")
    )

    stopdatenull_Active = objective.filter(
        (
            (
                F.col("StoppedAt").isNull()
            ) & (
                F.col("CampaignStatus").contains("ACTIVE")
            )
        )
    )

    other = objective.filter(
        ~(
            (
                F.col("StoppedAt").isNull()
            ) & (
                F.col("CampaignStatus").contains("ACTIVE")
            )
        )
    )

    return stopdatenull_Active, other

def getStatus(pausednulldate, adstatus, adsetbudget, other):
    '''
    '''
    obj = pausednulldate.alias("df1").join(
        adstatus.alias("df2"), (
            F.trim("df1.CampaignId") == F.trim("df2.CampaignId")
        ), how="left"
    ).join(

    adsetbudget.alias("df3"), (
        F.col("df1.Adsetid") == F.col("df3.Adsetid")
    ), how="inner"
    ).select(
        "df1.*", "df2.AdId","df2.Status", 
        F.col("df3.Status").alias("AdsetStatus")
    )

    obj = obj.withColumn(
        "Status1", F.when(
            casestatus, F.col("CampaignStatus")
        ).otherwise(F.lit("undefined"))
    ).withColumn(
        "Status1", F.when(
            F.col("Status1") == "undefined",
            F.when(
                caseundefined, F.lit("PAUSED")

            ).otherwise(F.col("Status1"))

        ).otherwise(F.col("Status1"))
    ).drop(
        "Status", "CampaignStatus", "AdsetStatus"
    )

    oth = other.withColumn(
        "Status1", caseotherstatus
    ).drop("CampaignStatus")

    oth1 = oth.alias("df1").join(
        adstatus.alias("df2"), (
            F.col("df1.CampaignId") == F.col("df2.CampaignId")
        )
    ).select("df1.*", "df2.AdId")

    status = obj.unionByName(oth1)

    return status

def getStatusAndObj(status, objectivedf, adsetinfo):
    statusandbudget = status.alias("df1").join(
        objectivedf.alias("df2"), F.trim("df1.Objective") == F.trim("df2.objective")
    ).select("df1.*", "df2.Category")

    statusandbudget = statusandbudget.select(*objcol).drop(
        "optimization_goal"
    )

    statusandbudget = statusandbudget.alias("df1").join(
        adsetinfo.alias("df2"), F.col("df1.AdsetId") == F.col("df2.AdSetId"), how="left"
    ).select("df1.*", "df2.AdId","df2.AdName", "df2.AdSetName").dropDuplicates(
        subset=["campaignId", "AdsetId", "AdId"]
    ).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).saveAsTable(
        "analyticadebuddha.metaadtype.statusandobjective"
    )

    return None

def main():
    '''
    '''
    adsetbudget = spark.table("analyticadebuddha.metaads.adsetbudget")
    campaigninfo = spark.table("analyticadebuddha.metaads.campaigninfo")
    estimated  = spark.read.parquet("/mnt/meta/adset/adsetobjective/objective")
    adsetobjective  = spark.read.parquet("/mnt/meta/adset/adsetobjective/objective")
    adstatus = spark.table("analyticadebuddha.metaads.adstatus")
    objectivedf = spark.table("analyticadebuddha.businesstable.facebookobjective")

    adsetinfo = spark.read.parquet(fileposition).select(
        F.col("ad_id").alias("AdId"), F.col("adset_id").alias("AdSetId"), 
        F.col("ad_name").alias("AdName"), F.col("adset_name").alias("AdSetName")
    )
    
    getBudget(adsetbudget, campaigninfo)
    pausednulldate, other = objInitial(campaigninfo, adsetobjective)
    status = getStatus(pausednulldate, adstatus, adsetbudget, other)
    getStatusAndObj(status, objectivedf, adsetinfo)

    return None

if __name__ == "__main__":
    main()