# Databricks notebook source
import pyspark.sql.functions as F
import getAdsPerformance as getAdsPerformance

catalog = "analyticadebuddha.metaads"

adsetvalcol = [
    "Date","AdId", "Clicks", "CPC", "CTR", 
    "Frequency", "Impressions", "Reach", "Spend"
]

adsetdimcol = ["AdId", "AdName", "CampaigId", "CampaignName", "AdsetId", "AdSetName"]

selectCols = [
    F.col("date_start").alias("Date"), F.col("ad_id").alias("AdId"), F.col("ad_name").alias("AdName"), 
    F.col("campaign_id").alias("CampaigId"), F.col("campaign_name").alias("CampaignName"), F.col("adset_id").alias("AdsetId"), 
    F.col("adset_name").alias("AdSetName"),
    F.col("Clicks"), F.col("CPC"), F.col("CTR"), F.col('Frequency'), F.col('Impressions'), F.col('Reach'), F.col('Spend')
]

fileplatform = "/mnt/meta/platformAndPosition/platform/platform"
fileposition = "/mnt/meta/platformAndPosition/position/position"
fileageAction = "/mnt/meta/ageAndgender/age/actionsandvalues/actions"
fileagevalues = "/mnt/meta/ageAndgender/age/actionsandvalues/action_values"
filegenderAction = "/mnt/meta/ageAndgender/age_gender/actionsandvalues/actions"
filegendervalues = "/mnt/meta/ageAndgender/age_gender/actionsandvalues/action_values"
fileagegender = "/mnt/meta/platformAndPosition2/age_gender/age_gender"
filegender = "/mnt/meta/ageAndgender/audienceGender/gender"
fileage = "/mnt/meta/ageAndgender/audienceAge/age"
fileevent = "analyticadebuddha.metaads.eventName"
fileadset = "/mnt/meta/adset/adset_campaign/campaign_adset"
fileexchange = "analyticadebuddha.metaadtype.USD_NPR"

pathagegendervalue = "/mnt/meta/platformAndPosition2/age_gender/actionsandvalues/actions"
pathagegendercur = "/mnt/meta/platformAndPosition2/age_gender/actionsandvalues/action_values"

fileplatformaction = "/mnt/meta/platformAndPosition/platform/actionsandvalues/actions" 
fileplatformvalues = "/mnt/meta/platformAndPosition/platform/actionsandvalues/action_values"
filepositioaction = "/mnt/meta/platformAndPosition/position/positionactions/actions"
filepositionvalues = "/mnt/meta/platformAndPosition/position/positionactions/action_values"

adsetvalcat = "adsetval"
adsetdimcat = "adsetdim"

platformconfig = {
    "valtable": "PlatFormPosition",
    "mul": "PlatformPerformance",
    "dimtable": "PlatFormPosition_dim",
    "colsname": "PlatformPosition",
    "cols": ["platform_position", "platform"],
    "colsconcat": ["platform_position", "platform"]

}

Ageconfig = {
    "valtable": "agegenderperformance",
    # "mul":"agegender",
    # "cols": ["age", "gender"],
    # "dimtable": "AgeGenderDim"
}

genderconfig = {
    "valtable": "gender"
}

platformconfigvalandcur = {
    "actionvalCurr": "platform_value_cur",
    "cols": ["date","ad_id", "value", "platform", "eventName"],
    "cols1": ["platform", "eventName"],
    "actionandvaluevaluecat": "platform_value",
    "valuedimcat": "platform_dim"
}

PlatforAndPosition = {
    "actionvalCurr": "position_value_cur",
    "cols": ["date","ad_id", "value", "platform", "platform_position", "eventName"],
    "cols1": ["platform", "platform_position"],
    "colsconcat": ["platform_position", "platform"],
    "colsname": "PlatformPosition",
    "actionandvaluevaluecat": "position_value",
    "valuedimcat": "position_dim"
}

platformconfig = {
    "valtable": "PlatFormPosition",
    "mul": "PlatformPerformance",
    "dimtable": "PlatFormPosition_dim",
    "colsname": "PlatformPosition",
    "colsconcat": ["platform_position","platform"],
    "cols": ["platform_position", "platform"]
}

configagevalandcur = {
    "actionvalCurr": "agevaluecur",
    "cols": ["date","ad_id", "value", "age", "eventName"],
    "cols1": ["age", "eventName"],
    "actionandvaluevaluecat": "agevalue",
    "valuedimcat" : "agedim"
}


configgendervalandcur = {
    "actionvalCurr": "agegendercur",
    "cols": ["date","ad_id", "value", "gender", "age", "eventName"],
    "cols1": ["age","gender", "eventName"],
    "actionandvaluevaluecat": "agegenderValue",
    "valuedimcat" : "agegenderdim"
}

eventCase = F.when(
    (
        F.col("custom_event_type").contains("FIND_LOCATION") 
        &  (F.col("name").contains("Booking"))
    ), F.col("id")
    ).when(
        (
            F.col("custom_event_type").contains("FIND_LOCATION") 
            & (F.lower("name").contains("flight search"))
        ), F.lit("search")
    ).when(
        (
            F.col("custom_event_type").contains("ADD_PAYMENT_INFO") 
            &  (F.col("name") != "Payment")
        ), F.lit(None)
    ).when(
        (
            F.col("custom_event_type").contains("INITIATED_CHECKOUT")
        ), F.lit("initiate_checkout")
    ).when(
        F.lower("name").contains("sector"), F.col("id")
    ).otherwise(
        F.col("custom_event_type")
)


case1 = (
    (
        F.trim(
            F.lower(F.col("action_type"))
        ).contains(F.trim(
            F.lower(F.col("eventName"))
        ))
    )
    # |
    # (
    #     F.col("action_type").contains("484671429019352")
    # )
    # |
    # (
    #     F.col("action_type").contains("1723645538194268")
    # )
)

naming = F.when(
    F.col("eventName").contains("initiate_checkout"), "Details"
).when(
    F.col("eventName").contains("add_payment_info"), "Payment"
).when(
    F.col("eventName").contains("484671429019352"), "Booking" #"Payment"
).when(
    F.lower("name").contains("sector"), F.concat(
        F.split("name", " ")[0], F.lit(" "), F.split("name", " ")[2]
    )
).otherwise(F.initcap("eventName"))
    
def getAdsetPerformace(adsetdata):
    """
    """
    adsetval = adsetdata.select(*adsetvalcol).distinct()
    adsetdim = adsetdata.select(*adsetdimcol).distinct()
    adsetval.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{adsetvalcat}")
    adsetdim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{adsetdimcat}")

    print(f"The tables for adset performance and adset dimension has been written to {catalog}")

    return None

def main():
    '''
    '''
    platformactions = spark.read.parquet(fileplatformaction)
    platformvalues = spark.read.parquet(fileplatformvalues)
    positionaction = spark.read.parquet(filepositioaction)
    positionvalues = spark.read.parquet(filepositionvalues)

    ageactionvalues = spark.read.parquet(fileagevalues)
    ageaction = spark.read.parquet(fileageAction)


    position = spark.read.parquet(fileposition)
    platform = spark.read.parquet(fileplatform)

    agegender = spark.read.parquet(fileagegender)

    ageandgendervalue = spark.read.parquet(pathagegendervalue)
    ageandgendercur = spark.read.parquet(pathagegendercur)

    eventname = spark.table(fileevent).withColumn("eventName", eventCase)
    adsetdata = spark.read.parquet(fileadset).select(*selectCols)

    getAdsetPerformace(adsetdata)

    getAdsPerformance.getActionAndValues(
        positionaction, eventname, value=True, dim=True, concat = True, config= PlatforAndPosition, #positionconfigvalandcur,
        naming=naming, catalog=catalog, case1=case1
    )


    getAdsPerformance.getActionAndValues(
        positionvalues, eventname, value=False, dim=False, concat = True, config=PlatforAndPosition, #positionconfigvalandcur,
        naming=naming, catalog=catalog, case1=case1
    )

    getAdsPerformance.getActionAndValues(
        ageandgendervalue, eventname, value=True, dim=False, config=configgendervalandcur,
        naming=naming, catalog=catalog, case1=case1
    )

    getAdsPerformance.getActionAndValues(
        ageandgendercur, eventname, value=False, dim=False, config=configgendervalandcur,
        naming=naming, catalog=catalog, case1=case1
    )

    getAdsPerformance.getAdsPerformance(
        df1=position, df2=platform, dim=True, concat = True,
        config=platformconfig, catalog=catalog
    )

    getAdsPerformance.getAdsPerformance(
        df1 = agegender, config=Ageconfig, 
        catalog=catalog
    )

    return None

if __name__ == "__main__":
    main()