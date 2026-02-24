# Databricks notebook source
import pyspark.sql.functions as F

catalog = "analyticadebuddha.ecommerce"

pathdefult = "/mnt/ga/reporting/ecommerce/sectorsource/sessionDefaultChannelGroup"
pathmedium = "/mnt/ga/reporting/ecommerce/sectorsource/sessionMedium"
pathsource = "/mnt/ga/reporting/ecommerce/sectorsource/sessionSource"
pathcampaign ="/mnt/ga/reporting/ecommerce/sectorsource/sessionCampaignName"

pathcity = "/mnt/ga/reporting/ecommerce/sectordetails/city"
pathcountry = "/mnt/ga/reporting/ecommerce/sectordetails/country"
pathticket = "/mnt/ga/reporting/ecommerce/sectordetails/ticketTypes"
pathflightdate = "/mnt/ga/reporting/ecommerce/sectordetails/flightdate"
pathclass = "/mnt/ga/reporting/ecommerce/sectordetails/class"

pathcityStoP = "/mnt/ga/reporting/ecommerce/sectordetailsStoP/city"
pathcountryStoP = "/mnt/ga/reporting/ecommerce/sectordetailsStoP/country"
pathticketStoP = "/mnt/ga/reporting/ecommerce/sectordetailsStoP/ticketTypes"
pathflightdateStoP = "/mnt/ga/reporting/ecommerce/sectordetailsStoP/flightdate"
pathclassStoP = "/mnt/ga/reporting/ecommerce/sectordetailsStoP/class"

pathcampaignStoP = "/mnt/ga/reporting/ecommerce/sectorsourceStoP/sessionCampaignName"
pathdefultStoP = "/mnt/ga/reporting/ecommerce/sectorsourceStoP/sessionDefaultChannelGroup"
pathmediumStoP = "/mnt/ga/reporting/ecommerce/sectorsourceStoP/sessionMedium"
pathsourceStoP = "/mnt/ga/reporting/ecommerce/sectorsourceStoP/sessionSource"

pathcityandcountry = "/mnt/ga/reporting/ecommerce/countryAndCity/countryAndCity"

ecomcols1 = ['date', 'totalUsers','itemsPurchased','itemRevenue','itemRefundAmount','itemDiscountAmount','key']
ecomcols2 = ["eventName", "sector","category", "Name", "key"]

cityandcountrycols = ["sector", "Country", "City"]

sourcecols1 = ['date','totalUsers','itemsPurchased','itemRevenue','itemRefundAmount','itemDiscountAmount','key']
sourcecols2 = ['eventName','Sector','ChannelName', 'Name', 'key']

ecomkey = [F.col("eventName"), F.col("sector"), F.col("category"), F.col("Name")]
sourcekey = [F.col("eventName"), F.col("Sector"), F.col("ChannelName"), F.col("Name")]

sourcetable = "ecommercevalues_source"
ecomtable = "ecommercevalues"

def toModel(m2, tablename):
    m2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{tablename}")
    
    print(f"The table {tablename} has been created")

def getConsolidated(dfs, cols1, cols2, key, tablename):
    """
    """
    consolidated = dfs[0]
    for df in dfs[1:]:
        consolidated = consolidated.unionByName(df, allowMissingColumns=True)

    consolidated = consolidated.withColumn(
        "key", F.hash(
            F.concat(
                *key
            )
        )
    ).drop("property")
    
    m1 = consolidated.select(*cols1)
    m2  = consolidated.select(*cols2).distinct()

    m1.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{tablename}")
    
    return m2

def main():
    """
    """
    city = spark.read.parquet(pathcity).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "city": "category"
        }
    ).withColumn("Name", F.lit("City"))

    class_ = spark.read.parquet(pathclass).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "itemCategory": "category"
        }       
    ).withColumn("Name", F.lit("class"))

    # country = spark.read.parquet(pathcountry).withColumnsRenamed(
    #     {
    #         "customEvent:sector": "sector",
    #         "country": "category"
    #     }
    # ).withColumn("Name", F.lit("Country"))

    tickettypes = spark.read.parquet(pathticket).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "itemCategory4": "category"
        }
    ).withColumn("Name", F.lit("ticketTypes"))

    flightdate = spark.read.parquet(pathflightdate).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "itemCategory3": "category"
        }
    ).withColumn("Name", F.lit("flightdate"))

    cityStoP = spark.read.parquet(pathcityStoP).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "city": "category"
        }
    ).withColumn("Name", F.lit("City"))

    class_StoP = spark.read.parquet(pathclassStoP).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "itemCategory": "category"
        }       
    ).withColumn("Name", F.lit("class"))

    # countryStoP = spark.read.parquet(pathcountryStoP).withColumnsRenamed(
    #     {
    #         "customEvent:sector": "sector",
    #         "country": "category"
    #     }
    # ).withColumn("Name", F.lit("Country"))

    tickettypesStoP = spark.read.parquet(pathticketStoP).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "itemCategory4": "category"
        }
    ).withColumn("Name", F.lit("ticketTypes"))

    flightdateStoP = spark.read.parquet(pathflightdateStoP).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
            "itemCategory3": "category"
        }
    ).withColumn("Name", F.lit("flightdate"))

    cityandcountry = spark.read.parquet(pathcityandcountry).withColumnsRenamed(
        {
            "customEvent:sector": "sector",
        }
    ).withColumns(
        {
            "oneof": F.when(
                ~ F.col("country").isin(
                    ["Nepal", "India"]
                ), "Other"
            ).otherwise(
                F.col("country")
            ),
            "Name":F.lit("CityAndCountry")
        }
    )

    defult = spark.read.parquet(pathdefult).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionDefaultChannelGroup": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionDefaultChannelGroup"))


    medium = spark.read.parquet(pathmedium).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionMedium": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionMedium"))


    source = spark.read.parquet(pathsource).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionSource": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionSource"))

    campaign = spark.read.parquet(pathcampaign).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionCampaignName": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionCampaignName"))

    defultStoP = spark.read.parquet(pathdefultStoP).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionDefaultChannelGroup": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionDefaultChannelGroup"))

    mediumStoP = spark.read.parquet(pathmediumStoP).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionMedium": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionMedium"))


    sourceStoP = spark.read.parquet(pathsourceStoP).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionSource": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionSource"))

    campaignStoP = spark.read.parquet(pathcampaignStoP).withColumnsRenamed(
        {
            "customEvent:sector": "Sector",
            "sessionCampaignName": "ChannelName"
        }
    ).withColumn("Name", F.lit("sessionCampaignName"))

    dfssource = [defult, medium, source, campaign, defultStoP, mediumStoP, sourceStoP, campaignStoP] 
    dfsecom = [city, tickettypes, flightdate, class_, cityStoP, tickettypesStoP, flightdateStoP, class_StoP]

    m2 = getConsolidated(dfs=dfssource, cols1=sourcecols1, cols2=sourcecols2, key=sourcekey, tablename=sourcetable)
    toModel(m2=m2, tablename="sourcecat")

    m2 = getConsolidated(dfs=dfsecom, cols1=ecomcols1, cols2=ecomcols2, key=ecomkey, tablename=ecomtable)
    toModel(m2=m2, tablename="ecomcat")

    cityandcountry.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.country")

    return None

if __name__ == "__main__":
    main()