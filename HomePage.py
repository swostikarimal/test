# Databricks notebook source
import pyspark.sql.functions as F

pathnav = "analyticadebuddha.google.navigation_blog_click"
royalsearch = "analyticadebuddha.google.royalclub_searchpanel"
promotion = "analyticadebuddha.google.selectpromotion"
sliderpath = "analyticadebuddha.google.homepageslider"
pathevent = "/mnt/ga/reporting/events_pagepath/events"
unifiedPagePathScreenpath = "/mnt/ga/reporting/events_pagepath/unifiedPagePathScreen"


filteroffer = F.col("customEvent:previous_url") == "https://www.buddhaair.com/"
filternav = (F.col("customEvent:custom_title") == "Homepage")

filterevent = [
    "homepageslider", "select_promotion", "navigation_clicked", "royalclub_buttons", "blog_clicked", "searchpanel_click"
]

cols = [
    "date", "click_text", "click_url", "eventCount", "totalUsers", "newUsers", 
    "sessions", "userEngagementDuration", "averageSessionDuration", "Location"
]

caseblog = F.when(
    (
        (
            F.col("Location").contains("Blog Clicked")
        ) 
        & (
            (F.col("click_text") == "") 
            | (F.col("click_text").contains("(not set)"))
        )
    ), F.initcap(
        F.array_join(
            F.split(
                F.split(
                    F.col("click_url"), "/"
                ).getItem(4), "-"
            ), " "
        )
    )
).otherwise(F.col("click_text"))

caselocation = F.when(
    F.col("Location") == "searchpanel_click", "Search Panel"
    ).when(
        F.col("Location").contains("homepageslider"), "Home Slider" 
    ).when(
        F.col("Location").contains("select_promotion"), "Select Promotion"
    ).when(
        F.col("Location").contains("navigation_clicked"), "Navigation Clicked"
    ).when(
        F.col("Location").contains("blog_clicked"), "Blog Clicked"
    ).when(
        F.col("Location").contains("royalclub_buttons"), "Royalclub Buttons"
    )

def consolidatation(dfs:list):
    df1 = dfs[0]
    for df in dfs[1:]:
        df1 = df1.unionByName(df, allowMissingColumns=True)

    df1 = df1.withColumn(
        "Location", caselocation 
    ).withColumn(
        "click_text", caseblog 
    )

    df1.select(cols).write.mode(
        "overwrite"
    ).option(
        "overwriteSchema", "true"
    ).saveAsTable(
        "analyticadebuddha.google.homepage"
    )

    return None

def createHomePage():
    navigationblog = spark.table(
        pathnav
    ).withColumnRenamed(
        "eventName", "Location"
    ).withColumnRenamed(
        "linkText", "click_text"
    ).withColumnRenamed(
        "linkUrl", "click_url"
    )

    nav = navigationblog.filter((F.col("Location").contains("navigation_clicked") & filternav))
    blog = navigationblog.filter((F.col("Location")).contains("blog_clicked"))

    royalclubsearchpanel = spark.table(
        royalsearch
    ).withColumnRenamed(
        "eventName", "Location"
    ).withColumnRenamed(
        "customEvent:click_text", "click_text"
    )

    offer = spark.table(
        promotion
    ).filter(
        filteroffer
    ).withColumnRenamed(
        "itemsClickedInPromotion", "eventCount"
    ).withColumnRenamed(
        "eventName", "Location"
    ).withColumnRenamed(
        "itemName","click_text"
    ) 

    slider = spark.table(
        sliderpath
    ).withColumnRenamed(
        "eventName", "Location"
    ).withColumnRenamed(
        "customEvent:homepage_slider_imgname", "click_text"
    ).withColumnRenamed(
        "customEvent:click_url", "click_url"
    ).select(cols)

    dfs = [nav, blog, royalclubsearchpanel, offer, slider]

    spark.read.parquet(unifiedPagePathScreenpath).filter(F.trim("unifiedPagePathScreen") == "/").write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        "analyticadebuddha.google.PagePathScreen"
    )
        
    consolidatation(dfs)

    return None

if __name__ == "__main__":
    createHomePage()

# COMMAND ----------

spark.table("analyticadebuddha.google.homepage").display()

# COMMAND ----------

