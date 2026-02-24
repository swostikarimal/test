# Databricks notebook source
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace 

homesliderpath = "/mnt/ga/reporting/CTA/homepage/homepageslider"
searchpanelpath = "/mnt/ga/reporting/CTA/homepage/searchpanel_click"
promotionpath = "/mnt/ga/reporting/CTA/homepage/select_promotion"
blogclickedpath = "/mnt/ga/reporting/CTA/homepage/blog_clicked"
navigationclickedpath = "/mnt/ga/reporting/CTA/homepage/navigation_clicked"
royalclubpath = "/mnt/ga/reporting/CTA/homepage/royalclub_buttons"

filterpromotion = (F.trim("customEvent:previous_url") == "https://www.buddhaair.com/")
otherfilter = F.col("customEvent:custom_title").contains("Homepage")

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

def getHomePageEvent():
    homeslider = spark.read.parquet(homesliderpath).filter(
        F.trim("eventName").contains("homepageslider")
    )
    searchpanel = spark.read.parquet(searchpanelpath).filter(
        (F.trim("eventName").contains("searchpanel_click"))
    ) 

    promotion = spark.read.parquet(promotionpath).withColumnRenamed(
        "itemsClickedInPromotion", "eventCount"
    ).filter(
        filterpromotion & (F.col("eventName").contains("select_promotion"))
    ).drop("customEvent:previous_url")

    blogclicked = spark.read.parquet(blogclickedpath).filter(
        (F.trim("eventName").contains("blog_clicked"))
    ).drop("customEvent:custom_title")

    navigationclicked = spark.read.parquet(navigationclickedpath).filter(
        otherfilter & (F.trim("eventName").contains("navigation_clicked"))
    ).drop("customEvent:custom_title")

    royalclub = spark.read.parquet(royalclubpath).filter(
        (F.trim("eventName").contains("royalclub_buttons"))
    ).drop("customEvent:custom_title")

    dfs = [homeslider, searchpanel, promotion, blogclicked, navigationclicked, royalclub]

    df1 = dfs[0]

    for df in dfs[1:]:
        df1 = df1.unionByName(df, allowMissingColumns=True)

    df1.withColumnRenamed("eventName", "Location").withColumn(
        "Location", caselocation
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.google.totalEvent")

if __name__ == "__main__":
    getHomePageEvent()