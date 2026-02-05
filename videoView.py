# Databricks notebook source
import pyspark.sql.functions as F
from getAdsPerformance import concat_ as platconcat

pathdemvideo25 = "/mnt/meta/platformAndPosition/position_estimated/demoactions/video_p25_watched_actions"
pathdemvideo50 = "/mnt/meta/platformAndPosition/position_estimated/demoactions/video_p50_watched_actions"
pathdemvideo75 = "/mnt/meta/platformAndPosition/position_estimated/demoactions/video_p75_watched_actions"
pathdemvideo95 = "/mnt/meta/platformAndPosition/position_estimated/demoactions/video_p95_watched_actions"

pathposvideo25 = "/mnt/meta/platformAndPosition/position_estimated/positionactions/video_p25_watched_actions"
pathposvideo50 = "/mnt/meta/platformAndPosition/position_estimated/positionactions/video_p50_watched_actions"
pathposvideo75 = "/mnt/meta/platformAndPosition/position_estimated/positionactions/video_p75_watched_actions"
pathposvideo95 = "/mnt/meta/platformAndPosition/position_estimated/positionactions/video_p95_watched_actions"

selcol_gen = ['Date', F.col('campaign_id').alias("CampaignId"), 'Value', 'Age', 'Gender', 'VideoLen']
selcol_pos= ['Date', F.col('campaign_id').alias("CampaignId"), 'Value', 'Platform', F.col('platform_position').alias("platformposition"), 'platformposition1', 'VideoLen']

configposition = {
    "colsname": "platformposition",
    "colsconcat":["platform_position", "platform"]
}

def getVideos(videolis, videolis1):
    '''
    '''
    v25_p = videolis[0]
    v25_g = videolis1[0]

    for video in videolis1[1:]:
        v25_g = v25_g.union(video)

    for video in videolis[1:]:
        v25_p = v25_p.union(video)

    v25_g.select(*selcol_gen).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        "analyticadebuddha.metaadtype.videoview_gender"
    )

    v25_p = platconcat(df=v25_p, config = configposition)

    v25_p.select(*selcol_pos).drop("action_type").write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(
        "analyticadebuddha.metaadtype.videoview_position"
    )

    return None

def main():
    '''
    '''
    videg1 = spark.read.parquet(pathdemvideo25).withColumn("VideoLen", F.lit("P25"))
    videg2 = spark.read.parquet(pathdemvideo50).withColumn("VideoLen", F.lit("P50"))
    videg3 = spark.read.parquet(pathdemvideo75).withColumn("VideoLen", F.lit("P75"))
    videg4 = spark.read.parquet(pathdemvideo95).withColumn("VideoLen", F.lit("P95"))

    videp1 = spark.read.parquet(pathposvideo25).withColumn("VideoLen", F.lit("P25"))
    videp2 = spark.read.parquet(pathposvideo50).withColumn("VideoLen", F.lit("P50"))
    videp3 = spark.read.parquet(pathposvideo75).withColumn("VideoLen", F.lit("P75"))
    videp4 = spark.read.parquet(pathposvideo95).withColumn("VideoLen", F.lit("P95"))

    videolis = [videp1, videp2, videp3, videp4]
    videolis1 = [videg1, videg2, videg3, videg4]

    getVideos(videolis, videolis1)

    return None

if __name__ == "__main__":
    main()