# Databricks notebook source
# gold_competitor_analysis
from datetime import timedelta
import pyspark.sql.functions as F

silver  = "analyticadebuddha.silver_competitor_analysis"
catalog = "analyticadebuddha"
schema  = "gold_competitor_analysis"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# ── Read from Silver & Bronze ──────────────────────────────
final   = spark.read.table(f"{silver}.competitor_analysis_final")
airtype = spark.read.table(f"analyticadebuddha.bronze_competitor_analysis.aircraft_type")
sector  = spark.read.table(f"analyticadebuddha.bronze_competitor_analysis.sector_pair")
pax_    = spark.read.table(f"analyticadebuddha.bronze_competitor_analysis.airline_pax") \
               .filter((F.col("pax") > 0) & (F.col("aircraft_type_id").isNotNull()))

# ── Dim: airlines ──────────────────────────────────────────
airlines = spark.read.table(
    f"analyticadebuddha.bronze_competitor_analysis.airline"
).select(
    F.col("id").alias("airline_id"),
    F.col("name").alias("airline_name")
)

# ── Dim: aircraft_info ─────────────────────────────────────
aircraft_info = airtype.select(
    F.col("id").alias("aircraft_type_id"),
    F.col("name").alias("aircraft_type_name"),
    F.col("seat_capacity")
)

# ── Dim: sectors ───────────────────────────────────────────
sectors = sector.select(
    F.col("id").alias("sector_pair_id"),
    F.col("code").alias("sector_code"),
    F.col("inbound"),
    F.col("outbound")
)

# ── Dim: dates ─────────────────────────────────────────────
dates = pax_.select("date").distinct().select(
    F.col("date").alias("flight_date"),
    F.year("date").alias("year"),
    F.month("date").alias("month"),
    F.quarter("date").alias("quarter"),
    F.dayofmonth("date").alias("day"),
    F.date_format("date", "MMMM").alias("month_name"),
    F.dayofweek("date").alias("day_of_week"),
    F.date_format("date", "EEEE").alias("day_name")
)

# ── Fact: flight_performance ───────────────────────────────
flight_performance = final.join(
    spark.read.table("analyticadebuddha.bronze_competitor_analysis.airline")
        .select(F.col("id").alias("airline_id"), F.col("name").alias("Airlines")),
    on="Airlines"
).join(
    airtype.select(F.col("id").alias("aircraft_type_id"), F.col("name").alias("AircraftType")),
    on="AircraftType"
).join(
    sector.select(F.col("id").alias("sector_pair_id"), F.col("code").alias("SectorPair")),
    on="SectorPair"
).select(
    F.col("airline_id"),
    F.col("aircraft_type_id"),
    F.col("sector_pair_id"),
    F.col("FlightDate").alias("flight_date"),
    F.col("TotalSecPass_").alias("airline_passengers"),
    F.col("TotalSecPass").alias("total_sector_passengers"),
    F.col("NFlights").alias("number_of_flights"),
    F.col("AvgOccupancy").alias("avg_occupancy_pct"),
    F.col("Share").alias("market_share_pct")
)

# ── Dim: route ─────────────────────────────────────────────
# Get Buddha Air airline_id
buddha_id = airlines.filter(F.col("airline_name") == "Buddha Air") \
                    .select("airline_id").collect()[0][0]

# Get latest date from flight_performance
latest_date    = flight_performance.agg(F.max("flight_date")).collect()[0][0]
recent30_start = latest_date - timedelta(days=29)
prior30_start  = latest_date - timedelta(days=59)
prior30_end    = latest_date - timedelta(days=30)

# Tag each row with windowed pax buckets before aggregation
fp_tagged = flight_performance \
    .withColumn("_buddha_pax_all",
        F.when(F.col("airline_id") == buddha_id, F.col("airline_passengers"))
         .otherwise(0)) \
    .withColumn("_buddha_pax30",
        F.when(
            (F.col("flight_date") >= F.lit(recent30_start)) &
            (F.col("airline_id") == buddha_id),
            F.col("airline_passengers"))
         .otherwise(0)) \
    .withColumn("_total_pax30",
        F.when(F.col("flight_date") >= F.lit(recent30_start),
            F.col("total_sector_passengers"))
         .otherwise(0)) \
    .withColumn("_buddha_pax60",
        F.when(
            F.col("flight_date").between(F.lit(prior30_start), F.lit(prior30_end)) &
            (F.col("airline_id") == buddha_id),
            F.col("airline_passengers"))
         .otherwise(0)) \
    .withColumn("_total_pax60",
        F.when(
            F.col("flight_date").between(F.lit(prior30_start), F.lit(prior30_end)),
            F.col("total_sector_passengers"))
         .otherwise(0))

# Aggregate per route
route_agg = fp_tagged.groupBy("sector_pair_id").agg(
    F.sum("total_sector_passengers").alias("total_pax"),
    F.sum("_buddha_pax_all").alias("buddha_pax"),
    F.countDistinct("flight_date").alias("days"),
    F.sum("_buddha_pax30").alias("buddha_pax30"),
    F.sum("_total_pax30").alias("total_pax30"),
    F.sum("_buddha_pax60").alias("buddha_pax60"),
    F.sum("_total_pax60").alias("total_pax60")
)

# Compute ratios and labels
route_agg = route_agg \
    .withColumn("share",
        F.when(F.col("total_pax") > 0,
            F.col("buddha_pax") / F.col("total_pax")).otherwise(0)) \
    .withColumn("daily_pax",
        F.when(F.col("days") > 0,
            F.col("total_pax") / F.col("days")).otherwise(0)) \
    .withColumn("share30",
        F.when(F.col("total_pax30") > 0,
            F.col("buddha_pax30") / F.col("total_pax30")).otherwise(0)) \
    .withColumn("share60",
        F.when(F.col("total_pax60") > 0,
            F.col("buddha_pax60") / F.col("total_pax60")).otherwise(0)) \
    .withColumn("quadrant",
        F.when((F.col("share") >= 0.43) & (F.col("daily_pax") >= 1000), "Defend")
         .when((F.col("share") >= 0.43) & (F.col("daily_pax") <  1000), "Niche Routes")
         .when((F.col("share") <  0.43) & (F.col("daily_pax") >= 1000), "Attack Opportunity")
         .otherwise("Reconsider")) \
    .withColumn("trend_label",
        F.when(F.col("share30") >= F.col("share60"), "Gaining Share")
         .otherwise("Losing Share"))

# Join sector info and compute cluster/origin/destination
dim_route = route_agg.join(
    sector.select(
        F.col("id").alias("sector_pair_id"),
        F.col("code").alias("sector_pair"),
        F.col("inbound").alias("origin"),
        F.col("outbound").alias("destination")
    ),
    on="sector_pair_id"
).withColumn("route_cluster",
    F.when(F.col("sector_pair").contains("KTM"), "KTM Hub")
     .when(F.col("sector_pair").contains("PKR"), "PKR Hub")
     .otherwise("Other")
).select(
    "sector_pair_id",
    "sector_pair",
    "origin",
    "destination",
    "route_cluster",
    "quadrant",
    "trend_label"
)

# ── Save Gold Tables ───────────────────────────────────────
gold_tables = {
    "airlines":           airlines,
    "aircraft_info":      aircraft_info,
    "sectors":            sectors,
    "dates":              dates,
    "flight_performance": flight_performance,
    "dim_route":          dim_route          # ← added
}

for table_name, df in gold_tables.items():
    df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.{table_name}")
    print(f"✓ {table_name} saved")

print("\n✅ Gold layer complete!")

# COMMAND ----------

