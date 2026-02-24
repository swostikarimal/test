# Databricks notebook source
# gold_competitor_analysis
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

# ── Save Gold Tables ───────────────────────────────────────
gold_tables = {
    "airlines":           airlines,
    "aircraft_info":      aircraft_info,
    "sectors":            sectors,
    "dates":              dates,
    "flight_performance": flight_performance
}

for table_name, df in gold_tables.items():
    df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.{table_name}")
    print(f"✓ {table_name} saved")

print("\n✅ Gold layer complete!")
