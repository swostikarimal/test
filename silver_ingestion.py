# Databricks notebook source
# silver_competitor_analysis
import pyspark.sql.functions as F

bronze  = "analyticadebuddha.bronze_competitor_analysis"
catalog = "analyticadebuddha"
schema  = "silver_competitor_analysis"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# ── Read from Bronze ───────────────────────────────────────
pax     = spark.read.table(f"{bronze}.airline_pax")
airline = spark.read.table(f"{bronze}.airline")
airtype = spark.read.table(f"{bronze}.aircraft_type")
sector  = spark.read.table(f"{bronze}.sector_pair")

# ── Filter ─────────────────────────────────────────────────
pax_ = pax.filter(
    (F.col("pax") > 0) & (F.col("aircraft_type_id").isNotNull())
)

# ── Aggregate ──────────────────────────────────────────────
totalpassenger = pax_.groupby(
    "date", "aircraft_type_id", "airline_id", "sector_pair_id"
).agg(
    F.sum("pax").alias("Passengers"),
    F.count("date").alias("NFlights")
)

# ── Join Dimensions ────────────────────────────────────────
selectCols = [
    F.col("df1.date").alias("FlightDate"),
    F.col("df2.name").alias("Airlines"),
    F.col("df3.name").alias("AircraftType"),
    F.col("df3.seat_capacity").alias("SeatCapacity"),
    F.col("df4.code").alias("SectorPair"),
    F.col("df1.Passengers"),
    F.col("df1.NFlights")
]

info = totalpassenger.alias("df1") \
    .join(airline.alias("df2"),  F.col("df1.airline_id")       == F.col("df2.id")) \
    .join(airtype.alias("df3"),  F.col("df1.aircraft_type_id") == F.col("df3.id")) \
    .join(sector.alias("df4"),   F.col("df1.sector_pair_id")   == F.col("df4.id")) \
    .select(*selectCols)

# ── Sector Total Passengers ────────────────────────────────
totalsecpas = info.groupby("FlightDate", "SectorPair").agg(
    F.sum("Passengers").alias("TotalSecPass")
)

# ── Airline + Sector Summary ───────────────────────────────
totalsecpas_air = info.groupby(
    "FlightDate", "Airlines", "AircraftType", "SeatCapacity", "SectorPair"
).agg(
    F.sum("Passengers").alias("TotalSecPass_"),
    F.sum("NFlights").alias("NFlights"),
    F.round(
        F.sum("Passengers") / (F.avg("SeatCapacity") * F.sum("NFlights")) * 100, 2
    ).alias("AvgOccupancy")
)

# ── Final with Market Share ────────────────────────────────
final = totalsecpas_air.alias("df1").join(
    totalsecpas.alias("df2"),
    (F.col("df1.FlightDate") == F.col("df2.FlightDate")) &
    (F.col("df1.SectorPair") == F.col("df2.SectorPair"))
).select(
    "df1.*", "df2.TotalSecPass"
).withColumn(
    "Share", F.round((F.col("TotalSecPass_") / F.col("TotalSecPass")) * 100, 2)
)

# ── Save Silver Tables ─────────────────────────────────────
silver_tables = {
    "flight_info":                  info,
    "sector_total_passengers":      totalsecpas,
    "airline_sector_summary":       totalsecpas_air,
    "competitor_analysis_final":    final
}

for table_name, df in silver_tables.items():
    df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.{table_name}")
    print(f"✓ {table_name} saved")

print("\n Silver layer complete!")

display (df)