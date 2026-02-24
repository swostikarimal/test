# Databricks notebook source
# bronze_ingestion
import pyspark.sql.functions as F

# ── Connection ─────────────────────────────────────────────
jdbc_hostname = "54.169.250.227"
jdbc_port     = 5432
jdbc_database = "csd"
jdbc_username = "data"
jdbc_password = "mI83D6iQ8Qjb9ejX"

jdbc_url = f"jdbc:postgresql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

connection_properties = {
    "user":     jdbc_username,
    "password": jdbc_password,
    "driver":   "org.postgresql.Driver"
}

catalog = "analyticadebuddha"
schema  = "bronze_competitor_analysis"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# ── Ingest all tables raw as-is from PostgreSQL ────────────
tables = {
    "airline":                      "airline_report_airline",
    "aircraft_type":                "airline_report_aircrafttype",
    "airline_aircraft_type":        "airline_report_airline_aircraft_type",
    "airline_pax":                  "airline_report_airlinepax",
    "sector_pair":                  "airline_report_sectorpair"
}

for table_name, pg_table in tables.items():
    df = spark.read.jdbc(url=jdbc_url, table=pg_table, properties=connection_properties)
    
    # Add ingestion metadata
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_table", F.lit(pg_table))
    
    df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{catalog}.{schema}.{table_name}")
    
    print(f"✓ {table_name} → {df.count()} rows ingested")

print("\n Bronze ingestion complete!")