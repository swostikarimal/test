# Databricks notebook source
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.dialects.postgresql import insert

host = "54.169.250.227"
port = "5432"
database = "csd"
user = "data"
password = "mI83D6iQ8Qjb9ejX"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

with engine.connect() as conn:
    print("Connected to database successfully")

# COMMAND ----------

from sqlalchemy import create_engine, inspect

inspector = inspect (engine)
tables = inspector.get_table_names()

for table in tables:
    if "airline" in table: 
        print(table)

# COMMAND ----------

import pandas as pd
from sqlalchemy import Table, MetaData, text

with engine.begin() as conn:
    df = pd.read_sql_query(text("SELECT * FROM airline_report_sectorpair"), con=conn)

df
display (df)


# COMMAND ----------

import pandas as pd
from sqlalchemy import Table, MetaData, text

with engine.begin() as conn:
    df = pd.read_sql_query(text("SELECT * FROM airline_report_airline"), con=conn)

df
display(df)


# COMMAND ----------

import pandas as pd
from sqlalchemy import Table, MetaData, text

with engine.begin() as conn:
    df = pd.read_sql_query(text("SELECT * FROM airline_report_aircrafttype"), con=conn)

df
display(df)


# COMMAND ----------

import pandas as pd
from sqlalchemy import Table, MetaData, text

with engine.begin() as conn:
    df = pd.read_sql_query(text("SELECT * FROM airline_report_airline_aircraft_type"),con=conn)

df
display(df)

# COMMAND ----------

# import pyspark.sql.functions as F
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("AirlineCompetitorAnalysis").getOrCreate()

# jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

# connection_properties = {
#     "user": user,
#     "password": password,
#     "driver": "org.postgresql.Driver"
# }

# start_date = '2025-12-01'
# end_date = '2025-12-30'

# df1 = spark.read.jdbc(url=jdbc_url, table="airline_report_airlinepax", properties=connection_properties)
# df2 = spark.read.jdbc(url=jdbc_url, table="airline_report_airline", properties=connection_properties)
# df3 = spark.read.jdbc(url=jdbc_url, table="airline_report_aircrafttype", properties=connection_properties)
# df4 = spark.read.jdbc(url=jdbc_url, table="airline_report_sectorpair", properties=connection_properties)

# selectCols = [
#     F.col("df1.date").alias("FlightDate"), 
#     F.col("df2.name").alias("Airlines"), 
#     F.col("df3.name").alias("AircraftType"), 
#     F.col("df3.seat_capacity").alias("SeatCapacity"),
#     F.col("df4.code").alias("SectorPair"), 
#     F.col("df1.pax").alias("Passengers"), 
#     F.col("df1.flight_index").alias("FlightIndex")
# ]

# result_df = df1.alias("df1") \
#     .join(df2.alias("df2"), F.col("df1.airline_id") == F.col("df2.id"), "left") \
#     .join(df3.alias("df3"), F.col("df1.aircraft_type_id") == F.col("df3.id"), "left") \
#     .join(df4.alias("df4"), F.col("df1.sector_pair_id") == F.col("df4.id"), "left") \
#     .filter(
#         (F.col("df1.date") >= start_date) & 
#         (F.col("df1.date") <= end_date) &
#         F.col("df1.airline_id").isNotNull() &
#         F.col("df1.aircraft_type_id").isNotNull()
#     ) \
#     .select(selectCols) \
#     .orderBy("FlightDate", "Airlines")

# print(f"{result_df.count()} records from {start_date} to {end_date}")
# result_df.show(truncate=False)

# display (result_df)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AirlineCompetitorAnalysis").getOrCreate()

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

df1 = spark.read.jdbc(url=jdbc_url, table="airline_report_airlinepax", properties=connection_properties)
df2 = spark.read.jdbc(url=jdbc_url, table="airline_report_airline", properties=connection_properties)
df3 = spark.read.jdbc(url=jdbc_url, table="airline_report_aircrafttype", properties=connection_properties)
df4 = spark.read.jdbc(url=jdbc_url, table="airline_report_sectorpair", properties=connection_properties)

selectCols = [
    F.col("df1.date").alias("FlightDate"), 
    F.col("df2.name").alias("Airlines"), 
    F.col("df3.name").alias("AircraftType"), 
    F.col("df3.seat_capacity").alias("SeatCapacity"),
    F.col("df4.code").alias("SectorPair"), 
    F.col("df1.pax").alias("Passengers"), 
    F.col("df1.flight_index").alias("FlightIndex")
]

result_df = df1.alias("df1") \
    .join(df2.alias("df2"), F.col("df1.airline_id") == F.col("df2.id"), "left") \
    .join(df3.alias("df3"), F.col("df1.aircraft_type_id") == F.col("df3.id"), "left") \
    .join(df4.alias("df4"), F.col("df1.sector_pair_id") == F.col("df4.id"), "left") \
    .filter(
        F.col("df1.airline_id").isNotNull() &
        F.col("df1.aircraft_type_id").isNotNull()
    ) \
    .select(selectCols) \
    .orderBy("FlightDate", "Airlines")

print(f"{result_df.count()} total records")
result_df.show(truncate=False)

display(result_df)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AirlineCompetitorAnalysis").getOrCreate()

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

start_date = '2025-12-01'
end_date = '2025-12-30'

# Read tables
df1 = spark.read.jdbc(url=jdbc_url, table="airline_report_airlinepax", properties=connection_properties)
df2 = spark.read.jdbc(url=jdbc_url, table="airline_report_airline", properties=connection_properties)
df3 = spark.read.jdbc(url=jdbc_url, table="airline_report_aircrafttype", properties=connection_properties)
df4 = spark.read.jdbc(url=jdbc_url, table="airline_report_sectorpair", properties=connection_properties)

# Join and select base dataset
result_df = (
    df1.alias("df1")
    .join(df2.alias("df2"), F.col("df1.airline_id") == F.col("df2.id"), "left")
    .join(df3.alias("df3"), F.col("df1.aircraft_type_id") == F.col("df3.id"), "left")
    .join(df4.alias("df4"), F.col("df1.sector_pair_id") == F.col("df4.id"), "left")
    .filter(
        (F.col("df1.airline_id").isNotNull()) &
        (F.col("df1.aircraft_type_id").isNotNull()) &
        (F.col("df1.date").between(start_date, end_date))
    )
    .select(
        F.col("df1.date").alias("FlightDate"),
        F.col("df2.name").alias("Airlines"),
        F.col("df3.name").alias("AircraftType"),
        F.col("df3.seat_capacity").alias("SeatCapacity"),
        F.col("df4.code").alias("SectorPair"),
        F.col("df1.pax").alias("Passengers"),
        F.col("df1.flight_index").alias("FlightIndex")
    )
)

# Sector-wise aggregation
sector_wise = (
    result_df
    .groupBy("FlightDate", "SectorPair")
    .agg(
        F.sum("Passengers").alias("SectorTotalPassengers"),
        F.countDistinct("FlightIndex").alias("SectorTotalFlights")
    )
)

# Airline + Aircraft-wise aggregation
airline_aircraft_wise = (
    result_df
    .groupBy(
        "FlightDate",
        "SectorPair",
        "Airlines",
        "AircraftType",
        "SeatCapacity"
    )
    .agg(
        F.sum("Passengers").alias("TotalPassengers"),
        F.countDistinct("FlightIndex").alias("NFlights")
    )
)

# Final dataset (NO occupancy, NO passenger share)
final_df = (
    airline_aircraft_wise.alias("a")
    .join(
        sector_wise.alias("s"),
        (F.col("a.FlightDate") == F.col("s.FlightDate")) &
        (F.col("a.SectorPair") == F.col("s.SectorPair")),
        "left"
    )
    .select(
        F.col("a.FlightDate"),
        F.col("a.SectorPair"),
        F.col("a.Airlines"),
        F.col("a.AircraftType"),
        F.col("a.SeatCapacity"),
        F.col("a.TotalPassengers"),
        F.col("a.NFlights"),
        F.col("s.SectorTotalPassengers"),
        F.col("s.SectorTotalFlights")
    )
    .orderBy("FlightDate", "SectorPair", "Airlines", "AircraftType")
)

final_df.display()

