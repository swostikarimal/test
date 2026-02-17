# Databricks notebook source
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("analyticadebuddha.competitor_bronze.competitor_raw")


# COMMAND ----------

bronze_df = spark.read.table("airline_catalog.bronze.airline_report_airline_raw")
display(bronze_df)



# COMMAND ----------

bronze_df = spark.read.table("airline_catalog.bronze.airline_report_airline_aircraft_type_raw")
display(bronze_df)

# COMMAND ----------

bronze_df = spark.read.table("airline_catalog.bronze.airline_report_aircrafttype_raw")
display(bronze_df)

# COMMAND ----------

bronze_df = spark.read.table("airline_catalog.bronze.airline_report_airlinepax_raw")
display(bronze_df)

# COMMAND ----------

bronze_df = spark.read.table("airline_catalog.bronze.airline_report_sectorpair_raw")
display(bronze_df)

# COMMAND ----------

bronze_df.printSchema()
bronze_df.show(5)
display (bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC SILVER - SECTORPAIR

# COMMAND ----------

bronze_sector = spark.read.table("airline_catalog.bronze.airline_report_sectorpair_raw")

from pyspark.sql.functions import col, to_date, trim, upper
dim_sector = (
    bronze_sector
    .dropDuplicates()
    .withColumn("inbound", upper(trim("inbound")))
    .withColumn("outbound", upper(trim("outbound")))
    .withColumn("sector_code", upper(trim("code")))
)

dim_sector.write.format("delta").mode("overwrite")\
    .saveAsTable("airline_catalog.silver.dim_sector")

display(dim_sector)

# COMMAND ----------

bronze_aircraft = spark.read.table("airline_catalog.bronze.airline_report_aircrafttype_raw")

dim_aircrafttype = bronze_aircraft.dropDuplicates()

dim_aircrafttype.write.format("delta").mode("overwrite") \
    .saveAsTable("airline_catalog.silver.dim_aircrafttype")

display (dim_aircrafttype)


# COMMAND ----------

bronze_map = spark.read.table("airline_catalog.bronze.airline_report_airline_aircraft_type_raw")

dim_airline_aircraft = bronze_map.dropDuplicates()

dim_airline_aircraft.write.format("delta").mode("overwrite") \
    .saveAsTable("airline_catalog.silver.[dim_airline_aircraft]")

display (dim_airline_aircraft)


# COMMAND ----------

from pyspark.sql.functions import to_date, col

bronze_pax = spark.read.table("airline_catalog.bronze.airline_report_airlinepax_raw")

fact_passenger = (
    bronze_pax
    .dropDuplicates()
    .withColumn("flight_date", to_date(col("date"), "yyyy-MM-dd"))
)

fact_passenger.write.format("delta").mode("overwrite") \
    .saveAsTable("airline_catalog.silver.fact_passenger")

display (fact_passenger)

# COMMAND ----------

bronze_airline = spark.read.table("airline_catalog.bronze.airline_report_airline_raw")

fact_flight = bronze_airline.dropDuplicates()

fact_flight.write.format("delta").mode("overwrite") \
    .saveAsTable("airline_catalog.silver.fact_flight")

display (fact_flight)


# COMMAND ----------

from pyspark.sql.functions import sum

fact_pax = spark.read.table("airline_catalog.silver.fact_passenger")
dim_sector = spark.read.table("airline_catalog.silver.dim_sector")

gold_pax_sector = (
    fact_pax
    .join(dim_sector, fact_pax.sector_pair_id == dim_sector.sector_code, "left")
    .groupBy("inbound", "outbound")
    .agg(sum("pax").alias("total_pax"))
)

gold_pax_sector.write.format("delta").mode("overwrite") \
    .saveAsTable("airline_catalog.gold.pax_by_sector")

display(gold_pax_sector)


# COMMAND ----------

from pyspark.sql.functions import sum, col, count

fact_pax = spark.read.table("airline_catalog.silver.fact_passenger")
dim_airline = spark.read.table("airline_catalog.silver.dim_airline_aircraft")
dim_sector = spark.read.table("airline_catalog.silver.dim_sector")
dim_aircraft = spark.read.table("airline_catalog.silver.dim_aircrafttype")
fact_flight = spark.read.table("airline_catalog.silver.fact_flight")

gold_df = fact_pax \
    .join(dim_airline, "airline_id") \
    .join(dim_sector, "sector_id") \
    .join(dim_aircraft, "aircraft_id") \
    .groupBy("flight_date","airline_name","sector_name","aircraft_type") \
    .agg(
        sum("pax").alias("total_pax"),
        sum("seat_capacity").alias("total_seats"),
        count("flight_id").alias("flights_count")
    )

gold_df = gold_df.withColumn(
    "avg_occupancy_rate",
    col("total_pax")/col("total_seats")
)
