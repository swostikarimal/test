# Databricks notebook source
# DBTITLE 1,Untitled
# Databricks notebook source
'''
Incremental loading of the data from mysql to datalake as required by business requirements
@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 09/09/2024
@VERSION	: 5.0

@HISTORY	:
 1.0 - Initial version
 2.0 - verified column changed to IntegerType; chunked data loading added
 3.0 - Inlined TrackAndUpdate module to remove external dependency
 4.0 - Fixed UnboundLocalError for since/date variables in FetchAndCreate
 5.0 - Performance optimisations for slow loading tables (flight_search etc.)
'''
import pymysql
import pandas as pd
from datetime import datetime
from functools import reduce
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace, unsupported_dtype
from pyspark.sql.types import *

# ── Tune these to balance memory vs speed ────────────────────
CHUNK_SIZE       = 200000
WRITE_PARTITIONS = 8        # replace coalesce(1) — tune to cluster size

def MysqlConnect(HostDB, UserDB, db, PasswordDB):
    """Connect to the RDS"""
    conn = pymysql.connect(
        host=f"{HostDB}",
        user=f"{UserDB}",
        database=f"{db}",
        password=f"{PasswordDB}"
    )
    if conn:
        print("Connected to MySQL successfully")
        return conn
    else:
        print("Error connecting to MySQL")

SchemaUser = StructType([
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("middle_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("password", StringType(), True),
    StructField("remember_token", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("contact", StringType(), True),
    StructField("country", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("address", StringType(), True),
    StructField("street", StringType(), True),
    StructField("user_code", StringType(), True),
    StructField("verified", IntegerType(), True),
    StructField("status", IntegerType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("login_attempt", StringType(), True),
    StructField("verification_code", StringType(), True),
    StructField("token", StringType(), True),
    StructField("type", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("image", StringType(), True),
    StructField("campaign_id", IntegerType(), True),
    StructField("dob", StringType(), True),
    StructField("fb_id", StringType(), True),
    StructField("google_id", StringType(), True),
    StructField("apple_id", StringType(), True),
    StructField("deleted_at", TimestampType(), True)
])

schema_payment = StructType([
    StructField("id", LongType(), True),
    StructField("flight_booking_id", StringType(), True),
    StructField("package_booking_id", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("ref_id", StringType(), True),
    StructField("pid", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("type", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("remarks", StringType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("nmb_co_branded_status", LongType(), True)
])


def read_legacy_data(fullPath):
    """Read existing parquet data without schema enforcement"""
    try:
        return (
            spark.read
            .option("mergeSchema", "false")
            .option("ignoreCorruptFiles", "true")
            .parquet(fullPath)
        )
    except Exception as e:
        print(f"Error reading legacy data from {fullPath}: {e}")
        return None


def FetchAndCreate(df, dbtable, table_name, fullPath):
    """Detects new rows vs existing parquet and returns only the delta"""
    CurrentYear = F.year(F.current_date())
    Filteryear  = F.year("created_at").between("2019", CurrentYear)

    # ── Safe defaults so variables are always bound ───────────
    new   = None
    dicts = {}
    date  = [None]
    since = [None]

    if not df.isEmpty():
        thisrun = unsupported_dtype(df).filter(Filteryear)
        try:
            if fullPath in ["/mnt/bronze/payment"]:
                old = read_legacy_data(fullPath).drop("year")
                if old is not None and not old.isEmpty():
                    if "discount_amount" in old.columns:
                        old = old.withColumn("discount_amount", F.col("discount_amount").cast("string"))
                    if "discount_amount" in thisrun.columns:
                        thisrun = thisrun.withColumn("discount_amount", F.col("discount_amount").cast("string"))
            else:
                old = spark.read.parquet(fullPath).filter(Filteryear).drop("year")

            # ── FIX 5: Cache both dataframes before heavy ops ─
            old.cache()
            thisrun.cache()

            # ── Align thisrun column types to match old ───────
            for col in old.columns:
                if col in thisrun.columns:
                    col_type = old.schema[col].dataType
                    thisrun  = thisrun.withColumn(col, F.col(col).cast(col_type))

            thisrun = thisrun.select(*old.columns)

            # ── FIX 2: Replace subtract() with anti-join ──────
            # subtract() does full shuffle + distinct — very slow
            # anti-join on 'id' is much faster for large tables
            new = thisrun.join(
                old.select("id"),
                on="id",
                how="left_anti"
            )

            # ── Only assign date/since if new has rows ────────
            if not new.isEmpty():
                date  = list(new.select(F.max("created_at")).first())
                since = list(old.select(F.min("created_at")).first())
            else:
                date  = list(old.select(F.max("created_at")).first())
                since = list(old.select(F.min("created_at")).first())

        except Exception as e:
            print(f"Error in FetchAndCreate for '{table_name}': {e}")
            try:
                date  = list(thisrun.select(F.max("created_at")).first())
                since = list(thisrun.select(F.min("created_at")).first())
                new   = thisrun
            except Exception as inner_e:
                print(f"  Fallback date extraction also failed: {inner_e}")
                date  = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                since = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                new   = thisrun

        finally:
            # ── Always unpersist cache after use ──────────────
            try:
                old.unpersist()
                thisrun.unpersist()
            except:
                pass

        dicts = {
            "FileName":    table_name,
            "Destination": fullPath,
            "DbTable":     dbtable,
            "Since":       since[0],
            "UptoDate":    date[0],
            "LastRun":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Active":      True
        }

    return new, dicts


def clean_pandas_dataframe(df):
    """Clean pandas DataFrame before converting to Spark DataFrame"""
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                pd.to_numeric(df[col], errors='raise')
            except Exception:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('nan',  None)
                df[col] = df[col].replace('None', None)
    return df


def clean_verified_column(Pdf):
    """
    Safely coerce 'verified' to nullable Int64 so it maps
    cleanly to IntegerType in SchemaUser.
    """
    if "verified" in Pdf.columns:
        Pdf["verified"] = pd.to_numeric(Pdf["verified"], errors="coerce")
        Pdf["verified"] = Pdf["verified"].astype("Int64")
    return Pdf


def getTableList(df):
    df     = df.toPandas()
    tables = list(df["DbTable"])
    return tables


def fetch_table_in_chunks(query_base, conn, chunk_size=CHUNK_SIZE, last_id=0):
    """
    FIX 4: Fetches MySQL data using keyset pagination (WHERE id > last_id)
    instead of LIMIT/OFFSET so performance stays constant across all chunks.

    @param query_base : base SQL without pagination
    @param conn       : active pymysql connection
    @param chunk_size : number of rows per chunk
    @param last_id    : starting id for keyset pagination
    """
    chunk_num = 0
    current_id = last_id

    while True:
        # ── Keyset pagination — always fast regardless of depth
        paginated_query = (
            f"{query_base} AND id > {current_id} "
            f"ORDER BY id "
            f"LIMIT {chunk_size}"
        )
        chunk = pd.read_sql_query(paginated_query, conn)

        if chunk.empty:
            print(f"  No more data after id={current_id}. Chunking complete.")
            break

        chunk_num  += 1
        current_id  = int(chunk["id"].max())    # move cursor to last id in chunk
        print(f"  Chunk {chunk_num}: {len(chunk)} rows (last id={current_id})")
        yield chunk


def getTables(configtable, tables=list, conn=str, write=False, path=str, db=str, schema=None):
    dictframe = []

    for dbtable in tables:
        date = configtable.filter(
            F.col("DbTable") == f"{dbtable}"
        ).select("UptoDate").collect()[0][0]

        CY = datetime.now().year
        print(f"\nLoading table '{dbtable}' from {date} onward ...")

        # ── FIX 4: Base query without ORDER BY/pagination ────
        # ORDER BY and pagination are handled inside fetch_table_in_chunks
        query_base = (
            f"SELECT * FROM {db}.{dbtable} "
            f"WHERE created_at >= '{date}' AND YEAR(created_at) <= {CY}"
        )

        all_chunks_spark = []

        for chunk_pdf in fetch_table_in_chunks(query_base, conn, chunk_size=CHUNK_SIZE):

            chunk_pdf = clean_verified_column(chunk_pdf)

            if dbtable == "users":
                chunk_df = spark.createDataFrame(chunk_pdf, schema=SchemaUser)
            else:
                chunk_df = spark.createDataFrame(clean_pandas_dataframe(chunk_pdf))

            all_chunks_spark.append(chunk_df)

        if not all_chunks_spark:
            print(f"  No data found for '{dbtable}' from {date} onward (year {CY})")
            continue

        # ── Union all chunks ──────────────────────────────────
        df = reduce(lambda a, b: a.union(b), all_chunks_spark)

        # ── FIX 1: Cache df before count to avoid double scan ─
        df.cache()
        total_rows = df.count()
        print(f"  Total rows loaded for '{dbtable}': {total_rows}")

        # ── Normalise table name ──────────────────────────────
        table = dbtable[:-1] if dbtable.endswith("s") else dbtable
        if table.startswith("tbl"):
            table = "_".join(table.split("_")[1:])
        table_name = str(table)

        # ── Rename columns ────────────────────────────────────
        for col in df.columns:
            if col.strip().endswith("code"):
                df = df.withColumnRenamed(col, col.strip()[:-4] + "id")
            else:
                new_col = (
                    col.strip()
                    .replace(" ", "_")
                    .replace("-", "_")
                    .replace(".", "_")
                    .replace("code", "id")
                )
                df = df.withColumnRenamed(col, new_col)

        # ── Write to parquet ──────────────────────────────────
        if write:
            fullPath = path + "/" + table_name
            newdata, dicts = FetchAndCreate(df, dbtable, table_name, fullPath)

            if newdata is not None and not newdata.isEmpty():
                dictframe.append(dicts)
                newdata = newdata.withColumn("year", F.year("created_at"))

                # ── FIX 1: Cache newdata before count ─────────
                newdata.cache()
                row_count = newdata.count()

                # ── FIX 3: Replace coalesce(1) with repartition
                # coalesce(1) funnels all data to one node — kills parallelism
                newdata.repartition(WRITE_PARTITIONS).write \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .partitionBy("year") \
                    .parquet(fullPath)

                newdata.unpersist()
                print(f"  Appended {row_count} new rows → {fullPath}")
            else:
                print(f"  No new data to append for '{table_name}'")

        # ── Always unpersist df after table is done ───────────
        df.unpersist()

    return dictframe


def updateConfig(configtable, dictframe):
    if len(dictframe) > 0:
        newconfig = (
            spark.createDataFrame(pd.DataFrame(dictframe))
            .withColumn("LastRun", F.lit(F.current_timestamp()))
        )
        old = configtable.alias("df1").join(
            newconfig.alias("df2"),
            F.col("df1.FileName") == F.col("df2.FileName"),
            how="left_anti"
        )
        if not old.isEmpty():
            newconfig = newconfig.union(old)

        newconfig.write \
            .mode("overwrite") \
            .option("mergeSchema",     "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable("analyticadebuddha.default.updates")

        configtable.write \
            .mode("append") \
            .option("mergeSchema",     "true") \
            .option("overwriteSchema", "true") \
            .parquet("/mnt/bronze/updatedhistory")
    else:
        print("No new data to append to the config table")


def main():
    """Call all the functions"""
    HostDB     = "13.215.173.246"
    UserDB     = dbutils.secrets.get(scope="database", key="db_user")
    PasswordDB = dbutils.secrets.get(scope="database", key="db_password")
    db         = dbutils.secrets.get(scope="database", key="db_name")

    conn        = MysqlConnect(HostDB, UserDB, db, PasswordDB)
    configtable = spark.table("analyticadebuddha.default.updates").filter(F.col("Active") == True)
    tables      = getTableList(df=configtable)
    dictframe   = getTables(configtable, tables=tables, conn=conn, write=True, path="/mnt/bronze", db=db)
    updateConfig(configtable, dictframe)


if __name__ == "__main__":
    main()