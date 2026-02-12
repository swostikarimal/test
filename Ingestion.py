# Databricks notebook source

# Databricks notebook source

"""
OPTIMIZED VERSION - Load all data at once
Key changes:
1. Removed chunking - loads all data in single query
2. Load old data ONCE
3. Compare at the END
4. Faster for smaller datasets
"""

import pymysql
import pandas as pd
from datetime import datetime
import pyspark.sql.functions as F
from TrackAndUpdate import trackAndTrace, unsupported_dtype
from pyspark.sql.types import *

def MysqlConnect(HostDB, UserDB, db, PasswordDB):
    """Connect to the RDS"""
    conn = pymysql.connect(
        host = f"{HostDB}",
        user = f"{UserDB}",
        database = f"{db}",
        password = f"{PasswordDB}"
    )
    if conn:
        print("Connected to MySQL successfully")
        return conn
    else:
        print("Error")

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

def clean_pandas_dataframe(df):
    """Clean pandas DataFrame before converting to Spark DataFrame"""
    df = df.where(pd.notnull(df), None)
    
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                pd.to_numeric(df[col], errors='raise')
            except:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].replace('None', None)
    
    return df

def read_legacy_data(fullPath):
    """Read existing parquet data without schema enforcement"""
    try:
        return spark.read.option("mergeSchema", "false").option("ignoreCorruptFiles", "true").parquet(fullPath)
    except Exception as e:
        print(f"Error reading legacy data: {e}")
        return None

def getTableList(df):
    df = df.toPandas()
    tables = list(df["DbTable"])
    return tables

def getTables_NoChunking(configtable, tables=list, conn=str, write=False, path=str, db=str, schema=None):
    """
    SIMPLIFIED APPROACH - No chunking, load all data at once
    1. Load all new MySQL data in single query
    2. Compare ONCE with old data
    3. Write result
    """
    dictframe = []
    
    for dbtable in tables:
        try:
            date = configtable.filter(F.col("DbTable") == f"{dbtable}").select("UptoDate").collect()[0][0]
            CY = datetime.now().year
            print(f"\n{'='*60}")
            print(f"Processing table: {dbtable}")
            print(f"Loading data from: {date} onwards")
            print(f"{'='*60}")
            
            # Determine table name
            if str(dbtable).endswith("s"):
                table = dbtable[:-1]
            else:
                table = dbtable

            if str(table).startswith("tbl"):
                table = "_".join(table.split("_")[1:])

            table_name = str(table)
            fullPath = path + "/" + table_name
            
            # Load all data at once (no chunking)
            query = f"SELECT * FROM {db}.{dbtable} WHERE created_at >= '{date}' AND YEAR(created_at) <= {CY}"
            
            print(f"⏳ Loading data from MySQL...")
            Pdf = pd.read_sql_query(query, conn)
            
            if Pdf.empty:
                print(f"⏭️  No new data for {dbtable}")
                continue
            
            print(f"✅ Loaded {len(Pdf):,} rows from MySQL")
            
            # Clean pandas data
            if "verified" in Pdf.columns:
                Pdf["verified"] = Pdf["verified"].astype("Int64")
            if "dob" in Pdf.columns:
                Pdf["dob"] = Pdf["dob"].astype(str).replace('nan', None)
            
            # Convert to Spark
            if dbtable == "users" and schema is not None:
                combined_df = spark.createDataFrame(Pdf, schema=schema)
            else:
                combined_df = spark.createDataFrame(clean_pandas_dataframe(Pdf))
            
            print(f"✅ Converted to Spark DataFrame: {combined_df.count():,} rows")
            
            # Rename columns
            for col in combined_df.columns:
                if col.strip().endswith("code"):
                    combined_df = combined_df.withColumnRenamed(col, col.strip()[:-4] + "id")
                else:
                    new_col = col.strip().replace(" ", "_").replace("-", "_").replace(".", "_").replace("code", "id")
                    combined_df = combined_df.withColumnRenamed(col, new_col)
            
            if write:
                print(f"🔍 Comparing with existing data...")
                
                # Now do the comparison ONCE
                CurrentYear = F.year(F.current_date())
                Filteryear = F.year("created_at").between("2019", CurrentYear)
                
                thisrun = unsupported_dtype(combined_df).filter(Filteryear)
                
                try:
                    # Read old data ONCE
                    if fullPath in ["/mnt/bronze/payment"]:
                        old = read_legacy_data(fullPath)
                        if old is not None and not old.isEmpty():
                            old = old.drop("year")
                            if "discount_amount" in old.columns:
                                old = old.withColumn("discount_amount", F.col("discount_amount").cast("string"))
                            if "discount_amount" in thisrun.columns:
                                thisrun = thisrun.withColumn("discount_amount", F.col("discount_amount").cast("string"))
                    else:
                        old = spark.read.parquet(fullPath).filter(Filteryear).drop("year")
                    
                    # Align schemas
                    for col in old.columns:
                        if col in thisrun.columns:
                            col_type = old.schema[col].dataType
                            thisrun = thisrun.withColumn(col, F.col(col).cast(col_type))
                    
                    thisrun = thisrun.select(*old.columns)
                    
                    # Find new records
                    print(f"🔎 Finding new records...")
                    new = thisrun.subtract(old)
                    
                    date = list(new.orderBy(F.col("created_at").desc()).select(F.max("created_at")).first())
                    since = list(old.orderBy(F.col("created_at").asc()).select(F.min("created_at")).first())
                    
                except Exception as e:
                    print(f"⚠️  No existing data, treating all as new")
                    new = thisrun
                    date = list(new.orderBy(F.col("created_at").desc()).select(F.max("created_at")).first())
                    since = date
                
                if not new.isEmpty():
                    new_count = new.count()
                    print(f"✅ Found {new_count:,} new records")
                    
                    # Add year partition and write
                    new = new.withColumn("year", F.year("created_at"))
                    
                    print(f"💾 Writing to {fullPath}...")
                    new.coalesce(1).write.mode("append").option("mergeSchema", "true").partitionBy("year").parquet(fullPath)
                    
                    print(f"✅ Successfully appended {new_count:,} rows")
                    
                    dicts = {
                        "FileName": table_name,
                        "Destination": fullPath,
                        "DbTable": dbtable,
                        "Since": since[0],
                        "UptoDate": date[0],
                        "LastRun": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Active": True
                    }
                    dictframe.append(dicts)
                else:
                    print(f"ℹ️  No new data to append")
            
            print(f"{'='*60}\n")
            
        except Exception as e:
            print(f"❌ Error processing table {dbtable}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    return dictframe

def updateConfig(configtable, dictframe):
    """Update configuration table"""
    if len(dictframe) > 0:
        newconfig = spark.createDataFrame(pd.DataFrame(dictframe)).withColumn("LastRun", F.lit(F.current_timestamp()))
        old = configtable.alias("df1").join(
            newconfig.alias("df2"), F.col("df1.FileName") == F.col("df2.FileName"), how="left_anti"
        )
        if not old.isEmpty():
            newconfig = newconfig.union(old)

        newconfig.write.mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.default.updates")
        configtable.write.mode("append").option("mergeSchema", "true").option("overwriteSchema", "true").parquet("/mnt/bronze/updatedhistory")
    else:
        print(f"No new data to be appended to the config table")

def main():
    """
    Main function - NO CHUNKING VERSION
    Loads all data at once
    """
    HostDB = "13.215.173.246"
    UserDB = dbutils.secrets.get(scope="database", key="db_user")
    PasswordDB = dbutils.secrets.get(scope="database", key="db_password")
    db = dbutils.secrets.get(scope="database", key="db_name") 
    
    conn = MysqlConnect(HostDB, UserDB, db, PasswordDB)
    configtable = spark.table("analyticadebuddha.default.updates").filter((F.col("Active") == True))
    tables = getTableList(df=configtable)
    
    # Exclude flight_searched table
    excluded_tables = ["tbl_flight_searches"]
    tables = [table for table in tables if table not in excluded_tables]
    
    print(f"⚠️  Excluded tables: {', '.join(excluded_tables)}")
    print(f"\n🚀 Starting data pipeline (no chunking)")
    print(f"📋 Tables to process: {len(tables)}")
    print(f"{'='*60}\n")
    
    dictframe = getTables_NoChunking(
        configtable, 
        tables=tables, 
        conn=conn, 
        write=True, 
        path="/mnt/bronze", 
        db=db, 
        schema=SchemaUser
    )
    
    updateConfig(configtable, dictframe)
    conn.close()
    
    print(f"\n✅ Pipeline completed!")

if __name__ == "__main__":
    main()
