# Databricks notebook source
'''
Incremental loading of the data from mysql to datalake as required by business requirements
@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 09/09/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''
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
        print("Connected to Mysql sucessfully")

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

def read_legacy_data(fullPath):
    """Read existing parquet data without schema enforcement"""
    try:
        return spark.read.option("mergeSchema", "false").option("ignoreCorruptFiles", "true").parquet(fullPath)
    except Exception as e:
        print(f"Error reading legacy data: {e}")
        return None

def FetchAndCreate(df, dbtable, table_name, fullPath):
    '''This keeps the track of the data and writes to the parquet file'''
    CurrentYear = F.year(F.current_date())
    Filteryear = F.year("created_at").between("2019", CurrentYear)

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

            for col in old.columns:
                if col in thisrun.columns:
                    col_type = old.schema[col].dataType
                    thisrun = thisrun.withColumn(col, F.col(col).cast(col_type))
            
            thisrun = thisrun.select(*old.columns)

            new = thisrun.subtract(old)
            date = list(new.orderBy(F.col("created_at").desc()).select(F.max("created_at")).first())
            since = date if old is None else list(old.orderBy(F.col("created_at").desc()).select(F.min("created_at")).first())
            
        except Exception as e:
            print(f"Error occured {e}")
            
        dicts = {
            "FileName": table_name,
            "Destination":fullPath,
            "DbTable": dbtable,
            "Since":since[0],
            "UptoDate": date[0],
            "LastRun": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Active": True
        } 
    return new, dicts

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

def getTableList(df):
    df = df.toPandas()
    tables = list(df["DbTable"])

    return tables

def getTables(configtable, tables=list, conn=str, write=False, path=str, db=str, schema=None):
    dictframe = []
    for dbtable in tables:
        date = configtable.filter(F.col("DbTable") == f"{dbtable}").select("UptoDate").collect()[0][0]
        CY = datetime.now().year
        print(f"Loading tabel {dbtable} onward {date}")
        query = f"SELECT * FROM {db}.{dbtable} WHERE created_at >= '{date}' AND YEAR(created_at) <= {CY}"
        Pdf = pd.read_sql_query(query, conn)

        if "verified" in Pdf.columns:
            Pdf["verified"] = Pdf["verified"].astype("Int64")  #  THIS IS THE FIX
        
        if "dob" in Pdf.columns:
        # Convert to string and handle nulls
            Pdf["dob"] = Pdf["dob"].astype(str).replace('nan', None)



        print(f"Converting pandas dataframe {dbtable}")

        if not Pdf.empty:
            if dbtable == "users":
                # Use your schema (SchemaUser) for users table
                df = spark.createDataFrame(Pdf, schema=SchemaUser)
            else:
                # For other tables, clean dataframe safely
                df = spark.createDataFrame(clean_pandas_dataframe(Pdf))

            if str(dbtable).endswith("s"):
                table = dbtable[:-1]

            if str(table).startswith("tbl"):
                table = "_".join(table.split("_")[1:])

            table_name = str(table)

            for col in df.columns:
                if col.strip().endswith("code"):
                    df = df.withColumnRenamed(col, col.strip()[:-4] + "id")
                else:
                    new_col = col.strip().replace(" ", "_").replace("-", "_").replace(".", "_").replace("code", "id")
                    df = df.withColumnRenamed(col, new_col)

            if write:
                fullPath = path + "/" + table_name
                newdata, dicts = FetchAndCreate(df, dbtable, table_name, fullPath)
                if not newdata.isEmpty():
                    dictframe.append(dicts)
                    newdata = newdata.withColumn(
                        "year", F.year("created_at")
                    )
                    newdata.coalesce(1).write.mode("append").option("mergeSchema", "true").partitionBy("year").parquet(fullPath)

                    f"The new data for file {fullPath} has been appended, count: {newdata.count()}"
                else:
                    print(f"There is no new data to be appended for the table {table_name}")
                
        else:
            print(f"No data found for {dbtable} in the database {db} for the period {date} onward for year {CY}")
        
    return dictframe

def updateConfig(configtable, dictframe):
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
    """call all the functions"""
    HostDB= "13.215.173.246"
    UserDB= dbutils.secrets.get(scope="database", key="db_user")
    PasswordDB =  dbutils.secrets.get(scope="database", key="db_password")
    db = dbutils.secrets.get(scope="database", key="db_name") 
    conn = MysqlConnect(HostDB, UserDB, db, PasswordDB)
    configtable = spark.table("analyticadebuddha.default.updates").filter((F.col("Active") == True))
    tables = getTableList(df=configtable)
    dictframe = getTables(configtable, tables=tables, conn=conn, write=True, path="/mnt/bronze", db=db, schema=SchemaUser)
    updateConfig(configtable, dictframe)

if __name__ == "__main__":
    main()
