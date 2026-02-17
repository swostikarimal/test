# Databricks notebook source
import pyspark.sql.functions as F
import dbconnect
import time

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA `analyticadebuddha`.`arju`
# MAGIC TO `bikashsh@buddhatech.info`;

# COMMAND ----------

HostDb = "45.117.153.177"
UserDB = "data"
db = "ricemill"
PasswordDB = "Aa7qWJ2GgGqP6aW"
engine = dbconnect.MysqlConnectAlchemy(HostDb, UserDB, db, PasswordDB)
engineconn = engine.connect()

# COMMAND ----------

from sqlalchemy import create_engine, inspect

inspector = inspect(engine)

tables = inspector.get_table_names()

# COMMAND ----------

tables

# COMMAND ----------

table = "harvester_expenses"
t1query = f"""
            SELECT count(*) AS Count, Min(updated_at) as FirstDate, Max(updated_at) as LatestDate FROM `ricemill`.`{table}`
        """

with engine.begin() as conn:
    df = pd.read_sql_query(text(t1query), con=conn)

# COMMAND ----------

df

# COMMAND ----------

import pandas as pd
from sqlalchemy import Table, MetaData, text

with engine.begin() as conn:
    df = pd.read_sql_query(text("SELECT * FROM harvester_expenses"), con=conn)

df.head()

# COMMAND ----------

def getTableStatus(tables, conn):
    '''
    '''
    tabledic = []
    tablenondate = []
    lentable = len(tables)
    for table in tables:
        print(f"Procession for table {table}")
        t1query = f"""
            SELECT count(*) AS Count, Min(updated_at) as FirstDate, Max(updated_at) as LatestDate FROM `ricemill`.`{table}`
        """
        try:
            t1 = pd.read_sql_query(text(t1query), con=conn)
            COUNTS = t1['Count'].iloc[0]
            FIRSTDATE = t1['FirstDate'].iloc[0]
            LATESTDATE = t1['LatestDate'].iloc[0]

            DICTABLE = {
                "TABLENAME": table,
                "COUNT": COUNTS,
                "FIRSTDATE": FIRSTDATE,
                "upDatedUpTo": LATESTDATE

            }
            tabledic.append(DICTABLE)
            print(
                f"The total numbers of records in table {table} is: {COUNTS} which contains the data since {FIRSTDATE} and the latest date is {LATESTDATE} based on CreatedDate \n"
            )

        except Exception as e:
            print(f"Error: {e}")
            tablenondate.append({
                "tablename": table
            })

        lentable -=1
        print(f"Remaining tables to iter: {lentable}")

        time.sleep(3)

    sdf = spark.createDataFrame(pd.DataFrame(tabledic))
    sdf1 = spark.createDataFrame(pd.DataFrame(tablenondate))

    # sdf1.write.mode("overwrite").format("delta").saveAsTable("analyticadebuddha.businesstable.tablenondate")

    return sdf, sdf1
    
with engine.begin() as conn:
    sdf, sdf1 = getTableStatus(tables, conn)

# COMMAND ----------

sdf1.display()

# COMMAND ----------

sdf.unionByName(sdf1, allowMissingColumns=True)

# COMMAND ----------

tableinclude = [
    "purchase_enquiries","purchase_enquiry_items","purchase_enquiry_vendors","purchase_order_items","purchase_orders",
    "purchase_payment_vouchers","purchase_quotation_items","purchase_quotations","purchase_request_items",
    "purchase_request_reasons","purchase_requests","purchase_return_items","purchase_returns","purchases"
]
spark.table("analyticadebuddha.arju.tablestatus").withColumn(
    "isActive", F.when(F.col("TABLENAME").isin(tableinclude), F.lit("True")).otherwise(F.lit("False"))
).write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("analyticadebuddha.arju.tablestatus")

# COMMAND ----------

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

HostDB= "13.215.173.246"
UserDB= dbutils.secrets.get(scope="database", key="db_user")
PasswordDB =  dbutils.secrets.get(scope="database", key="db_password")
db = dbutils.secrets.get(scope="database", key="db_name") 
conn = MysqlConnect(HostDB, UserDB, db, PasswordDB)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/bronze/test")

# COMMAND ----------

