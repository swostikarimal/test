# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
import pymysql
import time
import datetime
import dbconnect
from sqlalchemy import create_engine, text, inspect


today = datetime.datetime.now()
days7 = (pd.to_datetime(today) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

def getUpdatesAndKeys(pkcolumns, updateinfo):
    '''
    '''
    df = updateinfo.alias("df1").join(
        pkcolumns.alias("df2"), F.col("df1.TableName") == F.col("df2.table_name"), how="left"
    ).select("df1.*", "df2.columnspks")

    df.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("analyticadebuddha.default.updatefoandkeys")

    return df

# def getNoneNullTables(conn):
#     '''
#     '''
#     cursor = conn.cursor()
#     cursor.execute("SHOW TABLES")

#     tables = []
#     while True:
#         table = cursor.fetchone()
#         if table is None:
#             break
#         tables.append(table[0])

#     print("Non empty tables:")
#     non_empty_count = 0

#     for table_name in tables:
#         cursor.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1")
#         result = cursor.fetchone()
        
#         if result:
#             print(f"  {table_name}")
#             non_empty_count += 1

#     if non_empty_count == 0:
#         print(" No non-empty tables found")
#     else:
#         print(f"\nTotal non-empty tables: {non_empty_count}")

#     cursor.close()

#     return tables

# def getTableStatus(tables, conn):
#     '''
#     '''
#     tabledic = []
#     tablenondate = []
#     lentable = len(tables)

#     for table in tables:
#         print(f"Procession for table {table}")
#         t1query = f"""
#             SELECT count(*) AS Count, Min(updated_at) as FirstDate, Max(updated_at) as LatestDate FROM `operation`.`{table}`
#         """
#         try:
#             t1 = pd.read_sql(t1query, conn)
#             COUNTS = t1['Count'].iloc[0]
#             FIRSTDATE = t1['FirstDate'].iloc[0]
#             LATESTDATE = t1['LatestDate'].iloc[0]

#             DICTABLE = {
#                 "TABLENAME": table,
#                 "COUNT": COUNTS,
#                 "FIRSTDATE": FIRSTDATE,
#                 "upDatedUpTo": LATESTDATE

#             }
#             tabledic.append(DICTABLE)
#             print(
#                 f"The total numbers of records in table {table} is: {COUNTS} which contains the data since {FIRSTDATE} and the latest date is {LATESTDATE} based on CreatedDate \n"
#             )

#         except Exception as e:
#             print(f"Error: {e}")
#             tablenondate.append({
#                 "tablename": table
#             })

#         lentable -=1
#         print(f"Remaining tables to iter: {lentable}")

#         time.sleep(3)

#     sdf = spark.createDataFrame(pd.DataFrame(tabledic))
#     sdf1 = spark.createDataFrame(pd.DataFrame(tablenondate))

#     sdf1.write.mode("overwrite").format("delta").saveAsTable("analyticadebuddha.businesstable.tablenondate")

#     return sdf

def getPrioritiz(sdf):
    '''
    '''
    sdf.withColumns(
        {
            "Type": F.when(
            (F.col("COUNT") <= "5000") & (F.col("COUNT") > "0"), "SMALL"
            ).when(
                (F.col("COUNT") > "5000") & (F.col("COUNT") < "50000"), "MEDIUM"
            ).when(
                (F.col("COUNT") >= "50000"), "LARGE"
            ).otherwise(F.lit("EMPTY")),

            "FREQUENCY": F.when(
                    F.date_format("upDatedUpTo", "yyyy-MM-dd") >= days7, "RECENT"
            ).when(
                F.col("Type") == "EMPTY", "EMPTY"
            ).otherwise("SLOW"),

            "PRIOR": F.when(
                (
                    (
                        (F.col("Type") == "LARGE") | (F.col("Type") == "MEDIUM")
                    ) & (
                        (F.col("FREQUENCY") == "RECENT")
                    )
                ), "HIGH"
            ).when(
                (
                    (
                        (F.col("Type") == "SMALL") & (F.col("FREQUENCY") == "RECENT")
                    ) | (
                        (F.col("Type") == "MEDIUM") & (F.col("FREQUENCY") != "RECENT")
                    )
                ), "MEDIUM"
            ).otherwise(F.lit("LOW"))
        }

    ).write.mode("overwrite").option("overwriteSchema","true").option("mergeSchema","true").saveAsTable(
        "analyticadebuddha.businesstable.FO_PRIORITY"
    )

    return None

def main():
    '''
    '''
    HostDB = dbutils.secrets.get(scope="flightoperation", key="fo_db_host")
    db = "operation"
    UserDB= dbutils.secrets.get(scope="flightoperation", key="fo_db_user")
    PasswordDB =  dbutils.secrets.get(scope="flightoperation", key="fo_db_pass")
    conn = dbconnect.MysqlConnectPy(HostDB, UserDB, db, PasswordDB)

    updateinfo = spark.table(f"analyticadebuddha.default.flightop_updateinfo")
    pkcolumns = spark.table("analyticadebuddha.businesstable.flightoperation_keys")
    # tables = getNoneNullTables(conn=conn)
    # sdf = getTableStatus(tables, conn=conn)
    sdf = getUpdatesAndKeys(pkcolumns, updateinfo)
    getPrioritiz(sdf)

    return None

if __name__ == "__main__":
    main()